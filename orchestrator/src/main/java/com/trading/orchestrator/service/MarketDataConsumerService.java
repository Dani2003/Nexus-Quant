package com.trading.orchestrator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orchestrator.client.RiskGrpcClient;
import com.trading.orchestrator.client.RiskServiceException;
import com.trading.orchestrator.entity.OutboxEventEntity;
import com.trading.orchestrator.grpc.RiskResponse;
import com.trading.orchestrator.model.DecisionAuditRecord;
import com.trading.orchestrator.model.MarketTick;
import com.trading.orchestrator.model.Order;
import com.trading.orchestrator.model.RiskRejection;
import com.trading.orchestrator.model.TradingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Event-Driven Kafka Consumer service that listens to the market-ticks topic.
 * Fully utilizes Java 21 Project Loom Virtual Threads with:
 *  1. Reliable acknowledgment tied strictly to processing completion and durable Kafka publication.
 *  2. Atomic relational idempotency claim preventing concurrent worker races.
 *  3. Transactional Outbox pattern eliminating dual-write inconsistencies.
 *  4. Real-time Mark-to-Market PnL and automated Stop-Loss / Take-Profit protection via PortfolioService.
 *  5. Bounded concurrency & overload policy via Semaphore.
 *  6. Strict input validation rejecting invalid/non-finite prices.
 */
@Service
public class MarketDataConsumerService {

    private static final Logger log = LoggerFactory.getLogger(MarketDataConsumerService.class);

    private final RiskGrpcClient riskGrpcClient;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AuditStorageService auditStorageService;
    private final PortfolioService portfolioService;
    private final TransactionalOutboxService outboxService;

    private final double priceThreshold;
    private final double defaultTradeSize;
    private final String tradeOrdersTopic;
    private final String riskRejectionsTopic;
    private final long publishTimeoutMs;

    private final Semaphore concurrencyLimiter;
    private final long concurrencyTimeoutMs;

    // Fast in-memory LRU cache for secondary deduplication fallback
    private final Set<String> processedTicksCache = Collections.newSetFromMap(
            new LinkedHashMap<String, Boolean>(10_000, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > 10_000;
                }
            }
    );

    public MarketDataConsumerService(
            RiskGrpcClient riskGrpcClient,
            ObjectMapper objectMapper,
            KafkaTemplate<String, String> kafkaTemplate,
            AuditStorageService auditStorageService,
            double priceThreshold,
            double defaultTradeSize,
            String tradeOrdersTopic,
            String riskRejectionsTopic,
            long publishTimeoutMs,
            int maxConcurrentEvaluations,
            long concurrencyTimeoutMs) {
        this(riskGrpcClient, objectMapper, kafkaTemplate, auditStorageService, null, null,
                priceThreshold, defaultTradeSize, tradeOrdersTopic, riskRejectionsTopic,
                publishTimeoutMs, maxConcurrentEvaluations, concurrencyTimeoutMs);
    }

    @Autowired
    public MarketDataConsumerService(
            RiskGrpcClient riskGrpcClient,
            ObjectMapper objectMapper,
            KafkaTemplate<String, String> kafkaTemplate,
            AuditStorageService auditStorageService,
            PortfolioService portfolioService,
            TransactionalOutboxService outboxService,
            @Value("${trading.risk.threshold-price:150.0}") double priceThreshold,
            @Value("${trading.risk.default-proposed-trade-size:100.0}") double defaultTradeSize,
            @Value("${trading.kafka.trade-orders-topic:trade-orders}") String tradeOrdersTopic,
            @Value("${trading.kafka.risk-rejections-topic:risk-rejections}") String riskRejectionsTopic,
            @Value("${trading.kafka.publish-timeout-ms:3000}") long publishTimeoutMs,
            @Value("${trading.risk.max-concurrent-evaluations:50}") int maxConcurrentEvaluations,
            @Value("${trading.risk.concurrency-timeout-ms:25}") long concurrencyTimeoutMs) {

        this.riskGrpcClient = riskGrpcClient;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.auditStorageService = auditStorageService;
        this.portfolioService = portfolioService;
        this.outboxService = outboxService;
        this.priceThreshold = priceThreshold;
        this.defaultTradeSize = defaultTradeSize;
        this.tradeOrdersTopic = tradeOrdersTopic;
        this.riskRejectionsTopic = riskRejectionsTopic;
        this.publishTimeoutMs = publishTimeoutMs;
        this.concurrencyLimiter = new Semaphore(maxConcurrentEvaluations, true);
        this.concurrencyTimeoutMs = concurrencyTimeoutMs;

        log.info("MarketDataConsumerService initialized. Risk Threshold: >= {}, Max Concurrency: {}, Timeout: {}ms",
                priceThreshold, maxConcurrentEvaluations, concurrencyTimeoutMs);
    }

    /**
     * Kafka Listener for market data ticks.
     * Executed directly on Java 21 Virtual Threads and commits offsets ONLY after
     * processing, validation, risk evaluation, and output event publication succeed.
     */
    @KafkaListener(
            topics = "${trading.kafka.market-ticks-topic:market-ticks}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onMarketTick(
            @Payload String rawMessage,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partition,
            @Header(value = KafkaHeaders.OFFSET, required = false) Long offset,
            Acknowledgment acknowledgment) {

        try {
            processTick(rawMessage);

            // Tie acknowledgment strictly to durable outcome completion
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }

        } catch (RiskServiceException e) {
            log.error("[Risk Failure] RPC failed for tick from [{}-{}@{}]: {}. Propagating to Kafka error handler for retry.",
                    topic, partition, offset, e.getMessage());
            throw e;

        } catch (Exception e) {
            log.error("[Processing Error] Failure processing tick from [{}-{}@{}]: {}. Triggering Kafka error handler.",
                    topic, partition, offset, e.getMessage(), e);
            throw (e instanceof RuntimeException re) ? re : new RuntimeException("Tick processing error", e);
        }
    }

    /**
     * Core tick processing pipeline executing on a Project Loom Virtual Thread.
     */
    public void processTick(String rawMessage) {
        MarketTick tick;
        try {
            tick = objectMapper.readValue(rawMessage, MarketTick.class);
        } catch (JsonProcessingException e) {
            log.error("[Deserialization Error] Dropping malformed JSON tick: {}", rawMessage, e);
            publishRejectionDurable(RiskRejection.create("UNKNOWN", "UNKNOWN", 0.0, 0.0, 0.0, "MALFORMED_JSON_PAYLOAD"));
            return;
        }

        // Broadcast raw tick event to live stream
        auditStorageService.broadcastEvent(TradingEvent.fromTick(tick));

        // 1. Strict Input Validation
        String validationError = validateTick(tick);
        if (validationError != null) {
            log.warn("[Input Validation Rejected] Invalid tick: {} - Error: {}", rawMessage, validationError);
            RiskRejection rejection = RiskRejection.create(
                    tick.portfolioId(), tick.symbol(), tick.volume(), tick.price(), 0.0, validationError);
            publishRejectionDurable(rejection);
            auditStorageService.recordDecision(DecisionAuditRecord.create(
                    "INVALID:" + System.nanoTime(), tick.portfolioId(), tick.symbol(), tick.price(),
                    tick.volume(), 0.0, "REJECTED_VALIDATION", validationError, 0));
            return;
        }

        // 2. Check Idempotency Guard (Atomic database claim or in-memory fallback)
        String tickFingerprint = tick.symbol() + ":" + tick.price() + ":" + tick.timestamp() + ":" + tick.volume();
        if (outboxService != null) {
            TransactionalOutboxService.ClaimResult claimResult = outboxService.tryClaim(tickFingerprint);
            if (claimResult != TransactionalOutboxService.ClaimResult.NEW_CLAIM) {
                log.info("[Duplicate Tick Skipped] Atomic idempotency claim detected duplicate status: {} for {}",
                        claimResult, tickFingerprint);
                return;
            }
        } else {
            synchronized (processedTicksCache) {
                if (processedTicksCache.contains(tickFingerprint) || auditStorageService.isFingerprintProcessed(tickFingerprint)) {
                    log.info("[Duplicate Tick Skipped] In-memory idempotency guard detected replayed tick for {}", tickFingerprint);
                    return;
                }
            }
        }

        // 3. Mark-to-Market Real-Time Valuation & Automated Stop-Loss / Take-Profit Trigger Check
        if (portfolioService != null) {
            try {
                portfolioService.onMarketPriceTick(tick);
            } catch (Exception e) {
                log.error("Error evaluating portfolio protection rules for tick {}: {}", tick.symbol(), e.getMessage());
            }
        }

        log.info("[Tick Ingested] Symbol: {}, Price: {:.2f}, Volume: {}, Thread: {}",
                tick.symbol(), tick.price(), tick.volume(), Thread.currentThread());

        // 4. Price Threshold Check
        if (tick.price() >= priceThreshold) {
            log.warn("[Threshold Exceeded] Symbol: {} price {:.2f} >= {:.2f}. Requesting Risk Evaluation...",
                    tick.symbol(), tick.price(), priceThreshold);

            double proposedTradeSize = tick.volume() != 0.0 ? tick.volume() : defaultTradeSize;

            // 5. Bounded Concurrency & Overload Policy
            boolean permitAcquired = false;
            try {
                permitAcquired = concurrencyLimiter.tryAcquire(concurrencyTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            if (!permitAcquired) {
                log.error("[Overload Policy - Trade REJECTED] Concurrency limit exhausted! Cannot dispatch risk evaluation for {}", tick.symbol());
                RiskRejection rejection = RiskRejection.create(
                        tick.portfolioId(), tick.symbol(), proposedTradeSize, tick.price(), 0.0,
                        "OVERLOAD_CONCURRENCY_LIMIT_EXCEEDED");

                if (outboxService != null) {
                    try {
                        OutboxEventEntity outboxEvent = outboxService.recordRejection(
                                tickFingerprint, rejection, "REJECTED_OVERLOAD", 0, riskRejectionsTopic);
                        outboxService.publishOutboxEventNow(outboxEvent, publishTimeoutMs);
                    } catch (Exception e) {
                        log.error("Outbox recording failed for overload rejection: {}", e.getMessage());
                        publishRejectionDurable(rejection);
                    }
                } else {
                    publishRejectionDurable(rejection);
                }

                auditStorageService.recordDecision(DecisionAuditRecord.create(
                        tickFingerprint, tick.portfolioId(), tick.symbol(), tick.price(),
                        proposedTradeSize, 0.0, "REJECTED_OVERLOAD", "OVERLOAD_CONCURRENCY_LIMIT_EXCEEDED", 0));
                return;
            }

            try {
                // gRPC Call with enforced deadline on Java 21 Virtual Thread
                RiskResponse riskResponse = riskGrpcClient.evaluateRisk(
                        tick.portfolioId(),
                        tick.symbol(),
                        proposedTradeSize,
                        tick.price()
                );

                if (riskResponse.getIsApproved()) {
                    // 6a. Publish Approved Order DURABLY via Transactional Outbox
                    Order approvedOrder = Order.createApproved(
                            tick.portfolioId(),
                            tick.symbol(),
                            proposedTradeSize > 0 ? "BUY" : "SELL",
                            Math.abs(proposedTradeSize),
                            tick.price(),
                            riskResponse.getValueAtRisk()
                    );

                    if (outboxService != null) {
                        OutboxEventEntity outboxEvent = outboxService.recordApprovedOrder(
                                tickFingerprint, approvedOrder, riskResponse, tradeOrdersTopic);
                        outboxService.publishOutboxEventNow(outboxEvent, publishTimeoutMs);
                    } else {
                        publishOrderDurable(approvedOrder);
                    }

                    auditStorageService.recordOrder(approvedOrder);
                    auditStorageService.recordDecision(DecisionAuditRecord.create(
                            tickFingerprint, tick.portfolioId(), tick.symbol(), tick.price(),
                            proposedTradeSize, riskResponse.getValueAtRisk(), "APPROVED", "RISK_APPROVED",
                            riskResponse.getComputationTimeMs()));

                    log.info("[Trade APPROVED & DURABLY RECORDED] Order: {} | Symbol: {} | VaR: {:.2f} | Latency: {}ms",
                            approvedOrder.orderId(), tick.symbol(), riskResponse.getValueAtRisk(), riskResponse.getComputationTimeMs());

                } else {
                    // 6b. Publish Rejected Decision DURABLY via Transactional Outbox
                    RiskRejection rejection = RiskRejection.create(
                            tick.portfolioId(),
                            tick.symbol(),
                            proposedTradeSize,
                            tick.price(),
                            riskResponse.getValueAtRisk(),
                            "RISK_TOLERANCE_BREACH_VAR_EXCEEDED"
                    );

                    if (outboxService != null) {
                        OutboxEventEntity outboxEvent = outboxService.recordRejection(
                                tickFingerprint, rejection, "REJECTED_VAR_EXCEEDED",
                                riskResponse.getComputationTimeMs(), riskRejectionsTopic);
                        outboxService.publishOutboxEventNow(outboxEvent, publishTimeoutMs);
                    } else {
                        publishRejectionDurable(rejection);
                    }

                    auditStorageService.recordDecision(DecisionAuditRecord.create(
                            tickFingerprint, tick.portfolioId(), tick.symbol(), tick.price(),
                            proposedTradeSize, riskResponse.getValueAtRisk(), "REJECTED_VAR_EXCEEDED",
                            "RISK_TOLERANCE_BREACH_VAR_EXCEEDED", riskResponse.getComputationTimeMs()));

                    log.warn("[Trade REJECTED & DURABLY RECORDED] Risk tolerance breach for Symbol: {}, VaR: {:.2f}",
                            tick.symbol(), riskResponse.getValueAtRisk());
                }

            } catch (RiskServiceException e) {
                if (outboxService != null) {
                    outboxService.markClaimFailed(tickFingerprint);
                }
                throw e;
            } catch (Exception e) {
                if (outboxService != null) {
                    outboxService.markClaimFailed(tickFingerprint);
                }
                throw (e instanceof RuntimeException re) ? re : new RuntimeException("Error evaluating risk", e);
            } finally {
                concurrencyLimiter.release();
            }

        } else {
            log.debug("[Normal Flow] Symbol: {} price {:.2f} < threshold {:.2f}. Skipping risk evaluation.",
                    tick.symbol(), tick.price(), priceThreshold);
            if (outboxService != null) {
                outboxService.recordCompletedNormalFlow(tickFingerprint);
            }
        }

        // 7. Record fingerprint into idempotency cache AFTER successful outcome recording
        synchronized (processedTicksCache) {
            processedTicksCache.add(tickFingerprint);
        }
    }

    private String validateTick(MarketTick tick) {
        if (tick.symbol() == null || tick.symbol().trim().isEmpty() || "UNKNOWN".equalsIgnoreCase(tick.symbol())) {
            return "INVALID_SYMBOL_EMPTY_OR_UNKNOWN";
        }
        if (!Double.isFinite(tick.price()) || tick.price() <= 0.0) {
            return "INVALID_PRICE_NON_FINITE_OR_NON_POSITIVE";
        }
        if (!Double.isFinite(tick.volume())) {
            return "INVALID_VOLUME_NON_FINITE";
        }
        return null;
    }

    private void publishOrderDurable(Order order) {
        try {
            String json = objectMapper.writeValueAsString(order);
            kafkaTemplate.send(tradeOrdersTopic, order.symbol(), json)
                    .get(publishTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.error("CRITICAL: Failed to durably publish approved order {} to Kafka: {}", order.orderId(), e.getMessage(), e);
            throw new RuntimeException("Durable order publication failed for " + order.orderId(), e);
        }
    }

    private void publishRejectionDurable(RiskRejection rejection) {
        try {
            String json = objectMapper.writeValueAsString(rejection);
            kafkaTemplate.send(riskRejectionsTopic, rejection.symbol(), json)
                    .get(publishTimeoutMs, TimeUnit.MILLISECONDS);
            auditStorageService.broadcastEvent(TradingEvent.fromRejection(rejection));
        } catch (Exception e) {
            log.error("CRITICAL: Failed to durably publish rejection {} to Kafka: {}", rejection.rejectionId(), e.getMessage(), e);
            throw new RuntimeException("Durable rejection publication failed for " + rejection.rejectionId(), e);
        }
    }
}
