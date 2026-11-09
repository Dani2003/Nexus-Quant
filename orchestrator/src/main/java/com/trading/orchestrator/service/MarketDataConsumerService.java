package com.trading.orchestrator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orchestrator.client.RiskGrpcClient;
import com.trading.orchestrator.client.RiskServiceException;
import com.trading.orchestrator.grpc.RiskResponse;
import com.trading.orchestrator.model.MarketTick;
import com.trading.orchestrator.model.Order;
import com.trading.orchestrator.model.RiskRejection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 *  1. Reliable acknowledgment tied strictly to processing completion.
 *  2. Bounded concurrency & overload policy via Semaphore.
 *  3. Strict input validation rejecting invalid/non-finite prices.
 *  4. Idempotency guard preventing duplicate decisions from replayed ticks.
 *  5. Observable decision publishing for all approvals and rejections.
 */
@Service
public class MarketDataConsumerService {

    private static final Logger log = LoggerFactory.getLogger(MarketDataConsumerService.class);

    private final RiskGrpcClient riskGrpcClient;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    private final double priceThreshold;
    private final double defaultTradeSize;
    private final String tradeOrdersTopic;
    private final String riskRejectionsTopic;

    private final Semaphore concurrencyLimiter;
    private final long concurrencyTimeoutMs;

    // Bounded LRU cache for tick idempotency deduplication (capacity: 10,000)
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
            @Value("${trading.risk.threshold-price:150.0}") double priceThreshold,
            @Value("${trading.risk.default-proposed-trade-size:100.0}") double defaultTradeSize,
            @Value("${trading.kafka.trade-orders-topic:trade-orders}") String tradeOrdersTopic,
            @Value("${trading.kafka.risk-rejections-topic:risk-rejections}") String riskRejectionsTopic,
            @Value("${trading.risk.max-concurrent-evaluations:50}") int maxConcurrentEvaluations,
            @Value("${trading.risk.concurrency-timeout-ms:25}") long concurrencyTimeoutMs) {

        this.riskGrpcClient = riskGrpcClient;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.priceThreshold = priceThreshold;
        this.defaultTradeSize = defaultTradeSize;
        this.tradeOrdersTopic = tradeOrdersTopic;
        this.riskRejectionsTopic = riskRejectionsTopic;
        this.concurrencyLimiter = new Semaphore(maxConcurrentEvaluations, true);
        this.concurrencyTimeoutMs = concurrencyTimeoutMs;

        log.info("MarketDataConsumerService initialized. Risk Threshold: >= {}, Max Concurrency: {}, Timeout: {}ms",
                priceThreshold, maxConcurrentEvaluations, concurrencyTimeoutMs);
    }

    /**
     * Kafka Listener for market data ticks.
     * Executed directly on Java 21 Virtual Threads and commits offsets ONLY after
     * processing, validation, and risk evaluation succeed.
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

            // Tie acknowledgment to processing completion: commit only after success!
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }

        } catch (RiskServiceException e) {
            log.error("[Risk Failure] RPC failed for tick from [{}-{}@{}]: {}. Routing to error handler for retry.",
                    topic, partition, offset, e.getMessage());
            // Rethrow so Spring Kafka's retry and dead-letter handler can intervene deliberately
            throw e;

        } catch (Exception e) {
            log.error("[Fatal Processing Error] Unexpected error processing tick from [{}-{}@{}]: {}",
                    topic, partition, offset, e.getMessage(), e);
            throw new RuntimeException("Tick processing error", e);
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
            // Non-recoverable format error; record rejection and do not crash consumer
            publishRejection(RiskRejection.create("UNKNOWN", "UNKNOWN", 0.0, 0.0, 0.0, "MALFORMED_JSON_PAYLOAD"));
            return;
        }

        // 1. Strict Input Validation
        String validationError = validateTick(tick);
        if (validationError != null) {
            log.warn("[Input Validation Rejected] Invalid tick: {} - Error: {}", rawMessage, validationError);
            publishRejection(RiskRejection.create(
                    tick.portfolioId(), tick.symbol(), tick.volume(), tick.price(), 0.0, validationError));
            return;
        }

        // 2. Idempotency Guard (Deduplication on Replay)
        String tickFingerprint = tick.symbol() + ":" + tick.price() + ":" + tick.timestamp() + ":" + tick.volume();
        synchronized (processedTicksCache) {
            if (processedTicksCache.contains(tickFingerprint)) {
                log.info("[Duplicate Tick Skipped] Idempotency guard detected replayed tick for {}", tickFingerprint);
                return;
            }
            processedTicksCache.add(tickFingerprint);
        }

        log.info("[Tick Ingested] Symbol: {}, Price: {:.2f}, Volume: {}, Thread: {}",
                tick.symbol(), tick.price(), tick.volume(), Thread.currentThread());

        // 3. Price Threshold Check
        if (tick.price() >= priceThreshold) {
            log.warn("[Threshold Exceeded] Symbol: {} price {:.2f} >= {:.2f}. Requesting Risk Evaluation...",
                    tick.symbol(), tick.price(), priceThreshold);

            double proposedTradeSize = tick.volume() != 0.0 ? tick.volume() : defaultTradeSize;

            // 4. Bounded Concurrency & Overload Policy
            boolean permitAcquired = false;
            try {
                permitAcquired = concurrencyLimiter.tryAcquire(concurrencyTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            if (!permitAcquired) {
                log.error("[Overload Policy - Trade REJECTED] Concurrency limit exhausted! Cannot dispatch risk evaluation for {}", tick.symbol());
                publishRejection(RiskRejection.create(
                        tick.portfolioId(), tick.symbol(), proposedTradeSize, tick.price(), 0.0,
                        "OVERLOAD_CONCURRENCY_LIMIT_EXCEEDED"));
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
                    // 5a. Publish Approved Order
                    Order approvedOrder = Order.createApproved(
                            tick.portfolioId(),
                            tick.symbol(),
                            proposedTradeSize > 0 ? "BUY" : "SELL",
                            Math.abs(proposedTradeSize),
                            tick.price(),
                            riskResponse.getValueAtRisk()
                    );

                    publishOrder(approvedOrder);
                    log.info("[Trade APPROVED] Order: {} | Symbol: {} | VaR: {:.2f} | Latency: {}ms",
                            approvedOrder.orderId(), tick.symbol(), riskResponse.getValueAtRisk(), riskResponse.getComputationTimeMs());

                } else {
                    // 5b. Publish Rejected Decision
                    RiskRejection rejection = RiskRejection.create(
                            tick.portfolioId(),
                            tick.symbol(),
                            proposedTradeSize,
                            tick.price(),
                            riskResponse.getValueAtRisk(),
                            "RISK_TOLERANCE_BREACH_VAR_EXCEEDED"
                    );

                    publishRejection(rejection);
                    log.warn("[Trade REJECTED] Risk tolerance breach for Symbol: {}, VaR: {:.2f}",
                            tick.symbol(), riskResponse.getValueAtRisk());
                }

            } finally {
                concurrencyLimiter.release();
            }

        } else {
            log.debug("[Normal Flow] Symbol: {} price {:.2f} < threshold {:.2f}. Skipping risk evaluation.",
                    tick.symbol(), tick.price(), priceThreshold);
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

    private void publishOrder(Order order) {
        try {
            String json = objectMapper.writeValueAsString(order);
            kafkaTemplate.send(tradeOrdersTopic, order.symbol(), json);
        } catch (Exception e) {
            log.error("Failed to publish approved order to Kafka topic {}: {}", tradeOrdersTopic, e.getMessage(), e);
        }
    }

    private void publishRejection(RiskRejection rejection) {
        try {
            String json = objectMapper.writeValueAsString(rejection);
            kafkaTemplate.send(riskRejectionsTopic, rejection.symbol(), json);
        } catch (Exception e) {
            log.error("Failed to publish risk rejection to Kafka topic {}: {}", riskRejectionsTopic, e.getMessage(), e);
        }
    }
}
