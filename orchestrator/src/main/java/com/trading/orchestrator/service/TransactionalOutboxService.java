package com.trading.orchestrator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orchestrator.entity.IdempotencyClaimEntity;
import com.trading.orchestrator.entity.OutboxEventEntity;
import com.trading.orchestrator.entity.RiskDecisionEntity;
import com.trading.orchestrator.entity.TradeOrderEntity;
import com.trading.orchestrator.grpc.RiskResponse;
import com.trading.orchestrator.model.Order;
import com.trading.orchestrator.model.RiskRejection;
import com.trading.orchestrator.repository.IdempotencyClaimRepository;
import com.trading.orchestrator.repository.OutboxEventRepository;
import com.trading.orchestrator.repository.RiskDecisionRepository;
import com.trading.orchestrator.repository.TradeOrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Transactional Outbox & Durable Idempotency Service.
 * Features:
 *  1. Atomic conditional idempotency claims across concurrent workers and restarts.
 *  2. Relational persistence of risk decisions and trade orders.
 *  3. Outbox event recording in the same DB transaction, eliminating dual-write inconsistencies.
 *  4. Synchronous immediate publishing with guaranteed background recovery poller.
 *  5. Multi-instance coordination via atomic state transition claims.
 */
@Service
public class TransactionalOutboxService {

    private static final Logger log = LoggerFactory.getLogger(TransactionalOutboxService.class);

    public enum ClaimResult {
        NEW_CLAIM,
        DUPLICATE_COMPLETED,
        DUPLICATE_IN_FLIGHT
    }

    private final IdempotencyClaimRepository idempotencyClaimRepository;
    private final RiskDecisionRepository riskDecisionRepository;
    private final TradeOrderRepository tradeOrderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public TransactionalOutboxService(
            IdempotencyClaimRepository idempotencyClaimRepository,
            RiskDecisionRepository riskDecisionRepository,
            TradeOrderRepository tradeOrderRepository,
            OutboxEventRepository outboxEventRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        this.idempotencyClaimRepository = idempotencyClaimRepository;
        this.riskDecisionRepository = riskDecisionRepository;
        this.tradeOrderRepository = tradeOrderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Atomically claims a tick fingerprint.
     * Uses atomic database conditional updates to eliminate race conditions among concurrent workers.
     */
    @Transactional
    public ClaimResult tryClaim(String fingerprint) {
        Optional<IdempotencyClaimEntity> opt = idempotencyClaimRepository.findById(fingerprint);
        if (opt.isPresent()) {
            IdempotencyClaimEntity claim = opt.get();
            if ("COMPLETED".equalsIgnoreCase(claim.getStatus())) {
                return ClaimResult.DUPLICATE_COMPLETED;
            }

            long now = System.currentTimeMillis();
            long expirationThreshold = now - 30_000; // 30-second lease expiration for crashed workers

            // Atomic conditional update to safely reclaim stale or failed claims without worker collisions
            int updated = idempotencyClaimRepository.reclaimStaleOrFailed(fingerprint, now, expirationThreshold);
            if (updated > 0) {
                log.warn("[Idempotency] Successfully captured atomic lease for stale/failed claim: {}", fingerprint);
                return ClaimResult.NEW_CLAIM;
            } else {
                return ClaimResult.DUPLICATE_IN_FLIGHT;
            }
        }

        try {
            IdempotencyClaimEntity newClaim = new IdempotencyClaimEntity(fingerprint, "IN_FLIGHT");
            idempotencyClaimRepository.saveAndFlush(newClaim);
            return ClaimResult.NEW_CLAIM;
        } catch (Exception e) {
            log.warn("[Idempotency] Concurrent claim insertion collision for fingerprint {}: {}", fingerprint, e.getMessage());
            return ClaimResult.DUPLICATE_IN_FLIGHT;
        }
    }

    /**
     * Marks a claim as FAILED to allow immediate Kafka consumer retry.
     */
    @Transactional
    public void markClaimFailed(String fingerprint) {
        idempotencyClaimRepository.findById(fingerprint).ifPresent(claim -> {
            claim.setStatus("FAILED");
            idempotencyClaimRepository.save(claim);
        });
    }

    /**
     * Atomically writes an approved trade order, its risk decision record,
     * the outbox message, and marks the claim as COMPLETED.
     */
    @Transactional
    public OutboxEventEntity recordApprovedOrder(
            String tickFingerprint,
            Order order,
            RiskResponse riskResponse,
            String tradeOrdersTopic) throws JsonProcessingException {

        // 1. Relational Risk Decision
        RiskDecisionEntity decision = new RiskDecisionEntity(
                UUID.randomUUID().toString(),
                tickFingerprint,
                order.portfolioId(),
                order.symbol(),
                order.price(),
                order.quantity(),
                riskResponse.getValueAtRisk(),
                "APPROVED",
                "RISK_APPROVED",
                riskResponse.getComputationTimeMs(),
                System.currentTimeMillis()
        );
        riskDecisionRepository.save(decision);

        // 2. Relational Trade Order
        TradeOrderEntity orderEntity = new TradeOrderEntity(
                order.orderId(),
                order.portfolioId(),
                order.symbol(),
                order.side(),
                order.quantity(),
                order.price(),
                order.status().name(),
                order.valueAtRisk(),
                order.timestamp()
        );
        tradeOrderRepository.save(orderEntity);

        // 3. Outbox Event
        String payload = objectMapper.writeValueAsString(order);
        OutboxEventEntity outboxEvent = new OutboxEventEntity(
                UUID.randomUUID().toString(),
                tradeOrdersTopic,
                order.symbol(),
                payload
        );
        outboxEventRepository.save(outboxEvent);

        // 4. Complete Idempotency Claim
        idempotencyClaimRepository.findById(tickFingerprint).ifPresent(c -> {
            c.setStatus("COMPLETED");
            idempotencyClaimRepository.save(c);
        });

        return outboxEvent;
    }

    /**
     * Atomically writes a risk rejection, its decision audit record,
     * the outbox message, and marks the claim as COMPLETED.
     */
    @Transactional
    public OutboxEventEntity recordRejection(
            String tickFingerprint,
            RiskRejection rejection,
            String status,
            int latencyMs,
            String riskRejectionsTopic) throws JsonProcessingException {

        RiskDecisionEntity decision = new RiskDecisionEntity(
                UUID.randomUUID().toString(),
                tickFingerprint,
                rejection.portfolioId(),
                rejection.symbol(),
                rejection.price(),
                rejection.proposedTradeSize(),
                rejection.valueAtRisk(),
                status,
                rejection.reason(),
                latencyMs,
                System.currentTimeMillis()
        );
        riskDecisionRepository.save(decision);

        String payload = objectMapper.writeValueAsString(rejection);
        OutboxEventEntity outboxEvent = new OutboxEventEntity(
                UUID.randomUUID().toString(),
                riskRejectionsTopic,
                rejection.symbol(),
                payload
        );
        outboxEventRepository.save(outboxEvent);

        idempotencyClaimRepository.findById(tickFingerprint).ifPresent(c -> {
            c.setStatus("COMPLETED");
            idempotencyClaimRepository.save(c);
        });

        return outboxEvent;
    }

    /**
     * Marks claim completed for ticks that did not breach threshold (normal flow).
     */
    @Transactional
    public void recordCompletedNormalFlow(String tickFingerprint) {
        idempotencyClaimRepository.findById(tickFingerprint).ifPresent(c -> {
            c.setStatus("COMPLETED");
            idempotencyClaimRepository.save(c);
        });
    }

    /**
     * Publishes an outbox event to Kafka and marks it SENT on confirmation.
     */
    public void publishOutboxEventNow(OutboxEventEntity outboxEvent, long publishTimeoutMs) {
        try {
            kafkaTemplate.send(outboxEvent.getTopic(), outboxEvent.getPartitionKey(), outboxEvent.getPayload())
                    .get(publishTimeoutMs, TimeUnit.MILLISECONDS);
            markOutboxEventSent(outboxEvent.getEventId());
        } catch (Exception e) {
            log.error("[Outbox Error] Immediate publish failed for event {}: {}. Will be recovered by poller.",
                    outboxEvent.getEventId(), e.getMessage());
            throw new RuntimeException("Immediate publish failed for outbox event " + outboxEvent.getEventId(), e);
        }
    }

    @Transactional
    public void markOutboxEventSent(String eventId) {
        outboxEventRepository.findById(eventId).ifPresent(e -> {
            e.setStatus("SENT");
            e.setSentAt(System.currentTimeMillis());
            outboxEventRepository.save(e);
        });
    }

    /**
     * Background outbox recovery poller ensuring at-least-once delivery
     * even across abrupt process crashes. Uses atomic claim to coordinate safely across replicas.
     */
    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void processPendingOutboxEvents() {
        try {
            List<OutboxEventEntity> pending = outboxEventRepository.findByStatusOrderByCreatedAtAsc("PENDING", PageRequest.of(0, 50));
            for (OutboxEventEntity event : pending) {
                // Multi-instance atomic claim: transitions PENDING -> IN_FLIGHT
                int claimed = outboxEventRepository.claimOutboxEvent(event.getEventId());
                if (claimed == 0) {
                    continue; // Another instance already claimed this event
                }

                try {
                    kafkaTemplate.send(event.getTopic(), event.getPartitionKey(), event.getPayload())
                            .get(3000, TimeUnit.MILLISECONDS);
                    markOutboxEventSent(event.getEventId());
                    log.info("[Outbox Poller] Successfully recovered and published event {}", event.getEventId());
                } catch (Exception e) {
                    log.warn("[Outbox Poller] Could not deliver claimed event {}: {}", event.getEventId(), e.getMessage());
                    // Revert to PENDING so future runs can retry
                    event.setStatus("PENDING");
                    outboxEventRepository.save(event);
                }
            }
        } catch (Exception e) {
            log.error("[Outbox Poller] Error scanning pending events: {}", e.getMessage());
        }
    }
}
