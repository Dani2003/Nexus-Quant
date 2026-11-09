package com.trading.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orchestrator.entity.IdempotencyClaimEntity;
import com.trading.orchestrator.entity.OutboxEventEntity;
import com.trading.orchestrator.grpc.RiskResponse;
import com.trading.orchestrator.model.Order;
import com.trading.orchestrator.model.OrderStatus;
import com.trading.orchestrator.model.RiskRejection;
import com.trading.orchestrator.repository.IdempotencyClaimRepository;
import com.trading.orchestrator.repository.OutboxEventRepository;
import com.trading.orchestrator.repository.RiskDecisionRepository;
import com.trading.orchestrator.repository.TradeOrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransactionalOutboxServiceTest {

    @Mock
    private IdempotencyClaimRepository idempotencyClaimRepository;

    @Mock
    private RiskDecisionRepository riskDecisionRepository;

    @Mock
    private TradeOrderRepository tradeOrderRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private TransactionalOutboxService outboxService;

    @BeforeEach
    void setUp() {
        lenient().when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        outboxService = new TransactionalOutboxService(
                idempotencyClaimRepository,
                riskDecisionRepository,
                tradeOrderRepository,
                outboxEventRepository,
                kafkaTemplate,
                objectMapper
        );
    }

    @Test
    @DisplayName("tryClaim should return NEW_CLAIM when fingerprint is first seen")
    void shouldClaimNewFingerprint() {
        String fingerprint = "AAPL:175.0:1715000000000:100.0";
        when(idempotencyClaimRepository.findById(fingerprint)).thenReturn(Optional.empty());

        TransactionalOutboxService.ClaimResult result = outboxService.tryClaim(fingerprint);

        assertEquals(TransactionalOutboxService.ClaimResult.NEW_CLAIM, result);
        verify(idempotencyClaimRepository).saveAndFlush(any(IdempotencyClaimEntity.class));
    }

    @Test
    @DisplayName("tryClaim should return DUPLICATE_COMPLETED when fingerprint is already completed")
    void shouldDetectCompletedDuplicate() {
        String fingerprint = "AAPL:175.0:1715000000000:100.0";
        IdempotencyClaimEntity completed = new IdempotencyClaimEntity(fingerprint, "COMPLETED");
        when(idempotencyClaimRepository.findById(fingerprint)).thenReturn(Optional.of(completed));

        TransactionalOutboxService.ClaimResult result = outboxService.tryClaim(fingerprint);

        assertEquals(TransactionalOutboxService.ClaimResult.DUPLICATE_COMPLETED, result);
    }

    @Test
    @DisplayName("tryClaim should return DUPLICATE_IN_FLIGHT when active worker holds the lease")
    void shouldDetectActiveInFlightClaim() {
        String fingerprint = "AAPL:175.0:1715000000000:100.0";
        IdempotencyClaimEntity inFlight = new IdempotencyClaimEntity(fingerprint, "IN_FLIGHT");
        when(idempotencyClaimRepository.findById(fingerprint)).thenReturn(Optional.of(inFlight));

        TransactionalOutboxService.ClaimResult result = outboxService.tryClaim(fingerprint);

        assertEquals(TransactionalOutboxService.ClaimResult.DUPLICATE_IN_FLIGHT, result);
    }

    @Test
    @DisplayName("tryClaim should reclaim lease when active worker lease has expired")
    void shouldReclaimExpiredLease() {
        String fingerprint = "AAPL:175.0:1715000000000:100.0";
        IdempotencyClaimEntity stale = new IdempotencyClaimEntity(fingerprint, "IN_FLIGHT");
        when(idempotencyClaimRepository.findById(fingerprint)).thenReturn(Optional.of(stale));
        when(idempotencyClaimRepository.reclaimStaleOrFailed(eq(fingerprint), anyLong(), anyLong())).thenReturn(1);

        TransactionalOutboxService.ClaimResult result = outboxService.tryClaim(fingerprint);

        assertEquals(TransactionalOutboxService.ClaimResult.NEW_CLAIM, result);
    }

    @Test
    @DisplayName("recordApprovedOrder should persist decision, order, outbox event as IN_FLIGHT, and complete claim")
    void shouldRecordApprovedOrderAtomically() throws Exception {
        String fingerprint = "MSFT:250.0:1715000001000:50.0";
        Order order = new Order("ord-1", "GROWTH", "MSFT", "BUY", 50.0, 250.0, OrderStatus.APPROVED, 150.0, 1715000001000L);
        RiskResponse riskResponse = RiskResponse.newBuilder()
                .setIsApproved(true)
                .setValueAtRisk(150.0)
                .setComputationTimeMs(2)
                .build();

        IdempotencyClaimEntity claim = new IdempotencyClaimEntity(fingerprint, "IN_FLIGHT");
        when(idempotencyClaimRepository.findById(fingerprint)).thenReturn(Optional.of(claim));

        OutboxEventEntity outboxEvent = outboxService.recordApprovedOrder(fingerprint, order, riskResponse, "trade-orders");

        assertNotNull(outboxEvent);
        assertEquals("trade-orders", outboxEvent.getTopic());
        assertEquals("MSFT", outboxEvent.getPartitionKey());
        // Initialized as IN_FLIGHT to prevent race with poller
        assertEquals("IN_FLIGHT", outboxEvent.getStatus());

        verify(riskDecisionRepository).save(any());
        verify(tradeOrderRepository).save(any());
        verify(outboxEventRepository).save(any(OutboxEventEntity.class));
        assertEquals("COMPLETED", claim.getStatus());
    }

    @Test
    @DisplayName("recordRejection should persist decision audit, outbox event as IN_FLIGHT, and complete claim")
    void shouldRecordRejectionAtomically() throws Exception {
        String fingerprint = "TSLA:300.0:1715000002000:200.0";
        RiskRejection rejection = RiskRejection.create("GROWTH", "TSLA", 200.0, 300.0, 95000.0, "VAR_EXCEEDED");

        IdempotencyClaimEntity claim = new IdempotencyClaimEntity(fingerprint, "IN_FLIGHT");
        when(idempotencyClaimRepository.findById(fingerprint)).thenReturn(Optional.of(claim));

        OutboxEventEntity outboxEvent = outboxService.recordRejection(fingerprint, rejection, "REJECTED_VAR_EXCEEDED", 3, "risk-rejections");

        assertNotNull(outboxEvent);
        assertEquals("risk-rejections", outboxEvent.getTopic());
        assertEquals("TSLA", outboxEvent.getPartitionKey());
        assertEquals("IN_FLIGHT", outboxEvent.getStatus());
        verify(riskDecisionRepository).save(any());
        verify(outboxEventRepository).save(any(OutboxEventEntity.class));
        assertEquals("COMPLETED", claim.getStatus());
    }

    @Test
    @DisplayName("publishOutboxEventNow should publish to Kafka and mark event SENT")
    void shouldPublishOutboxEventNowAndMarkSent() {
        OutboxEventEntity event = new OutboxEventEntity("evt-123", "trade-orders", "AAPL", "{\"orderId\":\"1\"}");
        when(outboxEventRepository.findById("evt-123")).thenReturn(Optional.of(event));

        outboxService.publishOutboxEventNow(event, 3000L);

        verify(kafkaTemplate).send(eq("trade-orders"), eq("AAPL"), eq("{\"orderId\":\"1\"}"));
        assertEquals("SENT", event.getStatus());
        assertNotNull(event.getSentAt());
    }

    @Test
    @DisplayName("publishOutboxEventNow should demote to PENDING on Kafka failure")
    void shouldDemoteToPendingOnKafkaFailure() {
        OutboxEventEntity event = new OutboxEventEntity("evt-fail", "trade-orders", "AAPL", "{\"orderId\":\"1\"}");
        when(outboxEventRepository.findById("evt-fail")).thenReturn(Optional.of(event));

        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Broker down")));

        assertThrows(RuntimeException.class, () -> outboxService.publishOutboxEventNow(event, 3000L));

        assertEquals("PENDING", event.getStatus());
        verify(outboxEventRepository).save(event);
    }

    @Test
    @DisplayName("processPendingOutboxEvents should recover stale in-flight events and mark SENT")
    void shouldRecoverStaleInFlightEvents() {
        OutboxEventEntity staleEvent = new OutboxEventEntity("evt-stale", "trade-orders", "AAPL", "{}", "IN_FLIGHT");
        when(outboxEventRepository.findPendingOrStaleInFlight(anyLong(), any())).thenReturn(List.of(staleEvent));
        when(outboxEventRepository.claimOutboxEventWithLease(eq("evt-stale"), anyLong(), anyLong())).thenReturn(1);
        when(outboxEventRepository.findById("evt-stale")).thenReturn(Optional.of(staleEvent));

        outboxService.processPendingOutboxEvents();

        verify(kafkaTemplate).send(eq("trade-orders"), eq("AAPL"), eq("{}"));
        assertEquals("SENT", staleEvent.getStatus());
    }
}
