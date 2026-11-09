package com.trading.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orchestrator.client.RiskGrpcClient;
import com.trading.orchestrator.grpc.RiskResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MarketDataConsumerServiceTest {

    @Mock
    private RiskGrpcClient riskGrpcClient;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private Acknowledgment acknowledgment;

    private AuditStorageService auditStorageService;
    private MarketDataConsumerService consumerService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        lenient().when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        auditStorageService = new AuditStorageService();
        consumerService = new MarketDataConsumerService(
                riskGrpcClient,
                objectMapper,
                kafkaTemplate,
                auditStorageService,
                150.0,
                100.0,
                "trade-orders",
                "risk-rejections",
                3000L,
                50,
                25L
        );
    }

    @Test
    @DisplayName("Should validate tick, trigger gRPC RiskRequest, durably publish Order, and acknowledge Kafka offset")
    void shouldProcessValidTickAndAcknowledge() throws Exception {
        String jsonTick = """
            {
                "portfolio_id": "GROWTH_1",
                "symbol": "AAPL",
                "price": 175.50,
                "volume": 250.0,
                "timestamp": 1715000000000
            }
            """;

        RiskResponse approvedResponse = RiskResponse.newBuilder()
                .setIsApproved(true)
                .setValueAtRisk(450.25)
                .setComputationTimeMs(1)
                .build();

        when(riskGrpcClient.evaluateRisk(eq("GROWTH_1"), eq("AAPL"), eq(250.0), eq(175.50)))
                .thenReturn(approvedResponse);

        CountDownLatch latch = new CountDownLatch(1);
        Thread.ofVirtual().start(() -> {
            consumerService.onMarketTick(jsonTick, "market-ticks", 0, 100L, acknowledgment);
            latch.countDown();
        });

        assertTrue(latch.await(3, TimeUnit.SECONDS));

        // Verify risk evaluated
        verify(riskGrpcClient).evaluateRisk("GROWTH_1", "AAPL", 250.0, 175.50);
        // Verify approved order published to trade-orders topic
        verify(kafkaTemplate).send(eq("trade-orders"), eq("AAPL"), contains("\"status\":\"APPROVED\""));
        // Verify decision audit trail recorded
        assertEquals(1, auditStorageService.getRecentDecisions(10).size());
        assertEquals("APPROVED", auditStorageService.getRecentDecisions(10).get(0).status());
        // Verify Kafka offset acknowledged ONLY after success
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("Should reject invalid non-finite price without calling gRPC sidecar")
    void shouldRejectInvalidPrice() throws Exception {
        String invalidTick = """
            {
                "portfolio_id": "GROWTH_1",
                "symbol": "AAPL",
                "price": -50.0,
                "volume": 100.0,
                "timestamp": 1715000001000
            }
            """;

        consumerService.onMarketTick(invalidTick, "market-ticks", 0, 101L, acknowledgment);

        verify(riskGrpcClient, never()).evaluateRisk(anyString(), anyString(), anyDouble(), anyDouble());
        verify(kafkaTemplate).send(eq("risk-rejections"), eq("AAPL"), contains("INVALID_PRICE"));
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("Should detect and drop replayed duplicate tick via idempotency guard after durable completion")
    void shouldDropDuplicateTicks() throws Exception {
        String jsonTick = """
            {
                "portfolio_id": "GROWTH_1",
                "symbol": "MSFT",
                "price": 180.00,
                "volume": 50.0,
                "timestamp": 1715000002000
            }
            """;

        RiskResponse approvedResponse = RiskResponse.newBuilder()
                .setIsApproved(true)
                .setValueAtRisk(120.0)
                .setComputationTimeMs(1)
                .build();

        when(riskGrpcClient.evaluateRisk(anyString(), eq("MSFT"), anyDouble(), anyDouble()))
                .thenReturn(approvedResponse);

        // First delivery
        consumerService.onMarketTick(jsonTick, "market-ticks", 0, 102L, acknowledgment);
        // Second delivery (e.g. partition rebalance replay)
        consumerService.onMarketTick(jsonTick, "market-ticks", 0, 102L, acknowledgment);

        // Risk check and Order publishing should only execute ONCE
        verify(riskGrpcClient, times(1)).evaluateRisk(anyString(), eq("MSFT"), anyDouble(), anyDouble());
        verify(kafkaTemplate, times(1)).send(eq("trade-orders"), eq("MSFT"), anyString());
        // Both invocations acknowledge so Kafka does not hang
        verify(acknowledgment, times(2)).acknowledge();
    }
}
