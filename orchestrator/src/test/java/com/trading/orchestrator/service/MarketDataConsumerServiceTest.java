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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MarketDataConsumerServiceTest {

    @Mock
    private RiskGrpcClient riskGrpcClient;

    private ExecutorService virtualThreadExecutor;
    private MarketDataConsumerService consumerService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
        consumerService = new MarketDataConsumerService(
                riskGrpcClient,
                objectMapper,
                virtualThreadExecutor,
                150.0,
                100.0
        );
    }

    @Test
    @DisplayName("Should trigger gRPC RiskRequest on Virtual Thread when tick price exceeds threshold")
    void shouldTriggerRiskRequestWhenThresholdMet() throws Exception {
        String jsonTick = """
            {
                "portfolio_id": "TECH_FUND_1",
                "symbol": "AAPL",
                "price": 175.50,
                "volume": 250.0,
                "timestamp": 1715000000000
            }
            """;

        RiskResponse mockResponse = RiskResponse.newBuilder()
                .setIsApproved(true)
                .setValueAtRisk(450.25)
                .setComputationTimeMs(2)
                .build();

        when(riskGrpcClient.evaluateRisk(eq("TECH_FUND_1"), eq("AAPL"), eq(250.0), eq(175.50)))
                .thenReturn(mockResponse);

        CountDownLatch latch = new CountDownLatch(1);
        Thread.ofVirtual().name("test-vt").start(() -> {
            consumerService.processTick(jsonTick);
            latch.countDown();
        });

        assertTrue(latch.await(3, TimeUnit.SECONDS));

        verify(riskGrpcClient).evaluateRisk("TECH_FUND_1", "AAPL", 250.0, 175.50);
    }

    @Test
    @DisplayName("Should NOT trigger gRPC RiskRequest when tick price is below threshold")
    void shouldNotTriggerRiskRequestWhenBelowThreshold() throws Exception {
        String jsonTick = """
            {
                "portfolio_id": "TECH_FUND_1",
                "symbol": "AAPL",
                "price": 120.00,
                "volume": 100.0,
                "timestamp": 1715000000000
            }
            """;

        CountDownLatch latch = new CountDownLatch(1);
        Thread.ofVirtual().name("test-vt").start(() -> {
            consumerService.processTick(jsonTick);
            latch.countDown();
        });

        assertTrue(latch.await(3, TimeUnit.SECONDS));

        verify(riskGrpcClient, never()).evaluateRisk(eq("TECH_FUND_1"), eq("AAPL"), anyDouble(), anyDouble());
    }
}

