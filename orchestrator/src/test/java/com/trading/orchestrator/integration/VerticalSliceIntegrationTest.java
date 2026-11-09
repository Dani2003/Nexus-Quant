package com.trading.orchestrator.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orchestrator.client.RiskGrpcClient;
import com.trading.orchestrator.client.RiskServiceException;
import com.trading.orchestrator.grpc.RiskEngineProto;
import com.trading.orchestrator.grpc.RiskRequest;
import com.trading.orchestrator.grpc.RiskResponse;
import com.trading.orchestrator.grpc.RiskServiceGrpc;
import com.trading.orchestrator.service.AuditStorageService;
import com.trading.orchestrator.service.MarketDataConsumerService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class VerticalSliceIntegrationTest {

    private final String serverName = InProcessServerBuilder.generateName();
    private Server inProcessServer;
    private ManagedChannel inProcessChannel;

    private RiskGrpcClient riskGrpcClient;
    private AuditStorageService auditStorageService;
    private MarketDataConsumerService consumerService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private Acknowledgment acknowledgment;

    // Test risk engine mock state
    private final AtomicBoolean mockApproved = new AtomicBoolean(true);
    private final AtomicInteger mockComputationDelayMs = new AtomicInteger(0);
    private final AtomicInteger requestCounter = new AtomicInteger(0);

    @BeforeEach
    void setUp() throws Exception {
        lenient().when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Start In-Process Mock gRPC Server simulating Rust Risk Sidecar
        inProcessServer = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new RiskServiceGrpc.RiskServiceImplBase() {
                    @Override
                    public void evaluateRisk(RiskRequest request, StreamObserver<RiskResponse> responseObserver) {
                        requestCounter.incrementAndGet();

                        int delay = mockComputationDelayMs.get();
                        if (delay > 0) {
                            try {
                                Thread.sleep(delay);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }

                        RiskResponse response = RiskResponse.newBuilder()
                                .setIsApproved(mockApproved.get())
                                .setValueAtRisk(mockApproved.get() ? 350.0 : 95_000.0)
                                .setComputationTimeMs(Math.max(1, delay))
                                .build();

                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                    }
                })
                .build()
                .start();

        inProcessChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

        // Instantiate RiskGrpcClient targeting in-process channel with 50ms deadline
        riskGrpcClient = new RiskGrpcClient("localhost", 50051, 50L, Executors.newVirtualThreadPerTaskExecutor());

        // Replace internal channel and stub using reflection for pure in-process testing
        replaceClientChannelAndStub(riskGrpcClient, inProcessChannel);

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

    @AfterEach
    void tearDown() throws Exception {
        if (inProcessChannel != null) {
            inProcessChannel.shutdownNow();
        }
        if (inProcessServer != null) {
            inProcessServer.shutdownNow();
        }
    }

    private void replaceClientChannelAndStub(RiskGrpcClient client, ManagedChannel channel) throws Exception {
        Field channelField = RiskGrpcClient.class.getDeclaredField("channel");
        channelField.setAccessible(true);
        channelField.set(client, channel);

        Field stubField = RiskGrpcClient.class.getDeclaredField("blockingStub");
        stubField.setAccessible(true);
        stubField.set(client, RiskServiceGrpc.newBlockingStub(channel));
    }

    @Test
    @DisplayName("Path 1: Below threshold tick -> normal flow, no gRPC call, offset acknowledged")
    void testNormalFlowBelowThreshold() {
        String jsonTick = """
            {
                "portfolio_id": "GROWTH_FUND",
                "symbol": "AAPL",
                "price": 120.50,
                "volume": 100.0,
                "timestamp": 1715000000000
            }
            """;

        consumerService.onMarketTick(jsonTick, "market-ticks", 0, 10L, acknowledgment);

        assertEquals(0, requestCounter.get(), "No gRPC request should be made when price < threshold");
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("Path 2: Above threshold tick -> gRPC call, trade approved, durably published, offset acknowledged")
    void testApprovedTradeFlow() throws Exception {
        mockApproved.set(true);

        String jsonTick = """
            {
                "portfolio_id": "TECH_PORTFOLIO",
                "symbol": "AAPL",
                "price": 175.50,
                "volume": 200.0,
                "timestamp": 1715000001000
            }
            """;

        CountDownLatch latch = new CountDownLatch(1);
        Thread.ofVirtual().start(() -> {
            consumerService.onMarketTick(jsonTick, "market-ticks", 0, 11L, acknowledgment);
            latch.countDown();
        });

        assertTrue(latch.await(3, TimeUnit.SECONDS));

        assertEquals(1, requestCounter.get());
        verify(kafkaTemplate).send(eq("trade-orders"), eq("AAPL"), contains("\"status\":\"APPROVED\""));
        assertEquals(1, auditStorageService.getRecentDecisions(10).size());
        assertEquals("APPROVED", auditStorageService.getRecentDecisions(10).get(0).status());
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("Path 3: Above threshold tick -> risk tolerance breach, rejection durably published, offset acknowledged")
    void testRejectedRiskBreachFlow() throws Exception {
        mockApproved.set(false);

        String jsonTick = """
            {
                "portfolio_id": "TECH_PORTFOLIO",
                "symbol": "TSLA",
                "price": 250.00,
                "volume": 1000.0,
                "timestamp": 1715000002000
            }
            """;

        CountDownLatch latch = new CountDownLatch(1);
        Thread.ofVirtual().start(() -> {
            consumerService.onMarketTick(jsonTick, "market-ticks", 0, 12L, acknowledgment);
            latch.countDown();
        });

        assertTrue(latch.await(3, TimeUnit.SECONDS));

        assertEquals(1, requestCounter.get());
        verify(kafkaTemplate).send(eq("risk-rejections"), eq("TSLA"), contains("RISK_TOLERANCE_BREACH_VAR_EXCEEDED"));
        assertEquals(1, auditStorageService.getRecentDecisions(10).size());
        assertEquals("REJECTED_VAR_EXCEEDED", auditStorageService.getRecentDecisions(10).get(0).status());
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("Path 4: Sidecar unavailable -> fails closed, throws exception, does NOT acknowledge offset")
    void testSidecarUnavailableFailsClosedWithoutPrematureAck() throws Exception {
        // Stop the sidecar during request to simulate outage
        inProcessServer.shutdownNow();

        String jsonTick = """
            {
                "portfolio_id": "TECH_PORTFOLIO",
                "symbol": "NVDA",
                "price": 450.00,
                "volume": 50.0,
                "timestamp": 1715000003000
            }
            """;

        RiskServiceException ex = assertThrows(RiskServiceException.class, () ->
                consumerService.onMarketTick(jsonTick, "market-ticks", 0, 13L, acknowledgment)
        );

        assertEquals(Status.Code.UNAVAILABLE, ex.getStatusCode());
        // Critical resilience check: offset MUST NOT be acknowledged on RPC failure!
        verify(acknowledgment, never()).acknowledge();
        // No orders should ever be placed on sidecar outage
        verify(kafkaTemplate, never()).send(eq("trade-orders"), anyString(), anyString());
    }

    @Test
    @DisplayName("Path 5: Sidecar deadline exceeded (timeout) -> throws DEADLINE_EXCEEDED, offset NOT acknowledged")
    void testSidecarDeadlineExceeded() throws Exception {
        // Simulate sidecar delay exceeding client 50ms deadline
        mockComputationDelayMs.set(100);

        String jsonTick = """
            {
                "portfolio_id": "TECH_PORTFOLIO",
                "symbol": "MSFT",
                "price": 320.00,
                "volume": 100.0,
                "timestamp": 1715000004000
            }
            """;

        RiskServiceException ex = assertThrows(RiskServiceException.class, () ->
                consumerService.onMarketTick(jsonTick, "market-ticks", 0, 14L, acknowledgment)
        );

        assertEquals(Status.Code.DEADLINE_EXCEEDED, ex.getStatusCode());
        verify(acknowledgment, never()).acknowledge();
        verify(kafkaTemplate, never()).send(eq("trade-orders"), anyString(), anyString());
    }
}
