package com.trading.orchestrator.client;

import com.trading.orchestrator.grpc.RiskRequest;
import com.trading.orchestrator.grpc.RiskResponse;
import com.trading.orchestrator.grpc.RiskServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * gRPC Client communicating with the Rust Risk Compute Sidecar.
 * Integrates with Java 21 Project Loom Virtual Threads for ultra-high throughput
 * non-blocking RPC interactions.
 */
@Component
public class RiskGrpcClient {

    private static final Logger log = LoggerFactory.getLogger(RiskGrpcClient.class);

    private final ManagedChannel channel;
    private final RiskServiceGrpc.RiskServiceBlockingStub blockingStub;
    private final ExecutorService virtualThreadExecutor;

    public RiskGrpcClient(
            @Value("${trading.risk.grpc-host:localhost}") String host,
            @Value("${trading.risk.grpc-port:50051}") int port,
            ExecutorService virtualThreadPerTaskExecutor) {

        this.virtualThreadExecutor = virtualThreadPerTaskExecutor;
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // Sidecar local IPC communication
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .build();

        this.blockingStub = RiskServiceGrpc.newBlockingStub(channel);
        log.info("Initialized Risk gRPC Client targeting Rust Sidecar at {}:{}", host, port);
    }

    /**
     * Synchronously evaluate risk against the Rust sidecar.
     * When executed within a Java 21 Virtual Thread, this call yields the carrier thread,
     * ensuring zero OS thread blocking.
     */
    public RiskResponse evaluateRisk(String portfolioId, String assetSymbol, double proposedTradeSize, double currentPrice) {
        RiskRequest request = RiskRequest.newBuilder()
                .setPortfolioId(portfolioId)
                .setAssetSymbol(assetSymbol)
                .setProposedTradeSize(proposedTradeSize)
                .setCurrentPrice(currentPrice)
                .build();

        long startTime = System.nanoTime();
        RiskResponse response = blockingStub.evaluateRisk(request);
        long roundTripUs = (System.nanoTime() - startTime) / 1_000;

        log.info("[gRPC Response] Asset: {}, Price: {}, VaR: {}, Approved: {}, Rust compute: {}ms, gRPC round-trip: {}μs, Thread: {}",
                assetSymbol, currentPrice, response.getValueAtRisk(), response.getIsApproved(),
                response.getComputationTimeMs(), roundTripUs, Thread.currentThread());

        return response;
    }

    /**
     * Asynchronously evaluate risk on a dedicated Virtual Thread.
     */
    public CompletableFuture<RiskResponse> evaluateRiskAsync(
            String portfolioId, String assetSymbol, double proposedTradeSize, double currentPrice) {
        return CompletableFuture.supplyAsync(
                () -> evaluateRisk(portfolioId, assetSymbol, proposedTradeSize, currentPrice),
                virtualThreadExecutor);
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down Risk gRPC Channel...");
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while shutting down gRPC channel", e);
        }
    }
}

