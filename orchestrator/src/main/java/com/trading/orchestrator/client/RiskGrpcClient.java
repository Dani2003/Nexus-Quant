package com.trading.orchestrator.client;

import com.trading.orchestrator.grpc.RiskRequest;
import com.trading.orchestrator.grpc.RiskResponse;
import com.trading.orchestrator.grpc.RiskServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
 * Uses a blocking stub executed on Java 21 Project Loom Virtual Threads,
 * yielding carrier OS threads on network socket waits without thread pinning.
 */
@Component
public class RiskGrpcClient {

    private static final Logger log = LoggerFactory.getLogger(RiskGrpcClient.class);

    private final ManagedChannel channel;
    private final RiskServiceGrpc.RiskServiceBlockingStub blockingStub;
    private final ExecutorService virtualThreadExecutor;
    private final long grpcDeadlineMs;

    public RiskGrpcClient(
            @Value("${trading.risk.grpc-host:localhost}") String host,
            @Value("${trading.risk.grpc-port:50051}") int port,
            @Value("${trading.risk.grpc-deadline-ms:50}") long grpcDeadlineMs,
            ExecutorService virtualThreadPerTaskExecutor) {

        this.grpcDeadlineMs = grpcDeadlineMs;
        this.virtualThreadExecutor = virtualThreadPerTaskExecutor;
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // Sidecar local IPC communication
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .build();

        this.blockingStub = RiskServiceGrpc.newBlockingStub(channel);
        log.info("Initialized Risk gRPC Client targeting Rust Sidecar at {}:{} with deadline: {}ms",
                host, port, grpcDeadlineMs);
    }

    /**
     * Evaluates risk against the Rust sidecar with an enforced deadline.
     * When executed within a Java 21 Virtual Thread, this blocking call yields the carrier thread,
     * ensuring zero OS thread starvation.
     *
     * @throws RiskServiceException if the deadline is exceeded, sidecar is down, or request is rejected.
     */
    public RiskResponse evaluateRisk(String portfolioId, String assetSymbol, double proposedTradeSize, double currentPrice) {
        RiskRequest request = RiskRequest.newBuilder()
                .setPortfolioId(portfolioId)
                .setAssetSymbol(assetSymbol)
                .setProposedTradeSize(proposedTradeSize)
                .setCurrentPrice(currentPrice)
                .build();

        long startTime = System.nanoTime();
        try {
            // Enforce explicit gRPC deadline
            RiskResponse response = blockingStub
                    .withDeadlineAfter(grpcDeadlineMs, TimeUnit.MILLISECONDS)
                    .evaluateRisk(request);

            long roundTripUs = (System.nanoTime() - startTime) / 1_000;
            log.info("[gRPC Response] Asset: {}, Price: {}, VaR: {:.2f}, Approved: {}, Rust compute: {}ms, Round-trip: {}μs, Thread: {}",
                    assetSymbol, currentPrice, response.getValueAtRisk(), response.getIsApproved(),
                    response.getComputationTimeMs(), roundTripUs, Thread.currentThread());

            return response;

        } catch (StatusRuntimeException e) {
            long elapsedUs = (System.nanoTime() - startTime) / 1_000;
            Status.Code code = e.getStatus().getCode();

            if (code == Status.Code.DEADLINE_EXCEEDED) {
                log.error("[gRPC TIMEOUT] Risk evaluation exceeded deadline of {}ms for {} (took {}μs)",
                        grpcDeadlineMs, assetSymbol, elapsedUs);
                throw new RiskServiceException("Risk engine RPC deadline exceeded (" + grpcDeadlineMs + "ms)", code, e);
            } else if (code == Status.Code.UNAVAILABLE) {
                log.error("[gRPC UNAVAILABLE] Risk sidecar is unavailable or unreachable for {}", assetSymbol);
                throw new RiskServiceException("Risk engine sidecar is unavailable", code, e);
            } else {
                log.error("[gRPC ERROR] Risk evaluation failed for {}: {} - {}", assetSymbol, code, e.getMessage());
                throw new RiskServiceException("gRPC call failed with status " + code + ": " + e.getMessage(), code, e);
            }
        }
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
