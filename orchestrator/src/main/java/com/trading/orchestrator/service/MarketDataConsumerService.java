package com.trading.orchestrator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orchestrator.client.RiskGrpcClient;
import com.trading.orchestrator.grpc.RiskResponse;
import com.trading.orchestrator.model.MarketTick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;

/**
 * Event-Driven Kafka Consumer service that listens to the market-ticks topic.
 * Fully utilizes Java 21 Project Loom Virtual Threads for concurrent message processing
 * and non-blocking gRPC risk evaluation.
 */
@Service
public class MarketDataConsumerService {

    private static final Logger log = LoggerFactory.getLogger(MarketDataConsumerService.class);

    private final RiskGrpcClient riskGrpcClient;
    private final ObjectMapper objectMapper;
    private final ExecutorService virtualThreadExecutor;
    private final double priceThreshold;
    private final double defaultTradeSize;

    public MarketDataConsumerService(
            RiskGrpcClient riskGrpcClient,
            ObjectMapper objectMapper,
            ExecutorService virtualThreadPerTaskExecutor,
            @Value("${trading.risk.threshold-price:150.0}") double priceThreshold,
            @Value("${trading.risk.default-proposed-trade-size:100.0}") double defaultTradeSize) {
        this.riskGrpcClient = riskGrpcClient;
        this.objectMapper = objectMapper;
        this.virtualThreadExecutor = virtualThreadPerTaskExecutor;
        this.priceThreshold = priceThreshold;
        this.defaultTradeSize = defaultTradeSize;
        log.info("MarketDataConsumerService initialized. Risk Price Threshold: >= {}, Default Trade Size: {}",
                priceThreshold, defaultTradeSize);
    }

    /**
     * Kafka Listener for market data ticks. Executed concurrently on Java 21 Virtual Threads.
     */
    @KafkaListener(
            topics = "${trading.kafka.market-ticks-topic:market-ticks}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onMarketTick(
            @Payload String rawMessage,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partition,
            @Header(value = KafkaHeaders.OFFSET, required = false) Long offset) {

        // Verify running on a Virtual Thread (Thread.currentThread().isVirtual())
        boolean isVirtual = Thread.currentThread().isVirtual();
        log.debug("Received tick from [{}-{}@{}] on virtual thread? {} ({})",
                topic, partition, offset, isVirtual, Thread.currentThread());

        // Process concurrently using Virtual Threads to ensure zero head-of-line blocking
        virtualThreadExecutor.submit(() -> processTick(rawMessage));
    }

    /**
     * Core tick processing logic running on a dedicated Java 21 Virtual Thread.
     */
    public void processTick(String rawMessage) {
        try {
            MarketTick tick = objectMapper.readValue(rawMessage, MarketTick.class);
            log.info("[Tick Ingested] Symbol: {}, Price: {}, Volume: {}, Thread: {}",
                    tick.symbol(), tick.price(), tick.volume(), Thread.currentThread());

            // Trigger Risk Evaluation if price threshold condition is met
            if (tick.price() >= priceThreshold) {
                log.warn("[Threshold Exceeded] Symbol: {} price {:.2f} exceeded threshold {:.2f}. Dispatching gRPC RiskRequest to Rust sidecar...",
                        tick.symbol(), tick.price(), priceThreshold);

                double proposedTradeSize = tick.volume() > 0 ? tick.volume() : defaultTradeSize;

                // Non-blocking gRPC call on Loom Virtual Thread
                RiskResponse riskResponse = riskGrpcClient.evaluateRisk(
                        tick.portfolioId(),
                        tick.symbol(),
                        proposedTradeSize,
                        tick.price()
                );

                if (riskResponse.getIsApproved()) {
                    log.info("[Trade APPROVED] Symbol: {}, VaR: {:.2f}, Compute Time: {}ms",
                            tick.symbol(), riskResponse.getValueAtRisk(), riskResponse.getComputationTimeMs());
                } else {
                    log.warn("[Trade REJECTED - RISK BREACH] Symbol: {}, VaR: {:.2f} exceeded risk tolerance",
                            tick.symbol(), riskResponse.getValueAtRisk());
                }
            } else {
                log.debug("[Normal Flow] Symbol: {} price {:.2f} < threshold {:.2f}. Risk check skipped.",
                        tick.symbol(), tick.price(), priceThreshold);
            }

        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize market tick JSON: {}", rawMessage, e);
        } catch (Exception e) {
            log.error("Error processing market tick: {}", rawMessage, e);
        }
    }
}

