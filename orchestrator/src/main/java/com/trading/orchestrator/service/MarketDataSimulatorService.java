package com.trading.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orchestrator.model.MarketTick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * High-Throughput Synthetic Market Data Generator executing on Java 21 Virtual Threads.
 * Simulates multi-asset Brownian motion ticks for live demonstration, integration, and load testing.
 */
@Service
public class MarketDataSimulatorService {

    private static final Logger log = LoggerFactory.getLogger(MarketDataSimulatorService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String marketTicksTopic;

    private final AtomicBoolean isSimulating = new AtomicBoolean(false);

    // Initial asset prices
    private final Map<String, Double> assetPrices = new ConcurrentHashMap<>(Map.of(
            "AAPL", 175.50,
            "NVDA", 450.20,
            "TSLA", 248.80,
            "MSFT", 330.10
    ));

    public MarketDataSimulatorService(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${trading.kafka.market-ticks-topic:market-ticks}") String marketTicksTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.marketTicksTopic = marketTicksTopic;
    }

    /**
     * Publishes a single market tick into Kafka.
     */
    public void publishTick(MarketTick tick) {
        try {
            String json = objectMapper.writeValueAsString(tick);
            kafkaTemplate.send(marketTicksTopic, tick.symbol(), json);
            log.debug("Published manual tick: {} @ {:.2f}", tick.symbol(), tick.price());
        } catch (Exception e) {
            log.error("Failed to publish tick: {}", e.getMessage(), e);
        }
    }

    /**
     * Spawns a dedicated Java 21 Virtual Thread to stream synthetic ticks at a specified rate.
     */
    public void startSimulation(int ratePerSecond, int durationSeconds) {
        if (!isSimulating.compareAndSet(false, true)) {
            log.warn("Simulation is already running");
            return;
        }

        Thread.ofVirtual().name("market-simulator-vt").start(() -> {
            log.info("Starting synthetic market tick stream: {} ticks/sec for {} seconds on Virtual Thread",
                    ratePerSecond, durationSeconds);

            long delayMs = Math.max(1, 1000 / ratePerSecond);
            long endTime = System.currentTimeMillis() + (durationSeconds * 1000L);
            String[] symbols = assetPrices.keySet().toArray(new String[0]);

            try {
                while (System.currentTimeMillis() < endTime && isSimulating.get()) {
                    String symbol = symbols[ThreadLocalRandom.current().nextInt(symbols.length)];
                    double currentPrice = assetPrices.get(symbol);

                    // Geometric Brownian Motion step: +/- 0.5% drift & shock
                    double shock = ThreadLocalRandom.current().nextGaussian() * 0.005;
                    double newPrice = Math.max(1.0, currentPrice * (1.0 + shock));
                    newPrice = Math.round(newPrice * 100.0) / 100.0;
                    assetPrices.put(symbol, newPrice);

                    double volume = ThreadLocalRandom.current().nextDouble(50.0, 500.0);
                    volume = Math.round(volume * 10.0) / 10.0;

                    MarketTick tick = new MarketTick(
                            "QUANT_FUND_ALPHA",
                            symbol,
                            newPrice,
                            volume,
                            System.currentTimeMillis()
                    );

                    publishTick(tick);
                    TimeUnit.MILLISECONDS.sleep(delayMs);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Market simulator interrupted");
            } finally {
                isSimulating.set(false);
                log.info("Market tick simulation completed.");
            }
        });
    }

    public void stopSimulation() {
        isSimulating.set(false);
    }

    public boolean isRunning() {
        return isSimulating.get();
    }
}
