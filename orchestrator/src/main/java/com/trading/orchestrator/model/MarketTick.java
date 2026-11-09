package com.trading.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Immutable Market Tick model representing real-time price feeds streamed from Apache Kafka.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MarketTick(
        @JsonProperty("portfolio_id") String portfolioId,
        @JsonProperty("symbol") String symbol,
        @JsonProperty("price") double price,
        @JsonProperty("volume") double volume,
        @JsonProperty("timestamp") long timestamp
) {
    public MarketTick {
        if (portfolioId == null || portfolioId.isBlank()) {
            portfolioId = "DEFAULT_PORTFOLIO";
        }
        if (symbol == null || symbol.isBlank()) {
            symbol = "UNKNOWN";
        }
        if (timestamp <= 0) {
            timestamp = System.currentTimeMillis();
        }
    }
}

