package com.trading.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

/**
 * Immutable Risk Rejection audit record published when a proposed trade breaches risk tolerance.
 */
public record RiskRejection(
        @JsonProperty("rejection_id") String rejectionId,
        @JsonProperty("portfolio_id") String portfolioId,
        @JsonProperty("symbol") String symbol,
        @JsonProperty("proposed_trade_size") double proposedTradeSize,
        @JsonProperty("current_price") double currentPrice,
        @JsonProperty("value_at_risk") double valueAtRisk,
        @JsonProperty("reason") String reason,
        @JsonProperty("timestamp") long timestamp
) {
    public static RiskRejection create(String portfolioId, String symbol, double size, double price, double varMetric, String reason) {
        return new RiskRejection(
                UUID.randomUUID().toString(),
                portfolioId,
                symbol,
                size,
                price,
                varMetric,
                reason,
                System.currentTimeMillis()
        );
    }
}

