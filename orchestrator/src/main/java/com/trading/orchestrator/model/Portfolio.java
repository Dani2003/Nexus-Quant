package com.trading.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.Map;

/**
 * Immutable Portfolio summary model representing real-time cash, positions,
 * and mark-to-market equity.
 */
public record Portfolio(
        @JsonProperty("portfolio_id") String portfolioId,
        @JsonProperty("cash_balance") double cashBalance,
        @JsonProperty("positions") Map<String, Position> positions,
        @JsonProperty("total_equity") double totalEquity,
        @JsonProperty("unrealized_pnl") double unrealizedPnl,
        @JsonProperty("realized_pnl") double realizedPnl,
        @JsonProperty("timestamp") long timestamp
) {
    public static Portfolio createInitial(String portfolioId, double initialCash) {
        return new Portfolio(
                portfolioId,
                initialCash,
                Collections.emptyMap(),
                initialCash,
                0.0,
                0.0,
                System.currentTimeMillis()
        );
    }
}
