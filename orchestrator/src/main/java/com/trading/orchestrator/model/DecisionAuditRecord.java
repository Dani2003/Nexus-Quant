package com.trading.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

/**
 * Immutable audit record stored for every risk decision to ensure regulatory compliance,
 * auditability, and durable idempotency checks across retries.
 */
public record DecisionAuditRecord(
        @JsonProperty("record_id") String recordId,
        @JsonProperty("tick_fingerprint") String tickFingerprint,
        @JsonProperty("portfolio_id") String portfolioId,
        @JsonProperty("symbol") String symbol,
        @JsonProperty("price") double price,
        @JsonProperty("proposed_trade_size") double proposedTradeSize,
        @JsonProperty("value_at_risk") double valueAtRisk,
        @JsonProperty("status") String status,
        @JsonProperty("reason") String reason,
        @JsonProperty("latency_ms") int latencyMs,
        @JsonProperty("timestamp") long timestamp
) {
    public static DecisionAuditRecord create(
            String tickFingerprint, String portfolioId, String symbol, double price,
            double size, double varMetric, String status, String reason, int latencyMs) {
        return new DecisionAuditRecord(
                UUID.randomUUID().toString(),
                tickFingerprint,
                portfolioId,
                symbol,
                price,
                size,
                varMetric,
                status,
                reason,
                latencyMs,
                System.currentTimeMillis()
        );
    }
}

