package com.trading.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

/**
 * Immutable Trade Order record produced after successful risk approval.
 */
public record Order(
        @JsonProperty("order_id") String orderId,
        @JsonProperty("portfolio_id") String portfolioId,
        @JsonProperty("symbol") String symbol,
        @JsonProperty("side") String side,
        @JsonProperty("quantity") double quantity,
        @JsonProperty("price") double price,
        @JsonProperty("status") OrderStatus status,
        @JsonProperty("value_at_risk") double valueAtRisk,
        @JsonProperty("timestamp") long timestamp
) {
    public static Order createApproved(String portfolioId, String symbol, String side, double quantity, double price, double varMetric) {
        return new Order(
                UUID.randomUUID().toString(),
                portfolioId,
                symbol,
                side,
                quantity,
                price,
                OrderStatus.APPROVED,
                varMetric,
                System.currentTimeMillis()
        );
    }
}

