package com.trading.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

/**
 * Immutable execution fill report generated when a downstream matching engine or broker
 * fills an approved trade order.
 */
public record ExecutionReport(
        @JsonProperty("execution_id") String executionId,
        @JsonProperty("order_id") String orderId,
        @JsonProperty("portfolio_id") String portfolioId,
        @JsonProperty("symbol") String symbol,
        @JsonProperty("side") String side,
        @JsonProperty("executed_quantity") double executedQuantity,
        @JsonProperty("execution_price") double executionPrice,
        @JsonProperty("status") String status,
        @JsonProperty("timestamp") long timestamp
) {
    public static ExecutionReport createFilled(Order order, double executionPrice) {
        return new ExecutionReport(
                UUID.randomUUID().toString(),
                order.orderId(),
                order.portfolioId(),
                order.symbol(),
                order.side(),
                order.quantity(),
                executionPrice,
                "FILLED",
                System.currentTimeMillis()
        );
    }
}
