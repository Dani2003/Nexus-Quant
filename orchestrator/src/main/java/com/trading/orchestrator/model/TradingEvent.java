package com.trading.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Unified real-time event record streamed over Spring WebFlux SSE / WebSocket to trading dashboards.
 */
public record TradingEvent(
        @JsonProperty("event_type") String eventType,
        @JsonProperty("symbol") String symbol,
        @JsonProperty("price") double price,
        @JsonProperty("quantity") double quantity,
        @JsonProperty("value_at_risk") double valueAtRisk,
        @JsonProperty("approved") boolean approved,
        @JsonProperty("details") String details,
        @JsonProperty("timestamp") long timestamp
) {
    public static TradingEvent fromTick(MarketTick tick) {
        return new TradingEvent(
                "TICK",
                tick.symbol(),
                tick.price(),
                tick.volume(),
                0.0,
                false,
                "Market tick ingested",
                tick.timestamp()
        );
    }

    public static TradingEvent fromOrder(Order order) {
        return new TradingEvent(
                "ORDER_APPROVED",
                order.symbol(),
                order.price(),
                order.quantity(),
                order.valueAtRisk(),
                true,
                "Order approved and dispatched. Order ID: " + order.orderId(),
                order.timestamp()
        );
    }

    public static TradingEvent fromRejection(RiskRejection rejection) {
        return new TradingEvent(
                "RISK_REJECTED",
                rejection.symbol(),
                rejection.currentPrice(),
                rejection.proposedTradeSize(),
                rejection.valueAtRisk(),
                false,
                rejection.reason(),
                rejection.timestamp()
        );
    }
}

