package com.trading.orchestrator.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Durable entity for trade orders.
 */
@Entity
@Table(name = "trade_orders")
public class TradeOrderEntity {

    @Id
    @Column(name = "order_id", length = 64, nullable = false)
    private String orderId;

    @Column(name = "portfolio_id", length = 64, nullable = false)
    private String portfolioId;

    @Column(name = "symbol", length = 32, nullable = false)
    private String symbol;

    @Column(name = "side", length = 16, nullable = false)
    private String side;

    @Column(name = "quantity", nullable = false)
    private double quantity;

    @Column(name = "price", nullable = false)
    private double price;

    @Column(name = "status", length = 32, nullable = false)
    private String status;

    @Column(name = "value_at_risk", nullable = false)
    private double valueAtRisk;

    @Column(name = "timestamp", nullable = false)
    private long timestamp;

    public TradeOrderEntity() {}

    public TradeOrderEntity(
            String orderId, String portfolioId, String symbol, String side,
            double quantity, double price, String status, double valueAtRisk, long timestamp) {
        this.orderId = orderId;
        this.portfolioId = portfolioId;
        this.symbol = symbol;
        this.side = side;
        this.quantity = quantity;
        this.price = price;
        this.status = status;
        this.valueAtRisk = valueAtRisk;
        this.timestamp = timestamp;
    }

    public String getOrderId() { return orderId; }
    public String getPortfolioId() { return portfolioId; }
    public String getSymbol() { return symbol; }
    public String getSide() { return side; }
    public double getQuantity() { return quantity; }
    public double getPrice() { return price; }
    public String getStatus() { return status; }
    public double getValueAtRisk() { return valueAtRisk; }
    public long getTimestamp() { return timestamp; }
}
