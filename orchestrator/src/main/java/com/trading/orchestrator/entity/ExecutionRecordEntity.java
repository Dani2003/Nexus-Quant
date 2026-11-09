package com.trading.orchestrator.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Durable entity for order execution fill deduplication.
 * Prevents duplicate broker fill applications on Kafka redelivery or outbox replay.
 */
@Entity
@Table(name = "execution_records")
public class ExecutionRecordEntity {

    @Id
    @Column(name = "order_id", length = 64, nullable = false)
    private String orderId;

    @Column(name = "execution_id", length = 64, nullable = false)
    private String executionId;

    @Column(name = "portfolio_id", length = 64, nullable = false)
    private String portfolioId;

    @Column(name = "symbol", length = 32, nullable = false)
    private String symbol;

    @Column(name = "side", length = 16, nullable = false)
    private String side;

    @Column(name = "quantity", nullable = false)
    private double quantity;

    @Column(name = "execution_price", nullable = false)
    private double executionPrice;

    @Column(name = "timestamp", nullable = false)
    private long timestamp;

    public ExecutionRecordEntity() {}

    public ExecutionRecordEntity(
            String orderId, String executionId, String portfolioId, String symbol,
            String side, double quantity, double executionPrice, long timestamp) {
        this.orderId = orderId;
        this.executionId = executionId;
        this.portfolioId = portfolioId;
        this.symbol = symbol;
        this.side = side;
        this.quantity = quantity;
        this.executionPrice = executionPrice;
        this.timestamp = timestamp;
    }

    public String getOrderId() { return orderId; }
    public String getExecutionId() { return executionId; }
    public String getPortfolioId() { return portfolioId; }
    public String getSymbol() { return symbol; }
    public String getSide() { return side; }
    public double getQuantity() { return quantity; }
    public double getExecutionPrice() { return executionPrice; }
    public long getTimestamp() { return timestamp; }
}

