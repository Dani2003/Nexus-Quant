package com.trading.orchestrator.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Durable relational entity recording all risk decisions for auditability and regulatory compliance.
 */
@Entity
@Table(name = "risk_decisions")
public class RiskDecisionEntity {

    @Id
    @Column(name = "record_id", length = 64, nullable = false)
    private String recordId;

    @Column(name = "tick_fingerprint", length = 128, nullable = false)
    private String tickFingerprint;

    @Column(name = "portfolio_id", length = 64, nullable = false)
    private String portfolioId;

    @Column(name = "symbol", length = 32, nullable = false)
    private String symbol;

    @Column(name = "price", nullable = false)
    private double price;

    @Column(name = "proposed_trade_size", nullable = false)
    private double proposedTradeSize;

    @Column(name = "value_at_risk", nullable = false)
    private double valueAtRisk;

    @Column(name = "status", length = 32, nullable = false)
    private String status;

    @Column(name = "reason", length = 255)
    private String reason;

    @Column(name = "latency_ms", nullable = false)
    private int latencyMs;

    @Column(name = "timestamp", nullable = false)
    private long timestamp;

    public RiskDecisionEntity() {}

    public RiskDecisionEntity(
            String recordId, String tickFingerprint, String portfolioId, String symbol,
            double price, double proposedTradeSize, double valueAtRisk, String status,
            String reason, int latencyMs, long timestamp) {
        this.recordId = recordId;
        this.tickFingerprint = tickFingerprint;
        this.portfolioId = portfolioId;
        this.symbol = symbol;
        this.price = price;
        this.proposedTradeSize = proposedTradeSize;
        this.valueAtRisk = valueAtRisk;
        this.status = status;
        this.reason = reason;
        this.latencyMs = latencyMs;
        this.timestamp = timestamp;
    }

    public String getRecordId() { return recordId; }
    public String getTickFingerprint() { return tickFingerprint; }
    public String getPortfolioId() { return portfolioId; }
    public String getSymbol() { return symbol; }
    public double getPrice() { return price; }
    public double getProposedTradeSize() { return proposedTradeSize; }
    public double getValueAtRisk() { return valueAtRisk; }
    public String getStatus() { return status; }
    public String getReason() { return reason; }
    public int getLatencyMs() { return latencyMs; }
    public long getTimestamp() { return timestamp; }
}
