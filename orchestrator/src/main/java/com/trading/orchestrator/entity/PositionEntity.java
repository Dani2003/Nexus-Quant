package com.trading.orchestrator.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Relational entity storing active positions, mark-to-market prices,
 * and Stop-Loss / Take-Profit protection rules.
 */
@Entity
@Table(name = "positions")
public class PositionEntity {

    @Id
    @Column(name = "id", length = 128, nullable = false)
    private String id; // composite key: portfolioId + ":" + symbol

    @Column(name = "portfolio_id", length = 64, nullable = false)
    private String portfolioId;

    @Column(name = "symbol", length = 32, nullable = false)
    private String symbol;

    @Column(name = "quantity", nullable = false)
    private double quantity;

    @Column(name = "average_entry_price", nullable = false)
    private double averageEntryPrice;

    @Column(name = "current_market_price", nullable = false)
    private double currentMarketPrice;

    @Column(name = "unrealized_pnl", nullable = false)
    private double unrealizedPnl;

    @Column(name = "realized_pnl", nullable = false)
    private double realizedPnl;

    @Column(name = "stop_loss_price")
    private Double stopLossPrice;

    @Column(name = "take_profit_price")
    private Double takeProfitPrice;

    @Column(name = "updated_at", nullable = false)
    private long updatedAt;

    public PositionEntity() {}

    public PositionEntity(
            String portfolioId, String symbol, double quantity, double averageEntryPrice,
            double currentMarketPrice, double unrealizedPnl, double realizedPnl,
            Double stopLossPrice, Double takeProfitPrice) {
        this.id = portfolioId + ":" + symbol;
        this.portfolioId = portfolioId;
        this.symbol = symbol;
        this.quantity = quantity;
        this.averageEntryPrice = averageEntryPrice;
        this.currentMarketPrice = currentMarketPrice;
        this.unrealizedPnl = unrealizedPnl;
        this.realizedPnl = realizedPnl;
        this.stopLossPrice = stopLossPrice;
        this.takeProfitPrice = takeProfitPrice;
        this.updatedAt = System.currentTimeMillis();
    }

    public String getId() { return id; }
    public String getPortfolioId() { return portfolioId; }
    public String getSymbol() { return symbol; }
    public double getQuantity() { return quantity; }
    public void setQuantity(double quantity) { this.quantity = quantity; this.updatedAt = System.currentTimeMillis(); }
    public double getAverageEntryPrice() { return averageEntryPrice; }
    public void setAverageEntryPrice(double averageEntryPrice) { this.averageEntryPrice = averageEntryPrice; this.updatedAt = System.currentTimeMillis(); }
    public double getCurrentMarketPrice() { return currentMarketPrice; }
    public void setCurrentMarketPrice(double currentMarketPrice) { this.currentMarketPrice = currentMarketPrice; this.updatedAt = System.currentTimeMillis(); }
    public double getUnrealizedPnl() { return unrealizedPnl; }
    public void setUnrealizedPnl(double unrealizedPnl) { this.unrealizedPnl = unrealizedPnl; this.updatedAt = System.currentTimeMillis(); }
    public double getRealizedPnl() { return realizedPnl; }
    public void setRealizedPnl(double realizedPnl) { this.realizedPnl = realizedPnl; this.updatedAt = System.currentTimeMillis(); }
    public Double getStopLossPrice() { return stopLossPrice; }
    public void setStopLossPrice(Double stopLossPrice) { this.stopLossPrice = stopLossPrice; this.updatedAt = System.currentTimeMillis(); }
    public Double getTakeProfitPrice() { return takeProfitPrice; }
    public void setTakeProfitPrice(Double takeProfitPrice) { this.takeProfitPrice = takeProfitPrice; this.updatedAt = System.currentTimeMillis(); }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
}

