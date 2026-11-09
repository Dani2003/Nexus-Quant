package com.trading.orchestrator.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Relational entity storing portfolio cash balances and aggregate realized PnL.
 */
@Entity
@Table(name = "portfolios")
public class PortfolioEntity {

    @Id
    @Column(name = "portfolio_id", length = 64, nullable = false)
    private String portfolioId;

    @Column(name = "cash_balance", nullable = false)
    private double cashBalance;

    @Column(name = "total_realized_pnl", nullable = false)
    private double totalRealizedPnl;

    @Column(name = "updated_at", nullable = false)
    private long updatedAt;

    public PortfolioEntity() {}

    public PortfolioEntity(String portfolioId, double cashBalance, double totalRealizedPnl) {
        this.portfolioId = portfolioId;
        this.cashBalance = cashBalance;
        this.totalRealizedPnl = totalRealizedPnl;
        this.updatedAt = System.currentTimeMillis();
    }

    public String getPortfolioId() { return portfolioId; }
    public void setPortfolioId(String portfolioId) { this.portfolioId = portfolioId; }

    public double getCashBalance() { return cashBalance; }
    public void setCashBalance(double cashBalance) {
        this.cashBalance = cashBalance;
        this.updatedAt = System.currentTimeMillis();
    }

    public double getTotalRealizedPnl() { return totalRealizedPnl; }
    public void setTotalRealizedPnl(double totalRealizedPnl) {
        this.totalRealizedPnl = totalRealizedPnl;
        this.updatedAt = System.currentTimeMillis();
    }

    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
}

