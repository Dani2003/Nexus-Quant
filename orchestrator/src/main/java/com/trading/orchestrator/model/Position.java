package com.trading.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Thread-safe Position tracking model representing an active market position
 * for a specific symbol, supporting both LONG and SHORT exposures, mark-to-market
 * unrealized PnL, realized PnL, and Stop-Loss / Take-Profit rules.
 */
public record Position(
        @JsonProperty("symbol") String symbol,
        @JsonProperty("quantity") double quantity,
        @JsonProperty("average_entry_price") double averageEntryPrice,
        @JsonProperty("current_market_price") double currentMarketPrice,
        @JsonProperty("unrealized_pnl") double unrealizedPnl,
        @JsonProperty("realized_pnl") double realizedPnl,
        @JsonProperty("stop_loss_price") Double stopLossPrice,
        @JsonProperty("take_profit_price") Double takeProfitPrice
) {
    public static Position empty(String symbol) {
        return new Position(symbol, 0.0, 0.0, 0.0, 0.0, 0.0, null, null);
    }

    /**
     * Updates position on new execution fill report.
     * Computes weighted average entry price for increasing positions and
     * calculates realized PnL when closing/reducing positions.
     */
    public Position applyFill(double fillQty, double fillPrice, String side) {
        double signedFillQty = "BUY".equalsIgnoreCase(side) ? fillQty : -fillQty;
        double newQty = this.quantity + signedFillQty;
        double newRealizedPnl = this.realizedPnl;
        double newAvgEntryPrice = this.averageEntryPrice;

        if (Math.abs(newQty) < 1e-6) {
            // Position completely closed
            if (this.quantity > 0) {
                // Closed long
                newRealizedPnl += this.quantity * (fillPrice - this.averageEntryPrice);
            } else if (this.quantity < 0) {
                // Closed short
                newRealizedPnl += Math.abs(this.quantity) * (this.averageEntryPrice - fillPrice);
            }
            return new Position(symbol, 0.0, 0.0, fillPrice, 0.0, newRealizedPnl, null, null);
        }

        boolean isIncreasing = (this.quantity >= 0 && signedFillQty > 0) || (this.quantity <= 0 && signedFillQty < 0);

        if (isIncreasing) {
            // Weighted average entry price
            double totalCost = (Math.abs(this.quantity) * this.averageEntryPrice) + (Math.abs(signedFillQty) * fillPrice);
            newAvgEntryPrice = totalCost / Math.abs(newQty);
        } else {
            // Reducing position -> realize PnL
            double closedQty = Math.min(Math.abs(this.quantity), Math.abs(signedFillQty));
            if (this.quantity > 0) {
                newRealizedPnl += closedQty * (fillPrice - this.averageEntryPrice);
            } else {
                newRealizedPnl += closedQty * (this.averageEntryPrice - fillPrice);
            }

            if ((this.quantity > 0 && newQty < 0) || (this.quantity < 0 && newQty > 0)) {
                // Position flipped from Long to Short or vice-versa
                newAvgEntryPrice = fillPrice;
            }
        }

        double newUnrealizedPnl = computeUnrealizedPnl(newQty, newAvgEntryPrice, fillPrice);

        return new Position(
                symbol,
                newQty,
                newAvgEntryPrice,
                fillPrice,
                newUnrealizedPnl,
                newRealizedPnl,
                this.stopLossPrice,
                this.takeProfitPrice
        );
    }

    /**
     * Mark-to-market update when a new market tick price arrives.
     */
    public Position updateMarketPrice(double newMarketPrice) {
        double newUnrealizedPnl = computeUnrealizedPnl(this.quantity, this.averageEntryPrice, newMarketPrice);
        return new Position(
                symbol,
                this.quantity,
                this.averageEntryPrice,
                newMarketPrice,
                newUnrealizedPnl,
                this.realizedPnl,
                this.stopLossPrice,
                this.takeProfitPrice
        );
    }

    /**
     * Updates stop-loss and take-profit threshold prices.
     */
    public Position withProtectionRules(Double stopLossPrice, Double takeProfitPrice) {
        return new Position(
                symbol,
                this.quantity,
                this.averageEntryPrice,
                this.currentMarketPrice,
                this.unrealizedPnl,
                this.realizedPnl,
                stopLossPrice,
                takeProfitPrice
        );
    }

    private static double computeUnrealizedPnl(double qty, double entryPrice, double currentPrice) {
        if (Math.abs(qty) < 1e-6 || entryPrice <= 0.0) {
            return 0.0;
        }
        if (qty > 0) {
            // Long: profit when current > entry
            return qty * (currentPrice - entryPrice);
        } else {
            // Short: profit when entry > current
            return Math.abs(qty) * (entryPrice - currentPrice);
        }
    }
}
