package com.trading.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orchestrator.model.ExecutionReport;
import com.trading.orchestrator.model.MarketTick;
import com.trading.orchestrator.model.Order;
import com.trading.orchestrator.model.OrderStatus;
import com.trading.orchestrator.model.Portfolio;
import com.trading.orchestrator.model.Position;
import com.trading.orchestrator.model.TradingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Event-Driven Portfolio & Position Tracking Service with Automated Stop-Loss & Take-Profit Engine.
 * Features:
 *  1. Real-time Mark-to-Market (MtM) unrealized PnL updates on inbound market ticks.
 *  2. Realized PnL and cash balance tracking on order execution fill reports.
 *  3. Automated Stop-Loss & Take-Profit order generation for both LONG and SHORT exposures.
 */
@Service
public class PortfolioService {

    private static final Logger log = LoggerFactory.getLogger(PortfolioService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final AuditStorageService auditStorageService;
    private final String tradeOrdersTopic;
    private final long publishTimeoutMs;

    // Concurrent Portfolio Store: portfolioId -> InternalPortfolioState
    private final Map<String, InternalPortfolioState> portfolios = new ConcurrentHashMap<>();

    public PortfolioService(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            AuditStorageService auditStorageService,
            @Value("${trading.kafka.trade-orders-topic:trade-orders}") String tradeOrdersTopic,
            @Value("${trading.kafka.publish-timeout-ms:3000}") long publishTimeoutMs) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.auditStorageService = auditStorageService;
        this.tradeOrdersTopic = tradeOrdersTopic;
        this.publishTimeoutMs = publishTimeoutMs;
    }

    /**
     * Applies an execution fill report to update cash and active positions.
     */
    public synchronized void applyExecution(ExecutionReport report) {
        InternalPortfolioState state = getOrCreateState(report.portfolioId());
        state.applyFill(report);
        log.info("[Portfolio Updated] Portfolio: {} | Symbol: {} | Side: {} | FillPrice: {:.2f} | Total Equity: {:.2f}",
                report.portfolioId(), report.symbol(), report.side(), report.executionPrice(), state.toPortfolio().totalEquity());
    }

    /**
     * Evaluates open positions against incoming market ticks for:
     * 1. Mark-to-market unrealized PnL updates.
     * 2. Automated Stop-Loss triggers.
     * 3. Automated Take-Profit triggers.
     */
    public synchronized void onMarketPriceTick(MarketTick tick) {
        for (InternalPortfolioState state : portfolios.values()) {
            Position pos = state.positions.get(tick.symbol());
            if (pos == null || Math.abs(pos.quantity()) < 1e-6) {
                continue;
            }

            // 1. Mark-to-market update
            state.positions.put(tick.symbol(), pos.updateMarketPrice(tick.price()));

            // 2. Automated Stop-Loss / Take-Profit trigger check
            checkProtectionRules(state, pos, tick);
        }
    }

    private void checkProtectionRules(InternalPortfolioState state, Position pos, MarketTick tick) {
        boolean isLong = pos.quantity() > 0;
        boolean triggerExit = false;
        String exitReason = null;

        if (isLong) {
            if (pos.stopLossPrice() != null && tick.price() <= pos.stopLossPrice()) {
                triggerExit = true;
                exitReason = "STOP_LOSS_TRIGGERED";
            } else if (pos.takeProfitPrice() != null && tick.price() >= pos.takeProfitPrice()) {
                triggerExit = true;
                exitReason = "TAKE_PROFIT_TRIGGERED";
            }
        } else {
            // Short position: exit if price rises to stop-loss or drops to take-profit
            if (pos.stopLossPrice() != null && tick.price() >= pos.stopLossPrice()) {
                triggerExit = true;
                exitReason = "STOP_LOSS_TRIGGERED";
            } else if (pos.takeProfitPrice() != null && tick.price() <= pos.takeProfitPrice()) {
                triggerExit = true;
                exitReason = "TAKE_PROFIT_TRIGGERED";
            }
        }

        if (triggerExit) {
            String exitSide = isLong ? "SELL" : "BUY";
            double exitQty = Math.abs(pos.quantity());

            log.warn("[AUTOMATED PROTECTION EXIT] Reason: {} | Portfolio: {} | Symbol: {} | TriggerPrice: {:.2f} | ExitSide: {} | Qty: {}",
                    exitReason, state.portfolioId, pos.symbol(), tick.price(), exitSide, exitQty);

            // Generate protective market exit order
            Order exitOrder = new Order(
                    UUID.randomUUID().toString(),
                    state.portfolioId,
                    pos.symbol(),
                    exitSide,
                    exitQty,
                    tick.price(),
                    OrderStatus.APPROVED,
                    0.0,
                    System.currentTimeMillis()
            );

            // Publish directly to trade-orders
            try {
                String json = objectMapper.writeValueAsString(exitOrder);
                kafkaTemplate.send(tradeOrdersTopic, exitOrder.symbol(), json)
                        .get(publishTimeoutMs, TimeUnit.MILLISECONDS);

                auditStorageService.recordOrder(exitOrder);
                auditStorageService.broadcastEvent(new TradingEvent(
                        exitReason,
                        pos.symbol(),
                        tick.price(),
                        exitQty,
                        0.0,
                        true,
                        exitReason + ": Protective " + exitSide + " order dispatched. OrderId: " + exitOrder.orderId(),
                        System.currentTimeMillis()
                ));

                // Clear protection rules to prevent re-triggering while pending fill
                state.positions.put(pos.symbol(), pos.withProtectionRules(null, null));

            } catch (Exception e) {
                log.error("Failed to publish protective exit order for {}: {}", pos.symbol(), e.getMessage(), e);
            }
        }
    }

    /**
     * Configures stop-loss and take-profit prices for a position.
     */
    public synchronized boolean setProtectionRules(String portfolioId, String symbol, Double stopLossPrice, Double takeProfitPrice) {
        InternalPortfolioState state = getOrCreateState(portfolioId);
        Position pos = state.positions.get(symbol);
        if (pos == null || Math.abs(pos.quantity()) < 1e-6) {
            return false;
        }

        state.positions.put(symbol, pos.withProtectionRules(stopLossPrice, takeProfitPrice));
        log.info("Set protection rules for {} in portfolio {}: StopLoss: {}, TakeProfit: {}",
                symbol, portfolioId, stopLossPrice, takeProfitPrice);
        return true;
    }

    public Portfolio getPortfolio(String portfolioId) {
        return getOrCreateState(portfolioId).toPortfolio();
    }

    public List<Portfolio> getAllPortfolios() {
        List<Portfolio> list = new ArrayList<>();
        for (InternalPortfolioState state : portfolios.values()) {
            list.add(state.toPortfolio());
        }
        return list;
    }

    private InternalPortfolioState getOrCreateState(String portfolioId) {
        return portfolios.computeIfAbsent(portfolioId, id -> new InternalPortfolioState(id, 1_000_000.0));
    }

    /**
     * Internal mutable state container with synchronized mutators.
     */
    private static class InternalPortfolioState {
        final String portfolioId;
        double cashBalance;
        double totalRealizedPnl;
        final Map<String, Position> positions = new HashMap<>();

        InternalPortfolioState(String portfolioId, double initialCash) {
            this.portfolioId = portfolioId;
            this.cashBalance = initialCash;
            this.totalRealizedPnl = 0.0;
        }

        void applyFill(ExecutionReport report) {
            double notional = report.executedQuantity() * report.executionPrice();
            if ("BUY".equalsIgnoreCase(report.side())) {
                cashBalance -= notional;
            } else {
                cashBalance += notional;
            }

            Position currentPos = positions.getOrDefault(report.symbol(), Position.empty(report.symbol()));
            Position updatedPos = currentPos.applyFill(report.executedQuantity(), report.executionPrice(), report.side());

            totalRealizedPnl += (updatedPos.realizedPnl() - currentPos.realizedPnl());
            positions.put(report.symbol(), updatedPos);
        }

        Portfolio toPortfolio() {
            double totalUnrealizedPnl = 0.0;
            double holdingsMarketValue = 0.0;

            for (Position p : positions.values()) {
                totalUnrealizedPnl += p.unrealizedPnl();
                holdingsMarketValue += p.quantity() * p.currentMarketPrice();
            }

            double totalEquity = cashBalance + holdingsMarketValue;

            return new Portfolio(
                    portfolioId,
                    cashBalance,
                    Collections.unmodifiableMap(new HashMap<>(positions)),
                    totalEquity,
                    totalUnrealizedPnl,
                    totalRealizedPnl,
                    System.currentTimeMillis()
            );
        }
    }
}

