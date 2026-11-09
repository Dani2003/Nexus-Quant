package com.trading.orchestrator.controller;

import com.trading.orchestrator.model.DecisionAuditRecord;
import com.trading.orchestrator.model.ExecutionReport;
import com.trading.orchestrator.model.MarketTick;
import com.trading.orchestrator.model.Order;
import com.trading.orchestrator.model.Portfolio;
import com.trading.orchestrator.model.TradingEvent;
import com.trading.orchestrator.service.AuditStorageService;
import com.trading.orchestrator.service.MarketDataSimulatorService;
import com.trading.orchestrator.service.PortfolioService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

/**
 * Reactive Spring WebFlux Controller exposing REST operations, portfolio management,
 * and real-time SSE streaming.
 */
@RestController
@RequestMapping("/api/v1")
public class TradingController {

    private final MarketDataSimulatorService simulatorService;
    private final AuditStorageService auditStorageService;
    private final PortfolioService portfolioService;

    public TradingController(
            MarketDataSimulatorService simulatorService,
            AuditStorageService auditStorageService) {
        this(simulatorService, auditStorageService, null);
    }

    @Autowired
    public TradingController(
            MarketDataSimulatorService simulatorService,
            AuditStorageService auditStorageService,
            PortfolioService portfolioService) {
        this.simulatorService = simulatorService;
        this.auditStorageService = auditStorageService;
        this.portfolioService = portfolioService;
    }

    /**
     * Ingests a single market tick into Kafka.
     */
    @PostMapping("/market/tick")
    public Mono<Map<String, Object>> publishTick(@RequestBody MarketTick tick) {
        simulatorService.publishTick(tick);
        return Mono.just(Map.of(
                "status", "INGESTED",
                "symbol", tick.symbol(),
                "price", tick.price(),
                "timestamp", System.currentTimeMillis()
        ));
    }

    /**
     * Launches a synthetic market tick stream on a Java 21 Virtual Thread.
     */
    @PostMapping("/market/simulate")
    public Mono<Map<String, Object>> startSimulation(
            @RequestParam(defaultValue = "10") int rate,
            @RequestParam(defaultValue = "10") int duration) {
        simulatorService.startSimulation(rate, duration);
        return Mono.just(Map.of(
                "status", "SIMULATION_STARTED",
                "ratePerSecond", rate,
                "durationSeconds", duration
        ));
    }

    @PostMapping("/market/simulate/stop")
    public Mono<Map<String, Object>> stopSimulation() {
        simulatorService.stopSimulation();
        return Mono.just(Map.of("status", "SIMULATION_STOPPED"));
    }

    /**
     * Query recent risk evaluation decision audit records.
     */
    @GetMapping("/audit/decisions")
    public Mono<List<DecisionAuditRecord>> getDecisions(@RequestParam(defaultValue = "50") int limit) {
        return Mono.just(auditStorageService.getRecentDecisions(limit));
    }

    /**
     * Query recent approved orders.
     */
    @GetMapping("/audit/orders")
    public Mono<List<Order>> getOrders(@RequestParam(defaultValue = "50") int limit) {
        return Mono.just(auditStorageService.getRecentOrders(limit));
    }

    /**
     * Query recent execution fill reports.
     */
    @GetMapping("/audit/executions")
    public Mono<List<ExecutionReport>> getExecutions(@RequestParam(defaultValue = "50") int limit) {
        return Mono.just(auditStorageService.getRecentExecutions(limit));
    }

    /**
     * Retrieves portfolio state and mark-to-market positions.
     */
    @GetMapping("/portfolio/{portfolioId}")
    public Mono<Portfolio> getPortfolio(@PathVariable String portfolioId) {
        if (portfolioService == null) {
            return Mono.just(Portfolio.createInitial(portfolioId, 1_000_000.0));
        }
        return Mono.just(portfolioService.getPortfolio(portfolioId));
    }

    /**
     * Retrieves all active portfolios.
     */
    @GetMapping("/portfolios")
    public Mono<List<Portfolio>> getAllPortfolios() {
        if (portfolioService == null) {
            return Mono.just(List.of());
        }
        return Mono.just(portfolioService.getAllPortfolios());
    }

    /**
     * Sets automated Stop-Loss and Take-Profit rules for an open position.
     */
    @PostMapping("/portfolio/{portfolioId}/protection")
    public Mono<Map<String, Object>> setProtectionRules(
            @PathVariable String portfolioId,
            @RequestBody ProtectionRuleRequest request) {
        if (portfolioService == null) {
            return Mono.just(Map.of("status", "SERVICE_UNAVAILABLE"));
        }
        boolean updated = portfolioService.setProtectionRules(
                portfolioId, request.symbol(), request.stopLossPrice(), request.takeProfitPrice());
        return Mono.just(Map.of(
                "status", updated ? "PROTECTION_CONFIGURED" : "POSITION_NOT_FOUND",
                "portfolioId", portfolioId,
                "symbol", request.symbol(),
                "stopLossPrice", request.stopLossPrice() != null ? request.stopLossPrice() : 0.0,
                "takeProfitPrice", request.takeProfitPrice() != null ? request.takeProfitPrice() : 0.0
        ));
    }

    public record ProtectionRuleRequest(
            String symbol,
            Double stopLossPrice,
            Double takeProfitPrice
    ) {}

    /**
     * Real-time Server-Sent Events (SSE) feed streaming ticks, orders, and risk rejections live.
     */
    @GetMapping(value = "/stream/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<TradingEvent>> streamEvents() {
        return auditStorageService.getEventStream()
                .map(event -> ServerSentEvent.<TradingEvent>builder()
                        .event(event.eventType())
                        .data(event)
                        .build());
    }
}
