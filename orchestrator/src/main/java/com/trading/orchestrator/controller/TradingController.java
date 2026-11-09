package com.trading.orchestrator.controller;

import com.trading.orchestrator.model.DecisionAuditRecord;
import com.trading.orchestrator.model.ExecutionReport;
import com.trading.orchestrator.model.MarketTick;
import com.trading.orchestrator.model.Order;
import com.trading.orchestrator.model.TradingEvent;
import com.trading.orchestrator.service.AuditStorageService;
import com.trading.orchestrator.service.MarketDataSimulatorService;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
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
 * Reactive Spring WebFlux Controller exposing REST operations and real-time SSE streaming.
 */
@RestController
@RequestMapping("/api/v1")
public class TradingController {

    private final MarketDataSimulatorService simulatorService;
    private final AuditStorageService auditStorageService;

    public TradingController(
            MarketDataSimulatorService simulatorService,
            AuditStorageService auditStorageService) {
        this.simulatorService = simulatorService;
        this.auditStorageService = auditStorageService;
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

