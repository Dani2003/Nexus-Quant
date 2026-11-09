package com.trading.orchestrator.service;

import com.trading.orchestrator.model.DecisionAuditRecord;
import com.trading.orchestrator.model.ExecutionReport;
import com.trading.orchestrator.model.Order;
import com.trading.orchestrator.model.TradingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Thread-safe durable audit and event streaming service.
 * Tracks decision records, orders, execution fill reports, and broadcasts
 * live trading events to connected WebFlux SSE clients.
 */
@Service
public class AuditStorageService {

    private static final Logger log = LoggerFactory.getLogger(AuditStorageService.class);

    // Durable idempotency index: tickFingerprint -> DecisionAuditRecord
    private final Map<String, DecisionAuditRecord> fingerprintIndex = new ConcurrentHashMap<>(10_000);

    private final List<DecisionAuditRecord> decisionLog = new CopyOnWriteArrayList<>();
    private final List<Order> orderLog = new CopyOnWriteArrayList<>();
    private final List<ExecutionReport> executionLog = new CopyOnWriteArrayList<>();

    // Reactive multicast sink for live Server-Sent Events (SSE)
    private final Sinks.Many<TradingEvent> eventSink = Sinks.many().multicast().onBackpressureBuffer(1024, false);

    /**
     * Checks if a tick fingerprint has already been evaluated and durably recorded.
     */
    public boolean isFingerprintProcessed(String fingerprint) {
        return fingerprintIndex.containsKey(fingerprint);
    }

    /**
     * Stores a risk decision audit record, indexing it by fingerprint for idempotency.
     */
    public void recordDecision(DecisionAuditRecord record) {
        fingerprintIndex.put(record.tickFingerprint(), record);
        decisionLog.add(record);
        log.debug("Recorded decision audit: {} for symbol: {}", record.status(), record.symbol());
    }

    /**
     * Stores an approved order.
     */
    public void recordOrder(Order order) {
        orderLog.add(order);
        broadcastEvent(TradingEvent.fromOrder(order));
    }

    /**
     * Stores an order execution report.
     */
    public void recordExecution(ExecutionReport report) {
        executionLog.add(report);
        broadcastEvent(new TradingEvent(
                "ORDER_FILLED",
                report.symbol(),
                report.executionPrice(),
                report.executedQuantity(),
                0.0,
                true,
                "Order " + report.orderId() + " FILLED at " + report.executionPrice(),
                report.timestamp()
        ));
    }

    /**
     * Broadcasts a real-time event to reactive WebFlux clients.
     */
    public void broadcastEvent(TradingEvent event) {
        eventSink.tryEmitNext(event);
    }

    /**
     * Reactive stream of real-time trading platform events.
     */
    public Flux<TradingEvent> getEventStream() {
        return eventSink.asFlux();
    }

    public List<DecisionAuditRecord> getRecentDecisions(int limit) {
        int size = decisionLog.size();
        int from = Math.max(0, size - limit);
        List<DecisionAuditRecord> subList = new ArrayList<>(decisionLog.subList(from, size));
        Collections.reverse(subList);
        return subList;
    }

    public List<Order> getRecentOrders(int limit) {
        int size = orderLog.size();
        int from = Math.max(0, size - limit);
        List<Order> subList = new ArrayList<>(orderLog.subList(from, size));
        Collections.reverse(subList);
        return subList;
    }

    public List<ExecutionReport> getRecentExecutions(int limit) {
        int size = executionLog.size();
        int from = Math.max(0, size - limit);
        List<ExecutionReport> subList = new ArrayList<>(executionLog.subList(from, size));
        Collections.reverse(subList);
        return subList;
    }
}
