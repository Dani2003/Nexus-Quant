package com.trading.orchestrator.service;

import com.trading.orchestrator.entity.RiskDecisionEntity;
import com.trading.orchestrator.entity.TradeOrderEntity;
import com.trading.orchestrator.model.DecisionAuditRecord;
import com.trading.orchestrator.model.ExecutionReport;
import com.trading.orchestrator.model.Order;
import com.trading.orchestrator.model.OrderStatus;
import com.trading.orchestrator.model.TradingEvent;
import com.trading.orchestrator.repository.IdempotencyClaimRepository;
import com.trading.orchestrator.repository.RiskDecisionRepository;
import com.trading.orchestrator.repository.TradeOrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
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
 * Backed by durable JPA relational stores with in-memory caching and
 * live Server-Sent Events (SSE) streaming via Project Reactor.
 */
@Service
public class AuditStorageService {

    private static final Logger log = LoggerFactory.getLogger(AuditStorageService.class);

    private final RiskDecisionRepository riskDecisionRepository;
    private final TradeOrderRepository tradeOrderRepository;
    private final IdempotencyClaimRepository idempotencyClaimRepository;

    // Fast in-memory cache: tickFingerprint -> DecisionAuditRecord
    private final Map<String, DecisionAuditRecord> fingerprintIndex = new ConcurrentHashMap<>(10_000);

    private final List<DecisionAuditRecord> decisionLog = new CopyOnWriteArrayList<>();
    private final List<Order> orderLog = new CopyOnWriteArrayList<>();
    private final List<ExecutionReport> executionLog = new CopyOnWriteArrayList<>();

    // Reactive multicast sink for live Server-Sent Events (SSE)
    private final Sinks.Many<TradingEvent> eventSink = Sinks.many().multicast().onBackpressureBuffer(1024, false);

    public AuditStorageService() {
        this.riskDecisionRepository = null;
        this.tradeOrderRepository = null;
        this.idempotencyClaimRepository = null;
    }

    @Autowired
    public AuditStorageService(
            RiskDecisionRepository riskDecisionRepository,
            TradeOrderRepository tradeOrderRepository,
            IdempotencyClaimRepository idempotencyClaimRepository) {
        this.riskDecisionRepository = riskDecisionRepository;
        this.tradeOrderRepository = tradeOrderRepository;
        this.idempotencyClaimRepository = idempotencyClaimRepository;
    }

    /**
     * Checks if a tick fingerprint has already been evaluated and durably recorded.
     */
    public boolean isFingerprintProcessed(String fingerprint) {
        if (fingerprintIndex.containsKey(fingerprint)) {
            return true;
        }
        if (idempotencyClaimRepository != null) {
            return idempotencyClaimRepository.findById(fingerprint)
                    .map(claim -> "COMPLETED".equalsIgnoreCase(claim.getStatus()))
                    .orElse(false);
        }
        return false;
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

    /**
     * Queries recent decisions from durable database or falls back to in-memory log.
     */
    public List<DecisionAuditRecord> getRecentDecisions(int limit) {
        if (riskDecisionRepository != null) {
            try {
                List<RiskDecisionEntity> entities = riskDecisionRepository.findAllByOrderByTimestampDesc(PageRequest.of(0, limit));
                if (!entities.isEmpty()) {
                    List<DecisionAuditRecord> records = new ArrayList<>(entities.size());
                    for (RiskDecisionEntity e : entities) {
                        records.add(new DecisionAuditRecord(
                                e.getRecordId(),
                                e.getTickFingerprint(),
                                e.getPortfolioId(),
                                e.getSymbol(),
                                e.getPrice(),
                                e.getProposedTradeSize(),
                                e.getValueAtRisk(),
                                e.getStatus(),
                                e.getReason(),
                                e.getLatencyMs(),
                                e.getTimestamp()
                        ));
                    }
                    return records;
                }
            } catch (Exception e) {
                log.warn("Database query for decisions failed, falling back to in-memory log: {}", e.getMessage());
            }
        }

        int size = decisionLog.size();
        int from = Math.max(0, size - limit);
        List<DecisionAuditRecord> subList = new ArrayList<>(decisionLog.subList(from, size));
        Collections.reverse(subList);
        return subList;
    }

    /**
     * Queries recent approved orders from durable database or falls back to in-memory log.
     */
    public List<Order> getRecentOrders(int limit) {
        if (tradeOrderRepository != null) {
            try {
                List<TradeOrderEntity> entities = tradeOrderRepository.findAllByOrderByTimestampDesc(PageRequest.of(0, limit));
                if (!entities.isEmpty()) {
                    List<Order> orders = new ArrayList<>(entities.size());
                    for (TradeOrderEntity e : entities) {
                        OrderStatus status = OrderStatus.APPROVED;
                        try {
                            status = OrderStatus.valueOf(e.getStatus());
                        } catch (Exception ignored) {}
                        orders.add(new Order(
                                e.getOrderId(),
                                e.getPortfolioId(),
                                e.getSymbol(),
                                e.getSide(),
                                e.getQuantity(),
                                e.getPrice(),
                                status,
                                e.getValueAtRisk(),
                                e.getTimestamp()
                        ));
                    }
                    return orders;
                }
            } catch (Exception e) {
                log.warn("Database query for orders failed, falling back to in-memory log: {}", e.getMessage());
            }
        }

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
