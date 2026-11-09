package com.trading.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orchestrator.entity.ExecutionRecordEntity;
import com.trading.orchestrator.model.ExecutionReport;
import com.trading.orchestrator.model.Order;
import com.trading.orchestrator.repository.ExecutionRecordRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Transactional boundary for order execution fill processing.
 * Guarantees:
 *  1. Atomic uniqueness enforcement on orderId preventing double fills under concurrent delivery.
 *  2. Durable execution record and portfolio state persistence committed together.
 *  3. Kafka offset acknowledgment occurs strictly AFTER transaction commit.
 */
@Service
public class OrderExecutionTransactionService {

    private static final Logger log = LoggerFactory.getLogger(OrderExecutionTransactionService.class);

    public enum ProcessOrderResult {
        FILLED,
        DUPLICATE_SKIPPED
    }

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AuditStorageService auditStorageService;
    private final PortfolioService portfolioService;
    private final ExecutionRecordRepository executionRecordRepository;
    private final String executionsTopic;
    private final long publishTimeoutMs;

    public OrderExecutionTransactionService(
            ObjectMapper objectMapper,
            KafkaTemplate<String, String> kafkaTemplate,
            AuditStorageService auditStorageService,
            @Value("${trading.kafka.trade-executions-topic:trade-executions}") String executionsTopic,
            @Value("${trading.kafka.publish-timeout-ms:3000}") long publishTimeoutMs) {
        this(objectMapper, kafkaTemplate, auditStorageService, null, null, executionsTopic, publishTimeoutMs);
    }

    @Autowired
    public OrderExecutionTransactionService(
            ObjectMapper objectMapper,
            KafkaTemplate<String, String> kafkaTemplate,
            AuditStorageService auditStorageService,
            PortfolioService portfolioService,
            ExecutionRecordRepository executionRecordRepository,
            @Value("${trading.kafka.trade-executions-topic:trade-executions}") String executionsTopic,
            @Value("${trading.kafka.publish-timeout-ms:3000}") long publishTimeoutMs) {
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.auditStorageService = auditStorageService;
        this.portfolioService = portfolioService;
        this.executionRecordRepository = executionRecordRepository;
        this.executionsTopic = executionsTopic;
        this.publishTimeoutMs = publishTimeoutMs;
    }

    /**
     * Executes the broker fill within an isolated database transaction.
     * Uses database primary key unique constraint for concurrency-safe deduplication.
     */
    @Transactional
    public ProcessOrderResult processOrderFillTransaction(Order order) {
        if (executionRecordRepository != null && executionRecordRepository.existsById(order.orderId())) {
            log.warn("[Duplicate Order Fill Skipped] OrderId {} already executed and filled.", order.orderId());
            return ProcessOrderResult.DUPLICATE_SKIPPED;
        }

        // Simulate realistic micro-slippage: +/- 0.02%
        double slippageMultiplier = 1.0 + (ThreadLocalRandom.current().nextDouble(-0.0002, 0.0002));
        double executionPrice = Math.round(order.price() * slippageMultiplier * 100.0) / 100.0;
        ExecutionReport report = ExecutionReport.createFilled(order, executionPrice);

        // Atomic insert enforcing uniqueness via primary key
        if (executionRecordRepository != null) {
            try {
                ExecutionRecordEntity record = new ExecutionRecordEntity(
                        order.orderId(),
                        UUID.randomUUID().toString(),
                        order.portfolioId(),
                        order.symbol(),
                        order.side(),
                        order.quantity(),
                        executionPrice,
                        System.currentTimeMillis()
                );
                executionRecordRepository.saveAndFlush(record);
            } catch (DataIntegrityViolationException e) {
                log.warn("[Duplicate Order Race Detected] OrderId {} claimed concurrently by another worker. Skipping.", order.orderId());
                return ProcessOrderResult.DUPLICATE_SKIPPED;
            }
        }

        try {
            // Durably publish execution report to Kafka
            String jsonReport = objectMapper.writeValueAsString(report);
            kafkaTemplate.send(executionsTopic, report.symbol(), jsonReport)
                    .get(publishTimeoutMs, TimeUnit.MILLISECONDS);

            auditStorageService.recordExecution(report);

            // Update real-time portfolio cash and position holdings
            if (portfolioService != null) {
                portfolioService.applyExecution(report);
            }

            log.info("[Order FILLED] OrderId: {} | Symbol: {} | Qty: {} | ExecPrice: {:.2f} (Requested: {:.2f})",
                    order.orderId(), order.symbol(), order.quantity(), executionPrice, order.price());

            return ProcessOrderResult.FILLED;

        } catch (Exception e) {
            log.error("Failed to publish or apply execution report for order {}: {}", order.orderId(), e.getMessage(), e);
            throw (e instanceof RuntimeException re) ? re : new RuntimeException("Execution error", e);
        }
    }
}
