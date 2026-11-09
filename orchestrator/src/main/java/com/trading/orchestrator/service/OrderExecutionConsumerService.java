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
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Event-Driven Order Execution Consumer that listens to the trade-orders topic.
 * Simulates downstream exchange/broker matching, produces execution fill reports,
 * enforces durable execution deduplication (keyed by orderId) preventing double fills
 * on outbox/Kafka replay, and updates real-time portfolio holdings and cash balance.
 */
@Service
public class OrderExecutionConsumerService {

    private static final Logger log = LoggerFactory.getLogger(OrderExecutionConsumerService.class);

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AuditStorageService auditStorageService;
    private final PortfolioService portfolioService;
    private final ExecutionRecordRepository executionRecordRepository;
    private final String executionsTopic;
    private final long publishTimeoutMs;

    public OrderExecutionConsumerService(
            ObjectMapper objectMapper,
            KafkaTemplate<String, String> kafkaTemplate,
            AuditStorageService auditStorageService,
            @Value("${trading.kafka.trade-executions-topic:trade-executions}") String executionsTopic,
            @Value("${trading.kafka.publish-timeout-ms:3000}") long publishTimeoutMs) {
        this(objectMapper, kafkaTemplate, auditStorageService, null, null, executionsTopic, publishTimeoutMs);
    }

    @Autowired
    public OrderExecutionConsumerService(
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

    @KafkaListener(
            topics = "${trading.kafka.trade-orders-topic:trade-orders}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void onTradeOrder(String orderPayload, Acknowledgment ack) {
        try {
            Order order = objectMapper.readValue(orderPayload, Order.class);

            // 1. Durable Execution Deduplication Guard
            // Prevents replayed/redelivered trade orders from executing double fills
            if (executionRecordRepository != null && executionRecordRepository.existsById(order.orderId())) {
                log.warn("[Duplicate Order Fill Skipped] OrderId {} has already been executed and filled. Skipping duplicate fill.",
                        order.orderId());
                if (ack != null) {
                    ack.acknowledge();
                }
                return;
            }

            // 2. Simulate realistic micro-slippage: +/- 0.02%
            double slippageMultiplier = 1.0 + (ThreadLocalRandom.current().nextDouble(-0.0002, 0.0002));
            double executionPrice = Math.round(order.price() * slippageMultiplier * 100.0) / 100.0;

            ExecutionReport report = ExecutionReport.createFilled(order, executionPrice);

            // 3. Durably persist execution record to prevent duplicate fills on future redeliveries
            if (executionRecordRepository != null) {
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
                executionRecordRepository.save(record);
            }

            // 4. Durably record execution report to Kafka
            String jsonReport = objectMapper.writeValueAsString(report);
            kafkaTemplate.send(executionsTopic, report.symbol(), jsonReport)
                    .get(publishTimeoutMs, TimeUnit.MILLISECONDS);

            auditStorageService.recordExecution(report);

            // 5. Update real-time portfolio cash and position holdings
            if (portfolioService != null) {
                portfolioService.applyExecution(report);
            }

            log.info("[Order FILLED] OrderId: {} | Symbol: {} | Qty: {} | ExecPrice: {:.2f} (Requested: {:.2f})",
                    order.orderId(), order.symbol(), order.quantity(), executionPrice, order.price());

            if (ack != null) {
                ack.acknowledge();
            }

        } catch (Exception e) {
            log.error("Failed to execute order payload {}: {}", orderPayload, e.getMessage(), e);
            throw new RuntimeException("Order execution failure", e);
        }
    }
}
