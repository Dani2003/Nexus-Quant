package com.trading.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orchestrator.model.ExecutionReport;
import com.trading.orchestrator.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Event-Driven Order Execution Consumer that listens to the trade-orders topic.
 * Simulates downstream exchange/broker matching and produces execution fill reports.
 */
@Service
public class OrderExecutionConsumerService {

    private static final Logger log = LoggerFactory.getLogger(OrderExecutionConsumerService.class);

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AuditStorageService auditStorageService;
    private final String executionsTopic;
    private final long publishTimeoutMs;

    public OrderExecutionConsumerService(
            ObjectMapper objectMapper,
            KafkaTemplate<String, String> kafkaTemplate,
            AuditStorageService auditStorageService,
            @Value("${trading.kafka.trade-executions-topic:trade-executions}") String executionsTopic,
            @Value("${trading.kafka.publish-timeout-ms:3000}") long publishTimeoutMs) {
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.auditStorageService = auditStorageService;
        this.executionsTopic = executionsTopic;
        this.publishTimeoutMs = publishTimeoutMs;
    }

    @KafkaListener(
            topics = "${trading.kafka.trade-orders-topic:trade-orders}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onTradeOrder(String orderPayload, Acknowledgment ack) {
        try {
            Order order = objectMapper.readValue(orderPayload, Order.class);

            // Simulate realistic micro-slippage: +/- 0.02%
            double slippageMultiplier = 1.0 + (ThreadLocalRandom.current().nextDouble(-0.0002, 0.0002));
            double executionPrice = Math.round(order.price() * slippageMultiplier * 100.0) / 100.0;

            ExecutionReport report = ExecutionReport.createFilled(order, executionPrice);

            // Durably record execution report
            String jsonReport = objectMapper.writeValueAsString(report);
            kafkaTemplate.send(executionsTopic, report.symbol(), jsonReport)
                    .get(publishTimeoutMs, TimeUnit.MILLISECONDS);

            auditStorageService.recordExecution(report);

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

