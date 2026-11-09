package com.trading.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orchestrator.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

/**
 * Event-Driven Order Execution Consumer that listens to the trade-orders topic.
 * Delegates fill execution to OrderExecutionTransactionService and commits
 * Kafka offsets strictly AFTER the database transaction commits successfully.
 */
@Service
public class OrderExecutionConsumerService {

    private static final Logger log = LoggerFactory.getLogger(OrderExecutionConsumerService.class);

    private final ObjectMapper objectMapper;
    private final OrderExecutionTransactionService orderExecutionTransactionService;

    @Autowired
    public OrderExecutionConsumerService(
            ObjectMapper objectMapper,
            OrderExecutionTransactionService orderExecutionTransactionService) {
        this.objectMapper = objectMapper;
        this.orderExecutionTransactionService = orderExecutionTransactionService;
    }

    @KafkaListener(
            topics = "${trading.kafka.trade-orders-topic:trade-orders}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onTradeOrder(String orderPayload, Acknowledgment ack) {
        try {
            Order order = objectMapper.readValue(orderPayload, Order.class);

            // Execute fill within transactional database boundary
            orderExecutionTransactionService.processOrderFillTransaction(order);

            // Acknowledge Kafka offset ONLY AFTER database transaction has successfully committed
            if (ack != null) {
                ack.acknowledge();
            }

        } catch (Exception e) {
            log.error("Failed to execute order payload {}: {}. Triggering Kafka redelivery.", orderPayload, e.getMessage(), e);
            throw (e instanceof RuntimeException re) ? re : new RuntimeException("Order execution failure", e);
        }
    }
}
