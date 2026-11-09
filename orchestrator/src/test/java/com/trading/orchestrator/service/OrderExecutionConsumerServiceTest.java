package com.trading.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orchestrator.entity.ExecutionRecordEntity;
import com.trading.orchestrator.model.ExecutionReport;
import com.trading.orchestrator.model.Order;
import com.trading.orchestrator.model.OrderStatus;
import com.trading.orchestrator.repository.ExecutionRecordRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderExecutionConsumerServiceTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private PortfolioService portfolioService;

    @Mock
    private ExecutionRecordRepository executionRecordRepository;

    @Mock
    private Acknowledgment acknowledgment;

    private AuditStorageService auditStorageService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private OrderExecutionTransactionService transactionService;
    private OrderExecutionConsumerService consumerService;

    @BeforeEach
    void setUp() {
        lenient().when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        auditStorageService = new AuditStorageService();
        transactionService = new OrderExecutionTransactionService(
                objectMapper,
                kafkaTemplate,
                auditStorageService,
                portfolioService,
                executionRecordRepository,
                "trade-executions",
                3000L
        );
        consumerService = new OrderExecutionConsumerService(objectMapper, transactionService);
    }

    @Test
    @DisplayName("Should execute valid trade order and acknowledge Kafka only after transaction completes")
    void shouldExecuteOrderAndAcknowledge() throws Exception {
        Order order = new Order("ord-101", "PORT_1", "NVDA", "BUY", 100.0, 150.0, OrderStatus.APPROVED, 50.0, System.currentTimeMillis());
        String orderJson = objectMapper.writeValueAsString(order);

        when(executionRecordRepository.existsById("ord-101")).thenReturn(false);

        consumerService.onTradeOrder(orderJson, acknowledgment);

        verify(executionRecordRepository).saveAndFlush(any(ExecutionRecordEntity.class));
        verify(kafkaTemplate).send(eq("trade-executions"), eq("NVDA"), anyString());
        verify(portfolioService).applyExecution(any(ExecutionReport.class));
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("Should skip duplicate order execution without re-applying fill and acknowledge offset")
    void shouldSkipDuplicateOrderExecutionOnReplay() throws Exception {
        Order order = new Order("ord-102", "PORT_1", "NVDA", "BUY", 100.0, 150.0, OrderStatus.APPROVED, 50.0, System.currentTimeMillis());
        String orderJson = objectMapper.writeValueAsString(order);

        // Already executed
        when(executionRecordRepository.existsById("ord-102")).thenReturn(true);

        consumerService.onTradeOrder(orderJson, acknowledgment);

        // Verify fill was NOT applied again and NO new report published
        verify(executionRecordRepository, never()).saveAndFlush(any(ExecutionRecordEntity.class));
        verify(portfolioService, never()).applyExecution(any(ExecutionReport.class));
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());

        // Offset acknowledged so Kafka does not hang
        verify(acknowledgment).acknowledge();
    }
}
