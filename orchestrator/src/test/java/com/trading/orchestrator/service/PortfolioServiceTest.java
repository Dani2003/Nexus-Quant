package com.trading.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orchestrator.model.ExecutionReport;
import com.trading.orchestrator.model.MarketTick;
import com.trading.orchestrator.model.Order;
import com.trading.orchestrator.model.OrderStatus;
import com.trading.orchestrator.model.Portfolio;
import com.trading.orchestrator.model.Position;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class PortfolioServiceTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private AuditStorageService auditStorageService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private PortfolioService portfolioService;

    @BeforeEach
    void setUp() {
        lenient().when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        auditStorageService = new AuditStorageService();
        portfolioService = new PortfolioService(
                kafkaTemplate,
                objectMapper,
                auditStorageService,
                "trade-orders",
                3000L
        );
    }

    @Test
    @DisplayName("Should track position and cash on BUY execution fill")
    void shouldTrackPositionOnBuyFill() {
        Order order = new Order("ord-1", "PORT_1", "NVDA", "BUY", 100.0, 120.0, OrderStatus.APPROVED, 50.0, System.currentTimeMillis());
        ExecutionReport report = ExecutionReport.createFilled(order, 120.0);

        portfolioService.applyExecution(report);

        Portfolio portfolio = portfolioService.getPortfolio("PORT_1");
        assertNotNull(portfolio);
        // Initial cash was 1,000,000. Spent: 100 * 120 = 12,000 -> 988,000
        assertEquals(988_000.0, portfolio.cashBalance(), 0.01);
        Position pos = portfolio.positions().get("NVDA");
        assertNotNull(pos);
        assertEquals(100.0, pos.quantity());
        assertEquals(120.0, pos.averageEntryPrice());
    }

    @Test
    @DisplayName("Should update unrealized PnL on market tick")
    void shouldUpdateMarkToMarketOnTick() {
        Order order = new Order("ord-1", "PORT_1", "NVDA", "BUY", 100.0, 100.0, OrderStatus.APPROVED, 50.0, System.currentTimeMillis());
        portfolioService.applyExecution(ExecutionReport.createFilled(order, 100.0));

        // Market tick price rises to 110.0
        MarketTick tick = new MarketTick("PORT_1", "NVDA", 110.0, 50.0, System.currentTimeMillis());
        portfolioService.onMarketPriceTick(tick);

        Portfolio portfolio = portfolioService.getPortfolio("PORT_1");
        Position pos = portfolio.positions().get("NVDA");
        // Unrealized PnL: 100 * (110 - 100) = +1000.0
        assertEquals(1000.0, pos.unrealizedPnl(), 0.01);
        assertEquals(110.0, pos.currentMarketPrice());
    }

    @Test
    @DisplayName("Should trigger automated Stop-Loss protective exit order when price breaches threshold")
    void shouldTriggerStopLossOrder() {
        Order order = new Order("ord-1", "PORT_1", "TSLA", "BUY", 50.0, 200.0, OrderStatus.APPROVED, 50.0, System.currentTimeMillis());
        portfolioService.applyExecution(ExecutionReport.createFilled(order, 200.0));

        // Set stop loss at 190.0
        boolean ruleSet = portfolioService.setProtectionRules("PORT_1", "TSLA", 190.0, 230.0);
        assertTrue(ruleSet);

        // Market tick plunges to 188.0
        MarketTick plungeTick = new MarketTick("PORT_1", "TSLA", 188.0, 25.0, System.currentTimeMillis());
        portfolioService.onMarketPriceTick(plungeTick);

        // Verify protective SELL order was dispatched to trade-orders
        verify(kafkaTemplate).send(eq("trade-orders"), eq("TSLA"), contains("\"side\":\"SELL\""));
    }
}

