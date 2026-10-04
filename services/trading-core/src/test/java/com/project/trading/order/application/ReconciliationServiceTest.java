package com.project.trading.order.application;

import com.project.trading.broker.domain.BrokerConnectionState;
import com.project.trading.broker.domain.BrokerTradingPort;
import com.project.trading.broker.domain.BrokerTruthPort;
import com.project.trading.broker.domain.ObservedStatus;
import com.project.trading.execution.application.ExecutionLedger;
import com.project.trading.instrument.application.InstrumentService;
import com.project.trading.instrument.domain.Instrument;
import com.project.trading.order.domain.Order;
import com.project.trading.order.domain.OrderIntent;
import com.project.trading.order.domain.OrderStatus;
import com.project.trading.outbox.application.OutboxAppender;
import com.project.trading.position.application.PositionLedger;
import com.project.trading.position.domain.Position;
import com.project.trading.shared.domain.OrderType;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import com.project.trading.shared.domain.TimeInForce;
import com.project.trading.support.InMemoryOrderRepository;
import com.project.trading.support.MutableClock;
import com.project.trading.support.NoopTransactionManager;
import com.project.trading.support.TestProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Reconciliation against a scripted broker view, through the real update path. */
class ReconciliationServiceTest {

    private static final Instrument NVDA = new Instrument("NVDA", 4815747, "NVIDIA", "NASDAQ", "USD", "STK", 2);

    private final MutableClock clock = new MutableClock(Instant.parse("2026-09-28T10:00:00Z"));
    private final InMemoryOrderRepository orders = new InMemoryOrderRepository();
    private final BrokerTradingPort broker = mock(BrokerTradingPort.class);
    private final ExecutionLedger executions = mock(ExecutionLedger.class);
    private final PositionLedger positions = mock(PositionLedger.class);
    private final InstrumentService instruments = mock(InstrumentService.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final List<BrokerTruthPort.OrderView> brokerOrders = new ArrayList<>();
    private final List<BrokerTruthPort.ExecutionView> brokerExecutions = new ArrayList<>();
    private Map<Long, BigDecimal> brokerPositions = Map.of();

    private ReconciliationService reconciliation;

    @BeforeEach
    void setUp() {
        when(broker.connectionState()).thenReturn(BrokerConnectionState.READY);
        when(instruments.resolve("NVDA")).thenReturn(NVDA);
        NoopTransactionManager tm = new NoopTransactionManager();
        DriftReporter drift = new DriftReporter(registry);
        OrderUpdateService updates = new OrderUpdateService(orders, executions, positions,
                new OrderMetrics(new SimpleMeterRegistry(), orders), tm, new OrderEventFactory(),
                new OutboxAppender((message, at) -> { }, clock), drift, clock, registry);
        BrokerTruthPort truth = () -> Optional.of(new BrokerTruthPort.Snapshot(List.copyOf(brokerOrders),
                List.copyOf(brokerExecutions), brokerPositions, clock.instant()));
        reconciliation = new ReconciliationService(broker, truth, updates, orders, executions, positions, instruments,
                drift, clock, TestProperties.app(3), new ReconciliationSettings(Duration.ofSeconds(30),
                Duration.ofSeconds(10), Duration.ofSeconds(10), Duration.ofMinutes(15)), registry);
    }

    private Order order(String ref, OrderStatus status) {
        Order order = Order.create(UUID.randomUUID(), ref, "DU1", NVDA.conid(), "NVDA", OrderIntent.BUY, OrderType.LIMIT,
                Quantity.of("10"), Price.of("100.00"), TimeInForce.DAY, clock.instant());
        order.markSubmissionPending(clock.instant());
        if (status == OrderStatus.SUBMITTED) {
            order.acknowledge("B-" + ref, clock.instant());
        } else if (status == OrderStatus.UNKNOWN) {
            order.markUnknown(clock.instant());
        }
        return orders.save(order);
    }

    private double drift(String entity) {
        var counter = registry.find("reconciliation.drift").tag("entity", entity).counter();
        return counter == null ? 0 : counter.count();
    }

    private double gauge(String name) {
        return registry.get(name).gauge().value();
    }

    @Test
    void interruptedSubmissionRequiresPositiveBrokerTruthAndNeverResubmits() {
        Order order = order("interrupted", OrderStatus.SUBMISSION_PENDING);
        reconciliation.run();
        assertThat(orders.findById(order.id()).orElseThrow().status()).isEqualTo(OrderStatus.SUBMISSION_PENDING);
        verify(executions, never()).record(any());
        brokerOrders.add(new BrokerTruthPort.OrderView("B-interrupted", "interrupted", ObservedStatus.FILLED,
                Quantity.of("10"), "Filled"));
        brokerExecutions.add(new BrokerTruthPort.ExecutionView("E-interrupted", null, "interrupted", Quantity.of("10"),
                Price.of("99.50"), BigDecimal.ONE, clock.instant()));
        reconciliation.run();
        assertThat(orders.findById(order.id()).orElseThrow().status()).isEqualTo(OrderStatus.FILLED);
        verify(executions).record(any());
        verify(broker, never()).submit(any());
    }

    @Test
    void aMissedExecutionIsAppliedAndARemainingPositionDifferenceIsOnlyReported() {
        Order order = order("r1", OrderStatus.SUBMITTED);
        brokerOrders.add(new BrokerTruthPort.OrderView("B-r1", "r1", ObservedStatus.FILLED, Quantity.of("10"), "Filled"));
        brokerExecutions.add(new BrokerTruthPort.ExecutionView("E-1", null, "r1", Quantity.of("10"), Price.of("99.50"),
                BigDecimal.ONE, clock.instant()));
        brokerPositions = Map.of(NVDA.conid(), new BigDecimal("12"));
        when(positions.all()).thenReturn(List.of(new Position("NVDA", new BigDecimal("10"), new BigDecimal("99.50"),
                BigDecimal.ZERO, "USD")));

        ReconciliationService.Result result = reconciliation.run();

        assertThat(result.applied()).isEqualTo(1);
        Order filled = orders.findById(order.id()).orElseThrow();
        assertThat(filled.status()).isEqualTo(OrderStatus.FILLED);
        assertThat(filled.filledQuantity()).isEqualTo(Quantity.of("10"));
        verify(executions).record(any());
        verify(positions).applyFill(eq("NVDA"), eq("USD"), any(), eq(Quantity.of("10")), eq(Price.of("99.50")), any());
        assertThat(drift("position")).as("reported once, never overwritten").isEqualTo(1);
        when(executions.isRecorded("E-1")).thenReturn(true);
        reconciliation.run();
        assertThat(drift("position")).isEqualTo(1);
    }

    @Test
    void anUnknownOrderStaysUnknownAndIsReportedOnlyAfterTheReadyTimeWindow() {
        Order order = order("r2", OrderStatus.UNKNOWN);
        reconciliation.run();

        clock.advance(Duration.ofMinutes(10));
        reconciliation.run();
        when(broker.connectionState()).thenReturn(BrokerConnectionState.UNAVAILABLE);
        clock.advance(Duration.ofMinutes(30));
        reconciliation.run();
        when(broker.connectionState()).thenReturn(BrokerConnectionState.READY);
        reconciliation.run();
        assertThat(gauge("orders.unknown.unresolved")).as("time without a ready session does not count").isZero();

        clock.advance(Duration.ofMinutes(5));
        reconciliation.run();
        assertThat(gauge("orders.unknown.unresolved")).isEqualTo(1);
        assertThat(orders.findById(order.id()).orElseThrow().status()).isEqualTo(OrderStatus.UNKNOWN);
        verify(executions, never()).record(any());
    }

    @Test
    void externalOrdersAndExecutionsAreCountedOnceAndNotImported() {
        brokerOrders.add(new BrokerTruthPort.OrderView("X-1", null, ObservedStatus.WORKING, Quantity.ZERO, "Submitted"));
        brokerExecutions.add(new BrokerTruthPort.ExecutionView("X-E1", "X-1", "tws-order", Quantity.of("1"),
                Price.of("99.00"), null, clock.instant()));

        assertThat(reconciliation.run().external()).isEqualTo(2);
        assertThat(reconciliation.run().external()).isZero();
        assertThat(registry.get("reconciliation.external").counter().count()).isEqualTo(2);
        verify(executions, never()).record(any());
    }
}
