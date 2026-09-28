package com.project.trading.order.application;

import com.project.trading.broker.domain.BrokerConnectionState;
import com.project.trading.broker.domain.BrokerOrderUpdate;
import com.project.trading.broker.domain.BrokerOrderUpdateHandler;
import com.project.trading.broker.domain.BrokerTradingPort;
import com.project.trading.broker.domain.CancelResult;
import com.project.trading.broker.domain.SubmitResult;
import com.project.trading.execution.application.ExecutionLedger;
import com.project.trading.instrument.domain.Instrument;
import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.order.domain.Order;
import com.project.trading.order.domain.OrderIntent;
import com.project.trading.order.domain.OrderStatus;
import com.project.trading.shared.domain.OrderType;
import com.project.trading.shared.domain.TimeInForce;
import com.project.trading.position.application.PositionLedger;
import com.project.trading.shared.config.AppProperties;
import com.project.trading.shared.domain.DomainException;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import com.project.trading.support.InMemoryIdempotencyStore;
import com.project.trading.outbox.application.OutboxAppender;
import com.project.trading.support.InMemoryOrderRepository;
import com.project.trading.support.MutableClock;
import com.project.trading.support.NoopTransactionManager;
import com.project.trading.support.TestProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Order use cases with in-memory storage and a scripted broker. */
class OrderWorkflowTest {

    private static final Instrument NVDA = new Instrument("NVDA", 1, "NVIDIA", "MOCK", "USD", "STK", 2);

    private final MutableClock clock = new MutableClock(Instant.parse("2026-09-28T10:00:00Z"));
    private final InMemoryOrderRepository orders = new InMemoryOrderRepository();
    private final BrokerTradingPort broker = mock(BrokerTradingPort.class);
    private final OrderValidator validator = mock(OrderValidator.class);
    private final ExecutionLedger executions = mock(ExecutionLedger.class);
    private final PositionLedger positions = mock(PositionLedger.class);

    private OrderUpdateService updates;
    private PlaceOrderService place;
    private ConfirmOrderService confirm;
    private CancelOrderService cancel;
    private ConfirmationExpiry expiry;

    @BeforeEach
    void setUp() {
        build(3);
        when(broker.connectionState()).thenReturn(BrokerConnectionState.READY);
        when(validator.validate(any())).thenAnswer(inv -> {
            PlaceOrderCommand c = inv.getArgument(0);
            return new OrderValidator.ValidatedOrder(NVDA, c.intent(), c.orderType(), new Quantity(c.quantity()),
                    c.limitPrice() == null ? null : new Price(c.limitPrice()), c);
        });
    }

    private void build(int maxReplyDepth) {
        AppProperties props = TestProperties.app(maxReplyDepth);
        NoopTransactionManager tm = new NoopTransactionManager();
        OrderMetrics metrics = new OrderMetrics(new SimpleMeterRegistry(), orders);
        updates = new OrderUpdateService(orders, executions, positions, metrics, tm, new OrderEventFactory(),
                new OutboxAppender((message, at) -> { }, clock));
        SubmissionOutcomes outcomes = new SubmissionOutcomes(orders, updates, metrics, tm, clock, props);
        IdempotencyService idempotency = new IdempotencyService(new InMemoryIdempotencyStore(),
                JsonMapper.builder().build(), clock, props);
        place = new PlaceOrderService(idempotency, validator, orders, broker, outcomes, metrics, tm, clock, props);
        expiry = new ConfirmationExpiry(orders, tm, clock, props);
        confirm = new ConfirmOrderService(orders, broker, outcomes, expiry, clock);
        cancel = new CancelOrderService(orders, broker, tm, clock);
    }

    private static PlaceOrderCommand limitBuy(String qty) {
        return new PlaceOrderCommand("NVDA", OrderIntent.BUY, OrderType.LIMIT, new BigDecimal(qty),
                new BigDecimal("100.00"), TimeInForce.DAY);
    }

    private static BrokerOrderUpdate.Fill fill(String brokerOrderId, String executionId, String qty) {
        return new BrokerOrderUpdate.Fill(brokerOrderId, executionId, BrokerSide.BUY, Quantity.of(qty),
                Price.of("99.50"), BigDecimal.ONE, "USD", Instant.parse("2026-09-28T10:00:01Z"));
    }

    @Test
    void retryWithTheSameKeyReplaysTheOutcomeWithoutASecondSubmission() {
        when(broker.submit(any())).thenReturn(new SubmitResult.Accepted("B-1", List.of()));
        UUID key = UUID.randomUUID();

        PlaceOrderService.Placed first = place.place(key, limitBuy("10"));
        PlaceOrderService.Placed retry = place.place(key, limitBuy("10.00"));

        assertThat(first.status()).isEqualTo(201);
        assertThat(first.order().status()).isEqualTo(OrderStatus.SUBMITTED);
        assertThat(retry.status()).isEqualTo(201);
        assertThat(retry.order().id()).isEqualTo(first.order().id());
        verify(broker, times(1)).submit(any());
    }

    @Test
    void reusingAKeyForADifferentRequestIsAConflict() {
        when(broker.submit(any())).thenReturn(new SubmitResult.Accepted("B-1", List.of()));
        UUID key = UUID.randomUUID();
        place.place(key, limitBuy("10"));

        assertThatThrownBy(() -> place.place(key, limitBuy("11")))
                .isInstanceOf(DomainException.class).extracting("status").isEqualTo(409);
        verify(broker, times(1)).submit(any());
    }

    @Test
    void aValidationRejectionIsStoredAndReplayed() {
        doThrow(DomainException.invalidField("quantity", "too large")).when(validator).validate(any());
        UUID key = UUID.randomUUID();

        assertThatThrownBy(() -> place.place(key, limitBuy("10"))).isInstanceOf(DomainException.class);
        assertThatThrownBy(() -> place.place(key, limitBuy("10")))
                .isInstanceOf(DomainException.class)
                .satisfies(e -> {
                    DomainException d = (DomainException) e;
                    assertThat(d.status()).isEqualTo(422);
                    assertThat(d.fieldErrors()).extracting("field").containsExactly("quantity");
                });
        verify(validator, times(1)).validate(any());
        verify(broker, never()).submit(any());
    }

    @Test
    void aBrokerFailureBecomesUnknownAndIsNeverResubmitted() {
        when(broker.submit(any())).thenThrow(new IllegalStateException("socket timeout"));
        UUID key = UUID.randomUUID();

        PlaceOrderService.Placed placed = place.place(key, limitBuy("10"));
        PlaceOrderService.Placed retry = place.place(key, limitBuy("10"));

        assertThat(placed.status()).isEqualTo(202);
        assertThat(placed.order().status()).isEqualTo(OrderStatus.UNKNOWN);
        assertThat(retry.status()).isEqualTo(202);
        assertThat(retry.order().id()).isEqualTo(placed.order().id());
        verify(broker, times(1)).submit(any());
    }

    @Test
    void aChainedConfirmationEndsWithTheOrderSubmittedAndImmediatelyFilled() {
        when(broker.submit(any())).thenReturn(new SubmitResult.ConfirmationRequired("R1", "value above 50000"));
        when(broker.confirmReply("R1", true)).thenReturn(new SubmitResult.ConfirmationRequired("R2", "large order"));
        when(broker.confirmReply("R2", true)).thenReturn(new SubmitResult.Accepted("B-7", List.of(fill("B-7", "E-1", "10"))));
        when(executions.isRecorded(anyString())).thenReturn(false);

        PlaceOrderService.Placed placed = place.place(UUID.randomUUID(), limitBuy("10"));
        assertThat(placed.status()).isEqualTo(202);
        assertThat(placed.order().status()).isEqualTo(OrderStatus.PENDING_CONFIRMATION);
        assertThat(placed.order().replyMessage()).isEqualTo("value above 50000");

        Order afterFirst = confirm.confirm(placed.order().id(), true);
        assertThat(afterFirst.status()).isEqualTo(OrderStatus.PENDING_CONFIRMATION);
        assertThat(afterFirst.replyDepth()).isEqualTo(2);

        Order filled = confirm.confirm(placed.order().id(), true);
        assertThat(filled.status()).isEqualTo(OrderStatus.FILLED);
        assertThat(filled.averageFillPrice()).isEqualTo(Price.of("99.50"));
        verify(positions).applyFill("NVDA", "USD", BrokerSide.BUY, Quantity.of("10"), Price.of("99.50"),
                Instant.parse("2026-09-28T10:00:01Z"));
    }

    @Test
    void anExpiredConfirmationIsRejectedLocallyAndTheBrokerIsNotCalled() {
        when(broker.submit(any())).thenReturn(new SubmitResult.ConfirmationRequired("R1", "confirm?"));
        Order order = place.place(UUID.randomUUID(), limitBuy("10")).order();

        clock.advance(Duration.ofSeconds(31));

        assertThatThrownBy(() -> confirm.confirm(order.id(), true))
                .isInstanceOf(DomainException.class).extracting("status").isEqualTo(409);
        assertThat(orders.findById(order.id()).orElseThrow().status()).isEqualTo(OrderStatus.REJECTED);
        verify(broker, never()).confirmReply(anyString(), anyBoolean());
    }

    @Test
    void theBackgroundSweepWaitsForTheGracePeriodSoItNeverRacesAnInFlightAnswer() {
        when(broker.submit(any())).thenReturn(new SubmitResult.ConfirmationRequired("R1", "confirm?"));
        Order order = place.place(UUID.randomUUID(), limitBuy("10")).order();

        clock.advance(Duration.ofSeconds(59));
        expiry.sweep();
        assertThat(orders.findById(order.id()).orElseThrow().status()).isEqualTo(OrderStatus.PENDING_CONFIRMATION);

        clock.advance(Duration.ofSeconds(2));
        expiry.sweep();
        Order expired = orders.findById(order.id()).orElseThrow();
        assertThat(expired.status()).isEqualTo(OrderStatus.REJECTED);
        assertThat(expired.rejectionReason()).isEqualTo(ConfirmationExpiry.REASON);
    }

    @Test
    void declineRejectsTheOrderAndConfirmingANonPendingOrderIsAConflict() {
        when(broker.submit(any())).thenReturn(new SubmitResult.ConfirmationRequired("R1", "confirm?"));
        when(broker.confirmReply("R1", false)).thenReturn(new SubmitResult.Rejected("declined by the user"));

        Order order = place.place(UUID.randomUUID(), limitBuy("10")).order();
        Order declined = confirm.confirm(order.id(), false);

        assertThat(declined.status()).isEqualTo(OrderStatus.REJECTED);
        assertThatThrownBy(() -> confirm.confirm(order.id(), true))
                .isInstanceOf(DomainException.class).extracting("status").isEqualTo(409);
        verify(broker, times(1)).confirmReply(anyString(), anyBoolean());
    }

    @Test
    void exceedingTheConfirmationDepthRejectsTheOrderLocally() {
        build(1);
        when(broker.submit(any())).thenReturn(new SubmitResult.ConfirmationRequired("R1", "first"));
        when(broker.confirmReply("R1", true)).thenReturn(new SubmitResult.ConfirmationRequired("R2", "second"));

        Order order = place.place(UUID.randomUUID(), limitBuy("10")).order();
        Order result = confirm.confirm(order.id(), true);

        assertThat(result.status()).isEqualTo(OrderStatus.REJECTED);
        assertThat(result.rejectionReason()).contains("more confirmations");
    }

    @Test
    void aFillArrivingWhileCancelIsPendingWinsAndTheLaterCancelIsIgnored() {
        when(broker.submit(any())).thenReturn(new SubmitResult.Accepted("B-9", List.of()));
        when(broker.cancel("B-9")).thenReturn(new CancelResult.Requested());
        when(executions.isRecorded("E-9")).thenReturn(false, true);

        Order order = place.place(UUID.randomUUID(), limitBuy("10")).order();
        assertThat(cancel.cancel(order.id()).status()).isEqualTo(OrderStatus.CANCEL_PENDING);

        assertThat(updates.handle(fill("B-9", "E-9", "10"))).isEqualTo(BrokerOrderUpdateHandler.Outcome.APPLIED);
        assertThat(updates.handle(fill("B-9", "E-9", "10"))).isEqualTo(BrokerOrderUpdateHandler.Outcome.IGNORED);
        assertThat(updates.handle(new BrokerOrderUpdate.Cancelled("B-9", Instant.parse("2026-09-28T10:00:02Z"))))
                .isEqualTo(BrokerOrderUpdateHandler.Outcome.IGNORED);

        Order finalState = orders.findById(order.id()).orElseThrow();
        assertThat(finalState.status()).isEqualTo(OrderStatus.FILLED);
        assertThat(finalState.filledQuantity()).isEqualTo(Quantity.of("10"));
        verify(executions, times(1)).record(any());
        assertThatThrownBy(() -> cancel.cancel(order.id()))
                .isInstanceOf(DomainException.class).extracting("status").isEqualTo(409);
    }

    @Test
    void updatesForOrdersNotYetAcknowledgedLocallyAreNotReady() {
        when(broker.submit(any())).thenReturn(new SubmitResult.ConfirmationRequired("R1", "confirm?"));
        place.place(UUID.randomUUID(), limitBuy("10"));
        assertThat(updates.handle(fill("B-unknown", "E-1", "1"))).isEqualTo(BrokerOrderUpdateHandler.Outcome.NOT_READY);
        verify(executions, never()).record(any());
    }
}
