package com.project.trading.order.domain;

import com.project.trading.shared.domain.OrderType;
import com.project.trading.shared.domain.TimeInForce;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderTest {

    private static final Instant T0 = Instant.parse("2026-09-28T10:00:00Z");

    /** The lifecycle table, written out independently of the implementation. */
    private static final Map<OrderStatus, Set<OrderStatus>> EXPECTED = Map.of(
            OrderStatus.CREATED, EnumSet.of(OrderStatus.SUBMISSION_PENDING),
            OrderStatus.SUBMISSION_PENDING, EnumSet.of(OrderStatus.SUBMITTED, OrderStatus.PENDING_CONFIRMATION,
                    OrderStatus.REJECTED, OrderStatus.FAILED, OrderStatus.UNKNOWN),
            OrderStatus.PENDING_CONFIRMATION, EnumSet.of(OrderStatus.SUBMITTED, OrderStatus.PENDING_CONFIRMATION,
                    OrderStatus.REJECTED, OrderStatus.UNKNOWN),
            OrderStatus.SUBMITTED, EnumSet.of(OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED,
                    OrderStatus.CANCEL_PENDING, OrderStatus.CANCELLED, OrderStatus.REJECTED),
            OrderStatus.PARTIALLY_FILLED, EnumSet.of(OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED,
                    OrderStatus.CANCEL_PENDING, OrderStatus.CANCELLED),
            OrderStatus.CANCEL_PENDING, EnumSet.of(OrderStatus.CANCELLED, OrderStatus.PARTIALLY_FILLED,
                    OrderStatus.FILLED),
            OrderStatus.UNKNOWN, EnumSet.of(OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED,
                    OrderStatus.CANCEL_PENDING, OrderStatus.CANCELLED, OrderStatus.REJECTED));

    static Order limitBuy(String quantity, String limit) {
        return Order.create(UUID.randomUUID(), "TC-1", "ACC", 1, "NVDA", OrderIntent.BUY, OrderType.LIMIT,
                Quantity.of(quantity), Price.of(limit), TimeInForce.DAY, T0);
    }

    static Order submitted(String quantity) {
        Order order = limitBuy(quantity, "100.00");
        order.markSubmissionPending(T0);
        order.acknowledge("B-1", T0);
        return order;
    }

    @Test
    void onlyTheLifecycleTableTransitionsAreAllowed() {
        for (OrderStatus from : OrderStatus.values()) {
            for (OrderStatus to : OrderStatus.values()) {
                boolean expected = EXPECTED.getOrDefault(from, Set.of()).contains(to);
                assertThat(from.canMoveTo(to)).as(from + " -> " + to).isEqualTo(expected);
            }
        }
    }

    @Test
    void terminalStatusesHaveNoExits() {
        for (OrderStatus s : EnumSet.of(OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.FAILED)) {
            assertThat(s.isTerminal()).isTrue();
            assertThat(EnumSet.allOf(OrderStatus.class)).noneMatch(s::canMoveTo);
        }
    }

    @Test
    void invalidTransitionIsRefused() {
        Order order = limitBuy("10", "100.00");
        assertThatThrownBy(() -> order.acknowledge("B-1", T0)).isInstanceOf(InvalidTransitionException.class);
        assertThat(order.status()).isEqualTo(OrderStatus.CREATED);
    }

    @Test
    void limitPriceMustMatchOrderType() {
        assertThatThrownBy(() -> Order.create(UUID.randomUUID(), "TC-1", "ACC", 1, "NVDA", OrderIntent.BUY,
                OrderType.MARKET, Quantity.of("1"), Price.of("1"), TimeInForce.DAY, T0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Order.create(UUID.randomUUID(), "TC-1", "ACC", 1, "NVDA", OrderIntent.BUY,
                OrderType.LIMIT, Quantity.of("1"), null, TimeInForce.DAY, T0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fillsAccumulateWithAWeightedAveragePrice() {
        Order order = submitted("10");
        assertThat(order.applyFill(Quantity.of("4"), Price.of("100.00"), T0)).isEqualTo(Order.UpdateResult.APPLIED);
        assertThat(order.status()).isEqualTo(OrderStatus.PARTIALLY_FILLED);
        order.applyFill(Quantity.of("6"), Price.of("101.00"), T0);
        assertThat(order.status()).isEqualTo(OrderStatus.FILLED);
        assertThat(order.filledQuantity()).isEqualTo(Quantity.of("10"));
        assertThat(order.averageFillPrice()).isEqualTo(Price.of("100.60"));
    }

    @Test
    void lateFillAfterCancelRequestIsHonoredAndLaterCancelIsIgnored() {
        Order order = submitted("10");
        order.requestCancel(T0);
        assertThat(order.applyFill(Quantity.of("10"), Price.of("99.00"), T0)).isEqualTo(Order.UpdateResult.APPLIED);
        assertThat(order.status()).isEqualTo(OrderStatus.FILLED);
        assertThat(order.confirmCancelled(T0)).isEqualTo(Order.UpdateResult.IGNORED);
        assertThat(order.status()).isEqualTo(OrderStatus.FILLED);
    }

    @Test
    void fillOnTerminalOrderIsIgnoredAndOverfillIsRefused() {
        Order order = submitted("10");
        order.applyFill(Quantity.of("10"), Price.of("100.00"), T0);
        assertThat(order.applyFill(Quantity.of("1"), Price.of("100.00"), T0)).isEqualTo(Order.UpdateResult.IGNORED);
        assertThat(order.filledQuantity()).isEqualTo(Quantity.of("10"));

        Order other = submitted("10");
        assertThatThrownBy(() -> other.applyFill(Quantity.of("11"), Price.of("100.00"), T0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(other.filledQuantity()).isEqualTo(Quantity.ZERO);
    }

    @Test
    void confirmationChainIsBounded() {
        Order order = limitBuy("10", "100.00");
        order.markSubmissionPending(T0);
        assertThat(order.requireConfirmation("R1", "first", 2, T0)).isTrue();
        assertThat(order.requireConfirmation("R2", "second", 2, T0)).isTrue();
        assertThat(order.replyDepth()).isEqualTo(2);
        assertThat(order.requireConfirmation("R3", "third", 2, T0)).isFalse();
        assertThat(order.status()).isEqualTo(OrderStatus.REJECTED);
        assertThat(order.replyId()).isNull();
    }

    @Test
    void rejectionReasonIsBounded() {
        Order order = limitBuy("10", "100.00");
        order.markSubmissionPending(T0);
        order.reject("x".repeat(900), T0);
        assertThat(order.rejectionReason()).hasSize(500);
    }
}
