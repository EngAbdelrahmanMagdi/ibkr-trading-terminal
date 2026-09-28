package com.project.trading.order.domain;

import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.shared.domain.OrderType;
import com.project.trading.shared.domain.TimeInForce;
import com.project.trading.shared.domain.Decimals;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * The order aggregate. All state changes go through the lifecycle state machine ({@link OrderStatus}); an
 * invalid transition throws {@link InvalidTransitionException}. Filled quantity only increases. Each transition is
 * recorded as a {@link StatusChange} until the order is saved, so that every persisted change is published once.
 */
public final class Order {

    /** A status transition that has not been persisted yet. */
    public record StatusChange(OrderStatus previous, OrderStatus next, Instant at) {
    }

    /** Outcome of applying a broker update to the order. */
    public enum UpdateResult {
        APPLIED,
        /** A duplicate or out-of-date update (for example a cancellation after the order filled). */
        IGNORED
    }

    private final UUID id;
    private final String clientOrderId;
    private final String accountId;
    private final long conid;
    private final String symbol;
    private final OrderIntent intent;
    private final BrokerSide brokerSide;
    private final OrderType orderType;
    private final Quantity quantity;
    private final Price limitPrice;
    private final TimeInForce timeInForce;
    private final Instant createdAt;

    private String brokerOrderId;
    private Quantity filledQuantity;
    private Price averageFillPrice;
    private OrderStatus status;
    private String replyId;
    private String replyMessage;
    private int replyDepth;
    private String rejectionReason;
    private Instant submittedAt;
    private Instant updatedAt;
    private final long version;
    private final List<StatusChange> pendingChanges = new ArrayList<>();

    /** Rebuilds an order from persisted state (all fields). */
    public Order(UUID id, String clientOrderId, String brokerOrderId, String accountId, long conid, String symbol,
                 OrderIntent intent, BrokerSide brokerSide, OrderType orderType, Quantity quantity,
                 Quantity filledQuantity, Price limitPrice, Price averageFillPrice, TimeInForce timeInForce,
                 OrderStatus status, String replyId, String replyMessage, int replyDepth, String rejectionReason,
                 Instant createdAt, Instant submittedAt, Instant updatedAt, long version) {
        this.id = Objects.requireNonNull(id);
        this.clientOrderId = Objects.requireNonNull(clientOrderId);
        this.brokerOrderId = brokerOrderId;
        this.accountId = Objects.requireNonNull(accountId);
        this.conid = conid;
        this.symbol = Objects.requireNonNull(symbol);
        this.intent = Objects.requireNonNull(intent);
        this.brokerSide = Objects.requireNonNull(brokerSide);
        this.orderType = Objects.requireNonNull(orderType);
        this.quantity = Objects.requireNonNull(quantity);
        this.filledQuantity = Objects.requireNonNull(filledQuantity);
        this.limitPrice = limitPrice;
        this.averageFillPrice = averageFillPrice;
        this.timeInForce = Objects.requireNonNull(timeInForce);
        this.status = Objects.requireNonNull(status);
        this.replyId = replyId;
        this.replyMessage = replyMessage;
        this.replyDepth = replyDepth;
        this.rejectionReason = rejectionReason;
        this.createdAt = Objects.requireNonNull(createdAt);
        this.submittedAt = submittedAt;
        this.updatedAt = Objects.requireNonNull(updatedAt);
        this.version = version;
        if ((orderType == OrderType.LIMIT) != (limitPrice != null)) {
            throw new IllegalArgumentException("a limit price is required for LIMIT orders and forbidden otherwise");
        }
    }

    /** Creates a validated order intent in CREATED. */
    public static Order create(UUID id, String clientOrderId, String accountId, long conid, String symbol,
                               OrderIntent intent, OrderType orderType, Quantity quantity, Price limitPrice,
                               TimeInForce timeInForce, Instant now) {
        return new Order(id, clientOrderId, null, accountId, conid, symbol, intent, OrderPolicy.brokerSide(intent),
                orderType, quantity, Quantity.ZERO, limitPrice, null, timeInForce, OrderStatus.CREATED, null, null,
                0, null, now, null, now, 0);
    }

    private void moveTo(OrderStatus next, Instant now) {
        if (!status.canMoveTo(next)) {
            throw new InvalidTransitionException(status, next);
        }
        pendingChanges.add(new StatusChange(status, next, now));
        status = next;
        updatedAt = now;
    }

    /** The transitions since the order was loaded or last saved, oldest first; the list is cleared. */
    public List<StatusChange> drainStatusChanges() {
        List<StatusChange> drained = List.copyOf(pendingChanges);
        pendingChanges.clear();
        return drained;
    }

    public void markSubmissionPending(Instant now) {
        moveTo(OrderStatus.SUBMISSION_PENDING, now);
    }

    /** The broker accepted the order as working. */
    public void acknowledge(String brokerOrderId, Instant now) {
        moveTo(OrderStatus.SUBMITTED, now);
        this.brokerOrderId = Objects.requireNonNull(brokerOrderId);
        this.replyId = null;
        this.replyMessage = null;
        if (submittedAt == null) {
            submittedAt = now;
        }
    }

    /**
     * The broker requires a confirmation. Returns false (and rejects the order) when the chain would exceed
     * maxDepth.
     */
    public boolean requireConfirmation(String replyId, String message, int maxDepth, Instant now) {
        if (replyDepth + 1 > maxDepth) {
            reject("the broker required more confirmations than allowed", now);
            return false;
        }
        moveTo(OrderStatus.PENDING_CONFIRMATION, now);
        this.replyId = Objects.requireNonNull(replyId);
        this.replyMessage = Objects.requireNonNull(message);
        this.replyDepth++;
        return true;
    }

    public void reject(String reason, Instant now) {
        moveTo(OrderStatus.REJECTED, now);
        this.rejectionReason = truncate(reason);
        this.replyId = null;
        this.replyMessage = null;
    }

    /** A definite failure before the broker could have received the order. */
    public void fail(String reason, Instant now) {
        moveTo(OrderStatus.FAILED, now);
        this.rejectionReason = truncate(reason);
    }

    /** The outcome is uncertain (for example a timeout after sending); never resubmitted. */
    public void markUnknown(Instant now) {
        moveTo(OrderStatus.UNKNOWN, now);
    }

    public void requestCancel(Instant now) {
        moveTo(OrderStatus.CANCEL_PENDING, now);
    }

    /**
     * Applies a fill. Fills after a cancel request are honored. A fill on a terminal order is ignored; a fill
     * beyond the order quantity is rejected.
     */
    public UpdateResult applyFill(Quantity fillQuantity, Price fillPrice, Instant now) {
        if (status.isTerminal()) {
            return UpdateResult.IGNORED;
        }
        Quantity newFilled = filledQuantity.plus(fillQuantity);
        if (fillQuantity.isZero() || newFilled.compareTo(quantity) > 0) {
            throw new IllegalArgumentException("fill quantity exceeds the open quantity");
        }
        OrderStatus next = newFilled.compareTo(quantity) == 0 ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED;
        moveTo(next, now);
        BigDecimal previousNotional = averageFillPrice == null ? BigDecimal.ZERO
                : averageFillPrice.value().multiply(filledQuantity.value());
        BigDecimal notional = previousNotional.add(fillPrice.value().multiply(fillQuantity.value()));
        averageFillPrice = new Price(notional.divide(newFilled.value(), Decimals.PRICE_SCALE, Decimals.ROUNDING));
        filledQuantity = newFilled;
        return UpdateResult.APPLIED;
    }

    /** The broker confirmed the cancellation. Ignored when the order is already terminal. */
    public UpdateResult confirmCancelled(Instant now) {
        if (status.isTerminal()) {
            return UpdateResult.IGNORED;
        }
        moveTo(OrderStatus.CANCELLED, now);
        return UpdateResult.APPLIED;
    }

    /** The broker rejected a working order. Ignored when the order is already terminal. */
    public UpdateResult brokerRejected(String reason, Instant now) {
        if (status.isTerminal()) {
            return UpdateResult.IGNORED;
        }
        reject(reason, now);
        return UpdateResult.APPLIED;
    }

    private static String truncate(String reason) {
        if (reason == null) {
            return null;
        }
        return reason.length() <= 500 ? reason : reason.substring(0, 500);
    }

    public UUID id() {
        return id;
    }

    public String clientOrderId() {
        return clientOrderId;
    }

    public String brokerOrderId() {
        return brokerOrderId;
    }

    public String accountId() {
        return accountId;
    }

    public long conid() {
        return conid;
    }

    public String symbol() {
        return symbol;
    }

    public OrderIntent intent() {
        return intent;
    }

    public BrokerSide brokerSide() {
        return brokerSide;
    }

    public OrderType orderType() {
        return orderType;
    }

    public Quantity quantity() {
        return quantity;
    }

    public Quantity filledQuantity() {
        return filledQuantity;
    }

    public Price limitPrice() {
        return limitPrice;
    }

    public Price averageFillPrice() {
        return averageFillPrice;
    }

    public TimeInForce timeInForce() {
        return timeInForce;
    }

    public OrderStatus status() {
        return status;
    }

    public String replyId() {
        return replyId;
    }

    public String replyMessage() {
        return replyMessage;
    }

    public int replyDepth() {
        return replyDepth;
    }

    public String rejectionReason() {
        return rejectionReason;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant submittedAt() {
        return submittedAt;
    }

    public Instant updatedAt() {
        return updatedAt;
    }

    public long version() {
        return version;
    }
}
