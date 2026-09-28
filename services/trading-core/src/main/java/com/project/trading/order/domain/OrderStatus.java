package com.project.trading.order.domain;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Persisted order lifecycle statuses and the allowed transitions between them. VALIDATING is not persisted:
 * validation happens before an order exists. UNKNOWN is resolved only from broker truth (reconciliation).
 */
public enum OrderStatus {
    CREATED,
    SUBMISSION_PENDING,
    PENDING_CONFIRMATION,
    SUBMITTED,
    PARTIALLY_FILLED,
    FILLED,
    CANCEL_PENDING,
    CANCELLED,
    REJECTED,
    FAILED,
    UNKNOWN;

    private static final Map<OrderStatus, Set<OrderStatus>> ALLOWED = Map.of(
            CREATED, EnumSet.of(SUBMISSION_PENDING),
            SUBMISSION_PENDING, EnumSet.of(SUBMITTED, PENDING_CONFIRMATION, REJECTED, FAILED, UNKNOWN),
            PENDING_CONFIRMATION, EnumSet.of(SUBMITTED, PENDING_CONFIRMATION, REJECTED, UNKNOWN),
            SUBMITTED, EnumSet.of(PARTIALLY_FILLED, FILLED, CANCEL_PENDING, CANCELLED, REJECTED),
            PARTIALLY_FILLED, EnumSet.of(PARTIALLY_FILLED, FILLED, CANCEL_PENDING, CANCELLED),
            CANCEL_PENDING, EnumSet.of(CANCELLED, PARTIALLY_FILLED, FILLED),
            UNKNOWN, EnumSet.of(SUBMITTED, PARTIALLY_FILLED, FILLED, CANCEL_PENDING, CANCELLED, REJECTED));

    /** Reports whether moving from this status to next is allowed. */
    public boolean canMoveTo(OrderStatus next) {
        return ALLOWED.getOrDefault(this, Set.of()).contains(next);
    }

    public boolean isTerminal() {
        return this == FILLED || this == CANCELLED || this == REJECTED || this == FAILED;
    }

    /** Open at the broker: fills or a cancellation may still arrive. */
    public boolean isWorking() {
        return this == SUBMITTED || this == PARTIALLY_FILLED || this == CANCEL_PENDING;
    }
}
