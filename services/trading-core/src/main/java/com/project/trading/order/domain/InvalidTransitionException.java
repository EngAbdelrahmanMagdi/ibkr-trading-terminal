package com.project.trading.order.domain;

/** A transition the order state machine does not allow. */
public class InvalidTransitionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final OrderStatus from;
    private final OrderStatus to;

    public InvalidTransitionException(OrderStatus from, OrderStatus to) {
        super("order cannot move from " + from + " to " + to);
        this.from = from;
        this.to = to;
    }

    public OrderStatus from() {
        return from;
    }

    public OrderStatus to() {
        return to;
    }
}
