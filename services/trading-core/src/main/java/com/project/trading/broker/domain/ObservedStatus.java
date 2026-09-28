package com.project.trading.broker.domain;

/** The broker's order status, mapped to broker-neutral values. Only CANCELLED and REJECTED end an order locally. */
public enum ObservedStatus {
    WORKING,
    PARTIALLY_FILLED,
    FILLED,
    CANCELLED,
    REJECTED,
    INACTIVE,
    OTHER
}
