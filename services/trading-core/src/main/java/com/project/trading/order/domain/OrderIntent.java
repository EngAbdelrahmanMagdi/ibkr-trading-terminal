package com.project.trading.order.domain;

/** The user's intent. Opening a short is a SELL at the broker, so intent and broker side differ. */
public enum OrderIntent {
    BUY, SELL, SHORT
}
