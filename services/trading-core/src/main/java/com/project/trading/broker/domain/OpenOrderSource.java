package com.project.trading.broker.domain;

import java.util.List;

/** Open limit orders with a broker order ID, from the system of record. */
public interface OpenOrderSource {

    List<OpenBrokerOrder> openLimitOrders();
}
