package com.project.trading.broker.domain;

import java.util.List;

/** Working orders with a broker order ID, from the system of record. */
public interface OpenOrderSource {

    /** Working limit orders (to rebuild a simulated order book). */
    List<OpenBrokerOrder> openLimitOrders();

    /** All working orders, oldest first, at most limit. */
    List<WorkingBrokerOrder> workingOrders(int limit);
}
