package com.project.trading.broker.domain;

import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.shared.domain.Quantity;

/** A working order (any type) with a broker order ID, as known to the trading application. */
public record WorkingBrokerOrder(String brokerOrderId, String clientOrderId, String symbol, BrokerSide side,
                                 Quantity quantity, Quantity filledQuantity) {
}
