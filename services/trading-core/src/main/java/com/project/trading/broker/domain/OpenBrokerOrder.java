package com.project.trading.broker.domain;

import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;

/** An open limit order as known to the trading application (used to rebuild a broker adapter's state). */
public record OpenBrokerOrder(String brokerOrderId, String symbol, BrokerSide side, Quantity openQuantity,
                              Price limitPrice, boolean cancelRequested) {
}
