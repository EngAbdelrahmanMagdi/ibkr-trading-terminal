package com.project.trading.broker.domain;

import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.shared.domain.OrderType;
import com.project.trading.shared.domain.TimeInForce;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;

import java.util.UUID;

/** A broker-neutral order submission. limitPrice is null for MARKET orders. */
public record BrokerOrderRequest(UUID orderId, String clientOrderId, String accountId, long conid, String symbol,
                                 BrokerSide side, OrderType orderType, Quantity quantity, Price limitPrice,
                                 TimeInForce timeInForce) {
}
