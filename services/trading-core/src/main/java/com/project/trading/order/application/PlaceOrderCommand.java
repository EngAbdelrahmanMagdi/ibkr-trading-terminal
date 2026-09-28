package com.project.trading.order.application;

import com.project.trading.order.domain.OrderIntent;
import com.project.trading.shared.domain.OrderType;
import com.project.trading.shared.domain.TimeInForce;

import java.math.BigDecimal;
import java.util.Objects;

/** A syntactically valid order request; business validation happens in {@link OrderValidator}. */
public record PlaceOrderCommand(String symbol, OrderIntent intent, OrderType orderType, BigDecimal quantity,
                                BigDecimal limitPrice, TimeInForce timeInForce) {

    public PlaceOrderCommand {
        Objects.requireNonNull(symbol, "symbol");
        Objects.requireNonNull(intent, "intent");
        Objects.requireNonNull(orderType, "orderType");
        Objects.requireNonNull(quantity, "quantity");
        Objects.requireNonNull(timeInForce, "timeInForce");
    }
}
