package com.project.trading.execution.domain;

import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** One fill of an order. commission is null when the broker did not report it. */
public record Execution(UUID id, UUID orderId, String brokerExecutionId, String symbol, BrokerSide side,
                        Quantity quantity, Price price, BigDecimal commission, String currency, Instant executedAt) {

    public Execution {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(brokerExecutionId, "brokerExecutionId");
        Objects.requireNonNull(symbol, "symbol");
        Objects.requireNonNull(side, "side");
        Objects.requireNonNull(quantity, "quantity");
        Objects.requireNonNull(price, "price");
        Objects.requireNonNull(currency, "currency");
        Objects.requireNonNull(executedAt, "executedAt");
        if (quantity.isZero()) {
            throw new IllegalArgumentException("execution quantity must be positive");
        }
        if (commission != null && commission.signum() < 0) {
            throw new IllegalArgumentException("commission must not be negative");
        }
    }
}
