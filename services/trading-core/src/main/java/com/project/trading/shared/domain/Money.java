package com.project.trading.shared.domain;

import java.math.BigDecimal;
import java.util.Objects;

/** A signed money amount in a currency, kept at {@value Decimals#MONEY_SCALE} decimals (HALF_EVEN). */
public record Money(BigDecimal amount, String currency) {

    public Money {
        Objects.requireNonNull(amount, "amount");
        Objects.requireNonNull(currency, "currency");
        amount = Decimals.money(amount);
    }

    public Money plus(BigDecimal other) {
        return new Money(amount.add(other), currency);
    }

    @Override
    public String toString() {
        return Decimals.format(amount, 2) + " " + currency;
    }
}
