package com.project.trading.shared.domain;

import java.math.BigDecimal;
import java.util.Objects;

/** A positive price with at most {@value Decimals#PRICE_SCALE} fractional digits. Never rounded on creation. */
public record Price(BigDecimal value) implements Comparable<Price> {

    public Price {
        Objects.requireNonNull(value, "value");
        if (value.signum() <= 0) {
            throw new IllegalArgumentException("price must be positive");
        }
        if (Decimals.significantScale(value) > Decimals.PRICE_SCALE) {
            throw new IllegalArgumentException("price has too many decimals");
        }
        value = value.setScale(Decimals.PRICE_SCALE);
    }

    public static Price of(String text) {
        return new Price(Decimals.parse(text));
    }

    @Override
    public int compareTo(Price other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return Decimals.format(value, 2);
    }
}
