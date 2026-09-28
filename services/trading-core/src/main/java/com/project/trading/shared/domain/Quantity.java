package com.project.trading.shared.domain;

import java.math.BigDecimal;
import java.util.Objects;

/** A non-negative quantity with at most {@value Decimals#QUANTITY_SCALE} fractional digits. */
public record Quantity(BigDecimal value) implements Comparable<Quantity> {

    public static final Quantity ZERO = new Quantity(BigDecimal.ZERO);

    public Quantity {
        Objects.requireNonNull(value, "value");
        if (value.signum() < 0) {
            throw new IllegalArgumentException("quantity must not be negative");
        }
        if (Decimals.significantScale(value) > Decimals.QUANTITY_SCALE) {
            throw new IllegalArgumentException("quantity has too many decimals");
        }
        value = value.setScale(Decimals.QUANTITY_SCALE);
    }

    public static Quantity of(String text) {
        return new Quantity(Decimals.parse(text));
    }

    public boolean isZero() {
        return value.signum() == 0;
    }

    public boolean isWhole() {
        return Decimals.significantScale(value) == 0;
    }

    public Quantity plus(Quantity other) {
        return new Quantity(value.add(other.value));
    }

    public Quantity minus(Quantity other) {
        return new Quantity(value.subtract(other.value));
    }

    @Override
    public int compareTo(Quantity other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return Decimals.format(value, 0);
    }
}
