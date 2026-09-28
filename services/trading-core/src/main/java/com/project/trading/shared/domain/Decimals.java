package com.project.trading.shared.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Scales and rounding for money, prices and quantities. Values are {@link BigDecimal} only: never float or
 * double. Computed values round HALF_EVEN; user-entered prices and quantities are validated, never rounded.
 */
public final class Decimals {

    /** Price scale (NUMERIC(19,6)). */
    public static final int PRICE_SCALE = 6;
    /** Money scale (NUMERIC(19,4)). */
    public static final int MONEY_SCALE = 4;
    /** Quantity scale (NUMERIC(19,4)). */
    public static final int QUANTITY_SCALE = 4;
    /** Rounding for computed values. */
    public static final RoundingMode ROUNDING = RoundingMode.HALF_EVEN;

    private Decimals() {
    }

    /** Parses a decimal string exactly; the scale of the input is kept. */
    public static BigDecimal parse(String text) {
        return new BigDecimal(text);
    }

    /** Significant fractional digits (trailing zeros ignored). */
    public static int significantScale(BigDecimal value) {
        return Math.max(0, value.stripTrailingZeros().scale());
    }

    public static BigDecimal money(BigDecimal value) {
        return value.setScale(MONEY_SCALE, ROUNDING);
    }

    public static BigDecimal price(BigDecimal value) {
        return value.setScale(PRICE_SCALE, ROUNDING);
    }

    public static BigDecimal quantity(BigDecimal value) {
        return value.setScale(QUANTITY_SCALE, ROUNDING);
    }

    /** Wire format: plain notation, trailing zeros removed, but at least minScale fractional digits. */
    public static String format(BigDecimal value, int minScale) {
        BigDecimal stripped = value.stripTrailingZeros();
        if (stripped.scale() < minScale) {
            stripped = stripped.setScale(minScale, RoundingMode.UNNECESSARY);
        }
        return stripped.toPlainString();
    }
}
