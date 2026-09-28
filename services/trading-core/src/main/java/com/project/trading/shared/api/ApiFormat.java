package com.project.trading.shared.api;

import com.project.trading.shared.domain.Decimals;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/** Wire formats: decimals as plain strings, instants as ISO-8601 UTC with a trailing Z. */
public final class ApiFormat {

    private ApiFormat() {
    }

    public static String decimal(BigDecimal value, int minScale) {
        return value == null ? null : Decimals.format(value, minScale);
    }

    public static String instant(Instant value) {
        return value == null ? null : DateTimeFormatter.ISO_INSTANT.format(value);
    }
}
