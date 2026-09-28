package com.project.trading.instrument.domain;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Shortability as reported by the broker. Details are present only when the broker provides them. Data older
 * than the configured maximum age counts as UNAVAILABLE, never as SHORTABLE or NOT_SHORTABLE.
 */
public record Shortability(ShortabilityStatus status, Long availableQuantity, BigDecimal borrowFeeRate, Instant asOf) {

    public Shortability {
        Objects.requireNonNull(status, "status");
    }

    public static Shortability unavailable() {
        return new Shortability(ShortabilityStatus.UNAVAILABLE, null, null, null);
    }

    /** The effective status: stale or undated explicit data is downgraded to UNAVAILABLE. */
    public ShortabilityStatus effectiveStatus(Instant now, Duration maxAge) {
        if (status == ShortabilityStatus.UNAVAILABLE || asOf == null || Duration.between(asOf, now).compareTo(maxAge) > 0) {
            return ShortabilityStatus.UNAVAILABLE;
        }
        return status;
    }
}
