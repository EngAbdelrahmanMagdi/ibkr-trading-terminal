package com.project.trading.marketdata.domain;

import com.project.trading.shared.domain.Price;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * The latest quote of a symbol from the market-data hot cache. Any price may be missing. A quote is usable for
 * trading decisions only when {@link #isFresh} holds: real-time delivery, not flagged stale, not halted, and
 * recent enough.
 */
public record ReferenceQuote(String symbol, Price bid, Price ask, Price last, Instant timestamp, boolean stale,
                             boolean realtime, boolean halted) {

    public ReferenceQuote {
        Objects.requireNonNull(symbol, "symbol");
        Objects.requireNonNull(timestamp, "timestamp");
    }

    public boolean isFresh(Instant now, Duration maxAge) {
        return !stale && realtime && !halted && Duration.between(timestamp, now).compareTo(maxAge) <= 0;
    }
}
