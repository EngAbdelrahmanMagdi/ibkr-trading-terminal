package com.project.trading.news.infrastructure;

import com.project.trading.news.domain.NewsUnavailable;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/** Shared provider quota and circuit state, bounded independently of symbol count. */
public class NewsFetchPolicy {
    private final NewsProperties properties;
    private final Clock clock;
    private Instant nextRequest = Instant.EPOCH;
    private Instant openUntil = Instant.EPOCH;
    private int failures;
    private boolean disabled;
    private boolean probe;
    public NewsFetchPolicy(NewsProperties properties, Clock clock) { this.properties = properties; this.clock = clock; }
    public synchronized void enter() {
        if (disabled || clock.instant().isBefore(openUntil) || probe) throw new NewsUnavailable("CIRCUIT_OPEN");
        if (failures >= properties.breakerFailures()) probe = true;
    }
    public void acquire(Instant deadline) {
        long wait;
        synchronized (this) {
            if (clock.instant().isBefore(openUntil) || disabled) throw new NewsUnavailable("CIRCUIT_OPEN");
            Instant at = clock.instant().isAfter(nextRequest) ? clock.instant() : nextRequest;
            if (!at.isBefore(deadline)) throw new NewsUnavailable("RATE_LIMIT");
            wait = Math.max(0, java.time.Duration.between(clock.instant(), at).toNanos());
            nextRequest = at.plusNanos(TimeUnit.MINUTES.toNanos(1) / properties.requestsPerMinute());
        }
        try { TimeUnit.NANOSECONDS.sleep(wait); }
        catch (InterruptedException exception) { Thread.currentThread().interrupt(); throw new NewsUnavailable("INTERRUPTED"); }
    }
    public synchronized void success() { failures = 0; probe = false; }
    /** 0 closed, 1 open, 2 disabled, 3 recovery probe. */
    public synchronized int state() { return disabled ? 2 : clock.instant().isBefore(openUntil) ? 1 : probe ? 3 : 0; }
    public synchronized void failure(String classification, long retryAfterSeconds) {
        probe = false;
        failures++;
        if (classification.equals("AUTH")) disabled = true;
        if (failures >= properties.breakerFailures() || classification.equals("RATE_LIMIT"))
            openUntil = clock.instant().plusSeconds(Math.max(properties.breakerCooldown().toSeconds(), retryAfterSeconds));
    }
}
