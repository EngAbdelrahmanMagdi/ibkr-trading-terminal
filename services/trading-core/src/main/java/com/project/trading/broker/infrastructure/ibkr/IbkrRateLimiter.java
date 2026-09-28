package com.project.trading.broker.infrastructure.ibkr;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

/**
 * This service's share of the IBKR session request budget. One token bucket at the configured allocation (burst
 * equal to one second of allocation) plus the documented per-endpoint limits. Waiting is bounded in both count
 * (queue capacity) and time (acquire timeout); anything beyond is rejected, never queued without limit. After a 429
 * from IBKR every request is rejected for the penalty cool-down.
 * <p>
 * Implemented as a generic cell rate algorithm: each bucket keeps its theoretical arrival time, and a request is
 * scheduled at the earliest time that conforms to every bucket it uses.
 */
final class IbkrRateLimiter {

    /** A request could not get a slot; it was not sent. */
    static final class RejectedException extends Exception {
        private static final long serialVersionUID = 1L;

        RejectedException(String reason) {
            super(reason);
        }
    }

    private static final class Bucket {
        final long interval;
        final long tolerance;
        long theoreticalArrival;

        Bucket(long interval, int burst, long now) {
            this.interval = interval;
            this.tolerance = (burst - 1) * interval;
            this.theoreticalArrival = now;
        }

        long earliest() {
            return theoreticalArrival - tolerance;
        }

        void commit(long at) {
            theoreticalArrival = Math.max(theoreticalArrival, at) + interval;
        }
    }

    private final Bucket global;
    private final Map<IbkrEndpoint, Bucket> endpoints = new EnumMap<>(IbkrEndpoint.class);
    private final int queueCapacity;
    private final long acquireTimeout;
    private final long cooldown;
    private final LongSupplier ticker;
    private final Timer waits;
    private final Counter rejectedQueueFull;
    private final Counter rejectedTimeout;
    private final Counter rejectedCooldown;
    private final Counter tooManyRequests;
    private int waiting;
    private long cooldownUntil;
    private boolean coolingDown;

    IbkrRateLimiter(int allocationPerSecond, int queueCapacity, Duration acquireTimeout, Duration cooldown,
                    LongSupplier ticker, MeterRegistry registry) {
        if (allocationPerSecond < 1 || queueCapacity < 1) {
            throw new IllegalArgumentException("allocation and queue capacity must be positive");
        }
        this.ticker = ticker;
        long now = ticker.getAsLong();
        this.global = new Bucket(TimeUnit.SECONDS.toNanos(1) / allocationPerSecond, allocationPerSecond, now);
        for (IbkrEndpoint endpoint : IbkrEndpoint.values()) {
            if (endpoint.minInterval() != null) {
                endpoints.put(endpoint, new Bucket(endpoint.minInterval().toNanos(), 1, now));
            }
        }
        this.queueCapacity = queueCapacity;
        this.acquireTimeout = acquireTimeout.toNanos();
        this.cooldown = cooldown.toNanos();
        this.waits = Timer.builder("ibkr.limiter.wait").description("Time spent waiting for an IBKR request slot")
                .register(registry);
        this.rejectedQueueFull = rejected(registry, "queue_full");
        this.rejectedTimeout = rejected(registry, "timeout");
        this.rejectedCooldown = rejected(registry, "cooldown");
        this.tooManyRequests = Counter.builder("ibkr.rate.limited").description("429 responses from IBKR")
                .register(registry);
    }

    private static Counter rejected(MeterRegistry registry, String reason) {
        return Counter.builder("ibkr.limiter.rejected").tag("reason", reason)
                .description("IBKR requests rejected by the local rate limiter").register(registry);
    }

    /** Waits (bounded) for a slot for one request to the endpoint. */
    void acquire(IbkrEndpoint endpoint) throws RejectedException {
        long now = ticker.getAsLong();
        long start;
        synchronized (this) {
            if (coolingDown && cooldownUntil - now > 0) {
                rejectedCooldown.increment();
                throw new RejectedException("IBKR requests are paused after a rate-limit response");
            }
            coolingDown = false;
            Bucket perEndpoint = endpoints.get(endpoint);
            start = Math.max(now, global.earliest());
            if (perEndpoint != null) {
                start = Math.max(start, perEndpoint.earliest());
            }
            long wait = start - now;
            if (wait > acquireTimeout) {
                rejectedTimeout.increment();
                throw new RejectedException("no IBKR request slot within the acquire timeout");
            }
            if (wait > 0 && waiting >= queueCapacity) {
                rejectedQueueFull.increment();
                throw new RejectedException("too many IBKR requests are waiting");
            }
            global.commit(start);
            if (perEndpoint != null) {
                perEndpoint.commit(start);
            }
            if (wait <= 0) {
                return;
            }
            waiting++;
        }
        try {
            parkUntil(start);
            waits.record(ticker.getAsLong() - now, TimeUnit.NANOSECONDS);
        } finally {
            synchronized (this) {
                waiting--;
            }
        }
    }

    private void parkUntil(long deadline) throws RejectedException {
        long remaining;
        while ((remaining = deadline - ticker.getAsLong()) > 0) {
            LockSupport.parkNanos(remaining);
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                throw new RejectedException("interrupted while waiting for an IBKR request slot");
            }
        }
    }

    /** IBKR answered 429: pause every request for the penalty cool-down. */
    synchronized void report429() {
        tooManyRequests.increment();
        coolingDown = true;
        cooldownUntil = ticker.getAsLong() + cooldown;
    }

    /** Requests currently waiting for a slot. */
    synchronized int waiting() {
        return waiting;
    }

    synchronized boolean isCoolingDown() {
        return coolingDown && cooldownUntil - ticker.getAsLong() > 0;
    }
}
