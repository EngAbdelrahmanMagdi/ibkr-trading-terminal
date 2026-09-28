package com.project.trading.broker.infrastructure.ibkr;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * The broker confirmation request (order reply) currently waiting for an answer, if any. IBKR requires a reply to
 * be answered before any further order, and documents that other requests meanwhile can invalidate it. So while
 * one is outstanding no new order is sent and this service's non-essential IBKR requests (order polling,
 * shortability, account metrics) are deferred. An entry expires with the confirmation TTL: after that the order is
 * never confirmed, so it was never transmitted.
 */
final class ReplyGate {

    private record Outstanding(String replyId, Instant expiresAt) {
    }

    private final Clock clock;
    private final Duration ttl;
    private Outstanding outstanding;

    ReplyGate(Clock clock, Duration ttl) {
        this.clock = clock;
        this.ttl = ttl;
    }

    synchronized void open(String replyId) {
        outstanding = new Outstanding(replyId, clock.instant().plus(ttl));
    }

    /** Clears the entry if it is the given reply (or any entry when replyId is null). */
    synchronized void close(String replyId) {
        if (outstanding != null && (replyId == null || outstanding.replyId().equals(replyId))) {
            outstanding = null;
        }
    }

    synchronized boolean isOpen() {
        if (outstanding != null && !clock.instant().isBefore(outstanding.expiresAt())) {
            outstanding = null;
        }
        return outstanding != null;
    }
}
