package com.project.trading.order.application;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/** Storage of Idempotency-Key records. Methods join the caller's transaction when one is active. */
public interface IdempotencyStore {

    /** A stored key. responseStatus and completedAt are null while the request is in progress. */
    record Entry(UUID key, String fingerprint, UUID orderId, Integer responseStatus, String problemBody,
                 Instant completedAt) {
    }

    /** Returns true when this call claimed the key. */
    boolean insertIfAbsent(UUID key, String fingerprint, Instant now);

    Optional<Entry> find(UUID key);

    /** Takes over an abandoned claim of the same request that never created an order. */
    boolean takeOverStale(UUID key, String fingerprint, Instant now, Instant staleBefore);

    void linkOrder(UUID key, UUID orderId);

    void complete(UUID key, int responseStatus, String problemBody, Instant at);
}
