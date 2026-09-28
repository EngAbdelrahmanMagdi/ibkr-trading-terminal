package com.project.trading.outbox.infrastructure;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * The relay's view of the outbox. Each method is one short transaction of its own; none is ever open while Kafka is
 * called. Result updates are fenced by the claiming instance, so a claim lost to lease expiry is left alone.
 */
interface OutboxRelayStore {

    /** A claimed event. reclaimed is true when a previous claim on it had expired. */
    record Claimed(UUID id, String topic, String key, String eventType, int eventVersion, String payload,
                   int attemptCount, Instant createdAt, boolean reclaimed) {
    }

    /** An event Kafka refused for itself; it is retried after backoff, and FAILED once maxAttempts is reached. */
    record BadRecord(UUID id, int attemptCount, String error) {
    }

    /**
     * Atomically claims up to batchSize publishable events, oldest first: PENDING, due, not claimed (or the claim
     * expired), and with every earlier event of the same topic and key already published.
     */
    List<Claimed> claim(UUID instance, int batchSize, Duration lease);

    /** Records the outcome of a round: published, refused events, and events released untouched. */
    void complete(UUID instance, Collection<UUID> published, Collection<BadRecord> badRecords,
                  Collection<UUID> released, Duration recordBackoffBase, Duration recordBackoffMax, int maxAttempts);
}
