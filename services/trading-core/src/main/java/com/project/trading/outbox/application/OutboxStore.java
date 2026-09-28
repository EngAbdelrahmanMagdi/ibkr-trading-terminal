package com.project.trading.outbox.application;

import java.time.Instant;

/** Durable storage of events to publish (PostgreSQL). */
public interface OutboxStore {

    /** Inserts a PENDING event in the caller's transaction. */
    void insert(OutboxMessage message, Instant createdAt);
}
