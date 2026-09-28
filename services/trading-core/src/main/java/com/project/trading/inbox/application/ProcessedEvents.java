package com.project.trading.inbox.application;

import java.util.UUID;

/** Durable record of the events each consumer has processed (at-least-once delivery made idempotent). */
public interface ProcessedEvents {

    /**
     * Marks the event processed for the consumer, in the caller's transaction. Returns false when it was already
     * processed (a redelivery), in which case the caller must not apply it again.
     */
    boolean markProcessed(String consumer, UUID eventId);
}
