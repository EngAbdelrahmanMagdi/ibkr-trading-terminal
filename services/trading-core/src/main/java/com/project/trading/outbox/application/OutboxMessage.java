package com.project.trading.outbox.application;

import java.util.Objects;
import java.util.UUID;

/**
 * An event to publish: the complete envelope (JSON) and where it goes. eventId is the envelope's eventId and the
 * outbox row ID, so a republished event keeps its identity.
 */
public record OutboxMessage(UUID eventId, String aggregateType, UUID aggregateId, String topic, String key,
                            String eventType, int eventVersion, String envelopeJson) {

    public OutboxMessage {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(aggregateType, "aggregateType");
        Objects.requireNonNull(aggregateId, "aggregateId");
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(envelopeJson, "envelopeJson");
    }
}
