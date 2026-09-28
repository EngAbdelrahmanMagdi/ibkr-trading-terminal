package com.project.trading.broker.domain;

import java.util.List;
import java.util.UUID;

/**
 * Applies the updates of one delivered event exactly once per event ID: the event is marked processed in the same
 * transaction as the changes, so a redelivered event changes nothing.
 */
public interface BrokerUpdateInbox {

    /**
     * Returns NOT_READY when an update must be delivered again later (nothing was applied or marked); APPLIED,
     * IGNORED (including an already processed event) or UNKNOWN_ORDER otherwise.
     */
    BrokerOrderUpdateHandler.Outcome ingest(UUID eventId, List<BrokerOrderUpdate> updates);
}
