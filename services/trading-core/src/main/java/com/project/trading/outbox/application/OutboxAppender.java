package com.project.trading.outbox.application;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;

/**
 * Records an event in the transaction that changes the business state it describes: both commit or neither does.
 * There is deliberately no way to append outside a transaction.
 */
@Component
public class OutboxAppender {

    private final OutboxStore store;
    private final Clock clock;

    public OutboxAppender(OutboxStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void append(OutboxMessage message) {
        store.insert(message, clock.instant());
    }
}
