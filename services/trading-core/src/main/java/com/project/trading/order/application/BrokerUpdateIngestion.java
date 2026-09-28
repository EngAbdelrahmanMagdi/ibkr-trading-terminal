package com.project.trading.order.application;

import com.project.trading.broker.domain.BrokerOrderUpdate;
import com.project.trading.broker.domain.BrokerOrderUpdateHandler;
import com.project.trading.broker.domain.BrokerUpdateInbox;
import com.project.trading.inbox.application.ProcessedEvents;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.UUID;

/**
 * Applies the updates of one broker-update event exactly once: in one transaction the event is marked processed and
 * every update is applied (with its executions, positions and outbox events). A redelivered event is skipped. When an
 * update is not ready (the order is not acknowledged locally yet), the whole transaction is rolled back so the event
 * can be delivered again.
 */
@Service
public class BrokerUpdateIngestion implements BrokerUpdateInbox {

    static final String CONSUMER = "trading-core-broker-updates";

    private final ProcessedEvents processed;
    private final BrokerOrderUpdateHandler updates;
    private final TransactionTemplate tx;

    public BrokerUpdateIngestion(ProcessedEvents processed, BrokerOrderUpdateHandler updates,
                                 PlatformTransactionManager transactionManager) {
        this.processed = processed;
        this.updates = updates;
        this.tx = new TransactionTemplate(transactionManager);
    }

    @Override
    public BrokerOrderUpdateHandler.Outcome ingest(UUID eventId, List<BrokerOrderUpdate> batch) {
        return tx.execute(status -> {
            if (!processed.markProcessed(CONSUMER, eventId)) {
                return BrokerOrderUpdateHandler.Outcome.IGNORED;
            }
            BrokerOrderUpdateHandler.Outcome result = BrokerOrderUpdateHandler.Outcome.IGNORED;
            for (BrokerOrderUpdate update : batch) {
                BrokerOrderUpdateHandler.Outcome outcome = updates.handle(update);
                if (outcome == BrokerOrderUpdateHandler.Outcome.NOT_READY) {
                    status.setRollbackOnly();
                    return outcome;
                }
                if (outcome == BrokerOrderUpdateHandler.Outcome.APPLIED
                        || (outcome == BrokerOrderUpdateHandler.Outcome.UNKNOWN_ORDER
                        && result == BrokerOrderUpdateHandler.Outcome.IGNORED)) {
                    result = outcome;
                }
            }
            return result;
        });
    }
}
