package com.project.trading.inbox.infrastructure;

import com.project.trading.inbox.application.ProcessedEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;

/** PostgreSQL table processed_events. The retention purge runs in bounded batches. */
@Repository
class JdbcProcessedEvents implements ProcessedEvents {

    private static final Logger log = LoggerFactory.getLogger(JdbcProcessedEvents.class);
    private static final int PURGE_BATCH = 1000;
    private static final int PURGE_MAX_BATCHES = 50;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final Duration retention;

    JdbcProcessedEvents(JdbcTemplate jdbc, PlatformTransactionManager transactionManager, Clock clock,
                        @Value("${app.inbox.retention}") Duration retention) {
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(transactionManager);
        this.clock = clock;
        this.retention = retention;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean markProcessed(String consumer, UUID eventId) {
        return jdbc.update("INSERT INTO processed_events (consumer_name, event_id, processed_at) VALUES (?, ?, ?)"
                + " ON CONFLICT (consumer_name, event_id) DO NOTHING", consumer, eventId, Timestamp.from(clock.instant())) == 1;
    }

    @Scheduled(fixedDelayString = "${app.inbox.purge-interval}", initialDelayString = "${app.inbox.purge-interval}")
    void purge() {
        try {
            Timestamp before = Timestamp.from(clock.instant().minus(retention));
            int total = 0;
            for (int i = 0; i < PURGE_MAX_BATCHES; i++) {
                int deleted = tx.execute(s -> jdbc.update("DELETE FROM processed_events WHERE ctid IN (SELECT ctid"
                        + " FROM processed_events WHERE processed_at < ? LIMIT ?)", before, PURGE_BATCH));
                total += deleted;
                if (deleted < PURGE_BATCH) {
                    break;
                }
            }
            if (total > 0) {
                log.info("processed-event retention: {} rows deleted", total);
            }
        } catch (RuntimeException e) {
            log.warn("processed-event retention purge failed: {}", e.getClass().getSimpleName());
        }
    }
}
