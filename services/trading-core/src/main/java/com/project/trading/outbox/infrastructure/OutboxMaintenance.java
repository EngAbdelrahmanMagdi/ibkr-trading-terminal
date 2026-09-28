package com.project.trading.outbox.infrastructure;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Outbox gauges (pending, failed, age of the oldest pending event) refreshed from PostgreSQL, and the retention purge
 * of published events. PENDING and FAILED events are never deleted.
 */
@Component
class OutboxMaintenance {

    private static final Logger log = LoggerFactory.getLogger(OutboxMaintenance.class);

    private final JdbcOutboxStore store;
    private final OutboxProperties properties;
    private final Clock clock;
    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong oldestPendingAgeSeconds = new AtomicLong();

    OutboxMaintenance(JdbcOutboxStore store, OutboxProperties properties, Clock clock, MeterRegistry registry) {
        this.store = store;
        this.properties = properties;
        this.clock = clock;
        registry.gauge("outbox.pending", pending);
        registry.gauge("outbox.failed", failed);
        registry.gauge("outbox.oldest.pending.age.seconds", oldestPendingAgeSeconds);
    }

    @Scheduled(fixedDelayString = "${app.metrics.status-refresh-interval}", initialDelayString = "${app.metrics.status-refresh-interval}")
    void refreshGauges() {
        try {
            JdbcOutboxStore.Stats stats = store.stats();
            pending.set(stats.pending());
            failed.set(stats.failed());
            oldestPendingAgeSeconds.set(stats.oldestPendingAt() == null ? 0
                    : Math.max(0, Duration.between(stats.oldestPendingAt(), clock.instant()).toSeconds()));
        } catch (RuntimeException e) {
            log.warn("outbox gauge refresh failed: {}", e.getClass().getSimpleName());
        }
    }

    @Scheduled(fixedDelayString = "${app.outbox.purge-interval}", initialDelayString = "${app.outbox.purge-interval}")
    void purge() {
        try {
            int deleted = store.purge(properties.retention(), properties.purgeBatchSize(), properties.purgeMaxBatches());
            if (deleted > 0) {
                log.info("outbox retention: {} published events deleted", deleted);
            }
        } catch (RuntimeException e) {
            log.warn("outbox retention purge failed: {}", e.getClass().getSimpleName());
        }
    }
}
