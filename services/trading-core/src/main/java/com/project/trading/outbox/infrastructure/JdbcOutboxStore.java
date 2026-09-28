package com.project.trading.outbox.infrastructure;

import com.project.trading.outbox.application.OutboxMessage;
import com.project.trading.outbox.application.OutboxStore;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/** PostgreSQL outbox (table outbox_events). Every statement uses bind parameters. */
@Repository
class JdbcOutboxStore implements OutboxStore, OutboxRelayStore {

    /** Counts for metrics; oldestPendingAt is null when nothing is pending. */
    record Stats(long pending, long failed, Instant oldestPendingAt) {
    }

    private static final String CLAIM = """
            WITH candidates AS (
              SELECT o.id, o.claimed_by AS previous_claim
              FROM outbox_events o
              WHERE o.status = 'PENDING' AND o.next_attempt_at <= now()
                AND (o.claim_expires_at IS NULL OR o.claim_expires_at < now())
                AND NOT EXISTS (SELECT 1 FROM outbox_events e
                                WHERE e.topic = o.topic AND e.record_key = o.record_key
                                  AND e.seq < o.seq AND e.status <> 'PUBLISHED')
              ORDER BY o.seq
              LIMIT ?
              FOR UPDATE OF o SKIP LOCKED)
            UPDATE outbox_events t
            SET claimed_by = ?, claim_expires_at = now() + (? * interval '1 millisecond')
            FROM candidates c
            WHERE t.id = c.id
            RETURNING t.id, t.seq, t.topic, t.record_key, t.event_type, t.event_version, t.payload::text AS payload,
                      t.attempt_count, t.created_at, c.previous_claim IS NOT NULL AS reclaimed""";

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;

    JdbcOutboxStore(JdbcTemplate jdbc, PlatformTransactionManager transactionManager) {
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(transactionManager);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void insert(OutboxMessage m, Instant createdAt) {
        Timestamp at = Timestamp.from(createdAt);
        jdbc.update("""
                INSERT INTO outbox_events (id, aggregate_type, aggregate_id, topic, record_key, event_type, event_version,
                                           payload, status, created_at, next_attempt_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, 'PENDING', ?, ?)""",
                m.eventId(), m.aggregateType(), m.aggregateId(), m.topic(), m.key(), m.eventType(), m.eventVersion(),
                m.envelopeJson(), at, at);
    }

    private record Row(Claimed claimed, long seq) {
    }

    @Override
    public List<Claimed> claim(UUID instance, int batchSize, Duration lease) {
        List<Row> rows = tx.execute(status -> jdbc.query(CLAIM, (rs, n) -> new Row(new Claimed(
                        rs.getObject("id", UUID.class), rs.getString("topic"), rs.getString("record_key"),
                        rs.getString("event_type"), rs.getInt("event_version"), rs.getString("payload"),
                        rs.getInt("attempt_count"), rs.getTimestamp("created_at").toInstant(), rs.getBoolean("reclaimed")),
                        rs.getLong("seq")),
                batchSize, instance, lease.toMillis()));
        return rows.stream().sorted(Comparator.comparingLong(Row::seq)).map(Row::claimed).toList();
    }

    @Override
    public void complete(UUID instance, Collection<UUID> published, Collection<BadRecord> badRecords,
                         Collection<UUID> released, Duration recordBackoffBase, Duration recordBackoffMax,
                         int maxAttempts) {
        tx.executeWithoutResult(status -> {
            batch("""
                    UPDATE outbox_events
                    SET status = 'PUBLISHED', published_at = now(), claimed_by = NULL, claim_expires_at = NULL,
                        last_error = NULL
                    WHERE id = ? AND claimed_by = ? AND status = 'PENDING'""", published, instance);
            batch("""
                    UPDATE outbox_events SET claimed_by = NULL, claim_expires_at = NULL
                    WHERE id = ? AND claimed_by = ?""", released, instance);
            for (BadRecord bad : badRecords) {
                long delay = backoff(bad.attemptCount(), recordBackoffBase, recordBackoffMax).toMillis();
                jdbc.update("""
                        UPDATE outbox_events
                        SET attempt_count = attempt_count + 1, last_error = ?, claimed_by = NULL, claim_expires_at = NULL,
                            next_attempt_at = now() + (? * interval '1 millisecond'),
                            status = CASE WHEN attempt_count + 1 >= ? THEN 'FAILED' ELSE 'PENDING' END
                        WHERE id = ? AND claimed_by = ?""",
                        bad.error(), delay, maxAttempts, bad.id(), instance);
            }
        });
    }

    private void batch(String sql, Collection<UUID> ids, UUID instance) {
        if (ids.isEmpty()) {
            return;
        }
        List<Object[]> args = new ArrayList<>(ids.size());
        for (UUID id : ids) {
            args.add(new Object[] {id, instance});
        }
        jdbc.batchUpdate(sql, args);
    }

    /** Exponential backoff with jitter: base * 2^attempt, capped, then a random value between half and all of it. */
    static Duration backoff(int attempt, Duration base, Duration max) {
        long cap = max.toMillis();
        long delay = base.toMillis() << Math.min(attempt, 20);
        delay = delay <= 0 ? cap : Math.min(delay, cap);
        return Duration.ofMillis(delay / 2 + ThreadLocalRandom.current().nextLong(delay / 2 + 1));
    }

    /** Deletes published events older than the retention, in bounded batches; never touches PENDING or FAILED. */
    int purge(Duration retention, int batchSize, int maxBatches) {
        int total = 0;
        for (int i = 0; i < maxBatches; i++) {
            int deleted = tx.execute(status -> jdbc.update("""
                    DELETE FROM outbox_events
                    WHERE id IN (SELECT id FROM outbox_events
                                 WHERE status = 'PUBLISHED' AND published_at < now() - (? * interval '1 millisecond')
                                 ORDER BY published_at
                                 LIMIT ?)""", retention.toMillis(), batchSize));
            total += deleted;
            if (deleted < batchSize) {
                break;
            }
        }
        return total;
    }

    Stats stats() {
        return jdbc.queryForObject("""
                SELECT count(*) FILTER (WHERE status = 'PENDING') AS pending,
                       count(*) FILTER (WHERE status = 'FAILED') AS failed,
                       min(created_at) FILTER (WHERE status = 'PENDING') AS oldest
                FROM outbox_events WHERE status <> 'PUBLISHED'""",
                (rs, n) -> new Stats(rs.getLong("pending"), rs.getLong("failed"),
                        rs.getTimestamp("oldest") == null ? null : rs.getTimestamp("oldest").toInstant()));
    }
}
