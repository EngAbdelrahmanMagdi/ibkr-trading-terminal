package com.project.trading.broker.infrastructure.kafka;

import com.project.trading.broker.domain.BrokerOrderUpdateHandler;
import com.project.trading.broker.domain.BrokerUpdateInbox;
import com.project.trading.broker.domain.ReconciliationRequests;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Consumes broker order observations (broker.order-updates.v1) on one thread and applies each event exactly once
 * through the inbox (processed event IDs are stored with the changes). Offsets are committed only after the records
 * of a poll were handled, so a crash redelivers them (at least once; redelivery is harmless).
 * <ul>
 *   <li>An update that is not ready (its order is not acknowledged locally yet) is retried with a bounded backoff,
 *   then skipped: reconciliation recovers it.</li>
 *   <li>Unreadable events, events for another account and unknown event types are counted and skipped (no DLQ:
 *   reconciliation against the broker is the recovery path).</li>
 *   <li>A partition assignment or the recovery from a Kafka failure requests a reconciliation run.</li>
 *   <li>A database failure rewinds to the last committed offsets and retries after a bounded backoff.</li>
 * </ul>
 */
class BrokerUpdateConsumer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(BrokerUpdateConsumer.class);
    private static final Duration POLL = Duration.ofSeconds(1);
    private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration ERROR_BACKOFF_MAX = Duration.ofSeconds(30);

    private final Supplier<Consumer<String, String>> consumerFactory;
    private final BrokerUpdateEvents events;
    private final BrokerUpdateInbox inbox;
    private final ReconciliationRequests reconciliation;
    private final BrokerUpdatesProperties properties;
    private final MeterRegistry registry;
    private final Map<String, Counter> outcomes = new ConcurrentHashMap<>();
    private volatile Consumer<String, String> consumer;
    private volatile boolean stopping;
    private volatile boolean running;
    private Thread thread;

    BrokerUpdateConsumer(Supplier<Consumer<String, String>> consumerFactory, BrokerUpdateEvents events,
                         BrokerUpdateInbox inbox, ReconciliationRequests reconciliation,
                         BrokerUpdatesProperties properties, MeterRegistry registry) {
        this.consumerFactory = consumerFactory;
        this.events = events;
        this.inbox = inbox;
        this.reconciliation = reconciliation;
        this.properties = properties;
        this.registry = registry;
    }

    @Override
    public void start() {
        stopping = false;
        thread = new Thread(this::loop, "broker-update-consumer");
        thread.setDaemon(true);
        thread.start();
        running = true;
    }

    @Override
    public void stop() {
        stopping = true;
        Consumer<String, String> c = consumer;
        if (c != null) {
            c.wakeup();
        }
        if (thread != null) {
            try {
                thread.join(properties.notReadyMaxWait().plusSeconds(15).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Stops after the web server has drained requests and before the datasource closes. */
    @Override
    public int getPhase() {
        return 0;
    }

    private void loop() {
        Duration backoff = Duration.ofSeconds(1);
        boolean failed = false;
        try {
            consumer = consumerFactory.get();
            consumer.subscribe(List.of(properties.topic()), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // Offsets are committed after each poll; nothing is in flight here.
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    if (!partitions.isEmpty()) {
                        reconciliation.requestReconciliation("broker update stream assigned");
                    }
                }
            });
            while (!stopping) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(POLL);
                    if (failed) {
                        failed = false;
                        backoff = Duration.ofSeconds(1);
                        reconciliation.requestReconciliation("broker update stream recovered");
                    }
                    for (ConsumerRecord<String, String> record : records) {
                        handle(record);
                    }
                    if (!records.isEmpty()) {
                        consumer.commitSync(COMMIT_TIMEOUT);
                    }
                } catch (WakeupException e) {
                    if (stopping) {
                        break;
                    }
                } catch (KafkaException | org.springframework.dao.DataAccessException
                         | org.springframework.transaction.TransactionException e) {
                    failed = true;
                    count("error");
                    log.warn("broker update consumption failed; retrying: {}", e.getClass().getSimpleName());
                    rewind();
                    sleep(backoff);
                    backoff = backoff.multipliedBy(2).compareTo(ERROR_BACKOFF_MAX) > 0 ? ERROR_BACKOFF_MAX : backoff.multipliedBy(2);
                }
            }
        } catch (RuntimeException e) {
            log.error("broker update consumer stopped", e);
        } finally {
            Consumer<String, String> c = consumer;
            if (c != null) {
                try {
                    c.close(CloseOptions.timeout(Duration.ofSeconds(5)));
                } catch (RuntimeException e) {
                    log.warn("Kafka consumer close failed: {}", e.getClass().getSimpleName());
                }
            }
        }
    }

    private void handle(ConsumerRecord<String, String> record) {
        BrokerUpdateEvents.Parsed parsed;
        try {
            parsed = events.parse(record.value());
        } catch (BrokerUpdateEvents.Rejected e) {
            count(e.reason());
            log.warn("broker update skipped ({}): {}", e.reason(), e.getMessage());
            return;
        }
        long deadline = System.nanoTime() + properties.notReadyMaxWait().toNanos();
        Duration wait = Duration.ofMillis(200);
        for (int attempt = 1; ; attempt++) {
            BrokerOrderUpdateHandler.Outcome outcome = inbox.ingest(parsed.eventId(), parsed.updates());
            if (outcome != BrokerOrderUpdateHandler.Outcome.NOT_READY) {
                count(outcome.name().toLowerCase(java.util.Locale.ROOT));
                return;
            }
            if (attempt >= properties.notReadyAttempts() || System.nanoTime() + wait.toNanos() > deadline || stopping) {
                count("not_ready_skipped");
                log.warn("broker update {} not applicable yet after {} attempts; reconciliation recovers it",
                        parsed.eventId(), attempt);
                return;
            }
            sleep(wait);
            wait = wait.multipliedBy(2);
        }
    }

    /** Goes back to the last committed offsets so that records of a failed poll are consumed again. */
    private void rewind() {
        try {
            Set<TopicPartition> assignment = consumer.assignment();
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(assignment, COMMIT_TIMEOUT);
            for (TopicPartition tp : assignment) {
                OffsetAndMetadata offset = committed.get(tp);
                if (offset != null) {
                    consumer.seek(tp, offset.offset());
                } else {
                    consumer.seekToBeginning(List.of(tp));
                }
            }
        } catch (RuntimeException e) {
            log.debug("rewind failed: {}", e.getClass().getSimpleName());
        }
    }

    private void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stopping = true;
        }
    }

    private void count(String outcome) {
        outcomes.computeIfAbsent(outcome, o -> Counter.builder("broker.updates.consumed").tag("outcome", o)
                .description("Broker order observations consumed, by outcome").register(registry)).increment();
    }
}
