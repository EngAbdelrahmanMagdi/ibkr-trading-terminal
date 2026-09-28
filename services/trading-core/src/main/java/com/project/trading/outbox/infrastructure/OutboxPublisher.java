package com.project.trading.outbox.infrastructure;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Relays outbox events to Kafka. A round has three steps, and no database transaction is open while Kafka is called:
 * <ol>
 *   <li>claim a bounded batch in one short transaction (a lease, so an abandoned claim becomes claimable again);</li>
 *   <li>send the batch with the idempotent producer, outside any transaction;</li>
 *   <li>record the results in a second short transaction, fenced by this instance's claim.</li>
 * </ol>
 * Failure handling:
 * <ul>
 *   <li>Kafka unreachable or misconfigured (timeouts, disconnects, authentication or authorization, missing metadata
 *   or topics, and anything unrecognized): the events stay PENDING with their attempt count untouched, and the relay
 *   pauses with capped exponential backoff, then probes with a single event per interval.</li>
 *   <li>An event Kafka refuses for itself (serialization, too large): its attempt count grows, it is retried after
 *   backoff, and after the configured attempts it becomes FAILED, which holds back only the events of its key.</li>
 * </ul>
 * Delivery is at least once: a crash between Kafka's acknowledgement and step 3 publishes the event again, with the
 * same event ID.
 */
class OutboxPublisher implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final JsonMapper JSON = JsonMapper.builder().build();
    /** Upper bound of rounds in one scheduled run, so a long backlog never monopolizes the thread. */
    private static final int MAX_ROUNDS_PER_RUN = 50;

    /** Outcome of one round. */
    record Round(int claimed, int published, int badRecords, boolean infrastructureFailure) {
    }

    private final OutboxRelayStore store;
    private final Supplier<Producer<String, String>> producerFactory;
    private final OutboxProperties properties;
    private final Clock clock;
    private final UUID instance = UUID.randomUUID();
    private final Counter published;
    private final Counter infrastructureFailures;
    private final Counter badRecordFailures;
    private final Counter reclaims;
    private final Timer latency;
    private final AtomicLong backoffMillis = new AtomicLong();
    private Producer<String, String> producer;
    private int outageStreak;
    private Instant pausedUntil = Instant.MIN;
    private ScheduledExecutorService executor;
    private volatile boolean running;
    private volatile boolean stopping;

    OutboxPublisher(OutboxRelayStore store, Supplier<Producer<String, String>> producerFactory,
                           OutboxProperties properties, Clock clock, MeterRegistry registry) {
        this.store = store;
        this.producerFactory = producerFactory;
        this.properties = properties;
        this.clock = clock;
        this.published = Counter.builder("outbox.published").description("Outbox events published to Kafka")
                .register(registry);
        this.infrastructureFailures = Counter.builder("outbox.publish.failure").tag("kind", "infrastructure")
                .description("Outbox publish failures").register(registry);
        this.badRecordFailures = Counter.builder("outbox.publish.failure").tag("kind", "bad_record")
                .description("Outbox publish failures").register(registry);
        this.reclaims = Counter.builder("outbox.claims.expired")
                .description("Outbox events claimed again after an earlier claim expired").register(registry);
        this.latency = Timer.builder("outbox.publish.latency")
                .description("Time from recording an event to Kafka acknowledging it").register(registry);
        registry.gauge("outbox.publisher.backoff.seconds", backoffMillis, v -> v.get() / 1000.0);
    }

    @Override
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "outbox-publisher");
            t.setDaemon(true);
            return t;
        });
        long interval = properties.pollInterval().toMillis();
        executor.scheduleWithFixedDelay(this::safeRun, interval, interval, TimeUnit.MILLISECONDS);
        running = true;
        log.info("outbox publisher started (instance {})", instance);
    }

    @Override
    public void stop() {
        stopping = true;
        running = false;
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(properties.maxSendTime().toSeconds() + 5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        if (producer != null) {
            try {
                producer.close(Duration.ofSeconds(5));
            } catch (RuntimeException e) {
                log.warn("Kafka producer close failed: {}", e.getClass().getSimpleName());
            }
        }
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

    private void safeRun() {
        try {
            runOnce();
        } catch (RuntimeException e) {
            log.error("outbox publisher run failed", e);
        }
    }

    /** One scheduled run: rounds until the backlog is drained, bounded; a single probe while Kafka is unavailable. */
    void runOnce() {
        if (clock.instant().isBefore(pausedUntil)) {
            return;
        }
        boolean probing = outageStreak > 0;
        for (int i = 0; i < MAX_ROUNDS_PER_RUN && !stopping; i++) {
            Round round = round(probing ? 1 : properties.batchSize());
            if (round.infrastructureFailure() || round.published() == 0 || probing) {
                return;
            }
        }
    }

    /** Claims, sends and records one batch of at most batchSize events. */
    Round round(int batchSize) {
        List<OutboxRelayStore.Claimed> claimed = store.claim(instance, batchSize, properties.lease());
        if (claimed.isEmpty()) {
            return new Round(0, 0, 0, false);
        }
        claimed.stream().filter(OutboxRelayStore.Claimed::reclaimed).forEach(c -> reclaims.increment());

        List<Future<RecordMetadata>> futures = new ArrayList<>(claimed.size());
        List<Throwable> sendErrors = new ArrayList<>(claimed.size());
        Producer<String, String> kafka;
        try {
            kafka = producer();
        } catch (RuntimeException e) {
            log.warn("Kafka producer unavailable: {}", e.getClass().getSimpleName());
            return finish(claimed, List.of(), List.of(), true);
        }
        for (OutboxRelayStore.Claimed event : claimed) {
            try {
                futures.add(kafka.send(record(event)));
                sendErrors.add(null);
            } catch (RuntimeException e) {
                futures.add(null);
                sendErrors.add(e);
            }
        }

        long deadline = System.nanoTime() + properties.maxSendTime().toNanos();
        List<UUID> acknowledged = new ArrayList<>();
        List<OutboxRelayStore.BadRecord> refused = new ArrayList<>();
        boolean infrastructureFailure = false;
        for (int i = 0; i < claimed.size(); i++) {
            OutboxRelayStore.Claimed event = claimed.get(i);
            Throwable error = sendErrors.get(i);
            if (error == null) {
                error = await(futures.get(i), deadline);
            }
            if (error == null) {
                acknowledged.add(event.id());
                latency.record(Duration.between(event.createdAt(), clock.instant()));
            } else if (isBadRecord(error)) {
                refused.add(new OutboxRelayStore.BadRecord(event.id(), event.attemptCount(), describe(error)));
            } else {
                infrastructureFailure = true;
                log.debug("outbox event {} not published: {}", event.id(), describe(error));
            }
        }
        List<UUID> unsent = claimed.stream().map(OutboxRelayStore.Claimed::id)
                .filter(id -> !acknowledged.contains(id) && refused.stream().noneMatch(b -> b.id().equals(id)))
                .toList();
        return finish(claimed, acknowledged, refused, infrastructureFailure, unsent);
    }

    private Round finish(List<OutboxRelayStore.Claimed> claimed, List<UUID> acknowledged,
                         List<OutboxRelayStore.BadRecord> refused, boolean infrastructureFailure) {
        List<UUID> unsent = claimed.stream().map(OutboxRelayStore.Claimed::id).toList();
        return finish(claimed, acknowledged, refused, infrastructureFailure, unsent);
    }

    private Round finish(List<OutboxRelayStore.Claimed> claimed, List<UUID> acknowledged,
                         List<OutboxRelayStore.BadRecord> refused, boolean infrastructureFailure, List<UUID> unsent) {
        store.complete(instance, acknowledged, refused, unsent, properties.recordBackoffBase(),
                properties.recordBackoffMax(), properties.maxAttempts());
        published.increment(acknowledged.size());
        badRecordFailures.increment(refused.size());
        for (OutboxRelayStore.BadRecord bad : refused) {
            log.warn("outbox event {} refused by Kafka (attempt {}): {}", bad.id(), bad.attemptCount() + 1, bad.error());
        }
        if (infrastructureFailure) {
            infrastructureFailures.increment();
            pause();
        } else if (!acknowledged.isEmpty() && outageStreak > 0) {
            log.info("Kafka reachable again; outbox publishing resumed");
            outageStreak = 0;
            backoffMillis.set(0);
        }
        return new Round(claimed.size(), acknowledged.size(), refused.size(), infrastructureFailure);
    }

    private void pause() {
        Duration delay = JdbcOutboxStore.backoff(outageStreak, properties.outageBackoffBase(), properties.outageBackoffMax());
        outageStreak++;
        backoffMillis.set(delay.toMillis());
        pausedUntil = clock.instant().plus(delay);
        if (outageStreak == 1) {
            log.warn("Kafka unavailable or misconfigured; outbox events stay pending and publishing pauses");
        }
    }

    /** Current pause after an outage (zero when publishing normally). */
    Duration currentBackoff() {
        return Duration.ofMillis(backoffMillis.get());
    }

    private Producer<String, String> producer() {
        if (producer == null) {
            producer = producerFactory.get();
        }
        return producer;
    }

    private static Throwable await(Future<RecordMetadata> future, long deadline) {
        try {
            future.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            return null;
        } catch (ExecutionException e) {
            return e.getCause() == null ? e : e.getCause();
        } catch (TimeoutException e) {
            future.cancel(false);
            return e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return e;
        }
    }

    /** Failures caused by the event itself; everything else is treated as Kafka being unavailable or misconfigured. */
    static boolean isBadRecord(Throwable error) {
        return error instanceof RecordTooLargeException || error instanceof RecordBatchTooLargeException
                || error instanceof SerializationException || error instanceof InvalidRecordException;
    }

    private static String describe(Throwable error) {
        String message = error.getMessage() == null ? "" : ": " + error.getMessage();
        String text = error.getClass().getSimpleName() + message;
        return text.length() <= 500 ? text : text.substring(0, 500);
    }

    /** The Kafka record: key accountId:orderId, the envelope as the value, tracing and routing headers. */
    private static ProducerRecord<String, String> record(OutboxRelayStore.Claimed event) {
        ProducerRecord<String, String> record = new ProducerRecord<>(event.topic(), event.key(), event.payload());
        record.headers().add(new RecordHeader("eventType", bytes(event.eventType())));
        record.headers().add(new RecordHeader("eventVersion", bytes(Integer.toString(event.eventVersion()))));
        String correlationId = correlationId(event.payload());
        if (correlationId != null) {
            record.headers().add(new RecordHeader("correlationId", bytes(correlationId)));
        }
        return record;
    }

    private static String correlationId(String payload) {
        try {
            JsonNode id = JSON.readTree(payload).get("correlationId");
            return id != null && id.isString() ? id.stringValue() : null;
        } catch (JacksonException e) {
            return null;
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
