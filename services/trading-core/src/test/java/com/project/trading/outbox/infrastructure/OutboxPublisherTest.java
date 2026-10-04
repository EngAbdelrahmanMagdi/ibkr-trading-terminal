package com.project.trading.outbox.infrastructure;

import com.project.trading.support.MutableClock;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Failure classification of the relay: Kafka being unreachable or misconfigured never consumes an event's attempts;
 * only an event Kafka refuses for itself does.
 */
class OutboxPublisherTest {

    private static final String PAYLOAD = "{\"eventId\":\"x\",\"correlationId\":\"3f1c2a9e-4b7d-4c8e-9f0a-1b2c3d4e5f60\"}";

    /** Records what the relay persists; claim returns the prepared events once. */
    private static final class FakeStore implements OutboxRelayStore {
        List<Claimed> next = List.of();
        int claims;
        final List<UUID> published = new ArrayList<>();
        final List<BadRecord> badRecords = new ArrayList<>();
        final List<UUID> released = new ArrayList<>();

        @Override
        public List<Claimed> claim(UUID instance, int batchSize, Duration lease) {
            claims++;
            List<Claimed> out = next.stream().limit(batchSize).toList();
            next = List.of();
            return out;
        }

        @Override
        public void complete(UUID instance, Collection<UUID> published, Collection<BadRecord> badRecords,
                             Collection<UUID> released, Duration base, Duration max, int maxAttempts) {
            this.published.addAll(published);
            this.badRecords.addAll(badRecords);
            this.released.addAll(released);
        }
    }

    private final MutableClock clock = new MutableClock(Instant.parse("2026-09-28T10:00:00Z"));
    private final FakeStore store = new FakeStore();
    private final MockProducer<String, String> producer =
            new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
    private final OutboxProperties properties = new OutboxProperties("localhost:9092", true, 100,
            Duration.ofMillis(500), Duration.ofSeconds(60), 10, Duration.ofSeconds(5), Duration.ofMinutes(10),
            Duration.ofSeconds(1), Duration.ofSeconds(60), Duration.ofSeconds(5), Duration.ofSeconds(15),
            Duration.ofSeconds(5), Duration.ofDays(7), Duration.ofHours(1), 1000, 20);
    private final OutboxPublisher publisher =
            new OutboxPublisher(store, () -> producer, properties, clock, new SimpleMeterRegistry());

    private static OutboxRelayStore.Claimed event(int attempts) {
        return new OutboxRelayStore.Claimed(UUID.randomUUID(), "trading.order-events.v1", "ACC:1", "ORDER_FILLED", 1,
                PAYLOAD, attempts, Instant.parse("2026-09-28T09:59:59Z"), false);
    }

    @Test
    void acknowledgedEventsArePublishedWithTheirKeyAndHeaders() {
        OutboxRelayStore.Claimed event = event(0);
        store.next = List.of(event);

        OutboxPublisher.Round round = publisher.round(100);

        assertThat(round.published()).isEqualTo(1);
        assertThat(store.published).containsExactly(event.id());
        assertThat(producer.history()).singleElement().satisfies(r -> {
            assertThat(r.key()).isEqualTo("ACC:1");
            assertThat(r.value()).isEqualTo(PAYLOAD);
            assertThat(r.headers().lastHeader("eventType").value()).asString().isEqualTo("ORDER_FILLED");
            assertThat(r.headers().lastHeader("correlationId")).isNotNull();
        });
    }

    static Stream<RuntimeException> infrastructureFailures() {
        return Stream.of(new TimeoutException("Topic not present in metadata after 5000 ms"),
                new TopicAuthorizationException(Set.of("trading.order-events.v1")),
                new UnknownTopicOrPartitionException("unknown topic"));
    }

    @ParameterizedTest
    @MethodSource("infrastructureFailures")
    void kafkaUnavailableOrMisconfiguredLeavesEventsPendingAndPausesTheRelay(RuntimeException failure) {
        OutboxRelayStore.Claimed event = event(3);
        store.next = List.of(event);
        producer.sendException = failure;

        OutboxPublisher.Round round = publisher.round(100);

        assertThat(round.infrastructureFailure()).isTrue();
        assertThat(store.released).containsExactly(event.id());
        assertThat(store.badRecords).isEmpty();
        assertThat(store.published).isEmpty();
        assertThat(publisher.currentBackoff()).isBetween(Duration.ofMillis(500), Duration.ofSeconds(1));

        int claims = store.claims;
        publisher.runOnce();
        assertThat(store.claims).as("no round while paused").isEqualTo(claims);

        clock.advance(Duration.ofSeconds(2));
        producer.sendException = null;
        store.next = List.of(event(0), event(0));
        publisher.runOnce();
        assertThat(store.published).as("a single probe after the pause").hasSize(1);
        assertThat(publisher.currentBackoff()).isZero();
    }

    @Test
    void anEventKafkaRefusesForItselfConsumesAnAttempt() {
        OutboxRelayStore.Claimed event = event(4);
        store.next = List.of(event);
        producer.sendException = new RecordTooLargeException("sentinel-private-token broker-response");

        OutboxPublisher.Round round = publisher.round(100);

        assertThat(round.infrastructureFailure()).isFalse();
        assertThat(store.badRecords).singleElement().satisfies(bad -> {
            assertThat(bad.id()).isEqualTo(event.id());
            assertThat(bad.attemptCount()).isEqualTo(4);
            assertThat(bad.error()).isEqualTo("RecordTooLargeException")
                    .doesNotContain("sentinel-private-token", "broker-response");
        });
        assertThat(store.released).isEmpty();
        assertThat(publisher.currentBackoff()).isZero();
    }

    @Test
    void outageBackoffIsCapped() {
        for (int i = 0; i < 20; i++) {
            store.next = List.of(event(0));
            producer.sendException = new TimeoutException("down");
            publisher.round(1);
        }
        assertThat(publisher.currentBackoff()).isBetween(Duration.ofSeconds(30), Duration.ofSeconds(60));
    }
}
