package com.project.trading.outbox.infrastructure;

import com.project.trading.outbox.application.OutboxMessage;
import com.project.trading.news.application.NewsIngestion;
import com.project.trading.news.domain.NewsProviderPort;
import com.project.trading.support.ContractSchemas;
import com.project.trading.support.TradingInfrastructure;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The outbox end to end against real PostgreSQL, Redis and Kafka (topics created explicitly, auto-creation off).
 * The relay is driven by the test, so every step is deterministic.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "app.outbox.publisher-enabled=false",
        "app.outbox.request-timeout=2s",
        "app.outbox.delivery-timeout=4s",
        "app.outbox.max-block=2s",
        "app.outbox.lease=15s",
        "app.outbox.outage-backoff-max=2s",
        "management.server.port=0",
        "spring.data.redis.timeout=5s",
        "spring.data.redis.connect-timeout=5s",
        "SECRETS_DIR=/nonexistent/"})
class OutboxKafkaIntegrationTest {

    private static final String ORDER_TOPIC = "trading.order-events.v1";
    private static final String EXECUTION_TOPIC = "trading.execution-events.v1";

    static final TradingInfrastructure INFRASTRUCTURE = TradingInfrastructure.start();
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    static {
        KAFKA.start();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(ORDER_TOPIC, 3, (short) 1), new NewTopic(EXECUTION_TOPIC, 3, (short) 1),
                    new NewTopic("news.raw.v1", 3, (short) 1)))
                    .all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        INFRASTRUCTURE.register(registry);
        registry.add("app.outbox.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @AfterAll
    static void stopKafka() {
        KAFKA.stop();
    }

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Autowired
    private Environment environment;
    @Autowired
    private StringRedisTemplate redis;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private JdbcOutboxStore store;
    @Autowired
    private OutboxProperties properties;
    @Autowired
    private PlatformTransactionManager transactionManager;
    @Autowired
    private NewsIngestion newsIngestion;
    @Autowired
    private NewsProviderPort newsProvider;

    @Test
    void newsRetrievalPersistsDeduplicatesCachesAndPublishesContractEventsAtomically() throws Exception {
        String symbol = "NEWS";
        String correlation = UUID.randomUUID().toString();
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + environment.getProperty("local.server.port")
                + "/api/v1/news?symbol=" + symbol + "&limit=20")).header("X-Correlation-Id", correlation).GET().build();
        var first = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(first.statusCode()).as(first.body()).isEqualTo(200);
        assertThat(first.headers().firstValue("X-News-Status")).contains("FRESH");
        assertThat(first.headers().firstValue("X-News-Last-Refreshed-At")).isPresent();
        var articles = JSON.readTree(first.body());
        ContractSchemas.assertEachValid("news/news-article.schema.json", articles);
        assertThat(articles.size()).isEqualTo(2);
        assertThat(redis.opsForValue().get("news:recent:FIXTURE:" + symbol)).isNotNull();
        var repeat = newsProvider.fetch(symbol, Instant.now().minus(Duration.ofDays(7)), Instant.now().plusSeconds(8));
        newsIngestion.ingest(newsProvider.name(), symbol, repeat, Instant.now(), correlation);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM news_articles WHERE primary_symbol=?", Long.class, symbol)).isEqualTo(2);
        var rows = jdbc.queryForList("SELECT id,payload::text FROM outbox_events WHERE aggregate_type='NEWS_ARTICLE' AND record_key=?", symbol);
        assertThat(rows).hasSize(2);
        for (var row : rows) {
            var event = JSON.readTree(row.get("payload").toString());
            ContractSchemas.assertValid("events/news-raw.schema.json", event);
            assertThat(event.path("correlationId").asString()).isEqualTo(correlation);
            assertThat(event.path("eventId").asString()).isEqualTo(row.get("id").toString());
        }
        var second = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(JSON.readTree(second.body())).isEqualTo(articles);
        // Force an outer transaction rollback: the article and its outbox entry must both disappear.
        var rolledBack = newsProvider.fetch("ROLLBACK", Instant.now().minus(Duration.ofDays(7)), Instant.now().plusSeconds(8));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new TransactionTemplate(transactionManager).execute(status -> {
            newsIngestion.admit(newsProvider.name(), "ROLLBACK", Instant.now());
            newsIngestion.ingest(newsProvider.name(), "ROLLBACK", rolledBack, Instant.now(), correlation);
            throw new IllegalStateException("Rollback test");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM news_articles WHERE primary_symbol='ROLLBACK'", Long.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE record_key='ROLLBACK'", Long.class)).isZero();
        OutboxPublisher publisher = relay();
        for (int round = 0; round < 4; round++) publisher.runOnce();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE record_key=? AND status='PUBLISHED'", Long.class, symbol)).isEqualTo(2);
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "news-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            consumer.subscribe(List.of("news.raw.v1"));
            var received = new ArrayList<ConsumerRecord<String, String>>();
            Instant deadline = Instant.now().plusSeconds(15);
            while (received.size() < 2 && Instant.now().isBefore(deadline))
                consumer.poll(Duration.ofMillis(200)).forEach(received::add);
            assertThat(received).hasSize(2);
            for (var record : received) {
                assertThat(record.key()).isEqualTo(symbol);
                ContractSchemas.assertValid("events/news-raw.schema.json", JSON.readTree(record.value()));
            }
        }
    }

    private OutboxPublisher relay() {
        return new OutboxPublisher(store, OutboxConfiguration.producerFactory(properties), properties, Clock.systemUTC(),
                new SimpleMeterRegistry());
    }

    private HttpResponse<String> placeOrder(String json) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + environment.getProperty("local.server.port")
                        + "/api/v1/orders")).timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json").header("Idempotency-Key", UUID.randomUUID().toString())
                .POST(HttpRequest.BodyPublishers.ofString(json)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private void freshQuote() {
        redis.opsForValue().set("quote:MOCK:NVDA", "{\"type\":\"quote\",\"symbol\":\"NVDA\",\"bid\":\"185.20\",\"ask\":\"185.30\","
                + "\"last\":\"185.25\",\"bidSize\":100,\"askSize\":100,\"volume\":1000,\"sequence\":1,\"timestamp\":\""
                + Instant.now() + "\",\"stale\":false,\"dataMode\":\"REALTIME\",\"halted\":false}", Duration.ofMinutes(5));
    }

    private List<Map<String, Object>> rows(String orderId) {
        return jdbc.queryForList("select id, topic, record_key, event_type, status, attempt_count from outbox_events"
                + " where aggregate_id = ?::uuid order by seq", orderId);
    }

    @Test
    void kafkaCanBeStoppedAndRestartedWithoutLosingCommittedEvents() throws Exception {
        OutboxPublisher relay = relay();
        long before = jdbc.queryForObject("select count(*) from outbox_events", Long.class);
        HttpResponse<String> invalid = placeOrder("{\"symbol\":\"NVDA\",\"intent\":\"BUY\",\"orderType\":\"MARKET\","
                + "\"quantity\":\"0\",\"timeInForce\":\"DAY\"}");
        assertThat(invalid.statusCode()).isEqualTo(422);
        assertThat(jdbc.queryForObject("select count(*) from outbox_events", Long.class))
                .as("a refused order writes no event").isEqualTo(before);

        KAFKA.getDockerClient().pauseContainerCmd(KAFKA.getContainerId()).exec();
        String orderId;
        try {
            freshQuote();
            HttpResponse<String> placed = placeOrder("{\"symbol\":\"NVDA\",\"intent\":\"BUY\",\"orderType\":\"MARKET\","
                    + "\"quantity\":\"10\",\"timeInForce\":\"DAY\"}");
            assertThat(placed.statusCode()).as(placed.body()).isEqualTo(201);
            orderId = JSON.readTree(placed.body()).get("id").stringValue();
            assertThat(rows(orderId)).extracting(r -> r.get("event_type")).containsExactly(
                    "ORDER_SUBMISSION_PENDING", "ORDER_SUBMITTED", "ORDER_FILLED", "EXECUTION_RECORDED");

            OutboxPublisher.Round round = relay.round(100);
            assertThat(round.infrastructureFailure()).isTrue();
            assertThat(rows(orderId)).allSatisfy(r -> {
                assertThat(r.get("status")).isEqualTo("PENDING");
                assertThat(r.get("attempt_count")).isEqualTo(0);
            });
        } finally {
            KAFKA.getDockerClient().unpauseContainerCmd(KAFKA.getContainerId()).exec();
        }

        Instant deadline = Instant.now().plusSeconds(60);
        while (rows(orderId).stream().anyMatch(r -> !"PUBLISHED".equals(r.get("status")))) {
            assertThat(Instant.now()).as("outbox drained after Kafka came back").isBefore(deadline);
            relay.runOnce();
            Thread.sleep(200);
        }

        List<ConsumerRecord<String, String>> records = consume(orderId, 4);
        List<String> orderEvents = new ArrayList<>();
        for (ConsumerRecord<String, String> r : records) {
            JsonNode event = JSON.readTree(r.value());
            ContractSchemas.assertValid(r.topic().equals(ORDER_TOPIC) ? "events/trading-order-event.schema.json"
                    : "events/trading-execution-event.schema.json", event);
            ObjectNode headers = JSON.createObjectNode();
            for (Header h : r.headers()) {
                headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
            }
            ContractSchemas.assertValid("events/kafka-headers.schema.json", headers);
            assertThat(r.key()).isEqualTo(event.get("accountId").stringValue() + ":" + orderId);
            assertThat(rows(orderId)).extracting(row -> row.get("id").toString()).contains(event.get("eventId").stringValue());
            if (r.topic().equals(ORDER_TOPIC)) {
                orderEvents.add(event.get("eventType").stringValue());
            }
        }
        assertThat(orderEvents).containsExactly("ORDER_SUBMISSION_PENDING", "ORDER_SUBMITTED", "ORDER_FILLED");
    }

    @Test
    void acknowledgementBeforeDatabaseCompletionReplaysTheSameEventIdentity() {
        String suffix = UUID.randomUUID().toString();
        UUID id = insert("ACK-GAP:" + suffix);
        OutboxRelayStore interrupted = new OutboxRelayStore() {
            @Override public List<Claimed> claim(UUID instance, int batchSize, Duration lease) {
                return store.claim(instance, batchSize, lease);
            }
            @Override public void complete(UUID instance, Collection<UUID> published, Collection<BadRecord> bad,
                                            Collection<UUID> released, Duration base, Duration max, int attempts) {
                assertThat(published).contains(id); // Kafka acknowledgement has already succeeded.
                throw new org.springframework.dao.DataAccessResourceFailureException("Injected completion outage");
            }
        };
        OutboxPublisher first = new OutboxPublisher(interrupted, OutboxConfiguration.producerFactory(properties),
                properties, Clock.systemUTC(), new SimpleMeterRegistry());
        try {
            assertThatThrownBy(() -> first.round(100)).isInstanceOf(org.springframework.dao.DataAccessResourceFailureException.class);
            assertThat(status(id)).isEqualTo("PENDING");
        } finally { first.stop(); }
        // Advance only the isolated test lease, representing a later relay after process death.
        jdbc.update("update outbox_events set claim_expires_at = now() - interval '1 second' where id = ?", id);
        OutboxPublisher restarted = relay();
        try {
            restarted.runOnce();
            assertThat(status(id)).isEqualTo("PUBLISHED");
            List<ConsumerRecord<String, String>> duplicates = consume(suffix, 2);
            assertThat(duplicates).allSatisfy(record -> {
                assertThat(JSON.readTree(record.value()).path("eventId").asString()).isEqualTo(id.toString());
                assertThat(record.key()).isEqualTo("ACK-GAP:" + suffix);
            });
            assertThat(duplicates.get(0).value()).isEqualTo(duplicates.get(1).value());
        } finally { restarted.stop(); }
    }

    @Test
    void claimsKeepPerKeyOrderAreLeasedAndFencedAndPurgeKeepsUnpublishedEvents() {
        String key = "ACC:" + UUID.randomUUID();
        UUID first = insert(key);
        UUID second = insert(key);
        UUID otherKey = insert("ACC:" + UUID.randomUUID());
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();

        List<OutboxRelayStore.Claimed> claimedByA = store.claim(a, 100, Duration.ofSeconds(30));
        assertThat(ids(claimedByA)).contains(first, otherKey).doesNotContain(second);
        assertThat(ids(store.claim(b, 100, Duration.ofSeconds(30)))).as("live leases are skipped")
                .doesNotContain(first, second, otherKey);

        store.complete(b, List.of(first), List.of(), List.of(), Duration.ofSeconds(1), Duration.ofSeconds(1), 1);
        assertThat(status(first)).as("a stale claimer cannot mark").isEqualTo("PENDING");

        jdbc.update("update outbox_events set claim_expires_at = now() - interval '1 second' where id = ?", first);
        List<OutboxRelayStore.Claimed> reclaimed = store.claim(b, 100, Duration.ofSeconds(30));
        assertThat(reclaimed).filteredOn(c -> c.id().equals(first)).singleElement()
                .satisfies(c -> assertThat(c.reclaimed()).isTrue());

        store.complete(b, List.of(), List.of(new OutboxRelayStore.BadRecord(first, 0, "RecordTooLargeException")),
                List.of(), Duration.ofSeconds(1), Duration.ofSeconds(1), 1);
        assertThat(status(first)).isEqualTo("FAILED");
        jdbc.update("update outbox_events set next_attempt_at = now() - interval '1 second', claimed_by = null,"
                + " claim_expires_at = null where id in (?, ?)", second, otherKey);
        assertThat(ids(store.claim(b, 100, Duration.ofSeconds(30)))).as("a FAILED event holds back only its key")
                .contains(otherKey).doesNotContain(second);

        store.complete(b, List.of(otherKey), List.of(), List.of(), Duration.ofSeconds(1), Duration.ofSeconds(1), 1);
        jdbc.update("update outbox_events set published_at = now() - interval '8 days' where id = ?", otherKey);
        store.purge(Duration.ofDays(7), 1000, 10);
        assertThat(jdbc.queryForObject("select count(*) from outbox_events where id in (?, ?, ?)", Long.class,
                first, second, otherKey)).as("only the old published event is purged").isEqualTo(2);
    }

    private UUID insert(String key) {
        UUID id = UUID.randomUUID();
        new TransactionTemplate(transactionManager).executeWithoutResult(s -> store.insert(new OutboxMessage(id, "ORDER",
                UUID.randomUUID(), ORDER_TOPIC, key, "ORDER_SUBMITTED", 1, "{\"eventId\":\"" + id + "\"}"), Instant.now()));
        return id;
    }

    private String status(UUID id) {
        return jdbc.queryForObject("select status from outbox_events where id = ?", String.class, id);
    }

    private static List<UUID> ids(List<OutboxRelayStore.Claimed> claimed) {
        return claimed.stream().map(OutboxRelayStore.Claimed::id).toList();
    }

    private static List<ConsumerRecord<String, String>> consume(String orderId, int expected) {
        List<ConsumerRecord<String, String>> matching = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of(ORDER_TOPIC, EXECUTION_TOPIC));
            Instant deadline = Instant.now().plusSeconds(30);
            while (matching.size() < expected && Instant.now().isBefore(deadline)) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    if (r.key().endsWith(":" + orderId)) {
                        matching.add(r);
                    }
                }
            }
        }
        assertThat(matching).hasSize(expected);
        return matching;
    }
}
