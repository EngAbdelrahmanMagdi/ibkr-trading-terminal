package com.project.trading.integration;

import com.project.trading.news.application.NewsEnrichmentIngestion;
import com.project.trading.news.application.NewsReader;
import com.project.trading.news.application.NewsService;
import com.project.trading.news.domain.NewsEnrichment;
import com.project.trading.news.domain.NewsInsightRepository;
import com.project.trading.support.TradingInfrastructure;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.dao.DataAccessResourceFailureException;
import io.micrometer.core.instrument.MeterRegistry;
import tools.jackson.databind.json.JsonMapper;
import org.testcontainers.kafka.KafkaContainer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doThrow;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "app.outbox.publisher-enabled=false", "SECRETS_DIR=/nonexistent/", "management.server.port=0"})
class NewsEnrichmentIntegrationTest {
    static final TradingInfrastructure INFRA = TradingInfrastructure.start();
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    static {
        KAFKA.start();
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic("news.enriched.v1", 1, (short) 1),
                    new NewTopic("news.enriched.v1.dlq", 1, (short) 1))).all().get();
        } catch (Exception e) { throw new IllegalStateException(e); }
    }
    @DynamicPropertySource static void properties(DynamicPropertyRegistry r) {
        INFRA.register(r); r.add("app.outbox.bootstrap-servers", KAFKA::getBootstrapServers);
    }
    @AfterAll static void close() { KAFKA.stop(); }
    @Autowired NewsService raw;
    @Autowired NewsReader reader;
    @Autowired NewsEnrichmentIngestion ingestion;
    @Autowired JdbcTemplate jdbc;
    @Autowired MeterRegistry metrics;
    @MockitoSpyBean NewsInsightRepository repository;

    @Test void firstIdentityWinsRawCacheHydratesAndPoisonIsPreserved() throws Exception {
        var article = raw.list("NVDA", 2).articles().getFirst();
        var insight = Map.<String, Object>of("summary", "Synthetic interpretation", "sentiment", "NEUTRAL",
                "sentimentScore", 0.0, "relevanceScore", 0.5, "confidence", 0.4, "catalysts", List.of(),
                "evidence", List.of(article.id().toString()), "flags", List.of("LOW_CONFIDENCE"));
        var first = new NewsEnrichment("news-insight.v1", "synthetic-news.v1", "1", Instant.parse("2026-10-03T12:00:00Z"), insight);
        UUID event = UUID.randomUUID();
        var observation = new NewsInsightRepository.Observation(event, article.id(), article.contentHash(), "NVDA", first);
        ingestion.apply(observation); ingestion.apply(observation);
        var other = new NewsEnrichment("arbitrary-later-name", "different-model", "999", Instant.now().plusSeconds(500), insight);
        ingestion.apply(new NewsInsightRepository.Observation(UUID.randomUUID(), article.id(), article.contentHash(), "NVDA", other));
        assertThat(reader.list("NVDA", 2).articles().stream().filter(v -> v.article().id().equals(article.id())).findFirst()
                .orElseThrow().enrichment()).isEqualTo(first);
        UUID bad = UUID.randomUUID();
        assertThatThrownBy(() -> ingestion.apply(new NewsInsightRepository.Observation(bad, article.id(), "0".repeat(64), "NVDA", first)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_events WHERE event_id=?", Long.class, bad)).isZero();
        byte[] poison = new byte[]{0, 1, 2, 3};
        try (var producer = new KafkaProducer<byte[], byte[]>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "key.serializer", ByteArraySerializer.class, "value.serializer", ByteArraySerializer.class));
             var consumer = new KafkaConsumer<byte[], byte[]>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                     "group.id", "poison-test-" + UUID.randomUUID(), "auto.offset.reset", "earliest",
                     "key.deserializer", ByteArrayDeserializer.class, "value.deserializer", ByteArrayDeserializer.class))) {
            producer.send(new ProducerRecord<>("news.enriched.v1", "NVDA".getBytes(java.nio.charset.StandardCharsets.UTF_8), poison)).get();
            consumer.subscribe(List.of("news.enriched.v1.dlq"));
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                var records = consumer.poll(Duration.ofMillis(500));
                assertThat(records.count()).isPositive();
                var record = records.iterator().next();
                assertThat(record.value()).isEqualTo(poison);
                assertThat(record.headers().lastHeader("dlq.sourceOffset")).isNotNull();
            });

            var second = raw.list("NVDA", 2).articles().get(1);
            UUID delivered = UUID.randomUUID();
            var secondInsight = new java.util.LinkedHashMap<>(insight);
            secondInsight.put("evidence", List.of(second.id().toString()));
            var expected = new NewsInsightRepository.Observation(delivered, second.id(), second.contentHash(), "NVDA",
                    new NewsEnrichment("news-insight.v1", "synthetic-news.v1", "1", first.enrichedAt(), secondInsight));
            doThrow(new DataAccessResourceFailureException("temporary database outage")).doCallRealMethod()
                    .when(repository).accept(expected);
            var envelope = Map.of("eventId", delivered.toString(), "eventType", "NEWS_ARTICLE_ENRICHED", "eventVersion", 1,
                    "occurredAt", first.enrichedAt().toString(), "source", "ai-insights", "correlationId", UUID.randomUUID().toString(),
                    "symbol", "NVDA", "payload", Map.of("articleId", second.id().toString(), "contentHash", second.contentHash(),
                            "enrichment", Map.of("promptVersion", "news-insight.v1", "model", "synthetic-news.v1", "modelVersion", "1",
                                    "enrichedAt", first.enrichedAt().toString(), "insight", secondInsight)));
            producer.send(new ProducerRecord<>("news.enriched.v1", "NVDA".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    JsonMapper.builder().build().writeValueAsBytes(envelope))).get();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(metrics.counter("news.enrichment.infrastructure.failure").count()).isPositive();
                assertThat(jdbc.queryForObject("SELECT count(*) FROM news_insights WHERE article_id=?", Long.class, second.id())).isOne();
                assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_events WHERE event_id=?", Long.class, delivered)).isOne();
            });
            assertThat(consumer.poll(Duration.ofSeconds(1)).count()).isZero();
        }
    }
}
