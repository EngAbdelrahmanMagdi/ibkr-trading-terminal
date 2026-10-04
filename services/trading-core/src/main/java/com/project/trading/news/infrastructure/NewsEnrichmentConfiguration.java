package com.project.trading.news.infrastructure;

import com.project.trading.inbox.application.ProcessedEvents;
import com.project.trading.news.application.NewsEnrichmentIngestion;
import com.project.trading.news.application.NewsReader;
import com.project.trading.news.application.NewsService;
import com.project.trading.news.domain.NewsInsightRepository;
import com.project.trading.news.infrastructure.kafka.NewsEnrichedConsumer;
import com.project.trading.news.infrastructure.kafka.NewsEnrichedEvents;
import com.project.trading.news.infrastructure.kafka.NewsEnrichedDeadLetters;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import java.time.Clock;
import java.util.Map;

@Configuration(proxyBeanMethods = false)
public class NewsEnrichmentConfiguration {
    @Bean NewsInsightRepository newsInsightRepository(NamedParameterJdbcTemplate jdbc) {
        return new PostgresNewsInsightRepository(jdbc);
    }
    @Bean NewsReader newsReader(NewsService raw, NewsInsightRepository insights, MeterRegistry metrics) {
        return new NewsReader(raw, insights, metrics);
    }
    @Bean NewsEnrichmentIngestion newsEnrichmentIngestion(ProcessedEvents inbox, NewsInsightRepository repository, MeterRegistry metrics) {
        return new NewsEnrichmentIngestion(inbox, repository, metrics);
    }
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(name = "news.enrichment.enabled", havingValue = "true", matchIfMissing = true)
    Producer<byte[], byte[]> newsDeadLetterProducer(@Value("${app.outbox.bootstrap-servers}") String brokers) {
        return new KafkaProducer<>(com.project.trading.shared.config.KafkaTls.configure(Map.of("bootstrap.servers", brokers, "acks", "all", "enable.idempotence", true,
                "max.block.ms", 5000, "request.timeout.ms", 5000, "delivery.timeout.ms", 15000,
                "max.request.size", 2097152,
                "key.serializer", ByteArraySerializer.class, "value.serializer", ByteArraySerializer.class)));
    }
    @Bean
    @ConditionalOnProperty(name = "news.enrichment.enabled", havingValue = "true", matchIfMissing = true)
    NewsEnrichedConsumer newsEnrichedConsumer(@Value("${app.outbox.bootstrap-servers}") String brokers,
            NewsEnrichmentIngestion ingestion, Producer<byte[], byte[]> newsDeadLetterProducer, Clock clock, MeterRegistry metrics) {
        return new NewsEnrichedConsumer(() -> new KafkaConsumer<>(com.project.trading.shared.config.KafkaTls.configure(Map.of("bootstrap.servers", brokers,
                "group.id", NewsEnrichmentIngestion.CONSUMER, "enable.auto.commit", false,
                "auto.offset.reset", "earliest", "isolation.level", "read_committed", "max.poll.records", 50,
                "key.deserializer", ByteArrayDeserializer.class, "value.deserializer", ByteArrayDeserializer.class))),
                new NewsEnrichedEvents(), ingestion, new NewsEnrichedDeadLetters(newsDeadLetterProducer, clock), metrics);
    }
}
