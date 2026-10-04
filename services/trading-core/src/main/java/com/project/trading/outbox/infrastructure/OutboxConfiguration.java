package com.project.trading.outbox.infrastructure;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Wires the outbox relay. Kafka never gates startup or readiness: the producer is created lazily by the relay thread,
 * and while Kafka is unavailable events simply stay in the outbox.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(OutboxProperties.class)
public class OutboxConfiguration {

    @Bean
    @ConditionalOnProperty(name = "app.outbox.publisher-enabled", havingValue = "true", matchIfMissing = true)
    OutboxPublisher outboxPublisher(JdbcOutboxStore store, OutboxProperties properties, Clock clock,
                                    MeterRegistry registry) {
        List<String> violations = properties.violations();
        if (!violations.isEmpty()) {
            throw new IllegalStateException("Invalid outbox settings: " + String.join("; ", violations));
        }
        return new OutboxPublisher(store, producerFactory(properties), properties, clock, registry);
    }

    /**
     * An idempotent producer that waits for all in-sync replicas. Its own retries are bounded by the delivery timeout;
     * five in-flight requests per connection keep per-partition ordering with idempotence enabled.
     */
    static Supplier<Producer<String, String>> producerFactory(OutboxProperties p) {
        Map<String, Object> config = Map.ofEntries(
                Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, p.bootstrapServers()),
                Map.entry(ProducerConfig.CLIENT_ID_CONFIG, "trading-core-outbox"),
                Map.entry(ProducerConfig.ACKS_CONFIG, "all"),
                Map.entry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true),
                Map.entry(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5),
                Map.entry(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) p.requestTimeout().toMillis()),
                Map.entry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) p.deliveryTimeout().toMillis()),
                Map.entry(ProducerConfig.MAX_BLOCK_MS_CONFIG, p.maxBlock().toMillis()),
                Map.entry(ProducerConfig.LINGER_MS_CONFIG, 5),
                Map.entry(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10_000),
                Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
                Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
        return () -> new KafkaProducer<>(com.project.trading.shared.config.KafkaTls.configure(config));
    }
}
