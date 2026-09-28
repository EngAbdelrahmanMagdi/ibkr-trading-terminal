package com.project.trading.broker.infrastructure.kafka;

import com.project.trading.broker.domain.BrokerUpdateInbox;
import com.project.trading.broker.domain.ReconciliationRequests;
import com.project.trading.shared.config.AppProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Wires the consumer of broker order observations; selected only in IBKR_PAPER mode (the simulated broker delivers its
 * updates in-process). Kafka never gates startup or readiness: the consumer connects on its own thread.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "app.runtime-mode", havingValue = "IBKR_PAPER")
@EnableConfigurationProperties(BrokerUpdatesProperties.class)
public class BrokerUpdatesConfiguration {

    @Bean
    BrokerUpdateConsumer brokerUpdateConsumer(BrokerUpdatesProperties properties, AppProperties app,
                                              BrokerUpdateInbox inbox, ReconciliationRequests reconciliation,
                                              MeterRegistry registry) {
        return new BrokerUpdateConsumer(consumerFactory(properties), new BrokerUpdateEvents(app.accountId(), app.currency()),
                inbox, reconciliation, properties, registry);
    }

    /** A new group starts at the earliest retained record: processing is idempotent, so replay is harmless. */
    static Supplier<Consumer<String, String>> consumerFactory(BrokerUpdatesProperties p) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, p.bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, p.group(),
                ConsumerConfig.CLIENT_ID_CONFIG, "trading-core-broker-updates",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, p.maxPollRecords(),
                ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10_000,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return () -> new KafkaConsumer<>(config);
    }
}
