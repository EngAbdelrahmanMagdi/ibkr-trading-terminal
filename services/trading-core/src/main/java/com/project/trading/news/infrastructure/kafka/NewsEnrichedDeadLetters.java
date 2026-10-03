package com.project.trading.news.infrastructure.kafka;

import com.project.trading.news.application.NewsEnrichmentIngestion;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class NewsEnrichedDeadLetters {
    private final Producer<byte[], byte[]> producer;
    private final Clock clock;
    public NewsEnrichedDeadLetters(Producer<byte[], byte[]> producer, Clock clock) {
        this.producer = producer; this.clock = clock;
    }
    public void send(ConsumerRecord<byte[], byte[]> record) {
        var headers = new RecordHeaders();
        int bytes = 0;
        int count = 0;
        for (var header : record.headers()) {
            String name = header.key().toLowerCase(Locale.ROOT);
            if (name.startsWith("dlq.") || name.contains("authorization") || name.contains("cookie")
                    || name.contains("secret") || name.contains("token") || name.contains("key")) continue;
            int length = header.value() == null ? 0 : header.value().length;
            int nameLength = header.key().getBytes(StandardCharsets.UTF_8).length;
            if (nameLength > 128 || length > 1024 || count >= 32 || bytes + nameLength + length > 8192) continue;
            headers.add(header); bytes += nameLength + length; count++;
        }
        Map.of("dlq.reason", "invalid_enrichment", "dlq.exceptionClass", "InvalidEnrichment",
                "dlq.failedAt", clock.instant().toString(), "dlq.consumer", NewsEnrichmentIngestion.CONSUMER,
                "dlq.sourceTopic", record.topic(), "dlq.sourcePartition", Integer.toString(record.partition()),
                "dlq.sourceOffset", Long.toString(record.offset())).forEach((k, v) -> headers.add(k, v.getBytes(StandardCharsets.UTF_8)));
        try {
            producer.send(new ProducerRecord<>("news.enriched.v1.dlq", null, null, record.key(), record.value(), headers))
                    .get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new IllegalStateException("dead_letter_publication", e);
        }
    }
}
