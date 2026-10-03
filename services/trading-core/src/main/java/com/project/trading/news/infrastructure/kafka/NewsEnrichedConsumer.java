package com.project.trading.news.infrastructure.kafka;

import com.project.trading.news.application.NewsEnrichmentIngestion;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.context.SmartLifecycle;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class NewsEnrichedConsumer implements SmartLifecycle {
    private final Supplier<Consumer<byte[], byte[]>> factory;
    private final NewsEnrichedEvents events;
    private final NewsEnrichmentIngestion ingestion;
    private final NewsEnrichedDeadLetters deadLetters;
    private final MeterRegistry metrics;
    private volatile boolean running;
    private volatile Consumer<byte[], byte[]> consumer;
    private Thread thread;
    public NewsEnrichedConsumer(Supplier<Consumer<byte[], byte[]>> factory, NewsEnrichedEvents events,
                               NewsEnrichmentIngestion ingestion, NewsEnrichedDeadLetters deadLetters, MeterRegistry metrics) {
        this.factory = factory; this.events = events; this.ingestion = ingestion;
        this.deadLetters = deadLetters; this.metrics = metrics;
    }
    @Override public void start() {
        running = true;
        thread = Thread.ofPlatform().daemon().name("news-enriched-consumer").start(this::loop);
    }
    private void loop() {
        try (var client = factory.get()) {
            consumer = client;
            client.subscribe(List.of("news.enriched.v1"));
            int failures = 0;
            boolean rewind = false;
            while (running) {
                try {
                    if (rewind) {
                        for (var tp : client.assignment()) {
                            var committed = client.committed(java.util.Set.of(tp), Duration.ofSeconds(5)).get(tp);
                            if (committed == null) client.seekToBeginning(List.of(tp));
                            else client.seek(tp, committed.offset());
                        }
                        client.resume(client.assignment());
                        rewind = false;
                    }
                    for (var record : client.poll(Duration.ofSeconds(1))) {
                        try {
                            var parsed = events.parse(record.value(), record.key() == null ? null :
                                    new String(record.key(), StandardCharsets.UTF_8));
                            ingestion.apply(parsed);
                        } catch (NewsEnrichedEvents.Unknown unknown) {
                            metrics.counter("news.enrichment.consumed", "outcome", "unknown").increment();
                        } catch (IllegalArgumentException | tools.jackson.core.JacksonException invalid) {
                            deadLetters.send(record);
                            metrics.counter("news.enrichment.dead.letters").increment();
                        }
                        client.commitSync(Map.of(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)), Duration.ofSeconds(5));
                    }
                    failures = 0;
                } catch (WakeupException e) {
                    if (running) throw e;
                } catch (RuntimeException failure) {
                    rewind = true;
                    metrics.counter("news.enrichment.infrastructure.failure").increment();
                    var assigned = client.assignment();
                    client.pause(assigned);
                    long until = System.nanoTime() + Duration.ofSeconds(Math.min(30, 1L << Math.min(failures++, 5))).toNanos();
                    while (running && System.nanoTime() < until) {
                        try { client.poll(Duration.ofMillis(250)); }
                        catch (RuntimeException unavailable) { break; }
                    }
                }
            }
        } catch (WakeupException e) {
            if (running) metrics.counter("news.enrichment.infrastructure.failure").increment();
        } finally { consumer = null; running = false; }
    }
    @Override public void stop() {
        running = false;
        if (consumer != null) consumer.wakeup();
        if (thread != null) try { thread.join(15000); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
    @Override public boolean isRunning() { return running; }
}
