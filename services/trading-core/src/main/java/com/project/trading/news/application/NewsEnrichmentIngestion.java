package com.project.trading.news.application;

import com.project.trading.inbox.application.ProcessedEvents;
import com.project.trading.news.domain.NewsInsightRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.transaction.annotation.Transactional;

public class NewsEnrichmentIngestion {
    public static final String CONSUMER = "trading-core-news-enrichment";
    private final ProcessedEvents inbox;
    private final NewsInsightRepository repository;
    private final MeterRegistry metrics;
    public NewsEnrichmentIngestion(ProcessedEvents inbox, NewsInsightRepository repository, MeterRegistry metrics) {
        this.inbox = inbox; this.repository = repository; this.metrics = metrics;
    }
    @Transactional
    public void apply(NewsInsightRepository.Observation observation) {
        if (!inbox.markProcessed(CONSUMER, observation.eventId())) {
            metrics.counter("news.enrichment.consumed", "outcome", "duplicate").increment();
            return;
        }
        var outcome = repository.accept(observation);
        metrics.counter("news.enrichment.consumed", "outcome", outcome.name()).increment();
    }
}
