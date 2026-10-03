package com.project.trading.news.application;

import com.project.trading.news.domain.NewsEnrichment;
import com.project.trading.news.domain.NewsInsightRepository;
import com.project.trading.news.domain.NewsView;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class NewsReader {
    public record Result(List<NewsView> articles, String status, Instant lastSuccess) { }
    private final NewsService raw;
    private final NewsInsightRepository insights;
    private final MeterRegistry metrics;
    public NewsReader(NewsService raw, NewsInsightRepository insights, MeterRegistry metrics) {
        this.raw = raw; this.insights = insights; this.metrics = metrics;
    }
    public Result list(String symbol, int limit) {
        var result = raw.list(symbol, limit);
        Map<UUID, NewsEnrichment> found;
        try { found = insights.findAll(result.articles().stream().map(a -> a.id()).toList()); }
        catch (org.springframework.dao.DataAccessException exception) {
            metrics.counter("news.enrichment.lookup.failure").increment();
            found = Map.of();
        }
        final var values = found;
        return new Result(result.articles().stream().map(a -> new NewsView(a, values.get(a.id()))).toList(),
                result.status(), result.lastSuccess());
    }
}
