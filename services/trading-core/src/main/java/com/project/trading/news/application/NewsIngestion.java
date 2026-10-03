package com.project.trading.news.application;

import com.project.trading.news.domain.NewsArticle;
import com.project.trading.news.domain.NewsPolicy;
import com.project.trading.news.domain.NewsRepository;
import com.project.trading.news.domain.NewsUnavailable;
import com.project.trading.outbox.application.OutboxAppender;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

public class NewsIngestion {
    private final NewsRepository repository;
    private final OutboxAppender outbox;
    private final NewsPolicy policy;
    private final MeterRegistry metrics;
    private final NewsEventFactory events = new NewsEventFactory();
    public NewsIngestion(NewsRepository repository, OutboxAppender outbox, NewsPolicy policy, MeterRegistry metrics) {
        this.repository = repository;
        this.outbox = outbox;
        this.policy = policy;
        this.metrics = metrics;
    }
    @Transactional
    public void admit(String provider, String symbol, Instant now) {
        repository.purge(now.minus(policy.retention()), policy.maxArticles());
        if (!repository.admit(provider, symbol, now, policy.maxSymbols())) throw new NewsUnavailable("CAPACITY");
    }
    @Transactional
    public void ingest(String provider, String symbol, List<NewsArticle> articles, Instant now, String correlation) {
        repository.purge(now.minus(policy.retention()), policy.maxArticles());
        for (NewsArticle article : articles) {
            boolean inserted = repository.insert(article, now, policy.maxArticles());
            repository.associate(article);
            if (inserted) outbox.append(events.ingested(article, now, correlation));
            metrics.counter("news.articles", "provider", provider, "outcome", inserted ? "accepted" : "duplicate")
                    .increment();
        }
        repository.succeeded(provider, symbol, now);
    }
    @Transactional
    public void failure(String provider, String symbol, Instant now, String failure) {
        repository.failed(provider, symbol, now, failure);
    }
    @Transactional
    public void cleanup(Instant now) {
        repository.purge(now.minus(policy.retention()), policy.maxArticles());
    }
}
