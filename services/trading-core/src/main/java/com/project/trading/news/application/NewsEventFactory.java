package com.project.trading.news.application;

import com.project.trading.news.domain.NewsArticle;
import com.project.trading.outbox.application.OutboxMessage;
import com.project.trading.shared.api.ApiFormat;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.UUID;

public class NewsEventFactory {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    public OutboxMessage ingested(NewsArticle article, Instant now, String correlation) {
        UUID eventId = UUID.randomUUID();
        var envelope = JSON.createObjectNode();
        envelope.put("eventId", eventId.toString());
        envelope.put("eventType", "NEWS_ARTICLE_INGESTED");
        envelope.put("eventVersion", 1);
        envelope.put("occurredAt", ApiFormat.instant(now));
        envelope.put("source", "trading-core");
        envelope.put("correlationId", correlation);
        envelope.put("symbol", article.primarySymbol());
        var payload = envelope.putObject("payload");
        payload.put("articleId", article.id().toString());
        payload.put("providerId", article.providerId());
        var symbols = payload.putArray("symbols");
        article.symbols().forEach(symbols::add);
        payload.put("headline", article.headline());
        payload.put("source", article.source());
        payload.put("url", article.url());
        payload.put("publishedAt", ApiFormat.instant(article.publishedAt()));
        payload.put("rawSummary", article.rawSummary());
        payload.put("contentHash", article.contentHash());
        return new OutboxMessage(eventId, "NEWS_ARTICLE", article.id(), "news.raw.v1", article.primarySymbol(),
                "NEWS_ARTICLE_INGESTED", 1, JSON.writeValueAsString(envelope));
    }
}
