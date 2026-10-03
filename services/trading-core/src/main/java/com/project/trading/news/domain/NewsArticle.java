package com.project.trading.news.domain;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record NewsArticle(UUID id, String provider, String providerId, String primarySymbol, List<String> symbols,
                          String headline, String source, String url, Instant publishedAt, String rawSummary,
                          String contentHash) {
    public NewsArticle {
        symbols = List.copyOf(symbols);
    }
}
