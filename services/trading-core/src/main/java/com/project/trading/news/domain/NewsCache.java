package com.project.trading.news.domain;

import java.time.Instant;
import java.util.List;

public interface NewsCache {
    record Snapshot(List<NewsArticle> articles, Instant lastSuccess) {
        public Snapshot { articles = List.copyOf(articles); }
    }
    Snapshot get(String provider, String symbol);
    void put(String provider, String symbol, Snapshot snapshot);
}
