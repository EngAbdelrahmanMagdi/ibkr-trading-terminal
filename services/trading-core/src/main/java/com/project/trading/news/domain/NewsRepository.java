package com.project.trading.news.domain;

import java.time.Instant;
import java.util.List;

public interface NewsRepository {
    record State(Instant lastSuccess, String failure) { }
    List<NewsArticle> recent(String provider, String symbol, Instant cutoff, int limit);
    State state(String provider, String symbol);
    boolean admit(String provider, String symbol, Instant now, int maximum);
    boolean insert(NewsArticle article, Instant now, int maximum);
    void associate(NewsArticle article);
    void succeeded(String provider, String symbol, Instant now);
    void failed(String provider, String symbol, Instant now, String classification);
    void purge(Instant cutoff, int ceiling);
}
