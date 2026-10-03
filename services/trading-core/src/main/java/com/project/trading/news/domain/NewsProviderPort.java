package com.project.trading.news.domain;

import java.time.Instant;
import java.util.List;

public interface NewsProviderPort {
    String name();
    List<NewsArticle> fetch(String symbol, Instant from, Instant deadline);
}
