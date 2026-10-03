package com.project.trading.news.domain;

import java.time.Duration;

public record NewsPolicy(Duration freshness, Duration window, Duration deadline, int workers, int queueCapacity,
                         int maxSymbols, Duration retention, int maxArticles, int maxColdRequests) {
}
