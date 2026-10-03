package com.project.trading.news.infrastructure;

import com.project.trading.news.domain.NewsPolicy;
import org.springframework.boot.context.properties.ConfigurationProperties;
import java.time.Duration;

@ConfigurationProperties("app.news")
public record NewsProperties(String provider, boolean privateDevelopment, String finnhubApiKey,
                             Duration freshness, Duration window, Duration deadline, int workers, int queueCapacity,
                             Duration connectTimeout, Duration attemptTimeout, int attempts, int requestsPerMinute,
                             int maxResponseBytes, int maxRecords, int maxSymbols, Duration retention, int maxArticles,
                             int breakerFailures, Duration breakerCooldown, int maxColdRequests) {
    public NewsProperties {
        if (!("FIXTURE".equals(provider) || "FINNHUB".equals(provider))) throw new IllegalArgumentException("Unknown news provider");
        if ("FINNHUB".equals(provider) && (!privateDevelopment || finnhubApiKey == null || finnhubApiKey.isBlank()))
            throw new IllegalArgumentException("Finnhub requires private development and a server-side credential");
        for (Duration duration : new Duration[]{freshness, window, deadline, connectTimeout, attemptTimeout, retention, breakerCooldown})
            if (duration == null || duration.isNegative() || duration.isZero() || duration.compareTo(Duration.ofDays(365)) > 0)
                throw new IllegalArgumentException("Invalid news duration");
        if (workers < 1 || workers > 4 || queueCapacity < 1 || queueCapacity > 1000 || attempts < 1 || attempts > 3
                || requestsPerMinute < 1 || requestsPerMinute > 60 || maxResponseBytes < 1024 || maxResponseBytes > 8 * 1024 * 1024
                || maxRecords < 1 || maxRecords > 500 || maxSymbols < 1 || maxSymbols > 10000
                || maxArticles < 1 || maxArticles > 100000 || breakerFailures < 1 || breakerFailures > 20
                || freshness.compareTo(retention) > 0
                || maxColdRequests < 1 || maxColdRequests > 16
                || connectTimeout.compareTo(deadline) > 0 || attemptTimeout.compareTo(deadline) > 0)
            throw new IllegalArgumentException("Invalid news bounds");
    }
    public NewsPolicy policy() { return new NewsPolicy(freshness, window, deadline, workers, queueCapacity, maxSymbols, retention, maxArticles, maxColdRequests); }
    @Override public String toString() { return "NewsProperties[provider=" + provider + "]"; }
}
