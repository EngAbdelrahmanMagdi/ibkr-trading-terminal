package com.project.trading.support;

import com.project.trading.shared.config.AppProperties;
import com.project.trading.shared.config.RuntimeMode;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

/** Application settings for tests without Spring. */
public final class TestProperties {

    private TestProperties() {
    }

    public static AppProperties app(int maxReplyDepth) {
        return new AppProperties(RuntimeMode.MOCK, false, "TEST-ACCOUNT", "USD",
                new AppProperties.Orders(new BigDecimal("10000"), new BigDecimal("1000000"), Duration.ofSeconds(10),
                        maxReplyDepth, Duration.ofHours(1), Duration.ofSeconds(30), Duration.ofSeconds(30)),
                new AppProperties.Portfolio(new BigDecimal("100000")),
                new AppProperties.Watchlist(List.of(), 200),
                new AppProperties.Queries(100, 500),
                new AppProperties.Http(List.of("http://localhost:3000"), 16384),
                new AppProperties.Idempotency(Duration.ofMillis(200), Duration.ofSeconds(60)));
    }
}
