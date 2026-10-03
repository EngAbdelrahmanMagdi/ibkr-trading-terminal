package com.project.trading.news.infrastructure;

import com.project.trading.news.domain.NewsArticle;
import com.project.trading.news.domain.NewsProviderPort;
import com.project.trading.news.domain.NewsUnavailable;
import io.micrometer.core.instrument.MeterRegistry;
import tools.jackson.databind.json.JsonMapper;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class FinnhubNewsProvider implements NewsProviderPort {
    private final NewsHttpClient http;
    private final NewsProperties properties;
    private final NewsNormalizer normalizer;
    private final NewsFetchPolicy policy;
    private final Clock clock;
    private final MeterRegistry metrics;
    public FinnhubNewsProvider(NewsHttpClient http, NewsProperties properties, NewsNormalizer normalizer,
                               NewsFetchPolicy policy, Clock clock, MeterRegistry metrics) {
        this.http = http; this.properties = properties; this.normalizer = normalizer;
        this.policy = policy; this.clock = clock; this.metrics = metrics;
        metrics.gauge("news.provider.circuit", policy, NewsFetchPolicy::state);
    }
    @Override public String name() { return "FINNHUB"; }
    @Override public List<NewsArticle> fetch(String symbol, Instant from, Instant deadline) {
        policy.enter();
        String query = "symbol=" + encode(symbol) + "&from=" + from.atOffset(ZoneOffset.UTC).toLocalDate()
                + "&to=" + clock.instant().atOffset(ZoneOffset.UTC).toLocalDate() + "&token=" + encode(properties.finnhubApiKey());
        String failure = "TRANSPORT";
        long cooldown = 0;
        try {
            for (int attempt = 0; attempt < properties.attempts(); attempt++) {
                policy.acquire(deadline);
                try {
                    var response = http.get(query, deadline);
                    int status = response.statusCode();
                    metrics.counter("external.api.requests", "provider", "finnhub", "endpoint", "company-news",
                            "outcome", status == 200 ? "ok" : status == 429 ? "rate_limited" : "error").increment();
                    if (status == 429) {
                        cooldown = retryAfter(response.headers().firstValue("Retry-After").orElse("60"));
                        throw new NewsUnavailable("RATE_LIMIT");
                    }
                    if (status == 401 || status == 403) throw new NewsUnavailable("AUTH");
                    if (status >= 500) throw new NewsUnavailable("UPSTREAM");
                    if (status != 200) throw new NewsUnavailable("RESPONSE");
                    var articles = parse(response.body(), symbol, from);
                    policy.success();
                    return articles;
                } catch (NewsUnavailable exception) {
                    failure = exception.classification();
                    if (failure.equals("TRANSPORT")) metrics.counter("external.api.requests", "provider", "finnhub",
                            "endpoint", "company-news", "outcome", "transport").increment();
                    if (!(failure.equals("TRANSPORT") || failure.equals("UPSTREAM")) || attempt + 1 == properties.attempts()) throw exception;
                    long pause = ThreadLocalRandom.current().nextLong(100, 301);
                    if (!clock.instant().plusMillis(pause).isBefore(deadline)) throw new NewsUnavailable("DEADLINE");
                    try { TimeUnit.MILLISECONDS.sleep(pause); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new NewsUnavailable("INTERRUPTED"); }
                }
            }
        } catch (NewsUnavailable exception) {
            policy.failure(exception.classification(), cooldown);
            throw exception;
        }
        throw new NewsUnavailable(failure);
    }
    private List<NewsArticle> parse(byte[] bytes, String symbol, Instant from) {
        final tools.jackson.databind.JsonNode root;
        try { root = JsonMapper.builder().build().readTree(bytes); }
        catch (tools.jackson.core.JacksonException exception) { throw new NewsUnavailable("MALFORMED"); }
        if (root == null || !root.isArray() || root.size() > properties.maxRecords()) throw new NewsUnavailable("MALFORMED");
        var articles = new ArrayList<NewsArticle>();
        for (var record : root) {
            try {
                if (!record.isObject() || !record.path("datetime").isIntegralNumber()
                        || !record.path("headline").isString() || !record.path("source").isString()
                        || !record.path("url").isString()) throw new IllegalArgumentException("record");
                var article = normalizer.normalize(name(), record.path("id").isIntegralNumber() ? record.path("id").asString() : null,
                        symbol, record.path("related").asString(), record.path("headline").asString(), record.path("source").asString(),
                        record.path("url").asString(), Instant.ofEpochSecond(record.path("datetime").asLong()),
                        record.path("summary").isString() ? record.path("summary").asString() : null, from, clock.instant());
                articles.add(article);
            } catch (IllegalArgumentException | java.time.DateTimeException exception) {
                metrics.counter("news.articles", "provider", name(), "outcome", "invalid").increment();
            }
        }
        if (root.size() > 0 && articles.isEmpty()) throw new NewsUnavailable("MALFORMED");
        return List.copyOf(articles);
    }
    private long retryAfter(String value) {
        try { return Math.min(3600, Math.max(1, Long.parseLong(value))); }
        catch (NumberFormatException exception) {
            try { return Math.min(3600, Math.max(1, java.time.Duration.between(clock.instant(),
                    java.time.ZonedDateTime.parse(value, java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME).toInstant()).toSeconds())); }
            catch (java.time.DateTimeException invalid) { return 60; }
        }
    }
    private static String encode(String value) { return URLEncoder.encode(value, StandardCharsets.UTF_8); }
}
