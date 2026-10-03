package com.project.trading.news.infrastructure;

import com.project.trading.news.domain.NewsCache;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.json.JsonMapper;
import java.util.UUID;
import java.time.Instant;

public class RedisNewsCache implements NewsCache {
    private final StringRedisTemplate redis;
    private final NewsProperties properties;
    private final MeterRegistry metrics;
    private static final JsonMapper JSON = JsonMapper.builder().build();
    public RedisNewsCache(StringRedisTemplate redis, NewsProperties properties, MeterRegistry metrics) {
        this.redis = redis; this.properties = properties; this.metrics = metrics;
    }
    @Override public Snapshot get(String provider, String symbol) {
        try {
            String value = redis.opsForValue().get(key(provider, symbol));
            if (value == null || value.length() > 8 * 1024 * 1024) return null;
            var root = JSON.readTree(value);
            var entries = new java.util.ArrayList<com.project.trading.news.domain.NewsArticle>();
            if (!root.path("articles").isArray() || root.path("articles").size() > 500) return null;
            for (var a : root.path("articles")) {
                var symbols = new java.util.ArrayList<String>();
                a.path("symbols").forEach(s -> symbols.add(s.asString()));
                entries.add(new com.project.trading.news.domain.NewsArticle(UUID.fromString(a.path("id").asString()), provider,
                        a.path("providerId").asString(), a.path("primarySymbol").asString(), symbols,
                        a.path("headline").asString(), a.path("source").asString(), a.path("url").asString(),
                        Instant.parse(a.path("publishedAt").asString()), a.path("rawSummary").isNull() ? null : a.path("rawSummary").asString(),
                        a.path("contentHash").asString()));
            }
            return new Snapshot(entries, Instant.parse(root.path("lastSuccess").asString()));
        } catch (RuntimeException exception) { metrics.counter("news.cache.failure").increment(); return null; }
    }
    @Override public void put(String provider, String symbol, Snapshot snapshot) {
        try {
            var root = JSON.createObjectNode();
            root.put("lastSuccess", snapshot.lastSuccess().toString());
            var entries = root.putArray("articles");
            for (var a : snapshot.articles()) {
                var item = entries.addObject();
                item.put("id", a.id().toString()); item.put("providerId", a.providerId());
                item.put("primarySymbol", a.primarySymbol()); var symbols = item.putArray("symbols"); a.symbols().forEach(symbols::add);
                item.put("headline", a.headline()); item.put("source", a.source()); item.put("url", a.url());
                item.put("publishedAt", a.publishedAt().toString()); item.put("rawSummary", a.rawSummary()); item.put("contentHash", a.contentHash());
            }
            String value = JSON.writeValueAsString(root);
            if (value.length() <= 8 * 1024 * 1024) redis.opsForValue().set(key(provider, symbol), value, properties.freshness());
        } catch (RuntimeException exception) { metrics.counter("news.cache.failure").increment(); }
    }
    private static String key(String provider, String symbol) { return "news:recent:" + provider + ":" + symbol; }
}
