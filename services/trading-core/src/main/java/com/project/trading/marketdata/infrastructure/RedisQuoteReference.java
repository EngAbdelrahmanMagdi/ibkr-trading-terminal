package com.project.trading.marketdata.infrastructure;

import com.project.trading.marketdata.domain.QuoteReferencePort;
import com.project.trading.marketdata.domain.ReferenceQuote;
import com.project.trading.shared.domain.Price;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Optional;

/**
 * Reads quotes that the Realtime Gateway writes to Redis under {@code quote:{SOURCE}:{SYMBOL}}. Redis is a
 * disposable cache: any failure or malformed value is treated as "no quote" (never as a price).
 */
public class RedisQuoteReference implements QuoteReferencePort {

    private static final Logger log = LoggerFactory.getLogger(RedisQuoteReference.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final String keyPrefix;
    private final Counter hits;
    private final Counter misses;
    private final Counter errors;

    public RedisQuoteReference(StringRedisTemplate redis, ObjectMapper mapper, String source, MeterRegistry registry) {
        this.redis = redis;
        this.mapper = mapper;
        this.keyPrefix = "quote:" + source + ":";
        this.hits = Counter.builder("redis.hit").description("Quote cache hits").register(registry);
        this.misses = Counter.builder("redis.miss").description("Quote cache misses").register(registry);
        this.errors = Counter.builder("redis.errors").description("Quote cache read or decode failures")
                .register(registry);
    }

    @Override
    public Optional<ReferenceQuote> latest(String symbol) {
        String raw;
        try {
            raw = redis.opsForValue().get(keyPrefix + symbol);
        } catch (RuntimeException e) {
            errors.increment();
            log.warn("quote cache read failed category={}", e.getClass().getSimpleName());
            return Optional.empty();
        }
        if (raw == null) {
            misses.increment();
            return Optional.empty();
        }
        try {
            JsonNode node = mapper.readTree(raw);
            ReferenceQuote quote = new ReferenceQuote(symbol, price(node, "bid"), price(node, "ask"),
                    price(node, "last"), Instant.parse(node.get("timestamp").asString()),
                    node.path("stale").asBoolean(true), "REALTIME".equals(node.path("dataMode").asString("")),
                    node.path("halted").asBoolean(false));
            hits.increment();
            return Optional.of(quote);
        } catch (RuntimeException e) {
            errors.increment();
            log.warn("quote cache value could not be decoded for {}", symbol);
            return Optional.empty();
        }
    }

    private static Price price(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        return Price.of(value.asString());
    }
}
