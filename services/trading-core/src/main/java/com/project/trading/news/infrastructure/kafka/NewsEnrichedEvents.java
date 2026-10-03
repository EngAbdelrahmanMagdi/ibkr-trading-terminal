package com.project.trading.news.infrastructure.kafka;

import com.networknt.schema.InputFormat;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SpecificationVersion;
import com.project.trading.news.domain.NewsEnrichment;
import com.project.trading.news.domain.NewsInsightRepository;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.core.type.TypeReference;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Canonical schema validation over the known projection; unknown future fields are ignored. */
public final class NewsEnrichedEvents {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String BASE = "https://contracts.trading-terminal.invalid/schemas/";
    private static final List<String> FILES = List.of("events/news-enriched.schema.json", "events/envelope.schema.json",
            "news/enrichment.schema.json", "news/news-insight.schema.json", "common/primitives.schema.json");
    private final SchemaRegistry registry;
    public static final class Unknown extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
    public NewsEnrichedEvents() {
        Map<String, String> sources = new LinkedHashMap<>();
        for (String name : FILES) {
            try (var stream = getClass().getResourceAsStream("/contracts/schemas/" + name)) {
                if (stream == null) throw new IllegalStateException("canonical schema missing");
                sources.put(BASE + name, new String(stream.readAllBytes(), StandardCharsets.UTF_8));
            } catch (IOException e) { throw new IllegalStateException("canonical schema unavailable", e); }
        }
        registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12, builder -> builder
                .schemaRegistryConfig(SchemaRegistryConfig.builder().formatAssertionsEnabled(true).build()).schemas(sources));
    }
    private static ObjectNode fields(JsonNode node, Set<String> names) {
        if (!node.isObject()) throw new IllegalArgumentException("shape");
        ObjectNode result = JSON.createObjectNode();
        for (String name : names) if (node.has(name)) result.set(name, node.get(name));
        return result;
    }
    public NewsInsightRepository.Observation parse(byte[] value, String key) {
        if (value == null || value.length > 1048576) throw new IllegalArgumentException("size");
        JsonNode original = JSON.readTree(value);
        if (original == null || !original.isObject()) throw new IllegalArgumentException("shape");
        if (!original.path("eventType").asString().equals("NEWS_ARTICLE_ENRICHED")
                || original.path("eventVersion").asInt() != 1) throw new Unknown();
        var event = fields(original, Set.of("eventId", "eventType", "eventVersion", "occurredAt", "source",
                "correlationId", "symbol", "payload"));
        var payload = fields(event.path("payload"), Set.of("articleId", "contentHash", "enrichment"));
        var enrichment = fields(payload.path("enrichment"), Set.of("promptVersion", "model", "modelVersion", "enrichedAt", "insight"));
        var insight = fields(enrichment.path("insight"), Set.of("summary", "sentiment", "sentimentScore", "relevanceScore",
                "catalysts", "confidence", "evidence", "flags"));
        if (insight.path("sentiment").isString() && !Set.of("POSITIVE", "NEGATIVE", "NEUTRAL", "MIXED")
                .contains(insight.path("sentiment").asString())) throw new Unknown();
        var catalysts = JSON.createArrayNode();
        if (!insight.path("catalysts").isArray()) throw new IllegalArgumentException("catalysts");
        for (JsonNode catalyst : insight.path("catalysts")) {
            if (catalyst.path("type").isString() && !Set.of("EARNINGS", "GUIDANCE", "M_AND_A", "ANALYST_RATING",
                    "REGULATORY", "PRODUCT", "MANAGEMENT", "MACRO", "OTHER").contains(catalyst.path("type").asString())) throw new Unknown();
            catalysts.add(fields(catalyst, Set.of("type", "description")));
        }
        for (JsonNode flag : insight.path("flags")) if (flag.isString() && !Set.of("LOW_CONFIDENCE", "UNSUPPORTED_NUMBER",
                "SYMBOL_MISMATCH").contains(flag.asString())) throw new Unknown();
        insight.set("catalysts", catalysts); enrichment.set("insight", insight);
        payload.set("enrichment", enrichment); event.set("payload", payload);
        if (!registry.getSchema(SchemaLocation.of(BASE + "events/news-enriched.schema.json"))
                .validate(event.toString(), InputFormat.JSON).isEmpty()) throw new IllegalArgumentException("schema");
        UUID article = UUID.fromString(payload.path("articleId").asString());
        if (!event.path("symbol").asString().equals(key) || insight.path("evidence").size() != 1
                || !insight.path("evidence").get(0).asString().equals(article.toString())) throw new IllegalArgumentException("evidence_key");
        var result = new NewsEnrichment(enrichment.path("promptVersion").asString(), enrichment.path("model").asString(),
                enrichment.path("modelVersion").isNull() ? null : enrichment.path("modelVersion").asString(),
                Instant.parse(enrichment.path("enrichedAt").asString()),
                JSON.convertValue(insight, new TypeReference<Map<String, Object>>() { }));
        return new NewsInsightRepository.Observation(UUID.fromString(event.path("eventId").asString()), article,
                payload.path("contentHash").asString(), key, result);
    }
}
