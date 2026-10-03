package com.project.trading.news.infrastructure.kafka;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NewsEnrichedEventsTest {
    @Test void canonicalValidationEvidenceAndForwardCompatibility() throws Exception {
        var json = JsonMapper.builder().build();
        var root = (ObjectNode) json.readTree(Files.readString(Path.of("../../tests/contract/fixtures/events/news-enriched/valid/enriched.json")));
        var parser = new NewsEnrichedEvents();
        root.put("futureField", "ignored");
        assertThat(parser.parse(root.toString().getBytes(StandardCharsets.UTF_8), "NVDA").enrichment().insight())
                .containsEntry("sentiment", "POSITIVE");
        assertThatThrownBy(() -> parser.parse(root.toString().getBytes(StandardCharsets.UTF_8), "AAPL"))
                .isInstanceOf(IllegalArgumentException.class);
        ((ObjectNode) root.path("payload").path("enrichment").path("insight")).put("confidence", 5);
        assertThatThrownBy(() -> parser.parse(root.toString().getBytes(StandardCharsets.UTF_8), "NVDA"))
                .isInstanceOf(IllegalArgumentException.class);
        root.put("eventType", "NEWS_FUTURE_EVENT");
        assertThatThrownBy(() -> parser.parse(root.toString().getBytes(StandardCharsets.UTF_8), "NVDA"))
                .isInstanceOf(NewsEnrichedEvents.Unknown.class);
    }
}
