package com.project.trading.news.domain;

import java.time.Instant;
import java.util.Map;

public record NewsEnrichment(String promptVersion, String model, String modelVersion, Instant enrichedAt,
                             Map<String, Object> insight) {
    public NewsEnrichment { insight = Map.copyOf(insight); }
}
