package com.project.trading.news.domain;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface NewsInsightRepository {
    record Observation(UUID eventId, UUID articleId, String contentHash, String symbol, NewsEnrichment enrichment) { }
    enum Outcome { ACCEPTED, EXISTING, EXPIRED }
    Map<UUID, NewsEnrichment> findAll(List<UUID> articleIds);
    Outcome accept(Observation observation);
}
