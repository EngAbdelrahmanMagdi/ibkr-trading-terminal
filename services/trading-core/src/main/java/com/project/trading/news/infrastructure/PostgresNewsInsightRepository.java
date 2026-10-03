package com.project.trading.news.infrastructure;

import com.project.trading.news.domain.NewsEnrichment;
import com.project.trading.news.domain.NewsInsightRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PostgresNewsInsightRepository implements NewsInsightRepository {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private final NamedParameterJdbcTemplate jdbc;
    public PostgresNewsInsightRepository(NamedParameterJdbcTemplate jdbc) { this.jdbc = jdbc; }
    @Override public Map<UUID, NewsEnrichment> findAll(List<UUID> ids) {
        if (ids.isEmpty()) return Map.of();
        Map<UUID, NewsEnrichment> result = new LinkedHashMap<>();
        jdbc.query("SELECT * FROM news_insights WHERE article_id IN (:ids)", Map.of("ids", ids), row -> {
            result.put(row.getObject("article_id", UUID.class), new NewsEnrichment(row.getString("prompt_version"),
                    row.getString("model"), row.getString("model_version"), row.getTimestamp("enriched_at").toInstant(),
                    JSON.readValue(row.getString("insight"), new TypeReference<Map<String, Object>>() { })));
        });
        return result;
    }
    @Override public Outcome accept(Observation o) {
        var article = jdbc.query("SELECT content_hash,primary_symbol FROM news_articles WHERE id=:id FOR KEY SHARE",
                Map.of("id", o.articleId()), (row, index) -> List.of(row.getString(1), row.getString(2)));
        if (article.isEmpty()) return Outcome.EXPIRED;
        if (!article.getFirst().equals(List.of(o.contentHash(), o.symbol())))
            throw new IllegalArgumentException("article_association");
        var e = o.enrichment();
        int inserted = jdbc.update("""
                INSERT INTO news_insights(article_id,accepted_processing_id,content_hash,prompt_version,model,
                    model_version,enriched_at,insight)
                VALUES (:id,:event,:hash,:prompt,:model,:version,:at,CAST(:insight AS jsonb))
                ON CONFLICT (article_id) DO NOTHING
                """, new MapSqlParameterSource().addValue("id", o.articleId()).addValue("event", o.eventId())
                .addValue("hash", o.contentHash()).addValue("prompt", e.promptVersion()).addValue("model", e.model())
                .addValue("version", e.modelVersion()).addValue("at", Timestamp.from(e.enrichedAt()))
                .addValue("insight", JSON.writeValueAsString(e.insight())));
        return inserted == 1 ? Outcome.ACCEPTED : Outcome.EXISTING;
    }
}
