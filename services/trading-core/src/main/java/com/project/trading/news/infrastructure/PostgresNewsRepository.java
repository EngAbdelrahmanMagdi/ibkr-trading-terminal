package com.project.trading.news.infrastructure;

import com.project.trading.news.domain.NewsArticle;
import com.project.trading.news.domain.NewsRepository;
import com.project.trading.news.domain.NewsUnavailable;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PostgresNewsRepository implements NewsRepository {
    private final NamedParameterJdbcTemplate jdbc;
    public PostgresNewsRepository(NamedParameterJdbcTemplate jdbc) { this.jdbc = jdbc; }
    @Override public List<NewsArticle> recent(String provider, String symbol, Instant cutoff, int limit) {
        return jdbc.query("""
                SELECT a.*, ARRAY(SELECT s.symbol FROM news_article_symbols s WHERE s.article_id=a.id
                                  ORDER BY s.symbol) AS symbols
                FROM news_articles a JOIN news_article_symbols selected ON selected.article_id=a.id
                WHERE a.provider=:provider AND selected.symbol=:symbol AND a.published_at>=:cutoff
                ORDER BY a.published_at DESC, a.id DESC LIMIT :limit
                """, Map.of("provider", provider, "symbol", symbol, "cutoff", Timestamp.from(cutoff), "limit", limit),
                (row, index) -> new NewsArticle(row.getObject("id", UUID.class), row.getString("provider"),
                        row.getString("provider_id"), row.getString("primary_symbol"),
                        Arrays.asList((String[]) row.getArray("symbols").getArray()), row.getString("headline"),
                        row.getString("source"), row.getString("canonical_url"),
                        row.getTimestamp("published_at").toInstant(), row.getString("raw_summary"),
                        row.getString("content_hash")));
    }
    @Override public State state(String provider, String symbol) {
        var states = jdbc.query("SELECT last_success_at,failure FROM news_fetch_state WHERE provider=:p AND symbol=:s",
                Map.of("p", provider, "s", symbol), (row, index) -> new State(
                        row.getTimestamp(1) == null ? null : row.getTimestamp(1).toInstant(), row.getString(2)));
        return states.isEmpty() ? new State(null, null) : states.getFirst();
    }
    private void lock() {
        jdbc.query("SELECT pg_advisory_xact_lock(310011)", Map.of(), (row, index) -> Boolean.TRUE);
    }
    @Override public boolean admit(String provider, String symbol, Instant now, int maximum) {
        lock();
        if (jdbc.queryForObject("SELECT count(*) FROM news_fetch_state WHERE provider=:p AND symbol=:s",
                Map.of("p", provider, "s", symbol), Long.class) > 0) return true;
        if (jdbc.queryForObject("SELECT count(*) FROM news_fetch_state", Map.of(), Long.class) >= maximum) return false;
        jdbc.update("INSERT INTO news_fetch_state(provider,symbol,last_attempt_at) VALUES (:p,:s,:now)",
                Map.of("p", provider, "s", symbol, "now", Timestamp.from(now)));
        return true;
    }
    @Override public boolean insert(NewsArticle article, Instant now, int maximum) {
        lock();
        var identity = Map.of("p", article.provider(), "pid", article.providerId(), "hash", NewsNormalizer.hash(article.url()));
        if (jdbc.queryForObject("""
                SELECT count(*) FROM news_articles WHERE provider=:p AND (provider_id=:pid OR canonical_url_hash=:hash)
                """, identity, Long.class) > 0) return false;
        if (jdbc.queryForObject("SELECT count(*) FROM news_articles", Map.of(), Long.class) >= maximum)
            throw new NewsUnavailable("CAPACITY");
        var parameters = new org.springframework.jdbc.core.namedparam.MapSqlParameterSource()
                .addValue("id", article.id()).addValue("p", article.provider()).addValue("pid", article.providerId())
                .addValue("primary", article.primarySymbol()).addValue("headline", article.headline())
                .addValue("source", article.source()).addValue("url", article.url()).addValue("hash", NewsNormalizer.hash(article.url()))
                .addValue("published", Timestamp.from(article.publishedAt())).addValue("now", Timestamp.from(now))
                .addValue("summary", article.rawSummary()).addValue("content", article.contentHash());
        return jdbc.update("""
                INSERT INTO news_articles(id,provider,provider_id,primary_symbol,headline,source,canonical_url,
                    canonical_url_hash,published_at,ingested_at,raw_summary,content_hash)
                VALUES (:id,:p,:pid,:primary,:headline,:source,:url,:hash,:published,:now,:summary,:content)
                ON CONFLICT DO NOTHING
                """, parameters) == 1;
    }
    @Override public void associate(NewsArticle article) {
        for (String symbol : article.symbols()) {
            jdbc.update("""
                    INSERT INTO news_article_symbols(article_id,symbol)
                    SELECT id,:s FROM news_articles WHERE provider=:p AND (provider_id=:pid OR canonical_url_hash=:hash)
                    AND (SELECT count(*) FROM news_article_symbols WHERE article_id=news_articles.id)<50
                    ORDER BY CASE WHEN provider_id=:pid THEN 0 ELSE 1 END LIMIT 1 ON CONFLICT DO NOTHING
                    """, Map.of("s", symbol, "p", article.provider(), "pid", article.providerId(), "hash", NewsNormalizer.hash(article.url())));
        }
    }
    @Override public void succeeded(String provider, String symbol, Instant now) {
        jdbc.update("""
                UPDATE news_fetch_state SET last_attempt_at=:now,last_success_at=:now,failure=NULL
                WHERE provider=:p AND symbol=:s
                """, Map.of("p", provider, "s", symbol, "now", Timestamp.from(now)));
    }
    @Override public void failed(String provider, String symbol, Instant now, String classification) {
        jdbc.update("UPDATE news_fetch_state SET last_attempt_at=:now,failure=:failure WHERE provider=:p AND symbol=:s",
                Map.of("p", provider, "s", symbol, "now", Timestamp.from(now), "failure", classification));
    }
    @Override public void purge(Instant cutoff, int ceiling) {
        lock();
        jdbc.update("""
                DELETE FROM news_articles WHERE id IN (
                    SELECT a.id FROM news_articles a WHERE
                      (a.published_at<:cutoff OR a.id IN (
                        SELECT id FROM news_articles ORDER BY published_at DESC,id DESC OFFSET :ceiling))
                      AND NOT EXISTS (SELECT 1 FROM outbox_events o WHERE o.aggregate_type='NEWS_ARTICLE'
                                      AND o.aggregate_id=a.id AND o.status<>'PUBLISHED')
                    ORDER BY a.published_at LIMIT 500)
                """, Map.of("cutoff", Timestamp.from(cutoff), "ceiling", ceiling));
        jdbc.update("""
                DELETE FROM news_fetch_state WHERE (provider,symbol) IN (
                    SELECT provider,symbol FROM news_fetch_state WHERE last_attempt_at<:cutoff LIMIT 500)
                """, Map.of("cutoff", Timestamp.from(cutoff)));
    }
}
