package com.project.trading.news.infrastructure;

import com.project.trading.news.domain.NewsArticle;
import com.project.trading.news.domain.NewsProviderPort;
import org.springframework.core.io.ClassPathResource;
import tools.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/** Synthetic records pass through the same normalizer and ingestion pipeline as private providers. */
public class FixtureNewsProvider implements NewsProviderPort {
    private final Clock clock;
    private final NewsNormalizer normalizer;
    public FixtureNewsProvider(Clock clock, NewsNormalizer normalizer) { this.clock = clock; this.normalizer = normalizer; }
    @Override public String name() { return "FIXTURE"; }
    @Override public List<NewsArticle> fetch(String symbol, Instant from, Instant deadline) {
        var articles = new ArrayList<NewsArticle>();
        Instant day = clock.instant().truncatedTo(ChronoUnit.DAYS);
        try (var stream = new ClassPathResource("news/synthetic-news.json").getInputStream()) {
            var records = JsonMapper.builder().build().readTree(stream);
            for (var record : records) {
                String id = symbol + "-" + day + "-" + record.path("id").asString();
                Instant published = day.plusSeconds(record.path("seconds").asInt());
                if (published.isBefore(from)) continue;
                articles.add(normalizer.normalize(name(), id, symbol, symbol,
                        record.path("headline").asString().replace("{symbol}", symbol), "Synthetic demo",
                        "https://news.trading-terminal.invalid/" + symbol + "/" + record.path("id").asString() + "?day=" + day,
                        published, record.path("summary").asString(), from, clock.instant()));
            }
        } catch (IOException exception) { throw new IllegalStateException("Synthetic news fixture unavailable"); }
        return List.copyOf(articles);
    }
}
