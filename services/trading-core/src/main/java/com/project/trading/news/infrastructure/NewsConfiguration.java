package com.project.trading.news.infrastructure;

import com.project.trading.news.application.NewsIngestion;
import com.project.trading.news.application.NewsService;
import com.project.trading.news.domain.NewsCache;
import com.project.trading.news.domain.NewsProviderPort;
import com.project.trading.news.domain.NewsRepository;
import com.project.trading.outbox.application.OutboxAppender;
import com.project.trading.shared.config.AppProperties;
import com.project.trading.shared.config.RuntimeModeGuard;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.time.Clock;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(NewsProperties.class)
public class NewsConfiguration {
    @Bean NewsRepository newsRepository(NamedParameterJdbcTemplate jdbc) { return new PostgresNewsRepository(jdbc); }
    @Bean NewsCache newsCache(StringRedisTemplate redis, NewsProperties properties, MeterRegistry metrics) {
        return new RedisNewsCache(redis, properties, metrics);
    }
    @Bean NewsNormalizer newsNormalizer() { return new NewsNormalizer(); }
    @Bean(destroyMethod = "close") NewsHttpClient newsHttpClient(NewsProperties properties, Clock clock) {
        return new NewsHttpClient(properties, clock);
    }
    @Bean NewsProviderPort newsProvider(NewsProperties properties, AppProperties app, Clock clock,
                                       NewsNormalizer normalizer, NewsHttpClient http, MeterRegistry metrics) {
        if (properties.provider().equals("FIXTURE")) return new FixtureNewsProvider(clock, normalizer);
        // Reuse the established trusted/private-origin deployment policy without coupling news to broker access.
        RuntimeModeGuard.check("IBKR_PAPER", app.trustedEnvironment(), app.http().corsAllowedOrigins());
        return new FinnhubNewsProvider(http, properties, normalizer, new NewsFetchPolicy(properties, clock), clock, metrics);
    }
    @Bean NewsIngestion newsIngestion(NewsRepository repository, OutboxAppender outbox, NewsProperties properties, MeterRegistry metrics) {
        return new NewsIngestion(repository, outbox, properties.policy(), metrics);
    }
    @Bean(destroyMethod = "close") NewsService newsService(NewsProviderPort provider, NewsRepository repository,
                                                         NewsCache cache, NewsIngestion ingestion, NewsProperties properties,
                                                         Clock clock, MeterRegistry metrics) {
        return new NewsService(provider, repository, cache, ingestion, properties.policy(), clock, metrics);
    }
}
