package com.project.trading.marketdata.config;

import com.project.trading.marketdata.domain.QuoteReferencePort;
import com.project.trading.marketdata.infrastructure.RedisQuoteReference;
import com.project.trading.shared.config.AppProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.ObjectMapper;

/**
 * Reference quotes come from the Realtime Gateway's hot cache. The key namespace follows the runtime mode, so
 * MOCK trading reads only simulated quotes and paper trading only broker quotes.
 */
@Configuration(proxyBeanMethods = false)
public class MarketDataConfiguration {

    @Bean
    QuoteReferencePort quoteReference(StringRedisTemplate redis, ObjectMapper mapper, AppProperties properties,
                                      MeterRegistry registry) {
        String source = switch (properties.runtimeMode()) {
            case MOCK -> "MOCK";
            case IBKR_PAPER -> "IBKR";
        };
        return new RedisQuoteReference(redis, mapper, source, registry);
    }
}
