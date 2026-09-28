package com.project.trading.broker.infrastructure.mock;

import com.project.trading.broker.domain.BrokerOrderUpdateHandler;
import com.project.trading.broker.domain.OpenOrderSource;
import com.project.trading.execution.application.ExecutionLedger;
import com.project.trading.instrument.domain.Instrument;
import com.project.trading.marketdata.domain.QuoteReferencePort;
import com.project.trading.shared.config.AppProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.function.Function;

/** Wires the simulated broker; selected only in MOCK mode. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "app.runtime-mode", havingValue = "MOCK")
@EnableConfigurationProperties(MockBrokerProperties.class)
public class MockBrokerConfiguration {

    @Bean
    MockInstrumentCatalog mockInstrumentCatalog() {
        return new MockInstrumentCatalog();
    }

    @Bean
    MockShortability mockShortability(MockBrokerProperties properties, Clock clock) {
        return new MockShortability(properties.shortability(), clock);
    }

    @Bean
    MockBrokerAccount mockBrokerAccount(ExecutionLedger executions, AppProperties app, Clock clock) {
        return new MockBrokerAccount(executions, app.portfolio().startingCash(), clock);
    }

    @Bean
    MockMarket mockMarket(QuoteReferencePort quotes, Clock clock, AppProperties app, MockBrokerProperties properties) {
        return new MockMarket(quotes, clock, app.orders().quoteMaxAge(), properties);
    }

    @Bean
    MockMatcher mockMatcher(MockMarket market, BrokerOrderUpdateHandler updates, OpenOrderSource openOrders,
                            MockInstrumentCatalog catalog, MockBrokerProperties properties) {
        return new MockMatcher(market, updates, openOrders, currencyOf(catalog), properties);
    }

    @Bean
    MockTradingAdapter mockTradingAdapter(MockMarket market, MockMatcher matcher, MockInstrumentCatalog catalog,
                                          MockBrokerProperties properties) {
        return new MockTradingAdapter(market, matcher, currencyOf(catalog), properties);
    }

    private static Function<String, String> currencyOf(MockInstrumentCatalog catalog) {
        return symbol -> catalog.resolve(symbol).map(Instrument::currency).orElse("USD");
    }
}
