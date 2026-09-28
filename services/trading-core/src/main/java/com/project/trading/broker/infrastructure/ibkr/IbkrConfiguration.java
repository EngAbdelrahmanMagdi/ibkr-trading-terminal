package com.project.trading.broker.infrastructure.ibkr;

import com.project.trading.broker.domain.BrokerOrderUpdateHandler;
import com.project.trading.broker.domain.OpenOrderSource;
import com.project.trading.shared.config.AppProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.time.Clock;
import java.util.List;

/**
 * Wires the IBKR paper trading adapter; selected only in IBKR_PAPER mode. The deployment rules of that mode
 * (trusted environment, private CORS origins) are enforced before any bean is created.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "app.runtime-mode", havingValue = "IBKR_PAPER")
@EnableConfigurationProperties(IbkrProperties.class)
public class IbkrConfiguration {

    @Bean
    ReplyGate ibkrReplyGate(Clock clock, AppProperties app) {
        return new ReplyGate(clock, app.orders().confirmationTtl());
    }

    @Bean
    IbkrHttp ibkrHttp(IbkrProperties ibkr, AppProperties app, MeterRegistry registry) {
        List<String> violations = ibkr.violations(app.orders().confirmationSweepGrace());
        if (!violations.isEmpty()) {
            throw new IllegalStateException("Invalid IBKR settings: " + String.join("; ", violations));
        }
        IbkrRateLimiter limiter = new IbkrRateLimiter(ibkr.allocation(), ibkr.limiterQueue(), ibkr.limiterTimeout(),
                ibkr.penaltyCooldown(), System::nanoTime, registry);
        return new IbkrHttp(IbkrHttp.newClient(Path.of(ibkr.caFile()), ibkr.connectTimeout()), ibkr.baseUrl(),
                ibkr.requestTimeout(), ibkr.maxResponseBytes(), limiter, registry);
    }

    @Bean
    IbkrSession ibkrSession(IbkrHttp http, ReplyGate replies, IbkrProperties ibkr, AppProperties app) {
        return new IbkrSession(http, app.accountId(), replies, ibkr.readinessTtl(), ibkr.requestTimeout(),
                System::nanoTime);
    }

    @Bean
    SmartInitializingSingleton ibkrStartupCheck(IbkrSession session) {
        return session::verifyAtStartup;
    }

    @Bean
    IbkrTradingAdapter ibkrTradingAdapter(IbkrHttp http, IbkrSession session, ReplyGate replies, IbkrProperties ibkr,
                                          AppProperties app) {
        return new IbkrTradingAdapter(http, session, replies, app.accountId(), ibkr.orderLockTimeout());
    }

    @Bean
    IbkrOrderPoller ibkrOrderPoller(IbkrHttp http, IbkrSession session, ReplyGate replies, OpenOrderSource orders,
                                    BrokerOrderUpdateHandler updates, IbkrProperties ibkr, AppProperties app,
                                    Clock clock) {
        return new IbkrOrderPoller(http, session, replies, orders, updates, app.currency(), ibkr.pollInterval(), clock);
    }

    @Bean
    IbkrInstrumentCatalog ibkrInstrumentCatalog(IbkrHttp http) {
        return new IbkrInstrumentCatalog(http);
    }

    @Bean
    IbkrShortability ibkrShortability(IbkrHttp http, ReplyGate replies, Clock clock, IbkrProperties ibkr) {
        return new IbkrShortability(http, replies, clock, ibkr.shortabilityTtl());
    }

    @Bean
    IbkrAccount ibkrAccount(IbkrHttp http, ReplyGate replies, IbkrProperties ibkr, AppProperties app, Clock clock) {
        return new IbkrAccount(http, replies, app.accountId(), app.currency(), clock, ibkr.accountTtl());
    }
}
