package com.project.trading.shared.config;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties(AppProperties.class)
public class CoreConfiguration {

    /**
     * Fails startup before any bean is created unless the runtime mode is supported. Only the simulated broker
     * exists in this version; IBKR_PAPER fails closed instead of starting without a broker.
     */
    @Bean
    static BeanFactoryPostProcessor runtimeModeGuard(Environment environment) {
        return beanFactory -> {
            String mode = environment.getProperty("app.runtime-mode", "");
            if (!RuntimeMode.MOCK.name().equals(mode)) {
                throw new IllegalStateException("Unsupported runtime mode '" + mode
                        + "': this version supports only APP_RUNTIME_MODE=MOCK (the IBKR paper trading adapter is not available yet)");
            }
        };
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
