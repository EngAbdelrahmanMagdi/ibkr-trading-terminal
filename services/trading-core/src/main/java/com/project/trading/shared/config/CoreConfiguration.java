package com.project.trading.shared.config;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;
import java.util.Arrays;
import java.util.List;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties(AppProperties.class)
public class CoreConfiguration {

    /**
     * Fails startup before any bean is created when the runtime mode's deployment rules are not met: IBKR_PAPER
     * trades a broker account and runs only in an explicitly trusted, private environment.
     */
    @Bean
    static BeanFactoryPostProcessor runtimeModeGuard(Environment environment) {
        return beanFactory -> {
            List<String> origins = Arrays.stream(environment.getProperty("app.http.cors-allowed-origins", String[].class,
                    new String[0])).map(String::trim).toList();
            RuntimeModeGuard.check(environment.getProperty("app.runtime-mode", ""),
                    environment.getProperty("app.trusted-environment", Boolean.class, false), origins);
        };
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
