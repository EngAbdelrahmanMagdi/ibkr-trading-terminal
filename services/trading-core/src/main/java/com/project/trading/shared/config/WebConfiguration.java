package com.project.trading.shared.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** CORS: an exact origin allowlist from configuration; no wildcards and no credentials. */
@Configuration(proxyBeanMethods = false)
public class WebConfiguration implements WebMvcConfigurer {

    private final AppProperties properties;

    public WebConfiguration(AppProperties properties) {
        this.properties = properties;
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOrigins(properties.http().corsAllowedOrigins().toArray(String[]::new))
                .allowedMethods("GET", "POST", "DELETE")
                .allowedHeaders("Content-Type", "Idempotency-Key", "X-Correlation-Id")
                .exposedHeaders("X-Correlation-Id", "X-News-Status", "X-News-Last-Refreshed-At", "Retry-After")
                .allowCredentials(false)
                .maxAge(600);
    }
}
