package com.project.trading.shared.api;

import com.project.trading.shared.config.AppProperties;
import com.project.trading.shared.domain.DomainException;
import org.springframework.stereotype.Component;

/** Resolves the limit query parameter: the configured default when absent, capped at the maximum. */
@Component
public class ApiLimits {

    private final AppProperties.Queries queries;

    public ApiLimits(AppProperties properties) {
        this.queries = properties.queries();
    }

    public int resolve(Integer requested) {
        if (requested == null) {
            return Math.min(queries.defaultLimit(), queries.maxLimit());
        }
        if (requested < 1) {
            throw DomainException.malformed("limit must be at least 1");
        }
        return Math.min(requested, queries.maxLimit());
    }
}
