package com.project.trading.order.application;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Reconciliation settings (prefix {@code app.reconciliation}).
 *
 * @param interval               time between periodic runs (at least as slow as the broker's order endpoints allow)
 * @param minSpacing             minimum time between two runs; requested runs are coalesced
 * @param initialDelay           delay of the first run after startup
 * @param unknownOrderAlertAfter ready-session time after which an unmatched UNKNOWN order is reported
 */
@Validated
@ConfigurationProperties("app.reconciliation")
public record ReconciliationSettings(@NotNull Duration interval, @NotNull Duration minSpacing,
                                     @NotNull Duration initialDelay, @NotNull Duration unknownOrderAlertAfter) {
}
