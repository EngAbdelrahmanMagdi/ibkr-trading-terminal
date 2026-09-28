package com.project.trading.order.application;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Reports a difference between local state and broker truth: one structured log event
 * ({@code event=reconciliation_drift}) and the counter {@code reconciliation_drift_total{entity,severity}}.
 * Severity {@code serious} means local state was wrong in a way that needs attention (for example a real execution
 * for an order recorded as rejected); {@code info} is a difference that was repaired or is expected.
 */
@Component
public class DriftReporter {

    public static final String INFO = "info";
    public static final String SERIOUS = "serious";

    private static final Logger log = LoggerFactory.getLogger(DriftReporter.class);

    private final MeterRegistry registry;

    public DriftReporter(MeterRegistry registry) {
        this.registry = registry;
    }

    public void report(String entity, String severity, UUID orderId, String detail) {
        Counter.builder("reconciliation.drift").tag("entity", entity).tag("severity", severity)
                .description("Differences between local state and broker truth").register(registry).increment();
        var event = SERIOUS.equals(severity) ? log.atError() : log.atWarn();
        event.addKeyValue("event", "reconciliation_drift").addKeyValue("entity", entity)
                .addKeyValue("severity", severity).addKeyValue("orderId", orderId == null ? null : orderId.toString())
                .log("reconciliation drift ({} {}): {}", severity, entity, detail);
    }
}
