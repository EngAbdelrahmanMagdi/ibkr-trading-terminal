package com.project.trading.broker.infrastructure.ibkr;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Settings of the IBKR Client Portal Gateway adapter (prefix {@code app.ibkr}), used only in IBKR_PAPER mode.
 *
 * @param baseUrl          the CP Gateway API root, https only (for example https://host.docker.internal:5000/v1/api)
 * @param caFile           PEM file with the CA that signed the CP Gateway certificate (the only trusted issuer)
 * @param sessionLimit     the documented request limit of one IBKR session (requests per second)
 * @param allocation       this service's share of the session limit (requests per second)
 * @param headroom         capacity kept free for other clients of the session (requests per second)
 * @param limiterQueue     requests that may wait for a rate-limit slot at the same time
 * @param limiterTimeout   the longest a request may wait for a slot before it is rejected
 * @param penaltyCooldown  after a 429 from IBKR, all requests stop for this long
 * @param readinessTtl     how long a broker readiness check is reused
 * @param orderLockTimeout how long an order command waits for another in-flight order command
 * @param pollInterval     order status polling interval (IBKR allows one live-orders request per 5 s)
 * @param shortabilityTtl  how long shortability data is reused
 * @param accountTtl       how long account metrics are reused
 */
@Validated
@ConfigurationProperties("app.ibkr")
public record IbkrProperties(
        @NotNull URI baseUrl,
        @NotBlank String caFile,
        @NotNull Duration connectTimeout,
        @NotNull Duration requestTimeout,
        @Min(1024) @Max(16 * 1024 * 1024) int maxResponseBytes,
        @Min(1) @Max(1000) int sessionLimit,
        @Min(1) @Max(1000) int allocation,
        @Min(1) @Max(1000) int headroom,
        @Min(1) @Max(1000) int limiterQueue,
        @NotNull Duration limiterTimeout,
        @NotNull Duration penaltyCooldown,
        @NotNull Duration readinessTtl,
        @NotNull Duration orderLockTimeout,
        @NotNull Duration pollInterval,
        @NotNull Duration shortabilityTtl,
        @NotNull Duration accountTtl) {

    /** IBKR allows one live-orders or trades request per 5 seconds. */
    static final Duration MIN_POLL_INTERVAL = Duration.ofSeconds(5);

    /**
     * Cross-field rules, checked at startup. confirmationSweepGrace is the order module's grace period, which must
     * cover an order command that is still waiting for the order lock and the broker.
     */
    List<String> violations(Duration confirmationSweepGrace) {
        List<String> errors = new ArrayList<>();
        if (!"https".equalsIgnoreCase(baseUrl.getScheme()) || baseUrl.getHost() == null) {
            errors.add("app.ibkr.base-url must be an https URL");
        }
        if (headroom <= 0 || allocation + headroom > sessionLimit) {
            errors.add("app.ibkr.allocation + app.ibkr.headroom must not exceed app.ibkr.session-limit (headroom > 0)");
        }
        if (pollInterval.compareTo(MIN_POLL_INTERVAL) < 0) {
            errors.add("app.ibkr.poll-interval must be at least 5s");
        }
        if (requestTimeout.plus(orderLockTimeout).compareTo(confirmationSweepGrace) >= 0) {
            errors.add("app.orders.confirmation-sweep-grace must exceed app.ibkr.request-timeout + app.ibkr.order-lock-timeout");
        }
        for (Duration d : List.of(connectTimeout, requestTimeout, limiterTimeout, penaltyCooldown, readinessTtl,
                orderLockTimeout, shortabilityTtl, accountTtl)) {
            if (d.isNegative() || d.isZero()) {
                errors.add("app.ibkr durations must be positive");
                break;
            }
        }
        return errors;
    }
}
