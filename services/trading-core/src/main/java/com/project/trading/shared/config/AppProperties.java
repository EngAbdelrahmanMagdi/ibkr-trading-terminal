package com.project.trading.shared.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

/** Application settings (prefix {@code app}). Defaults are in application.yml. */
@Validated
@ConfigurationProperties("app")
public record AppProperties(
        @NotNull RuntimeMode runtimeMode,
        @NotNull @Pattern(regexp = "^[A-Za-z0-9_-]{1,64}$") String accountId,
        @NotNull @Pattern(regexp = "^[A-Z]{3}$") String currency,
        @Valid @NotNull Orders orders,
        @Valid @NotNull Portfolio portfolio,
        @Valid @NotNull Watchlist watchlist,
        @Valid @NotNull Queries queries,
        @Valid @NotNull Http http,
        @Valid @NotNull Idempotency idempotency) {

    /** Order limits enforced before submission. */
    public record Orders(@NotNull @Positive BigDecimal maxQuantity, @NotNull @Positive BigDecimal maxNotional,
                         @NotNull Duration quoteMaxAge, @Min(1) @Max(10) int maxReplyDepth,
                         @NotNull Duration shortabilityMaxAge) {
    }

    /** The simulated cash account. */
    public record Portfolio(@NotNull @PositiveOrZero BigDecimal startingCash) {
    }

    public record Watchlist(@NotNull List<@Pattern(regexp = "^[A-Z][A-Z0-9.-]{0,11}$") String> defaultSymbols,
                            @Min(1) @Max(1000) int maxItems) {
    }

    /** Page sizes of list endpoints: the default and the cap. */
    public record Queries(@Min(1) int defaultLimit, @Min(1) @Max(10_000) int maxLimit) {
    }

    public record Http(@NotEmpty List<@Pattern(regexp = "^https?://[A-Za-z0-9.-]+(:[0-9]{1,5})?$") String> corsAllowedOrigins,
                       @Min(1024) @Max(1_048_576) int maxRequestBytes) {
    }

    /**
     * inProgressWait: how long a retry waits for a concurrent request with the same key to finish.
     * staleClaimAfter: after this time, a claim that never created an order (the request died before
     * submission) may be taken over by a retry.
     */
    public record Idempotency(@NotNull Duration inProgressWait, @NotNull Duration staleClaimAfter) {
    }
}
