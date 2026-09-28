package com.project.trading.broker.infrastructure.mock;

import com.project.trading.instrument.domain.ShortabilityStatus;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Settings of the simulated broker (prefix {@code app.mock}).
 *
 * @param replyNotionalThreshold       orders above this value need one confirmation
 * @param secondReplyNotionalThreshold orders above this value need a second, chained confirmation
 * @param rejectSymbols                symbols the simulated broker always rejects (to exercise rejections)
 * @param shortability                 per-symbol shortability; symbols not listed are SHORTABLE
 */
@Validated
@ConfigurationProperties("app.mock")
public record MockBrokerProperties(
        @NotNull @Positive BigDecimal replyNotionalThreshold,
        @NotNull @Positive BigDecimal secondReplyNotionalThreshold,
        @NotNull Duration replyTtl,
        @Min(1) @Max(10_000) int maxPendingReplies,
        @NotNull Duration matcherInterval,
        @Min(1) @Max(10_000) int matcherBatchSize,
        @Min(1) @Max(10_000) int maxOpenOrders,
        @NotNull Duration notReadyTimeout,
        @NotNull @PositiveOrZero BigDecimal commissionPerShare,
        @NotNull @PositiveOrZero BigDecimal commissionMinimum,
        @NotNull List<String> rejectSymbols,
        @NotNull Map<String, ShortabilityStatus> shortability) {
}
