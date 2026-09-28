package com.project.trading.broker.infrastructure.kafka;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Consumer of broker order observations (prefix {@code app.broker-updates}).
 *
 * @param maxPollRecords   records handled per poll (bounded memory)
 * @param notReadyAttempts attempts for an update whose order is not acknowledged locally yet
 * @param notReadyMaxWait  total time spent on those attempts before the update is left to reconciliation
 */
@Validated
@ConfigurationProperties("app.broker-updates")
public record BrokerUpdatesProperties(@NotBlank String bootstrapServers, @NotBlank String topic, @NotBlank String group,
                                      @Min(1) @Max(1000) int maxPollRecords, @Min(1) @Max(20) int notReadyAttempts,
                                      @NotNull Duration notReadyMaxWait) {
}
