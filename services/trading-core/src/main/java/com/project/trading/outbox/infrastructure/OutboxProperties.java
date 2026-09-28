package com.project.trading.outbox.infrastructure;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Outbox relay settings (prefix {@code app.outbox}).
 *
 * @param bootstrapServers   Kafka bootstrap servers
 * @param publisherEnabled   whether this instance relays events (events are always recorded)
 * @param batchSize          events claimed per round (at most one per key)
 * @param pollInterval       pause between rounds when there is nothing to publish
 * @param lease              how long a claim is held; an unfinished claim becomes claimable again afterwards
 * @param maxAttempts        attempts for an event that Kafka refuses for itself (for example too large) before FAILED
 * @param recordBackoffBase  first retry delay of such an event (doubles per attempt)
 * @param recordBackoffMax   cap of that delay
 * @param outageBackoffBase  first pause after Kafka is unreachable or misconfigured (doubles while it persists)
 * @param outageBackoffMax   cap of that pause; afterwards one probe per interval
 * @param requestTimeout     Kafka request timeout
 * @param deliveryTimeout    Kafka delivery timeout (bounds the producer's own retries)
 * @param maxBlock           longest the producer may block waiting for metadata or buffer space
 * @param retention          how long published events are kept before the purge deletes them
 * @param purgeInterval      interval of the purge job
 * @param purgeBatchSize     rows deleted per purge statement
 * @param purgeMaxBatches    statements per purge run
 */
@Validated
@ConfigurationProperties("app.outbox")
public record OutboxProperties(
        @NotBlank String bootstrapServers,
        boolean publisherEnabled,
        @Min(1) @Max(100) int batchSize,
        @NotNull Duration pollInterval,
        @NotNull Duration lease,
        @Min(1) @Max(100) int maxAttempts,
        @NotNull Duration recordBackoffBase,
        @NotNull Duration recordBackoffMax,
        @NotNull Duration outageBackoffBase,
        @NotNull Duration outageBackoffMax,
        @NotNull Duration requestTimeout,
        @NotNull Duration deliveryTimeout,
        @NotNull Duration maxBlock,
        @NotNull Duration retention,
        @NotNull Duration purgeInterval,
        @Min(1) @Max(10_000) int purgeBatchSize,
        @Min(1) @Max(1000) int purgeMaxBatches) {

    /** The longest a batch can take to send: blocking for metadata plus the delivery timeout, with a margin. */
    Duration maxSendTime() {
        return maxBlock.plus(deliveryTimeout).plusSeconds(2);
    }

    List<String> violations() {
        List<String> errors = new ArrayList<>();
        if (lease.compareTo(maxSendTime().plusSeconds(5)) < 0) {
            errors.add("app.outbox.lease must exceed max-block + delivery-timeout by at least 7s");
        }
        if (deliveryTimeout.compareTo(requestTimeout) <= 0) {
            errors.add("app.outbox.delivery-timeout must exceed app.outbox.request-timeout");
        }
        if (recordBackoffMax.compareTo(recordBackoffBase) < 0 || outageBackoffMax.compareTo(outageBackoffBase) < 0) {
            errors.add("app.outbox backoff caps must not be below their base delays");
        }
        for (Duration d : List.of(pollInterval, recordBackoffBase, outageBackoffBase, requestTimeout, maxBlock,
                retention, purgeInterval)) {
            if (d.isNegative() || d.isZero()) {
                errors.add("app.outbox durations must be positive");
                break;
            }
        }
        return errors;
    }
}
