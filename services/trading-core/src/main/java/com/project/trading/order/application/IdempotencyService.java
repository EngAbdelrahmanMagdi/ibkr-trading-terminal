package com.project.trading.order.application;

import com.project.trading.shared.config.AppProperties;
import com.project.trading.shared.domain.DomainException;
import com.project.trading.shared.domain.ErrorCategory;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Idempotency-Key handling for order placement. The key's primary key arbitrates concurrent requests, so at
 * most one request per key reaches the broker. Retries of the same request get the stored outcome (including
 * stored 4xx rejections); reuse of a key with a different request is a conflict.
 */
@Service
public class IdempotencyService {

    /** Result of claiming a key. */
    public sealed interface Claim {
        record Claimed() implements Claim {
        }

        /** The request completed earlier with this status and order. */
        record Completed(int status, UUID orderId) implements Claim {
        }

        /** The request is still being processed; orderId is set once the order exists. */
        record InProgress(Optional<UUID> orderId) implements Claim {
        }
    }

    record StoredProblem(String category, int status, String title, String detail,
                                 List<DomainException.FieldError> errors) {
    }

    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

    private final IdempotencyStore store;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final Duration inProgressWait;
    private final Duration staleClaimAfter;

    public IdempotencyService(IdempotencyStore store, ObjectMapper mapper, Clock clock, AppProperties properties) {
        this.store = store;
        this.mapper = mapper;
        this.clock = clock;
        this.inProgressWait = properties.idempotency().inProgressWait();
        this.staleClaimAfter = properties.idempotency().staleClaimAfter();
    }

    /** SHA-256 of the canonical request (decimals normalized, so "10" and "10.00" are the same request). */
    public static String fingerprint(PlaceOrderCommand c) {
        String canonical = String.join("|", c.symbol(), c.intent().name(), c.orderType().name(),
                c.quantity().stripTrailingZeros().toPlainString(),
                c.limitPrice() == null ? "" : c.limitPrice().stripTrailingZeros().toPlainString(),
                c.timeInForce().name());
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    /**
     * Claims the key, or returns the earlier outcome. Waits (bounded) for a concurrent request with the same
     * key to finish. Throws CONFLICT for a different request, and replays a stored rejection by throwing it.
     */
    public Claim claim(UUID key, String fingerprint) {
        Instant deadline = clock.instant().plus(inProgressWait);
        while (true) {
            Instant now = clock.instant();
            if (store.insertIfAbsent(key, fingerprint, now)) {
                return new Claim.Claimed();
            }
            Optional<IdempotencyStore.Entry> found = store.find(key);
            if (found.isEmpty()) {
                if (!now.isBefore(deadline)) {
                    throw DomainException.conflict("the request with this Idempotency-Key is still in progress");
                }
                sleep();
                continue;
            }
            IdempotencyStore.Entry entry = found.get();
            if (!entry.fingerprint().equals(fingerprint)) {
                throw DomainException.conflict("the Idempotency-Key was already used for a different request");
            }
            if (entry.completedAt() != null) {
                if (entry.problemBody() != null) {
                    throw replay(entry.problemBody());
                }
                return new Claim.Completed(entry.responseStatus(), entry.orderId());
            }
            if (entry.orderId() == null && store.takeOverStale(key, fingerprint, now, now.minus(staleClaimAfter))) {
                return new Claim.Claimed();
            }
            if (!now.isBefore(deadline)) {
                return new Claim.InProgress(Optional.ofNullable(entry.orderId()));
            }
            sleep();
        }
    }

    public void linkOrder(UUID key, UUID orderId) {
        store.linkOrder(key, orderId);
    }

    public void complete(UUID key, int status) {
        store.complete(key, status, null, clock.instant());
    }

    /** Stores a 4xx rejection so that retries get the same answer. */
    public void completeWithProblem(UUID key, DomainException e) {
        String body = mapper.writeValueAsString(new StoredProblem(e.category().name(), e.status(), e.title(),
                e.getMessage(), e.fieldErrors()));
        store.complete(key, e.status(), body, clock.instant());
    }

    private DomainException replay(String body) {
        StoredProblem p = mapper.readValue(body, StoredProblem.class);
        return new DomainException(ErrorCategory.valueOf(p.category()), p.status(), p.title(), p.detail(),
                p.errors() == null ? List.of() : p.errors());
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL_INTERVAL);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw DomainException.conflict("the request with this Idempotency-Key is still in progress");
        }
    }
}
