package com.project.trading.broker.infrastructure.ibkr;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The local share of the IBKR session budget: bounded waiting, documented endpoint limits, 429 cool-down. */
class IbkrRateLimiterTest {

    private final AtomicLong elapsed = new AtomicLong();

    private IbkrRateLimiter limiter(int allocation, int queue) {
        return new IbkrRateLimiter(allocation, queue, Duration.ofSeconds(3), Duration.ofMinutes(15),
                () -> System.nanoTime() + elapsed.get(), new SimpleMeterRegistry());
    }

    @Test
    void theAllocationMustLeaveHeadroomWithinTheSessionLimit() {
        IbkrProperties valid = properties(10, 4, 1, "https://localhost:5000/v1/api", Duration.ofSeconds(5));
        assertThat(valid.violations(Duration.ofSeconds(30))).isEmpty();

        assertThat(properties(10, 10, 1, "https://localhost:5000/v1/api", Duration.ofSeconds(5))
                .violations(Duration.ofSeconds(30))).anyMatch(v -> v.contains("headroom"));
        assertThat(properties(10, 4, 1, "http://localhost:5000/v1/api", Duration.ofSeconds(5))
                .violations(Duration.ofSeconds(30))).anyMatch(v -> v.contains("https"));
        assertThat(properties(10, 4, 1, "https://localhost:5000/v1/api", Duration.ofSeconds(1))
                .violations(Duration.ofSeconds(30))).anyMatch(v -> v.contains("poll-interval"));
        assertThat(valid.violations(Duration.ofSeconds(10))).anyMatch(v -> v.contains("sweep-grace"));
    }

    @Test
    void anEndpointLimitedToOneRequestPerFiveSecondsRejectsWhenTheWaitExceedsTheTimeout() throws Exception {
        IbkrRateLimiter limiter = limiter(10, 4);
        limiter.acquire(IbkrEndpoint.LIVE_ORDERS);

        assertThatThrownBy(() -> limiter.acquire(IbkrEndpoint.LIVE_ORDERS)).isInstanceOf(IbkrRateLimiter.RejectedException.class);
        assertThatCode(() -> limiter.acquire(IbkrEndpoint.PLACE_ORDER)).doesNotThrowAnyException();

        elapsed.addAndGet(Duration.ofSeconds(5).toNanos());
        assertThatCode(() -> limiter.acquire(IbkrEndpoint.LIVE_ORDERS)).doesNotThrowAnyException();
    }

    @Test
    void waitingIsBoundedByTheQueueCapacity() throws Exception {
        IbkrRateLimiter limiter = limiter(1, 1);
        limiter.acquire(IbkrEndpoint.PLACE_ORDER);
        CompletableFuture<Void> waiter = CompletableFuture.runAsync(() -> {
            try {
                limiter.acquire(IbkrEndpoint.PLACE_ORDER);
            } catch (IbkrRateLimiter.RejectedException e) {
                throw new IllegalStateException(e);
            }
        });
        while (limiter.waiting() == 0) {
            Thread.onSpinWait();
        }

        assertThatThrownBy(() -> limiter.acquire(IbkrEndpoint.PLACE_ORDER))
                .isInstanceOf(IbkrRateLimiter.RejectedException.class).hasMessageContaining("waiting");
        waiter.get();
    }

    @Test
    void a429PausesEveryRequestForTheCooldown() throws Exception {
        IbkrRateLimiter limiter = limiter(10, 4);
        limiter.report429();

        assertThat(limiter.isCoolingDown()).isTrue();
        assertThatThrownBy(() -> limiter.acquire(IbkrEndpoint.AUTH_STATUS)).isInstanceOf(IbkrRateLimiter.RejectedException.class);

        elapsed.addAndGet(Duration.ofMinutes(15).plusSeconds(1).toNanos());
        assertThat(limiter.isCoolingDown()).isFalse();
        assertThatCode(() -> limiter.acquire(IbkrEndpoint.AUTH_STATUS)).doesNotThrowAnyException();
    }

    private static IbkrProperties properties(int limit, int allocation, int headroom, String url, Duration poll) {
        return new IbkrProperties(URI.create(url), "/run/secrets/ibkr_ca", Duration.ofSeconds(3), Duration.ofSeconds(10),
                1 << 20, limit, allocation, headroom, 32, Duration.ofSeconds(5), Duration.ofMinutes(15),
                Duration.ofSeconds(5), Duration.ofSeconds(5), poll, Duration.ofMinutes(1), Duration.ofSeconds(10));
    }
}
