package com.project.trading.news.application;

import com.project.trading.news.domain.*;
import com.project.trading.shared.domain.DomainException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class NewsServiceTest {
    private static final Instant NOW = Instant.parse("2026-10-03T12:00:00Z");
    private static final NewsPolicy POLICY = new NewsPolicy(Duration.ofMinutes(5), Duration.ofDays(7), Duration.ofSeconds(8),
            1, 1, 1000, Duration.ofDays(30), 50000, 4);
    private final NewsRepository repository = mock(NewsRepository.class);
    private final NewsCache cache = mock(NewsCache.class);
    private final NewsIngestion ingestion = mock(NewsIngestion.class);
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();

    @Test void saturatedInternalQueueReturns503ButRetainsPriorResults() throws Exception {
        when(repository.state(anyString(), anyString())).thenReturn(new NewsRepository.State(null, null));
        when(repository.recent(anyString(), anyString(), any(), anyInt())).thenReturn(List.of());
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var provider = new NewsProviderPort() {
            @Override public String name() { return "FIXTURE"; }
            @Override public List<NewsArticle> fetch(String symbol, Instant from, Instant deadline) {
                entered.countDown();
                try { if (!release.await(8, TimeUnit.SECONDS)) throw new NewsUnavailable("DEADLINE"); }
                catch (InterruptedException exception) { Thread.currentThread().interrupt(); throw new NewsUnavailable("INTERRUPTED"); }
                return List.of();
            }
        };
        try (var service = new NewsService(provider, repository, cache, ingestion, POLICY, Clock.fixed(NOW, ZoneOffset.UTC), metrics)) {
            var first = CompletableFuture.supplyAsync(() -> service.list("NVDA", 20));
            assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
            var queued = CompletableFuture.supplyAsync(() -> service.list("AMD", 20));
            // Observe the bounded queue, rather than use a sleep or implementation reflection.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (metrics.get("news.refresh.queue").gauge().value() != 1 && System.nanoTime() < deadline) Thread.onSpinWait();
            assertThat(metrics.get("news.refresh.queue").gauge().value()).isEqualTo(1);
            assertThatThrownBy(() -> service.list("AAPL", 20)).isInstanceOfSatisfying(DomainException.class,
                    exception -> assertThat(exception.status()).isEqualTo(503));
            var retained = new NewsArticle(UUID.randomUUID(), "FIXTURE", "FIXTURE:1", "META", List.of("META"),
                    "Synthetic headline", "Synthetic demo", "https://example.com/a", NOW.minusSeconds(600), null, "0".repeat(64));
            when(cache.get("FIXTURE", "META")).thenReturn(new NewsCache.Snapshot(List.of(retained), NOW.minusSeconds(600)));
            var result = service.list("META", 20);
            assertThat(result.status()).isEqualTo("UNAVAILABLE");
            assertThat(result.articles()).containsExactly(retained);
            release.countDown();
            first.get(3, TimeUnit.SECONDS);
            queued.get(3, TimeUnit.SECONDS);
        } finally { release.countDown(); }
    }
    @Test void emptySuccessIsFreshAndDoesNotCallProviderAgain() {
        when(cache.get("FIXTURE", "NVDA")).thenReturn(new NewsCache.Snapshot(List.of(), NOW));
        when(repository.state("FIXTURE", "NVDA")).thenReturn(new NewsRepository.State(NOW, null));
        var provider = mock(NewsProviderPort.class);
        when(provider.name()).thenReturn("FIXTURE");
        try (var service = new NewsService(provider, repository, cache, ingestion, POLICY, Clock.fixed(NOW, ZoneOffset.UTC), metrics)) {
            assertThat(service.list("NVDA", 20).status()).isEqualTo("FRESH");
            assertThat(service.list("NVDA", 20).articles()).isEmpty();
            verify(provider, never()).fetch(anyString(), any(), any());
        }
    }
    @Test void duplicateColdRequestsShareOneFetchAndCannotExhaustTradingHttpThreads() throws Exception {
        when(repository.state(anyString(), anyString())).thenReturn(new NewsRepository.State(null, null));
        when(repository.recent(anyString(), anyString(), any(), anyInt())).thenReturn(List.of());
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var provider = mock(NewsProviderPort.class);
        when(provider.name()).thenReturn("FIXTURE");
        when(provider.fetch(anyString(), any(), any())).thenAnswer(invocation -> {
            entered.countDown();
            assertThat(release.await(8, TimeUnit.SECONDS)).isTrue();
            return List.of();
        });
        var policy = new NewsPolicy(POLICY.freshness(), POLICY.window(), POLICY.deadline(), 1, 1, 1000,
                Duration.ofDays(30), 50000, 1);
        try (var service = new NewsService(provider, repository, cache, ingestion, policy, Clock.fixed(NOW, ZoneOffset.UTC), metrics)) {
            var first = CompletableFuture.supplyAsync(() -> service.list("NVDA", 20));
            assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (metrics.get("news.cold.requests").gauge().value() != 1 && System.nanoTime() < deadline) Thread.onSpinWait();
            assertThat(metrics.get("news.cold.requests").gauge().value()).isEqualTo(1);
            assertThatThrownBy(() -> service.list("NVDA", 20)).isInstanceOfSatisfying(DomainException.class,
                    exception -> assertThat(exception.status()).isEqualTo(503));
            verify(provider, times(1)).fetch(eq("NVDA"), any(), any());
            release.countDown();
            first.get(3, TimeUnit.SECONDS);
        } finally { release.countDown(); }
    }
}
