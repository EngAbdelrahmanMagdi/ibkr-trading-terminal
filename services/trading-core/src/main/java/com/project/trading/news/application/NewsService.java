package com.project.trading.news.application;

import com.project.trading.news.domain.NewsArticle;
import com.project.trading.news.domain.NewsCache;
import com.project.trading.news.domain.NewsPolicy;
import com.project.trading.news.domain.NewsProviderPort;
import com.project.trading.news.domain.NewsRepository;
import com.project.trading.news.domain.NewsUnavailable;
import com.project.trading.shared.api.Correlation;
import com.project.trading.shared.domain.DomainException;
import com.project.trading.shared.domain.ErrorCategory;
import io.micrometer.core.instrument.MeterRegistry;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NewsService implements AutoCloseable {
    public record Result(List<NewsArticle> articles, String status, Instant lastSuccess) { }
    private final NewsProviderPort provider;
    private final NewsRepository repository;
    private final NewsCache cache;
    private final NewsIngestion ingestion;
    private final NewsPolicy policy;
    private final Clock clock;
    private final MeterRegistry metrics;
    private final ThreadPoolExecutor worker;
    private final java.util.concurrent.Semaphore coldRequests;
    private final Map<String, CompletableFuture<Void>> pending = new ConcurrentHashMap<>();

    public NewsService(NewsProviderPort provider, NewsRepository repository, NewsCache cache, NewsIngestion ingestion,
                       NewsPolicy policy, Clock clock, MeterRegistry metrics) {
        this.provider = provider;
        this.repository = repository;
        this.cache = cache;
        this.ingestion = ingestion;
        this.policy = policy;
        this.clock = clock;
        this.metrics = metrics;
        coldRequests = new java.util.concurrent.Semaphore(policy.maxColdRequests());
        worker = new ThreadPoolExecutor(policy.workers(), policy.workers(), 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(policy.queueCapacity()), Thread.ofPlatform().name("news-refresh-", 0).factory(),
                new ThreadPoolExecutor.AbortPolicy());
        metrics.gauge("news.refresh.queue", worker, pool -> pool.getQueue().size());
        metrics.gauge("news.refresh.active", worker, ThreadPoolExecutor::getActiveCount);
        metrics.gauge("news.cold.requests", coldRequests, permits -> policy.maxColdRequests() - permits.availablePermits());
    }
    public Result list(String symbol, int limit) {
        if (symbol == null || !symbol.matches("^[A-Z][A-Z0-9.-]{0,11}$"))
            throw DomainException.malformed("symbol must be an uppercase ticker");
        final NewsCache.Snapshot snapshot;
        try { snapshot = read(symbol); }
        catch (RuntimeException exception) { throw unavailable(); }
        final NewsRepository.State state;
        try { state = repository.state(provider.name(), symbol); }
        catch (RuntimeException exception) {
            if (snapshot.lastSuccess() != null) return result(snapshot, "UNAVAILABLE", limit);
            throw unavailable();
        }
        if (snapshot.lastSuccess() != null && state.failure() == null
                && snapshot.lastSuccess().plus(policy.freshness()).isAfter(clock.instant()))
            return result(snapshot, "FRESH", limit);
        // Capture tracing before handing work to a separate thread.
        CompletableFuture<Void> refresh = refresh(symbol, Correlation.current());
        if (snapshot.lastSuccess() != null) {
            return result(snapshot, refresh.isCompletedExceptionally() || state.failure() != null
                    ? "UNAVAILABLE" : "STALE", limit);
        }
        // Duplicate cold requests must not occupy the HTTP pool needed by trading commands.
        if (!coldRequests.tryAcquire()) throw unavailable();
        try {
            refresh.get(policy.deadline().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw unavailable();
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException exception) {
            throw unavailable();
        } finally {
            coldRequests.release();
        }
        try { return result(read(symbol), "FRESH", limit); }
        catch (RuntimeException exception) { throw unavailable(); }
    }
    private NewsCache.Snapshot read(String symbol) {
        var cached = cache.get(provider.name(), symbol);
        metrics.counter("news.cache", "outcome", cached == null ? "miss" : "hit").increment();
        if (cached != null) return new NewsCache.Snapshot(cached.articles().stream()
                .filter(article -> !article.publishedAt().isBefore(clock.instant().minus(policy.retention())))
                .toList(), cached.lastSuccess());
        var state = repository.state(provider.name(), symbol);
        return new NewsCache.Snapshot(repository.recent(provider.name(), symbol,
                clock.instant().minus(policy.retention()), 500), state.lastSuccess());
    }
    private synchronized CompletableFuture<Void> refresh(String symbol, String correlation) {
        var existing = pending.get(symbol);
        if (existing != null) return existing;
        var future = new CompletableFuture<Void>();
        Instant deadline = clock.instant().plus(policy.deadline());
        try {
            pending.put(symbol, future);
            worker.execute(() -> {
                long started = System.nanoTime();
                try {
                    if (!clock.instant().isBefore(deadline)) throw new NewsUnavailable("DEADLINE");
                    ingestion.admit(provider.name(), symbol, clock.instant());
                    metrics.counter("news.fetch", "provider", provider.name(), "outcome", "attempt").increment();
                    var articles = provider.fetch(symbol, clock.instant().minus(policy.window()), deadline);
                    if (!clock.instant().isBefore(deadline)) throw new NewsUnavailable("DEADLINE");
                    ingestion.ingest(provider.name(), symbol, articles, clock.instant(), correlation);
                    var fresh = new NewsCache.Snapshot(repository.recent(provider.name(), symbol,
                            clock.instant().minus(policy.retention()), 500), repository.state(provider.name(), symbol).lastSuccess());
                    cache.put(provider.name(), symbol, fresh);
                    metrics.counter("news.fetch", "provider", provider.name(), "outcome", "success").increment();
                    future.complete(null);
                } catch (RuntimeException exception) {
                    String failure = exception instanceof NewsUnavailable n ? n.classification() : "DEPENDENCY";
                    try { ingestion.failure(provider.name(), symbol, clock.instant(), failure); }
                    catch (RuntimeException ignored) { metrics.counter("news.persistence.failure").increment(); }
                    metrics.counter("news.fetch", "provider", provider.name(), "outcome", failure).increment();
                    future.completeExceptionally(new NewsUnavailable(failure));
                } finally {
                    pending.remove(symbol, future);
                    metrics.timer("news.fetch.duration", "provider", provider.name())
                            .record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
                }
            });
        } catch (RejectedExecutionException exception) {
            metrics.counter("news.refresh.rejected").increment();
            pending.remove(symbol, future);
            future.completeExceptionally(new NewsUnavailable("CAPACITY"));
        }
        return future;
    }
    private static Result result(NewsCache.Snapshot snapshot, String status, int limit) {
        return new Result(snapshot.articles().stream().limit(limit).toList(), status, snapshot.lastSuccess());
    }
    private static DomainException unavailable() {
        return new DomainException(ErrorCategory.SERVICE_UNAVAILABLE, 503, "News unavailable",
                "News cannot be refreshed right now. Please retry later.");
    }
    /** Keep database cleanup off the shared scheduler used by trading maintenance. */
    public void cleanup() {
        try {
            worker.execute(() -> {
                try { ingestion.cleanup(clock.instant()); }
                catch (RuntimeException exception) { metrics.counter("news.cleanup.failure").increment(); }
            });
        } catch (RejectedExecutionException exception) { metrics.counter("news.cleanup.deferred").increment(); }
    }
    @Override public void close() {
        worker.shutdownNow();
        pending.values().forEach(future -> future.completeExceptionally(new NewsUnavailable("SHUTDOWN")));
        pending.clear();
    }
}
