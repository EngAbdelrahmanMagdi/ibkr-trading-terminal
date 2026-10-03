package com.project.trading.news.infrastructure;

import com.project.trading.news.domain.NewsUnavailable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

public class NewsHttpClient implements AutoCloseable {
    private final HttpClient client;
    private final NewsProperties properties;
    private final Clock clock;
    private final URI base;
    public NewsHttpClient(NewsProperties properties, Clock clock) {
        this(properties, clock, URI.create("https://finnhub.io"));
    }
    // Package-private test seam; production configuration never accepts an arbitrary base URL.
    NewsHttpClient(NewsProperties properties, Clock clock, URI base) {
        this.properties = properties;
        this.clock = clock;
        this.base = base;
        client = HttpClient.newBuilder().connectTimeout(properties.connectTimeout())
                .followRedirects(HttpClient.Redirect.NEVER).build();
    }
    public HttpResponse<byte[]> get(String query, Instant deadline) {
        Duration remaining = Duration.between(clock.instant(), deadline);
        if (remaining.isNegative() || remaining.isZero()) throw new NewsUnavailable("DEADLINE");
        Duration timeout = remaining.compareTo(properties.attemptTimeout()) < 0 ? remaining : properties.attemptTimeout();
        var request = HttpRequest.newBuilder(base.resolve("/api/v1/company-news?" + query))
                .timeout(timeout).header("Accept", "application/json").GET().build();
        var future = client.sendAsync(request, response -> new LimitedBody(properties.maxResponseBytes()));
        try { return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS); }
        catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new NewsUnavailable("INTERRUPTED");
        } catch (java.util.concurrent.ExecutionException exception) {
            for (Throwable cause = exception; cause != null; cause = cause.getCause())
                if (cause instanceof NewsUnavailable unavailable) throw unavailable;
            throw new NewsUnavailable("TRANSPORT");
        } catch (java.util.concurrent.TimeoutException exception) {
            throw new NewsUnavailable("TRANSPORT");
        } finally { if (!future.isDone()) future.cancel(true); }
    }
    @Override public void close() { client.shutdownNow(); }
    private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final int limit;
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private final CompletableFuture<byte[]> result = new CompletableFuture<>();
        private Flow.Subscription subscription;
        LimitedBody(int limit) { this.limit = limit; }
        @Override public CompletionStage<byte[]> getBody() { return result; }
        @Override public void onSubscribe(Flow.Subscription next) { subscription = next; next.request(1); }
        @Override public void onNext(List<ByteBuffer> items) {
            for (ByteBuffer item : items) {
                if (buffer.size() + item.remaining() > limit) {
                    subscription.cancel();
                    result.completeExceptionally(new NewsUnavailable("SIZE"));
                    return;
                }
                byte[] bytes = new byte[item.remaining()];
                item.get(bytes);
                buffer.write(bytes, 0, bytes.length);
            }
            subscription.request(1);
        }
        @Override public void onError(Throwable failure) { result.completeExceptionally(new IOException("News response failed")); }
        @Override public void onComplete() { result.complete(buffer.toByteArray()); }
    }
}
