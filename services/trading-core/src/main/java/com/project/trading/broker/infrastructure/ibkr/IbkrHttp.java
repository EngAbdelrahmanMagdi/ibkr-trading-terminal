package com.project.trading.broker.infrastructure.ibkr;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.MissingNode;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * HTTPS client of the local IBKR Client Portal Gateway. Every request goes through the rate limiter, has a connect
 * and an overall timeout, is never redirected, and reads at most maxResponseBytes. The gateway certificate must
 * chain to the configured CA and match the host name (verification is never disabled). URLs, bodies and cookies
 * are never logged.
 * <p>
 * Failures carry whether the request can have reached IBKR: an order request that may have been sent must never be
 * treated as not sent.
 */
final class IbkrHttp {

    private static final Logger log = LoggerFactory.getLogger(IbkrHttp.class);

    /** A response: the status and the JSON body (a missing node when the body is empty or not JSON). */
    record Response(int status, JsonNode body) {
        boolean ok() {
            return status >= 200 && status < 300;
        }
    }

    /** A request that produced no usable response. */
    static final class CallException extends Exception {
        private static final long serialVersionUID = 1L;

        enum Kind {
            /** Definitely not sent (rate limiter, connection refused, TLS handshake failure). */
            NOT_SENT,
            /** Possibly sent: timeout, connection reset or an unreadable response. */
            MAYBE_SENT
        }

        private final Kind kind;

        CallException(Kind kind, String message) {
            super(message);
            this.kind = kind;
        }

        Kind kind() {
            return kind;
        }
    }

    private static final String USER_AGENT = "trading-terminal-trading-core";

    private final HttpClient client;
    private final String baseUrl;
    private final Duration requestTimeout;
    private final int maxResponseBytes;
    private final IbkrRateLimiter limiter;
    private final MeterRegistry registry;
    private final ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>();

    IbkrHttp(HttpClient client, URI baseUrl, Duration requestTimeout, int maxResponseBytes, IbkrRateLimiter limiter,
             MeterRegistry registry) {
        this.client = client;
        String base = baseUrl.toString();
        this.baseUrl = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        this.requestTimeout = requestTimeout;
        this.maxResponseBytes = maxResponseBytes;
        this.limiter = limiter;
        this.registry = registry;
    }

    /** The client used in production: trusts only the CA in caFile, TLS 1.2+, HTTP/1.1, no redirects. */
    static HttpClient newClient(Path caFile, Duration connectTimeout) {
        SSLParameters tls = new SSLParameters();
        tls.setProtocols(new String[] {"TLSv1.3", "TLSv1.2"});
        return baseBuilder(connectTimeout).sslContext(trusting(caFile)).sslParameters(tls).build();
    }

    /** Builder with everything but TLS (tests of the protocol logic use plain HTTP). */
    static HttpClient.Builder baseBuilder(Duration connectTimeout) {
        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(connectTimeout)
                .cookieHandler(new CookieManager(null, CookiePolicy.ACCEPT_ORIGINAL_SERVER));
    }

    private static SSLContext trusting(Path caFile) {
        try (InputStream in = Files.newInputStream(caFile)) {
            Collection<? extends Certificate> certificates = CertificateFactory.getInstance("X.509").generateCertificates(in);
            if (certificates.isEmpty()) {
                throw new IllegalStateException("the IBKR CA file contains no certificate");
            }
            KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
            store.load(null, null);
            int i = 0;
            for (Certificate certificate : certificates) {
                store.setCertificateEntry("ibkr-ca-" + i++, certificate);
            }
            TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(store);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, trust.getTrustManagers(), null);
            return context;
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalStateException("cannot load the IBKR CA file", e);
        }
    }

    Response get(IbkrEndpoint endpoint, String path) throws CallException {
        return send(endpoint, HttpRequest.newBuilder(uri(path)).GET());
    }

    Response post(IbkrEndpoint endpoint, String path, JsonNode body) throws CallException {
        byte[] bytes = body == null ? new byte[0] : IbkrJson.MAPPER.writeValueAsBytes(body);
        return send(endpoint, HttpRequest.newBuilder(uri(path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(bytes)));
    }

    Response delete(IbkrEndpoint endpoint, String path) throws CallException {
        return send(endpoint, HttpRequest.newBuilder(uri(path)).DELETE());
    }

    /** IBKR answered 429 elsewhere (for example seen by a caller): pause requests. */
    void reportRateLimited() {
        limiter.report429();
    }

    boolean isCoolingDown() {
        return limiter.isCoolingDown();
    }

    private URI uri(String path) {
        return URI.create(baseUrl + path);
    }

    private Response send(IbkrEndpoint endpoint, HttpRequest.Builder builder) throws CallException {
        try {
            limiter.acquire(endpoint);
        } catch (IbkrRateLimiter.RejectedException e) {
            count(endpoint, "not_sent");
            throw new CallException(CallException.Kind.NOT_SENT, e.getMessage());
        }
        HttpRequest request = builder.timeout(requestTimeout)
                .header("Accept", "application/json")
                .header("User-Agent", USER_AGENT)
                .build();
        CompletableFuture<HttpResponse<byte[]>> future = client.sendAsync(request, info -> new LimitedBody(maxResponseBytes));
        HttpResponse<byte[]> response;
        try {
            response = future.get(requestTimeout.toMillis() + 1000, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            count(endpoint, "timeout");
            throw new CallException(CallException.Kind.MAYBE_SENT, "IBKR request timed out");
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            count(endpoint, "error");
            throw new CallException(CallException.Kind.MAYBE_SENT, "interrupted during the IBKR request");
        } catch (ExecutionException e) {
            throw failure(endpoint, e.getCause());
        }
        int status = response.statusCode();
        if (status == 429) {
            limiter.report429();
        }
        count(endpoint, outcome(status));
        return new Response(status, parse(response.body(), status));
    }

    private CallException failure(IbkrEndpoint endpoint, Throwable cause) {
        if (cause instanceof HttpConnectTimeoutException || cause instanceof ConnectException
                || cause instanceof SSLHandshakeException) {
            count(endpoint, "not_sent");
            log.debug("IBKR connection failed: {}", cause.getClass().getSimpleName());
            return new CallException(CallException.Kind.NOT_SENT, "cannot connect to the IBKR gateway");
        }
        if (cause instanceof HttpTimeoutException) {
            count(endpoint, "timeout");
            return new CallException(CallException.Kind.MAYBE_SENT, "IBKR request timed out");
        }
        count(endpoint, "error");
        log.debug("IBKR request failed: {}", cause == null ? "unknown" : cause.getClass().getSimpleName());
        return new CallException(CallException.Kind.MAYBE_SENT, "IBKR request failed");
    }

    private static JsonNode parse(byte[] body, int status) throws CallException {
        if (body.length == 0) {
            return MissingNode.getInstance();
        }
        try {
            return IbkrJson.MAPPER.readTree(body);
        } catch (JacksonException e) {
            if (status >= 200 && status < 300) {
                throw new CallException(CallException.Kind.MAYBE_SENT, "unreadable IBKR response");
            }
            return MissingNode.getInstance();
        }
    }

    private static String outcome(int status) {
        if (status == 429) {
            return "rate_limited";
        }
        if (status >= 500) {
            return "server_error";
        }
        return status >= 400 ? "client_error" : "ok";
    }

    private void count(IbkrEndpoint endpoint, String outcome) {
        counters.computeIfAbsent(endpoint.metricName() + ':' + outcome, k -> Counter.builder("external.api.requests")
                .tag("provider", "ibkr").tag("endpoint", endpoint.metricName()).tag("outcome", outcome)
                .description("Requests to external providers").register(registry)).increment();
    }

    /** Collects the body, failing once it exceeds the limit (bounded memory per response). */
    private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {

        private final int limit;
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private final CompletableFuture<byte[]> result = new CompletableFuture<>();
        private Flow.Subscription subscription;

        LimitedBody(int limit) {
            this.limit = limit;
        }

        @Override
        public CompletionStage<byte[]> getBody() {
            return result;
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            subscription = s;
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            for (ByteBuffer item : items) {
                if (buffer.size() + item.remaining() > limit) {
                    subscription.cancel();
                    result.completeExceptionally(new IOException("IBKR response exceeds the size limit"));
                    return;
                }
                byte[] bytes = new byte[item.remaining()];
                item.get(bytes);
                buffer.write(bytes, 0, bytes.length);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            result.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            result.complete(buffer.toByteArray());
        }
    }
}
