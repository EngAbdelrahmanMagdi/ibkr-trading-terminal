package com.project.trading.broker.infrastructure.ibkr;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.net.URI;
import java.time.Duration;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

/** IBKR Client Portal Gateway fixtures (response shapes as documented) and an adapter client pointed at WireMock. */
final class IbkrStubs {

    static final String ACCOUNT = "DU1234567";
    static final String API = "/v1/api";

    private IbkrStubs() {
    }

    /** A client over plain HTTP to WireMock (TLS is covered by the application-level test). */
    static IbkrHttp http(WireMockExtension wm, IbkrRateLimiter limiter) {
        return http(wm, limiter, Duration.ofSeconds(5));
    }

    static IbkrHttp http(WireMockExtension wm, IbkrRateLimiter limiter, Duration requestTimeout) {
        return new IbkrHttp(IbkrHttp.baseBuilder(Duration.ofSeconds(2)).build(), URI.create(wm.baseUrl() + API),
                requestTimeout, 1 << 20, limiter, new SimpleMeterRegistry());
    }

    static IbkrRateLimiter limiter() {
        return new IbkrRateLimiter(50, 16, Duration.ofSeconds(2), Duration.ofMinutes(15), System::nanoTime,
                new SimpleMeterRegistry());
    }

    /** An authenticated, established, non-competing brokerage session on a paper account. */
    static void readySession(WireMockExtension wm, boolean paper, String account) {
        wm.stubFor(post(urlEqualTo(API + "/iserver/auth/status")).willReturn(okJson(
                "{\"authenticated\":true,\"competing\":false,\"connected\":true,\"established\":true,\"message\":\"\",\"fail\":\"\"}")));
        wm.stubFor(get(urlEqualTo(API + "/iserver/accounts")).willReturn(okJson(
                "{\"accounts\":[\"" + account + "\"],\"selectedAccount\":\"" + account + "\",\"isFT\":false,\"isPaper\":" + paper + "}")));
    }

    static void readySession(WireMockExtension wm) {
        readySession(wm, true, ACCOUNT);
    }

    static com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder json(int status, String body) {
        return aResponse().withStatus(status).withHeader("Content-Type", "application/json").withBody(body);
    }
}
