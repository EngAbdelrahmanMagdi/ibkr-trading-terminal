package com.project.trading.news.infrastructure;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.project.trading.news.domain.NewsUnavailable;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FinnhubNewsProviderTest {
    private static final Instant NOW = Instant.parse("2026-10-03T12:00:00Z");
    static NewsProperties settings(String provider, boolean privateDevelopment, String credential, int bytes) {
        return new NewsProperties(provider, privateDevelopment, credential, Duration.ofMinutes(5), Duration.ofDays(7),
                Duration.ofSeconds(8), 1, 16, Duration.ofSeconds(2), Duration.ofSeconds(3), 2, 30, bytes, 500, 1000,
                Duration.ofDays(30), 50000, 5, Duration.ofSeconds(60), 4);
    }
    @Test void mockNeedsNoCredentialAndPrivateProviderFailsClosed() {
        assertThat(settings("FIXTURE", false, "", 2048).provider()).isEqualTo("FIXTURE");
        assertThatThrownBy(() -> settings("FINNHUB", false, "test-token", 2048)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> settings("FINNHUB", true, "", 2048)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> settings("FIXTURE", false, "", 100)).isInstanceOf(IllegalArgumentException.class);
        assertThat(settings("FINNHUB", true, "test-token", 2048).toString()).doesNotContain("test-token");
    }
    @Test void mapsDocumentedPayloadAndNeverFollowsArticleLinks() {
        var server = new WireMockServer(wireMockConfig().dynamicPort());
        server.start();
        var properties = settings("FINNHUB", true, "test-token", 2048);
        var clock = Clock.fixed(NOW, ZoneOffset.UTC);
        try (var http = new NewsHttpClient(properties, clock, URI.create(server.baseUrl()))) {
            server.stubFor(get(urlPathEqualTo("/api/v1/company-news")).withQueryParam("symbol", equalTo("NVDA"))
                    .withQueryParam("token", equalTo("test-token")).willReturn(okJson("""
                    [{"id":42,"datetime":1791028800,"related":"NVDA,AMD","headline":"Product update",
                    "source":"Publisher","url":"https://example.com/story","summary":"Plain provider text"}]
                    """)));
            var provider = new FinnhubNewsProvider(http, properties, new NewsNormalizer(), new NewsFetchPolicy(properties, clock), clock, new SimpleMeterRegistry());
            var articles = provider.fetch("NVDA", NOW.minus(Duration.ofDays(7)), NOW.plusSeconds(8));
            assertThat(articles).hasSize(1);
            assertThat(articles.getFirst().providerId()).isEqualTo("FINNHUB:42");
            server.verify(1, getRequestedFor(urlPathEqualTo("/api/v1/company-news")));
        } finally { server.stop(); }
    }
    @Test void providerRateLimitOpensCircuitWithoutRepeatedRequestsOrLeakingCredential() {
        var server = new WireMockServer(wireMockConfig().dynamicPort());
        server.start();
        var properties = settings("FINNHUB", true, "test-token", 2048);
        var clock = Clock.fixed(NOW, ZoneOffset.UTC);
        try (var http = new NewsHttpClient(properties, clock, URI.create(server.baseUrl()))) {
            server.stubFor(get(anyUrl()).willReturn(aResponse().withStatus(429).withHeader("Retry-After", "60")));
            var provider = new FinnhubNewsProvider(http, properties, new NewsNormalizer(), new NewsFetchPolicy(properties, clock), clock, new SimpleMeterRegistry());
            assertThatThrownBy(() -> provider.fetch("NVDA", NOW.minusSeconds(60), NOW.plusSeconds(8)))
                    .isInstanceOf(NewsUnavailable.class).hasMessage("News refresh unavailable");
            assertThatThrownBy(() -> provider.fetch("AMD", NOW.minusSeconds(60), NOW.plusSeconds(8))).isInstanceOf(NewsUnavailable.class);
            server.verify(1, getRequestedFor(anyUrl()));
        } finally { server.stop(); }
    }
    @Test void rejectsRedirectMalformedAndOversizedResponses() {
        var server = new WireMockServer(wireMockConfig().dynamicPort());
        server.start();
        var clock = Clock.fixed(NOW, ZoneOffset.UTC);
        var properties = settings("FINNHUB", true, "test-token", 1024);
        try {
            for (var response : new com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder[]{
                    aResponse().withStatus(302).withHeader("Location", server.baseUrl() + "/article"),
                    okJson("{\"error\":\"bad response\"}"), okJson("[" + " ".repeat(2048) + "]")}) {
                server.resetAll();
                server.stubFor(get(urlPathEqualTo("/api/v1/company-news")).willReturn(response));
                try (var http = new NewsHttpClient(properties, clock, URI.create(server.baseUrl()))) {
                    var provider = new FinnhubNewsProvider(http, properties, new NewsNormalizer(), new NewsFetchPolicy(properties, clock), clock, new SimpleMeterRegistry());
                    assertThatThrownBy(() -> provider.fetch("NVDA", NOW.minusSeconds(60), NOW.plusSeconds(8))).isInstanceOf(NewsUnavailable.class);
                }
                server.verify(0, getRequestedFor(urlPathEqualTo("/article")));
            }
        } finally { server.stop(); }
    }
}
