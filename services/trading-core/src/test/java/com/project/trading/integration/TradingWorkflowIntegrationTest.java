package com.project.trading.integration;

import com.project.trading.broker.infrastructure.mock.MockMatcher;
import com.project.trading.support.ContractSchemas;
import com.project.trading.support.TradingInfrastructure;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The complete MOCK trading workflow over HTTP against real PostgreSQL (with the production role setup: Flyway as
 * the schema owner, the application as a DML-only role) and real Redis (ACL user, quotes in the gateway's format).
 * Every response is validated against the contract schemas.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "app.mock.matcher-interval=200ms",
        "management.server.port=0",
        "spring.data.redis.timeout=5s",
        "spring.data.redis.connect-timeout=5s",
        "SECRETS_DIR=/nonexistent/"})
class TradingWorkflowIntegrationTest {

    static final TradingInfrastructure INFRASTRUCTURE = TradingInfrastructure.start();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        INFRASTRUCTURE.register(registry);
    }

    private static final ObjectMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    record Response(int status, JsonNode body, java.net.http.HttpHeaders headers) {
    }

    @Autowired
    private Environment environment;
    @Autowired
    private StringRedisTemplate redis;
    @Autowired
    private MockMatcher matcher;

    // ---- helpers ------------------------------------------------------------------------------------------------

    private Response send(String method, String path, String body, Map<String, String> headers) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:"
                        + environment.getProperty("local.server.port") + path))
                .timeout(Duration.ofSeconds(10))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (body != null) {
            b.header("Content-Type", "application/json");
        }
        headers.forEach(b::header);
        HttpResponse<String> r = HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
        JsonNode node = r.body().isEmpty() ? null : JSON.readTree(r.body());
        return new Response(r.statusCode(), node, r.headers());
    }

    private Response get(String path) throws Exception {
        return send("GET", path, null, Map.of());
    }

    private Response placeOrder(UUID key, String json) throws Exception {
        return send("POST", "/api/v1/orders", json, Map.of("Idempotency-Key", key.toString()));
    }

    private Response placeOrder(String json) throws Exception {
        return placeOrder(UUID.randomUUID(), json);
    }

    private static String market(String symbol, String intent, String qty) {
        return "{\"symbol\":\"" + symbol + "\",\"intent\":\"" + intent + "\",\"orderType\":\"MARKET\",\"quantity\":\""
                + qty + "\",\"timeInForce\":\"DAY\"}";
    }

    private static String limit(String symbol, String intent, String qty, String price) {
        return "{\"symbol\":\"" + symbol + "\",\"intent\":\"" + intent + "\",\"orderType\":\"LIMIT\",\"quantity\":\""
                + qty + "\",\"limitPrice\":\"" + price + "\",\"timeInForce\":\"GTC\"}";
    }

    /** Writes a quote exactly as the Realtime Gateway does (the browser quote message under quote:MOCK:{SYMBOL}). */
    private void quote(String symbol, String bid, String ask, boolean stale, Instant at) {
        String json = "{\"type\":\"quote\",\"symbol\":\"" + symbol + "\",\"bid\":\"" + bid + "\",\"ask\":\"" + ask
                + "\",\"last\":\"" + bid + "\",\"bidSize\":100,\"askSize\":100,\"volume\":1000,\"sequence\":1,"
                + "\"timestamp\":\"" + at + "\",\"stale\":" + stale + ",\"dataMode\":\"REALTIME\",\"halted\":false}";
        ContractSchemas.assertValid("stream/quote.schema.json", JSON.readTree(json));
        redis.opsForValue().set("quote:MOCK:" + symbol, json, Duration.ofMinutes(5));
    }

    private void quote(String symbol, String bid, String ask) {
        quote(symbol, bid, ask, false, Instant.now());
    }

    private static JsonNode assertOrder(Response r, int status) {
        assertThat(r.status()).as(String.valueOf(r.body())).isEqualTo(status);
        ContractSchemas.assertValid("trading/order.schema.json", r.body());
        return r.body();
    }

    private static void assertProblem(Response r, int status, String category) {
        assertThat(r.status()).as(String.valueOf(r.body())).isEqualTo(status);
        assertThat(r.headers().firstValue("Content-Type")).hasValueSatisfying(v -> assertThat(v).startsWith("application/problem+json"));
        ContractSchemas.assertValid("common/problem.schema.json", r.body());
        assertThat(r.body().get("category").stringValue()).isEqualTo(category);
    }

    private JsonNode awaitOrder(String id, Predicate<JsonNode> condition) throws Exception {
        Instant deadline = Instant.now().plusSeconds(10);
        while (true) {
            JsonNode match = null;
            for (JsonNode o : get("/api/v1/orders?limit=500").body()) {
                if (o.get("id").stringValue().equals(id)) {
                    match = o;
                }
            }
            if (match != null && condition.test(match)) {
                return match;
            }
            assertThat(Instant.now()).as("order " + id + " condition not reached: " + match).isBefore(deadline);
            Thread.sleep(100);
        }
    }

    private JsonNode position(String symbol) throws Exception {
        Response r = get("/api/v1/positions");
        assertThat(r.status()).isEqualTo(200);
        ContractSchemas.assertEachValid("portfolio/position.schema.json", r.body());
        for (JsonNode p : r.body()) {
            if (p.get("symbol").stringValue().equals(symbol)) {
                return p;
            }
        }
        return null;
    }

    // ---- workflow -------------------------------------------------------------------------------------------------

    @Test
    void watchlistIsSeededAndManaged() throws Exception {
        Response list = get("/api/v1/watchlist");
        assertThat(list.status()).isEqualTo(200);
        ContractSchemas.assertValid("watchlist/watchlist.schema.json", list.body());
        assertThat(list.body().get("items")).extracting(i -> i.get("symbol").stringValue())
                .contains("NVDA", "AAPL", "SPY");

        assertProblem(send("POST", "/api/v1/watchlist/items", "{\"symbol\":\"NVDA\"}", Map.of()), 409, "CONFLICT");
        assertProblem(send("POST", "/api/v1/watchlist/items", "{\"symbol\":\"ZZZZ\"}", Map.of()), 404, "INSTRUMENT_NOT_FOUND");
        assertThat(send("DELETE", "/api/v1/watchlist/items/SPY", null, Map.of()).status()).isEqualTo(204);
        assertProblem(send("DELETE", "/api/v1/watchlist/items/SPY", null, Map.of()), 404, "VALIDATION");

        Response added = send("POST", "/api/v1/watchlist/items", "{\"symbol\":\"SPY\"}", Map.of());
        assertThat(added.status()).isEqualTo(201);
        ContractSchemas.assertValid("watchlist/watchlist.schema.json", added.body());
        JsonNode items = added.body().get("items");
        assertThat(items.get(items.size() - 1).get("symbol").stringValue()).isEqualTo("SPY");
        assertThat(items.get(items.size() - 1).get("position").asInt()).isGreaterThan(items.get(0).get("position").asInt());
    }

    @Test
    void marketOrdersFillImmediatelyAtTheAskAndTheBid() throws Exception {
        quote("AAPL", "189.95", "190.05");
        JsonNode buy = assertOrder(placeOrder(market("AAPL", "BUY", "10")), 201);
        assertThat(buy.get("status").stringValue()).isEqualTo("FILLED");
        assertThat(buy.get("averageFillPrice").stringValue()).isEqualTo("190.05");
        assertThat(buy.get("filledQuantity").stringValue()).isEqualTo("10");
        assertThat(buy.get("brokerOrderId").stringValue()).startsWith("MOCK-");

        JsonNode sell = assertOrder(placeOrder(market("AAPL", "SELL", "4")), 201);
        assertThat(sell.get("status").stringValue()).isEqualTo("FILLED");
        assertThat(sell.get("averageFillPrice").stringValue()).isEqualTo("189.95");

        Response executions = get("/api/v1/executions?orderId=" + buy.get("id").stringValue());
        ContractSchemas.assertEachValid("trading/execution.schema.json", executions.body());
        assertThat(executions.body()).hasSize(1);
        assertThat(executions.body().get(0).get("price").stringValue()).isEqualTo("190.05");
        assertThat(executions.body().get(0).get("commission").stringValue()).isEqualTo("1.00");

        JsonNode position = position("AAPL");
        assertThat(position.get("quantity").stringValue()).isEqualTo("6");
        assertThat(position.get("averageCost").stringValue()).isEqualTo("190.05");
        assertThat(position.get("realizedPnl").get("value").stringValue()).isEqualTo("-0.40");
        assertThat(position.get("marketValue").get("available").asBoolean()).isTrue();

        assertProblem(placeOrder(market("AAPL", "SELL", "7")), 422, "VALIDATION");
    }

    @Test
    void aConfirmedMarketOrderFillsAtTheQuoteAtConfirmationTime() throws Exception {
        quote("MSFT", "499.90", "500.00");
        JsonNode pending = assertOrder(placeOrder(market("MSFT", "BUY", "120")), 202);
        assertThat(pending.get("status").stringValue()).isEqualTo("PENDING_CONFIRMATION");
        assertThat(pending.get("pendingConfirmation").get("message").stringValue()).contains("50000");

        assertProblem(placeOrder(limit("NVDA", "BUY", "1", "1.00")), 409, "CONFLICT");

        quote("MSFT", "500.90", "501.00");
        Response confirmed = send("POST", "/api/v1/orders/" + pending.get("id").stringValue() + "/confirmation",
                "{\"confirm\":true}", Map.of());
        JsonNode filled = assertOrder(confirmed, 200);
        assertThat(filled.get("status").stringValue()).isEqualTo("FILLED");
        assertThat(filled.get("averageFillPrice").stringValue()).isEqualTo("501.00");
        assertThat(filled.get("pendingConfirmation").isNull()).isTrue();

        assertProblem(send("POST", "/api/v1/orders/" + pending.get("id").stringValue() + "/confirmation",
                "{\"confirm\":true}", Map.of()), 409, "CONFLICT");
    }

    @Test
    void aLimitOrderRestsSurvivesAMatcherRestartAndFillsWhenTheQuoteCrosses() throws Exception {
        quote("NVDA", "180.00", "180.10");
        JsonNode order = assertOrder(placeOrder(limit("NVDA", "BUY", "5", "179.00")), 201);
        assertThat(order.get("status").stringValue()).isEqualTo("SUBMITTED");
        Thread.sleep(600);
        assertThat(awaitOrder(order.get("id").stringValue(), o -> true).get("status").stringValue()).isEqualTo("SUBMITTED");

        matcher.stop();
        matcher.start();
        assertThat(matcher.openOrderCount()).isGreaterThanOrEqualTo(1);

        quote("NVDA", "178.80", "178.90");
        JsonNode filled = awaitOrder(order.get("id").stringValue(), o -> o.get("status").stringValue().equals("FILLED"));
        assertThat(filled.get("averageFillPrice").stringValue()).isEqualTo("178.90");
        ContractSchemas.assertValid("trading/order.schema.json", filled);
    }

    @Test
    void cancellingARestingOrderEndsCancelled() throws Exception {
        quote("AMD", "160.00", "160.10");
        JsonNode order = assertOrder(placeOrder(limit("AMD", "BUY", "3", "150.00")), 201);
        String id = order.get("id").stringValue();

        JsonNode cancelling = assertOrder(send("DELETE", "/api/v1/orders/" + id, null, Map.of()), 202);
        assertThat(cancelling.get("status").stringValue()).isIn("CANCEL_PENDING", "CANCELLED");
        awaitOrder(id, o -> o.get("status").stringValue().equals("CANCELLED"));

        assertProblem(send("DELETE", "/api/v1/orders/" + id, null, Map.of()), 409, "CONFLICT");
        assertProblem(send("DELETE", "/api/v1/orders/" + UUID.randomUUID(), null, Map.of()), 404, "VALIDATION");
    }

    @Test
    void shortOrdersFollowTheShortabilityPolicy() throws Exception {
        quote("META", "699.00", "699.50");
        JsonNode shortSale = assertOrder(placeOrder(market("META", "SHORT", "2")), 201);
        assertThat(shortSale.get("brokerSide").stringValue()).isEqualTo("SELL");
        assertThat(shortSale.get("averageFillPrice").stringValue()).isEqualTo("699.00");
        assertThat(position("META").get("quantity").stringValue()).isEqualTo("-2");

        quote("IONQ", "39.90", "40.00");
        assertProblem(placeOrder(market("IONQ", "SHORT", "1")), 422, "VALIDATION");

        quote("TSLA", "329.90", "330.10");
        JsonNode unavailable = assertOrder(placeOrder(market("TSLA", "SHORT", "1")), 201);
        assertThat(unavailable.get("status").stringValue()).isEqualTo("FILLED");

        Response search = get("/api/v1/instruments/search?q=T");
        ContractSchemas.assertEachValid("instruments/instrument.schema.json", search.body());
        JsonNode tsla = null;
        for (JsonNode i : search.body()) {
            if (i.get("symbol").stringValue().equals("TSLA")) {
                tsla = i;
            }
        }
        assertThat(tsla).isNotNull();
        assertThat(tsla.get("shortability").get("status").stringValue()).isEqualTo("UNAVAILABLE");
    }

    @Test
    void marketOrdersAreRefusedOnStaleQuotes() throws Exception {
        quote("SPY", "649.90", "650.00", true, Instant.now());
        assertProblem(placeOrder(market("SPY", "BUY", "1")), 409, "STALE_MARKET_DATA");
        quote("SPY", "649.90", "650.00", false, Instant.now().minusSeconds(60));
        assertProblem(placeOrder(market("SPY", "BUY", "1")), 409, "STALE_MARKET_DATA");
    }

    @Test
    void idempotentRetriesAndConcurrentDuplicatesCreateOneOrder() throws Exception {
        quote("SPY", "649.90", "650.00");
        UUID key = UUID.randomUUID();
        String body = limit("SPY", "BUY", "1", "600.00");
        JsonNode first = assertOrder(placeOrder(key, body), 201);
        JsonNode retry = assertOrder(placeOrder(key, limit("SPY", "BUY", "1.00", "600")), 201);
        assertThat(retry.get("id")).isEqualTo(first.get("id"));
        assertProblem(placeOrder(key, limit("SPY", "BUY", "2", "600.00")), 409, "CONFLICT");

        UUID concurrentKey = UUID.randomUUID();
        List<Callable<Response>> calls = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            calls.add(() -> placeOrder(concurrentKey, limit("SPY", "BUY", "1", "601.00")));
        }
        List<String> ids = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(4)) {
            for (Future<Response> f : pool.invokeAll(calls)) {
                ids.add(assertOrder(f.get(), 201).get("id").stringValue());
            }
        }
        assertThat(ids).hasSize(4).containsOnly(ids.get(0));

        int sameKeyOrders = 0;
        for (JsonNode o : get("/api/v1/orders?limit=500").body()) {
            if (o.get("limitPrice").isString() && o.get("limitPrice").stringValue().equals("601.00")) {
                sameKeyOrders++;
            }
        }
        assertThat(sameKeyOrders).isEqualTo(1);
    }

    @Test
    void malformedRequestsGetProblemResponses() throws Exception {
        String correlationId = UUID.randomUUID().toString();
        Response noKey = send("POST", "/api/v1/orders", market("NVDA", "BUY", "1"), Map.of("X-Correlation-Id", correlationId));
        assertProblem(noKey, 400, "VALIDATION");
        assertThat(noKey.body().get("correlationId").stringValue()).isEqualTo(correlationId);
        assertThat(noKey.headers().firstValue("X-Correlation-Id")).hasValue(correlationId);

        assertProblem(send("POST", "/api/v1/orders", market("NVDA", "BUY", "1"), Map.of("Idempotency-Key", "abc")), 400, "VALIDATION");
        assertProblem(placeOrder("{\"symbol\":"), 400, "VALIDATION");
        assertProblem(placeOrder("{\"symbol\":\"NVDA\",\"intent\":\"BUY\",\"orderType\":\"MARKET\",\"quantity\":1,\"timeInForce\":\"DAY\"}"), 400, "VALIDATION");
        assertProblem(placeOrder("{\"symbol\":\"NVDA\",\"intent\":\"BUY\",\"orderType\":\"MARKET\",\"quantity\":\"1\",\"timeInForce\":\"DAY\",\"extra\":1}"), 400, "VALIDATION");
        assertProblem(placeOrder(market("ZZZZ", "BUY", "1")), 404, "INSTRUMENT_NOT_FOUND");
        assertProblem(placeOrder("{\"symbol\":\"NVDA\",\"pad\":\"" + "x".repeat(20_000) + "\"}"), 413, "VALIDATION");
        assertProblem(get("/api/v1/orders?status=NOPE"), 400, "VALIDATION");
        assertProblem(get("/api/v1/instruments/search?q="), 400, "VALIDATION");

        Response generated = get("/api/v1/portfolio");
        assertThat(generated.headers().firstValue("X-Correlation-Id")).isPresent();
    }

    @Test
    void portfolioReportsSimulatedCashAndHonestMetrics() throws Exception {
        Response r = get("/api/v1/portfolio");
        assertThat(r.status()).isEqualTo(200);
        ContractSchemas.assertValid("portfolio/portfolio-summary.schema.json", r.body());
        assertThat(r.body().get("accountMode").stringValue()).isEqualTo("MOCK");
        assertThat(r.body().get("cash").get("available").asBoolean()).isTrue();
        assertThat(r.body().get("excessLiquidity").get("available").asBoolean()).isFalse();
        assertThat(r.body().get("excessLiquidity").get("value").isNull()).isTrue();
        assertThat(r.body().get("dayPnl").get("available").asBoolean()).isFalse();

        Response orders = get("/api/v1/orders?status=FILLED,CANCELLED&limit=5");
        ContractSchemas.assertEachValid("trading/order.schema.json", orders.body());
        assertThat(orders.body().size()).isLessThanOrEqualTo(5);
    }
}
