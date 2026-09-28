package com.project.trading.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.project.trading.support.ContractSchemas;
import com.project.trading.support.TradingInfrastructure;
import org.junit.jupiter.api.AfterAll;
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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * IBKR_PAPER-mode wiring end to end: the application in IBKR_PAPER mode against realistic IBKR Client Portal
 * Gateway fixtures (WireMock) served over HTTPS with a locally generated certificate, which is the only trusted CA
 * (hostname verification on). Real PostgreSQL and Redis; market references only from {@code quote:IBKR:*}.
 * BUY with a broker confirmation, fill and position via order polling, SELL, and a SHORT with unavailable
 * shortability that IBKR rejects. Every response is validated against the contract schemas. No real IBKR session
 * is involved.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "app.runtime-mode=IBKR_PAPER",
        "app.trusted-environment=true",
        "app.account-id=DU1234567",
        "app.http.cors-allowed-origins=http://localhost:3000",
        "app.watchlist.default-symbols=NVDA",
        "management.server.port=0",
        "spring.data.redis.timeout=5s",
        "spring.data.redis.connect-timeout=5s",
        "SECRETS_DIR=/nonexistent/"})
class IbkrPaperWorkflowIntegrationTest {

    private static final String ACCOUNT = "DU1234567";
    private static final String API = "/v1/api";
    private static final String ORDERS = API + "/iserver/account/" + ACCOUNT + "/orders";

    static final TradingInfrastructure INFRASTRUCTURE = TradingInfrastructure.start();
    static final Path CA_FILE;
    static final WireMockServer IBKR;

    static {
        try {
            Path dir = Files.createTempDirectory("ibkr-tls");
            String password = UUID.randomUUID().toString();
            Path keystore = dir.resolve("gateway.p12");
            CA_FILE = dir.resolve("ca.pem");
            keytool("-genkeypair", "-alias", "gateway", "-keyalg", "RSA", "-keysize", "2048", "-validity", "2",
                    "-dname", "CN=localhost", "-ext", "SAN=dns:localhost", "-storetype", "PKCS12",
                    "-keystore", keystore.toString(), "-storepass", password, "-keypass", password);
            keytool("-exportcert", "-rfc", "-alias", "gateway", "-keystore", keystore.toString(),
                    "-storepass", password, "-file", CA_FILE.toString());
            IBKR = new WireMockServer(wireMockConfig().httpDisabled(true).dynamicHttpsPort()
                    .keystorePath(keystore.toString()).keystorePassword(password).keyManagerPassword(password)
                    .keystoreType("PKCS12"));
            IBKR.start();
            stubGateway();
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        INFRASTRUCTURE.register(registry);
        registry.add("app.ibkr.base-url", () -> "https://localhost:" + IBKR.httpsPort() + API);
        registry.add("app.ibkr.ca-file", CA_FILE::toString);
    }

    @AfterAll
    static void stopGateway() {
        IBKR.stop();
    }

    private static void keytool(String... args) throws IOException, InterruptedException {
        List<String> command = new java.util.ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        process.getInputStream().readAllBytes();
        if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
            throw new IllegalStateException("keytool failed");
        }
    }

    /** Session, account and contract fixtures (response shapes as documented by IBKR). */
    private static void stubGateway() {
        IBKR.stubFor(post(urlEqualTo(API + "/iserver/auth/status")).willReturn(okJson(
                "{\"authenticated\":true,\"competing\":false,\"connected\":true,\"established\":true,\"message\":\"\",\"fail\":\"\"}")));
        IBKR.stubFor(get(urlEqualTo(API + "/iserver/accounts")).willReturn(okJson(
                "{\"accounts\":[\"" + ACCOUNT + "\"],\"selectedAccount\":\"" + ACCOUNT + "\",\"isFT\":false,\"isPaper\":true}")));
        contract("NVDA", 4815747, "NVIDIA CORP", "NASDAQ");
        contract("AMD", 4391, "ADVANCED MICRO DEVICES", "NASDAQ");
        IBKR.stubFor(post(urlEqualTo(API + "/iserver/contract/rules")).willReturn(okJson(
                "{\"orderTypes\":[\"limit\",\"market\"],\"increment\":0.01,\"incrementDigits\":2,\"tifTypes\":[\"DAY\",\"GTC\"]}")));
        IBKR.stubFor(get(urlPathEqualTo(API + "/iserver/marketdata/snapshot")).willReturn(okJson(
                "[{\"conid\":4391,\"_updated\":1790582400000,\"server_id\":\"q0\"}]")));
        IBKR.stubFor(post(urlEqualTo(ORDERS)).withRequestBody(matchingJsonPath("$.orders[0].side", equalTo("BUY")))
                .willReturn(okJson("""
                        [{"id":"8a1f33c2-reply-1","message":["The following order \\"BUY 10 NVDA NASDAQ.NMS @ MKT\\" exceeds the Percentage constraint of 3%.\\nAre you sure you want to submit this order?"],"isSuppressed":false,"messageIds":["o163"]}]""")));
        IBKR.stubFor(post(urlEqualTo(API + "/iserver/reply/8a1f33c2-reply-1")).withRequestBody(equalToJson("{\"confirmed\":true}"))
                .willReturn(okJson("[{\"order_id\":\"5001\",\"order_status\":\"Submitted\",\"encrypt_message\":\"1\"}]")));
        IBKR.stubFor(post(urlEqualTo(ORDERS)).withRequestBody(matchingJsonPath("$.orders[0].side", equalTo("SELL")))
                .withRequestBody(matchingJsonPath("$.orders[0].conid", equalTo("4815747")))
                .willReturn(okJson("[{\"order_id\":\"5002\",\"order_status\":\"PreSubmitted\"}]")));
        IBKR.stubFor(post(urlEqualTo(ORDERS)).withRequestBody(matchingJsonPath("$.orders[0].conid", equalTo("4391")))
                .willReturn(okJson("{\"error\":\"Order rejected: shares of AMD are not available for short sale.\"}")));
        IBKR.stubFor(get(urlEqualTo(API + "/portfolio/accounts")).willReturn(okJson(
                "[{\"id\":\"" + ACCOUNT + "\",\"accountId\":\"" + ACCOUNT + "\",\"currency\":\"USD\",\"type\":\"DEMO\"}]")));
        IBKR.stubFor(get(urlEqualTo(API + "/portfolio/" + ACCOUNT + "/ledger")).willReturn(okJson(
                "{\"BASE\":{\"cashbalance\":98145.5,\"netliquidationvalue\":100020.5,\"currency\":\"BASE\",\"key\":\"LedgerList\"}}")));
        IBKR.stubFor(get(urlEqualTo(API + "/iserver/account/pnl/partitioned")).willReturn(okJson(
                "{\"upnl\":{\"" + ACCOUNT + ".Core\":{\"rowType\":1,\"dpl\":15.7,\"nl\":100020.5,\"upl\":12.0,\"el\":95000.0,\"mv\":1852.5}}}")));
    }

    private static void contract(String symbol, long conid, String name, String exchange) {
        IBKR.stubFor(get(urlEqualTo(API + "/trsrv/stocks?symbols=" + symbol)).willReturn(okJson(
                "{\"" + symbol + "\":[{\"name\":\"" + name + "\",\"assetClass\":\"STK\",\"contracts\":[{\"conid\":" + conid
                        + ",\"exchange\":\"" + exchange + "\",\"isUS\":true}]}]}")));
        IBKR.stubFor(get(urlEqualTo(API + "/trsrv/secdef?conids=" + conid)).willReturn(okJson(
                "{\"secdef\":[{\"conid\":" + conid + ",\"currency\":\"USD\",\"name\":\"" + name + "\",\"assetClass\":\"STK\","
                        + "\"listingExchange\":\"" + exchange + "\",\"ticker\":\"" + symbol + "\",\"isUS\":true}]}")));
    }

    /** The broker's view of this application's orders: live orders (filled) and their executions. */
    private static void brokerFills(List<Map<String, String>> fills) {
        StringBuilder orders = new StringBuilder();
        StringBuilder trades = new StringBuilder();
        for (Map<String, String> f : fills) {
            orders.append(orders.isEmpty() ? "" : ",").append("""
                    {"acct":"%s","orderId":%s,"order_ref":"%s","status":"Filled","filledQuantity":%s,"remainingQuantity":0.0}"""
                    .formatted(ACCOUNT, f.get("orderId"), f.get("ref"), f.get("qty")));
            trades.append(trades.isEmpty() ? "" : ",").append("""
                    {"execution_id":"%s","order_ref":"%s","size":%s,"price":"%s","commission":"1.00","trade_time_r":%d,"account":"%s"}"""
                    .formatted(f.get("exec"), f.get("ref"), f.get("qty"), f.get("price"), Instant.now().toEpochMilli(), ACCOUNT));
        }
        IBKR.stubFor(get(urlEqualTo(API + "/iserver/account/orders")).willReturn(okJson(
                "{\"orders\":[" + orders + "],\"snapshot\":true}")));
        IBKR.stubFor(get(urlEqualTo(API + "/iserver/account/trades")).willReturn(okJson("[" + trades + "]")));
    }

    private static final ObjectMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Autowired
    private Environment environment;
    @Autowired
    private StringRedisTemplate redis;

    private record Response(int status, JsonNode body) {
    }

    private Response send(String method, String path, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:"
                        + environment.getProperty("local.server.port") + path))
                .timeout(Duration.ofSeconds(20))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (body != null) {
            b.header("Content-Type", "application/json");
        }
        if (path.equals("/api/v1/orders") && method.equals("POST")) {
            b.header("Idempotency-Key", UUID.randomUUID().toString());
        }
        HttpResponse<String> r = HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
        return new Response(r.statusCode(), r.body().isEmpty() ? null : JSON.readTree(r.body()));
    }

    private static JsonNode assertOrder(Response r, int status) {
        assertThat(r.status()).as(String.valueOf(r.body())).isEqualTo(status);
        ContractSchemas.assertValid("trading/order.schema.json", r.body());
        return r.body();
    }

    private JsonNode awaitOrder(String id, Predicate<JsonNode> condition) throws Exception {
        Instant deadline = Instant.now().plusSeconds(30);
        while (true) {
            JsonNode match = null;
            for (JsonNode o : send("GET", "/api/v1/orders?limit=500", null).body()) {
                if (o.get("id").stringValue().equals(id)) {
                    match = o;
                }
            }
            if (match != null && condition.test(match)) {
                return match;
            }
            assertThat(Instant.now()).as("order " + id + " condition not reached: " + match).isBefore(deadline);
            Thread.sleep(250);
        }
    }

    private String positionQuantity(String symbol) throws Exception {
        Response r = send("GET", "/api/v1/positions", null);
        ContractSchemas.assertEachValid("portfolio/position.schema.json", r.body());
        for (JsonNode p : r.body()) {
            if (p.get("symbol").stringValue().equals(symbol)) {
                return p.get("quantity").stringValue();
            }
        }
        return null;
    }

    private static String order(String symbol, String intent, String type, String qty, String limitPrice) {
        return "{\"symbol\":\"" + symbol + "\",\"intent\":\"" + intent + "\",\"orderType\":\"" + type + "\",\"quantity\":\""
                + qty + "\"" + (limitPrice == null ? "" : ",\"limitPrice\":\"" + limitPrice + "\"") + ",\"timeInForce\":\"DAY\"}";
    }

    /** A fresh NVDA quote exactly as the Realtime Gateway writes it in IBKR mode (the only key read in IBKR_PAPER). */
    private void freshQuote() {
        String quote = "{\"type\":\"quote\",\"symbol\":\"NVDA\",\"bid\":\"185.20\",\"ask\":\"185.30\",\"last\":\"185.25\","
                + "\"bidSize\":100,\"askSize\":100,\"volume\":1000,\"sequence\":1,\"timestamp\":\"" + Instant.now()
                + "\",\"stale\":false,\"dataMode\":\"REALTIME\",\"halted\":false}";
        ContractSchemas.assertValid("stream/quote.schema.json", JSON.readTree(quote));
        redis.opsForValue().set("quote:IBKR:NVDA", quote, Duration.ofMinutes(5));
    }

    @Test
    void buySellAndShortAgainstPaperFixtures() throws Exception {
        freshQuote();

        // BUY: the broker asks for confirmation; the user confirms; the order is acknowledged.
        JsonNode pending = assertOrder(send("POST", "/api/v1/orders", order("NVDA", "BUY", "MARKET", "10", null)), 202);
        assertThat(pending.get("status").stringValue()).isEqualTo("PENDING_CONFIRMATION");
        assertThat(pending.get("pendingConfirmation").get("message").stringValue()).contains("Percentage constraint");
        String buyId = pending.get("id").stringValue();
        String buyRef = pending.get("clientOrderId").stringValue();

        JsonNode submitted = assertOrder(send("POST", "/api/v1/orders/" + buyId + "/confirmation", "{\"confirm\":true}"), 200);
        assertThat(submitted.get("status").stringValue()).isEqualTo("SUBMITTED");
        assertThat(submitted.get("brokerOrderId").stringValue()).isEqualTo("5001");
        IBKR.verify(postRequestedFor(urlEqualTo(ORDERS)).withRequestBody(equalToJson("""
                {"orders":[{"acctId":"DU1234567","conid":4815747,"cOID":"%s","orderType":"MKT","side":"BUY","tif":"DAY","quantity":10}]}"""
                .formatted(buyRef))));

        // The fill arrives through order polling and updates the position.
        brokerFills(List.of(Map.of("orderId", "5001", "ref", buyRef, "qty", "10.0", "exec", "0000e0d5.5001.01.01", "price", "185.28")));
        JsonNode filled = awaitOrder(buyId, o -> o.get("status").stringValue().equals("FILLED"));
        assertThat(filled.get("averageFillPrice").stringValue()).isEqualTo("185.28");
        assertThat(positionQuantity("NVDA")).isEqualTo("10");

        // SELL part of the long position.
        freshQuote();
        JsonNode sell = assertOrder(send("POST", "/api/v1/orders", order("NVDA", "SELL", "MARKET", "4", null)), 201);
        assertThat(sell.get("status").stringValue()).isEqualTo("SUBMITTED");
        assertThat(sell.get("brokerSide").stringValue()).isEqualTo("SELL");
        brokerFills(List.of(
                Map.of("orderId", "5001", "ref", buyRef, "qty", "10.0", "exec", "0000e0d5.5001.01.01", "price", "185.28"),
                Map.of("orderId", "5002", "ref", sell.get("clientOrderId").stringValue(), "qty", "4.0",
                        "exec", "0000e0d5.5002.01.01", "price", "185.21")));
        awaitOrder(sell.get("id").stringValue(), o -> o.get("status").stringValue().equals("FILLED"));
        assertThat(positionQuantity("NVDA")).isEqualTo("6");

        // SHORT with unavailable shortability is not blocked locally; IBKR rejects it.
        JsonNode shortOrder = assertOrder(send("POST", "/api/v1/orders", order("AMD", "SHORT", "LIMIT", "5", "150.00")), 201);
        assertThat(shortOrder.get("status").stringValue()).isEqualTo("REJECTED");
        assertThat(shortOrder.get("brokerSide").stringValue()).isEqualTo("SELL");
        assertThat(shortOrder.get("rejectionReason").stringValue()).contains("not available for short sale");

        // Account metrics come from the broker; buying power is not reported and stays unavailable.
        Response portfolio = send("GET", "/api/v1/portfolio", null);
        assertThat(portfolio.status()).isEqualTo(200);
        ContractSchemas.assertValid("portfolio/portfolio-summary.schema.json", portfolio.body());
        assertThat(portfolio.body().get("accountMode").stringValue()).isEqualTo("IBKR_PAPER");
        assertThat(portfolio.body().get("cash").get("value").stringValue()).isEqualTo("98145.50");
        assertThat(portfolio.body().get("netLiquidation").get("value").stringValue()).isEqualTo("100020.50");
        assertThat(portfolio.body().get("excessLiquidity").get("value").stringValue()).isEqualTo("95000.00");
        assertThat(portfolio.body().get("dayPnl").get("value").stringValue()).isEqualTo("15.70");
        assertThat(portfolio.body().get("buyingPower").get("available").asBoolean()).isFalse();
    }
}
