package com.project.trading.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.project.trading.support.ContractSchemas;
import com.project.trading.support.TradingInfrastructure;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
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
 * Real Kafka carries broker order observations (as the realtime gateway publishes them) to Trading Core.
 * <ul>
 *   <li>BUY with a broker confirmation; its fill is never published (a lost event) and reconciliation against the
 *   broker brings the order and position to broker truth;</li>
 *   <li>SELL filled through a published broker update; redelivered and duplicate observations change nothing;</li>
 *   <li>an order whose submission timed out (UNKNOWN) is resolved through its client order reference;</li>
 *   <li>a SHORT with unavailable shortability that IBKR rejects; account metrics from the broker.</li>
 * </ul>
 * Every response and published event is validated against the contract schemas. No real IBKR session is involved.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "app.runtime-mode=IBKR_PAPER",
        "app.trusted-environment=true",
        "app.account-id=DU1234567",
        "app.http.cors-allowed-origins=http://localhost:3000",
        "app.watchlist.default-symbols=NVDA",
        "app.ibkr.request-timeout=2s",
        "app.reconciliation.interval=2s",
        "app.reconciliation.min-spacing=1s",
        "app.reconciliation.initial-delay=1s",
        "management.server.port=0",
        "spring.data.redis.timeout=5s",
        "spring.data.redis.connect-timeout=5s",
        "SECRETS_DIR=/nonexistent/"})
class IbkrPaperWorkflowIntegrationTest {

    private static final String ACCOUNT = "DU1234567";
    private static final String API = "/v1/api";
    private static final String ORDERS = API + "/iserver/account/" + ACCOUNT + "/orders";

    static final TradingInfrastructure INFRASTRUCTURE = TradingInfrastructure.start();
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    private static final String BROKER_UPDATES = "broker.order-updates.v1";
    static final Path CA_FILE;
    static final WireMockServer IBKR;

    static {
        KAFKA.start();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(BROKER_UPDATES, 3, (short) 1),
                    new NewTopic("trading.order-events.v1", 3, (short) 1),
                    new NewTopic("trading.execution-events.v1", 3, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

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
        registry.add("app.broker-updates.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("app.outbox.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @AfterAll
    static void stopGateway() {
        IBKR.stop();
        KAFKA.stop();
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
        brokerView(List.of(), List.of());
        // A BUY of AMD whose answer never arrives in time: the order's outcome is unknown.
        IBKR.stubFor(post(urlEqualTo(ORDERS)).withRequestBody(matchingJsonPath("$.orders[0].conid", equalTo("4391")))
                .withRequestBody(matchingJsonPath("$.orders[0].side", equalTo("BUY")))
                .willReturn(okJson("[{\"order_id\":\"5003\",\"order_status\":\"Submitted\"}]").withFixedDelay(4000)));
    }

    private static void contract(String symbol, long conid, String name, String exchange) {
        IBKR.stubFor(get(urlEqualTo(API + "/trsrv/stocks?symbols=" + symbol)).willReturn(okJson(
                "{\"" + symbol + "\":[{\"name\":\"" + name + "\",\"assetClass\":\"STK\",\"contracts\":[{\"conid\":" + conid
                        + ",\"exchange\":\"" + exchange + "\",\"isUS\":true}]}]}")));
        IBKR.stubFor(get(urlEqualTo(API + "/trsrv/secdef?conids=" + conid)).willReturn(okJson(
                "{\"secdef\":[{\"conid\":" + conid + ",\"currency\":\"USD\",\"name\":\"" + name + "\",\"assetClass\":\"STK\","
                        + "\"listingExchange\":\"" + exchange + "\",\"ticker\":\"" + symbol + "\",\"isUS\":true}]}")));
    }

    /**
     * The broker's view used by reconciliation: its current-day orders and recent executions (response shapes as
     * documented by IBKR). Each order is {orderId, ref, status, filled}; each execution {exec, ref, qty, price}.
     */
    private static void brokerView(List<List<String>> orders, List<List<String>> executions) {
        StringBuilder o = new StringBuilder();
        for (List<String> row : orders) {
            o.append(o.isEmpty() ? "" : ",").append("""
                    {"acct":"%s","orderId":%s,"order_ref":"%s","status":"%s","filledQuantity":%s,"remainingQuantity":0.0}"""
                    .formatted(ACCOUNT, row.get(0), row.get(1), row.get(2), row.get(3)));
        }
        StringBuilder t = new StringBuilder();
        for (List<String> row : executions) {
            t.append(t.isEmpty() ? "" : ",").append("""
                    {"execution_id":"%s","order_ref":"%s","size":%s,"price":"%s","commission":"1.00","trade_time_r":%d,"account":"%s"}"""
                    .formatted(row.get(0), row.get(1), row.get(2), row.get(3), Instant.now().toEpochMilli(), ACCOUNT));
        }
        IBKR.stubFor(get(urlEqualTo(API + "/iserver/account/orders")).willReturn(okJson("{\"orders\":[" + o + "],\"snapshot\":true}")));
        IBKR.stubFor(get(urlEqualTo(API + "/iserver/account/trades")).willReturn(okJson("[" + t + "]")));
    }

    /** Publishes a broker execution observation exactly as the realtime gateway does (validated against the contract). */
    private static void publishExecution(UUID eventId, String brokerOrderId, String ref, String executionId, String qty,
                                         String price) throws Exception {
        String event = """
                {"eventId":"%s","eventType":"BROKER_EXECUTION_OBSERVED","eventVersion":1,"occurredAt":"%s",
                 "source":"realtime-gateway","correlationId":"%s","accountId":"%s",
                 "payload":{"brokerOrderId":"%s","clientOrderRef":"%s","observedStatus":"FILLED","brokerStatusRaw":"Filled",
                  "filledQuantity":"%s","remainingQuantity":"0","averagePrice":null,
                  "execution":{"brokerExecutionId":"%s","quantity":"%s","price":"%s","executedAt":"%s"},
                  "sourceTimestamp":"%s"}}""".formatted(eventId, Instant.now(), UUID.randomUUID(), ACCOUNT, brokerOrderId,
                ref, qty, executionId, qty, price, Instant.now(), Instant.now());
        ContractSchemas.assertValid("events/broker-order-update.schema.json", JSON.readTree(event));
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            producer.send(new ProducerRecord<>(BROKER_UPDATES, ACCOUNT + ":" + brokerOrderId, event)).get();
        }
    }

    private static final ObjectMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Autowired
    private Environment environment;
    @Autowired
    private StringRedisTemplate redis;
    @Autowired
    private JdbcTemplate jdbc;

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
    void buySellUnknownAndShortAgainstPaperFixtures() throws Exception {
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

        // The broker filled the BUY, but its stream update was lost: reconciliation brings the order to broker truth.
        brokerView(List.of(List.of("5001", buyRef, "Filled", "10.0")),
                List.of(List.of("0000e0d5.5001.01.01", buyRef, "10.0", "185.28")));
        JsonNode filled = awaitOrder(buyId, o -> o.get("status").stringValue().equals("FILLED"));
        assertThat(filled.get("averageFillPrice").stringValue()).isEqualTo("185.28");
        assertThat(positionQuantity("NVDA")).isEqualTo("10");

        // SELL: filled through a broker update on Kafka; the same event again and a new event for the same execution
        // change nothing.
        freshQuote();
        JsonNode sell = assertOrder(send("POST", "/api/v1/orders", order("NVDA", "SELL", "MARKET", "4", null)), 201);
        assertThat(sell.get("status").stringValue()).isEqualTo("SUBMITTED");
        assertThat(sell.get("brokerSide").stringValue()).isEqualTo("SELL");
        String sellRef = sell.get("clientOrderId").stringValue();
        UUID sellEvent = UUID.randomUUID();
        publishExecution(sellEvent, "5002", sellRef, "0000e0d5.5002.01.01", "4", "185.21");
        awaitOrder(sell.get("id").stringValue(), o -> o.get("status").stringValue().equals("FILLED"));
        assertThat(positionQuantity("NVDA")).isEqualTo("6");
        UUID duplicate = UUID.randomUUID();
        publishExecution(sellEvent, "5002", sellRef, "0000e0d5.5002.01.01", "4", "185.21");
        publishExecution(duplicate, "5002", sellRef, "0000e0d5.5002.01.01", "4", "185.21");
        awaitProcessed(duplicate);
        assertThat(jdbc.queryForObject("select count(*) from executions where broker_execution_id = ?", Long.class,
                "0000e0d5.5002.01.01")).isEqualTo(1);
        assertThat(positionQuantity("NVDA")).isEqualTo("6");

        // An order whose submission timed out has an unknown outcome; the broker knows it by its client reference.
        JsonNode unknown = assertOrder(send("POST", "/api/v1/orders", order("AMD", "BUY", "LIMIT", "3", "150.00")), 202);
        assertThat(unknown.get("status").stringValue()).isEqualTo("UNKNOWN");
        brokerView(List.of(List.of("5001", buyRef, "Filled", "10.0"),
                        List.of("5003", unknown.get("clientOrderId").stringValue(), "Submitted", "0.0")),
                List.of(List.of("0000e0d5.5001.01.01", buyRef, "10.0", "185.28")));
        JsonNode resolved = awaitOrder(unknown.get("id").stringValue(), o -> o.get("status").stringValue().equals("SUBMITTED"));
        assertThat(resolved.get("brokerOrderId").stringValue()).isEqualTo("5003");

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

    private void awaitProcessed(UUID eventId) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(30);
        while (jdbc.queryForObject("select count(*) from processed_events where event_id = ?", Long.class, eventId) == 0) {
            assertThat(Instant.now()).as("event " + eventId + " not processed").isBefore(deadline);
            Thread.sleep(200);
        }
    }
}
