package com.project.trading.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformerV2;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.project.trading.support.TradingInfrastructure;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

/** Kills a separate Core JVM while a local HTTPS broker holds its acknowledgement. */
class BrokerCallCrashIntegrationTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private static final String API = "/v1/api";
    private static final String SUBMIT = API + "/iserver/account/DU1234567/orders";

    @Test
    void restartReconcilesPositiveBrokerTruthWithoutResendingInterruptedSubmission() throws Exception {
        Path artifacts = Files.createTempDirectory(Path.of("target"), "broker-crash-");
        String password = UUID.randomUUID().toString();
        Path key = artifacts.resolve("broker.p12");
        Path ca = artifacts.resolve("ca.pem");
        keytool("-genkeypair", "-alias", "broker", "-keyalg", "RSA", "-keysize", "2048", "-validity", "2",
                "-dname", "CN=localhost", "-ext", "SAN=dns:localhost", "-storetype", "PKCS12",
                "-keystore", key.toString(), "-storepass", password);
        keytool("-exportcert", "-rfc", "-alias", "broker", "-keystore", key.toString(),
                "-storepass", password, "-file", ca.toString());
        CountDownLatch received = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();
        AtomicReference<String> ref = new AtomicReference<>();
        ResponseDefinitionTransformerV2 observer = new ResponseDefinitionTransformerV2() {
            @Override public String getName() { return "submission-observer"; }
            @Override public ResponseDefinition transform(ServeEvent event) {
                if (SUBMIT.equals(event.getRequest().getUrl())) {
                    ref.set(JSON.readTree(event.getRequest().getBodyAsString()).path("orders").get(0).path("cOID").stringValue());
                    invocations.incrementAndGet();
                    received.countDown();
                }
                return event.getResponseDefinition();
            }
        };
        WireMockServer broker = new WireMockServer(wireMockConfig().httpDisabled(true).dynamicHttpsPort()
                .keystorePath(key.toString()).keystorePassword(password).keyManagerPassword(password)
                .keystoreType("PKCS12").extensions(observer));
        Process child = null;
        TradingInfrastructure infrastructure = TradingInfrastructure.start();
        try {
            broker.start();
            fixtures(broker);
            Map<String, Object> properties = new HashMap<>();
            infrastructure.register((name, value) -> properties.put(name, value.get()));
            int port;
            try (ServerSocket socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
            child = start(properties, broker.httpsPort(), ca, port, artifacts.resolve("first.log"));
            awaitReady(child, port);
            freshQuote(properties);
            String body = "{\"symbol\":\"NVDA\",\"intent\":\"BUY\",\"orderType\":\"MARKET\",\"quantity\":\"1\",\"timeInForce\":\"DAY\"}";
            var pending = HTTP.sendAsync(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/orders"))
                    .timeout(Duration.ofSeconds(60)).header("Content-Type", "application/json")
                    .header("Idempotency-Key", UUID.randomUUID().toString())
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
            boolean sent = received.await(30, TimeUnit.SECONDS);
            assertThat(sent).as("broker received submission before termination; HTTP outcome=%s",
                    pending.isDone() ? pending.get().body() : "still pending").isTrue();
            assertThat(status(properties)).isEqualTo("SUBMISSION_PENDING");
            assertThat(count(properties, "executions")).isZero();
            child.destroyForcibly();
            assertThat(child.waitFor(10, TimeUnit.SECONDS)).isTrue();
            pending.cancel(true); // An uncertain HTTP result is never resubmitted.
            assertThat(status(properties)).isEqualTo("SUBMISSION_PENDING");
            broker.stubFor(get(urlEqualTo(API + "/iserver/account/orders")).willReturn(okJson(
                    "{\"orders\":[{\"acct\":\"DU1234567\",\"orderId\":7001,\"order_ref\":\"" + ref.get()
                            + "\",\"status\":\"Filled\",\"filledQuantity\":1,\"remainingQuantity\":0}],\"snapshot\":true}")));
            broker.stubFor(get(urlEqualTo(API + "/iserver/account/trades")).willReturn(okJson(
                    "[{\"execution_id\":\"crash-recovery.1\",\"order_ref\":\"" + ref.get()
                            + "\",\"size\":1,\"price\":\"185.30\",\"commission\":\"1.00\",\"trade_time_r\":"
                            + Instant.now().toEpochMilli() + ",\"account\":\"DU1234567\"}]")));
            child = start(properties, broker.httpsPort(), ca, port, artifacts.resolve("restarted.log"));
            awaitReady(child, port);
            Instant deadline = Instant.now().plusSeconds(60);
            while (!"FILLED".equals(status(properties)) && Instant.now().isBefore(deadline)) Thread.sleep(250);
            assertThat(status(properties)).as("broker journal=%s", broker.getAllServeEvents().stream()
                    .map(e -> e.getRequest().getMethod() + " " + e.getRequest().getUrl() + " -> "
                            + e.getResponseDefinition().getStatus()).toList()).isEqualTo("FILLED");
            assertThat(count(properties, "executions")).isEqualTo(1);
            Thread.sleep(6000); // A further reconciliation sees the same execution.
            assertThat(count(properties, "executions")).isEqualTo(1);
            assertThat(count(properties, "orders")).isEqualTo(1);
            assertThat(invocations.get()).as("no submission replay after restart").isEqualTo(1);
        } finally {
            if (child != null && child.isAlive()) {
                child.destroyForcibly();
                child.waitFor(10, TimeUnit.SECONDS);
            }
            broker.stop();
            infrastructure.close();
        }
    }

    private static Process start(Map<String, Object> properties, int brokerPort, Path ca, int port, Path log) throws Exception {
        List<String> command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m", "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                "com.project.trading.TradingCoreApplication"));
        properties.forEach((name, value) -> command.add("--" + name + "=" + value));
        command.addAll(List.of("--spring.config.import=", "--server.port=" + port, "--management.server.port=0",
                "--app.runtime-mode=IBKR_PAPER", "--app.trusted-environment=true", "--app.account-id=DU1234567",
                "--app.ibkr.base-url=https://localhost:" + brokerPort + API, "--app.ibkr.ca-file=" + ca.toAbsolutePath(),
                "--app.ibkr.request-timeout=25s", "--app.orders.confirmation-sweep-grace=35s",
                "--app.orders.quote-max-age=60s", "--spring.data.redis.timeout=5s", "--spring.data.redis.connect-timeout=5s",
                "--app.outbox.publisher-enabled=false", "--news.enrichment.enabled=false",
                "--app.broker-updates.bootstrap-servers=localhost:1", "--app.reconciliation.interval=1s",
                "--app.reconciliation.min-spacing=1s", "--app.reconciliation.initial-delay=1s"));
        command.add("--logging.level.com.project.trading.broker.infrastructure.ibkr=DEBUG");
        return new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    }

    private static void awaitReady(Process child, int port) throws Exception {
        // Startup on a shared Docker test host is not a service latency benchmark.
        Instant deadline = Instant.now().plusSeconds(240);
        while (Instant.now().isBefore(deadline)) {
            assertThat(child.isAlive()).as("Core child exited; inspect target/broker-crash-* logs").isTrue();
            try {
                if (HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/orders"))
                        .timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.discarding()).statusCode() == 200) return;
            } catch (java.io.IOException ignored) { /* Startup is bounded by the deadline. */ }
            Thread.sleep(250);
        }
        throw new AssertionError("Core child did not become ready");
    }

    private static String status(Map<String, Object> properties) throws Exception {
        return scalar(properties, "select status from trading.orders");
    }
    private static long count(Map<String, Object> properties, String table) throws Exception {
        return Long.parseLong(scalar(properties, "select count(*) from trading." + table));
    }
    private static String scalar(Map<String, Object> properties, String sql) throws Exception {
        try (var connection = DriverManager.getConnection(properties.get("spring.datasource.url").toString(),
                properties.get("spring.datasource.username").toString(), properties.get("spring.datasource.password").toString());
             var statement = connection.createStatement(); var rows = statement.executeQuery(sql)) {
            assertThat(rows.next()).isTrue();
            return rows.getString(1);
        }
    }

    private static void freshQuote(Map<String, Object> properties) {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(properties.get("spring.data.redis.host").toString(),
                (Integer) properties.get("spring.data.redis.port"));
        config.setUsername(properties.get("spring.data.redis.username").toString());
        config.setPassword(properties.get("spring.data.redis.password").toString());
        LettuceConnectionFactory factory = new LettuceConnectionFactory(config);
        factory.afterPropertiesSet();
        try {
            new StringRedisTemplate(factory).opsForValue().set("quote:IBKR:NVDA", """
                    {"type":"quote","symbol":"NVDA","bid":"185.20","ask":"185.30","last":"185.25",
                    "bidSize":100,"askSize":100,"volume":1000,"sequence":1,"timestamp":"%s",
                    "stale":false,"dataMode":"REALTIME","halted":false}""".formatted(Instant.now()), Duration.ofMinutes(5));
        } finally { factory.destroy(); }
    }

    private static void fixtures(WireMockServer broker) {
        broker.stubFor(post(urlEqualTo(API + "/iserver/auth/status")).willReturn(okJson(
                "{\"authenticated\":true,\"competing\":false,\"connected\":true,\"established\":true}")));
        broker.stubFor(get(urlEqualTo(API + "/iserver/accounts")).willReturn(okJson(
                "{\"accounts\":[\"DU1234567\"],\"selectedAccount\":\"DU1234567\",\"isPaper\":true}")));
        broker.stubFor(get(urlEqualTo(API + "/trsrv/stocks?symbols=NVDA")).willReturn(okJson(
                "{\"NVDA\":[{\"name\":\"NVIDIA CORP\",\"assetClass\":\"STK\",\"contracts\":[{\"conid\":4815747,\"exchange\":\"NASDAQ\",\"isUS\":true}]}]}")));
        broker.stubFor(get(urlEqualTo(API + "/trsrv/secdef?conids=4815747")).willReturn(okJson(
                "{\"secdef\":[{\"conid\":4815747,\"currency\":\"USD\",\"name\":\"NVIDIA CORP\",\"assetClass\":\"STK\",\"listingExchange\":\"NASDAQ\",\"ticker\":\"NVDA\",\"isUS\":true}]}")));
        broker.stubFor(post(urlEqualTo(API + "/iserver/contract/rules")).willReturn(okJson(
                "{\"orderTypes\":[\"limit\",\"market\"],\"increment\":0.01,\"incrementDigits\":2,\"tifTypes\":[\"DAY\",\"GTC\"]}")));
        broker.stubFor(get(urlEqualTo(API + "/iserver/account/orders")).willReturn(okJson("{\"orders\":[],\"snapshot\":true}")));
        broker.stubFor(get(urlEqualTo(API + "/iserver/account/trades")).willReturn(okJson("[]")));
        broker.stubFor(post(urlEqualTo(SUBMIT)).willReturn(okJson("[{\"order_id\":\"7001\",\"order_status\":\"Submitted\"}]").withFixedDelay(30000)));
    }

    private static void keytool(String... args) throws Exception {
        List<String> command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "keytool").toString()));
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        assertThat(process.waitFor(60, TimeUnit.SECONDS)).isTrue();
        assertThat(process.exitValue()).isZero();
    }
}
