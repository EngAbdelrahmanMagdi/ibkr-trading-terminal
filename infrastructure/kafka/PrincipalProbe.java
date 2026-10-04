import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;

/** Tests the broker mapping and a real authorization request before provisioning application ACLs. */
class PrincipalProbe {
    public static void main(String[] args) throws Exception {
        String rules = "RULE:^CN=(kafka-broker|kafka-operator|trading-core|realtime-gateway|ai-insights)$/$1/,DEFAULT";
        var mapper = SslPrincipalMapper.fromRules(rules);
        for (String identity : List.of("kafka-broker", "kafka-operator", "trading-core", "realtime-gateway", "ai-insights")) {
            if (!mapper.getName("CN=" + identity).equals(identity)) throw new IllegalStateException("principal_mapping");
        }
        if (mapper.getName("CN=ai-insights,OU=foreign").equals("ai-insights")) throw new IllegalStateException("subject_mapping");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:29092"));
        properties.setProperty("security.protocol", "SSL");
        properties.setProperty("ssl.keystore.type", "PEM");
        properties.setProperty("ssl.truststore.type", "PEM");
        properties.setProperty("ssl.truststore.location", "/run/secrets/kafka_ca");
        properties.setProperty("ssl.endpoint.identification.algorithm", "https");
        properties.setProperty("default.api.timeout.ms", "30000");
        properties.setProperty("request.timeout.ms", "20000");
        String identity = args[0];
        properties.setProperty("client.id", "principal-probe-" + identity);
        properties.setProperty("ssl.keystore.location", "/run/secrets/probe_" + identity);
        if (args.length > 1 && args[1].equals("tls-failure")) {
            rejectTls(properties);
            return;
        }
        try (Admin client = Admin.create(properties)) {
            // Creation always performs authorization, even when the topic already exists.
            boolean administrative = identity.equals("kafka-broker") || identity.equals("kafka-operator");
            boolean denied = false;
            try { client.createTopics(List.of(new NewTopic("news.raw.v1", Integer.parseInt(System.getenv().getOrDefault("KAFKA_TOPIC_PARTITIONS", "3")), (short) 1))).all().get(40, TimeUnit.SECONDS); }
            catch (java.util.concurrent.ExecutionException failure) {
                denied = failure.getCause() instanceof org.apache.kafka.common.errors.TopicAuthorizationException
                        || failure.getCause() instanceof org.apache.kafka.common.errors.ClusterAuthorizationException;
                if (!denied && !(failure.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) throw failure;
            }
            if (denied == administrative) throw new IllegalStateException("administration_policy:" + identity);
            if (args.length > 1 && args[1].equals("access")) {
                String allowed = identity.equals("realtime-gateway") ? "trading.order-events.v1" : "news.raw.v1";
                String forbidden = identity.equals("trading-core") ? "news.ai-state.v1"
                        : identity.equals("realtime-gateway") ? "news.raw.v1" : "trading.execution-events.v1";
                client.describeTopics(List.of(allowed)).allTopicNames().get(40, TimeUnit.SECONDS);
                try {
                    client.describeTopics(List.of(forbidden)).allTopicNames().get(40, TimeUnit.SECONDS);
                    throw new IllegalStateException("cross_service_access:" + identity);
                } catch (java.util.concurrent.ExecutionException failure) {
                    if (!(failure.getCause() instanceof org.apache.kafka.common.errors.TopicAuthorizationException)) throw failure;
                }
                try {
                    client.describeConsumerGroups(List.of("foreign-service-group")).all().get(40, TimeUnit.SECONDS);
                    throw new IllegalStateException("cross_service_group:" + identity);
                } catch (java.util.concurrent.ExecutionException failure) {
                    if (!(failure.getCause() instanceof org.apache.kafka.common.errors.GroupAuthorizationException)) throw failure;
                }
                try {
                    client.describeTransactions(List.of("foreign-service-transaction")).all().get(40, TimeUnit.SECONDS);
                    throw new IllegalStateException("cross_service_transaction:" + identity);
                } catch (java.util.concurrent.ExecutionException failure) {
                    if (!(failure.getCause() instanceof org.apache.kafka.common.errors.TransactionalIdAuthorizationException)) throw failure;
                }
                System.out.println("verified least-privilege topic access User:" + identity);
            }
        }
        Path evidence = Path.of("/audit/principals.log");
        String observed = Files.readString(evidence);
        String expected = args.length > 1 && args[1].equals("unknown") ? "CN=kafka-unmatched" : identity;
        if (!observed.contains("Principal = User:" + expected + " ")) throw new IllegalStateException("broker_principal_not_observed:" + identity);
        System.out.println("verified Kafka principal User:" + identity);
    }

    private static void rejectTls(Properties properties) throws Exception {
        var certificates = java.security.cert.CertificateFactory.getInstance("X.509");
        String pem = Files.readString(Path.of(properties.getProperty("ssl.keystore.location")));
        String encoded = pem.split("-----BEGIN PRIVATE KEY-----")[1].split("-----END PRIVATE KEY-----")[0].replaceAll("\\s", "");
        var key = java.security.KeyFactory.getInstance("RSA").generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(java.util.Base64.getDecoder().decode(encoded)));
        var leaf = certificates.generateCertificate(new java.io.ByteArrayInputStream(pem.substring(pem.indexOf("-----BEGIN CERTIFICATE-----")).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        var ca = certificates.generateCertificate(Files.newInputStream(Path.of("/run/secrets/kafka_ca")));
        var keys = java.security.KeyStore.getInstance("PKCS12"); keys.load(null, null);
        keys.setKeyEntry("identity", key, new char[0], new java.security.cert.Certificate[]{leaf});
        var trust = java.security.KeyStore.getInstance("PKCS12"); trust.load(null, null); trust.setCertificateEntry("ca", ca);
        var km = javax.net.ssl.KeyManagerFactory.getInstance(javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm()); km.init(keys, new char[0]);
        var tm = javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm()); tm.init(trust);
        var context = javax.net.ssl.SSLContext.getInstance("TLS"); context.init(km.getKeyManagers(), tm.getTrustManagers(), null);
        String[] endpoint = properties.getProperty("bootstrap.servers").split(":");
        try (var socket = (javax.net.ssl.SSLSocket) context.getSocketFactory().createSocket()) {
            socket.connect(new java.net.InetSocketAddress(endpoint[0], Integer.parseInt(endpoint[1])), 5000);
            socket.setSoTimeout(5000);
            socket.setEnabledProtocols(new String[]{"TLSv1.2"});
            var parameters = socket.getSSLParameters(); parameters.setEndpointIdentificationAlgorithm("HTTPS"); socket.setSSLParameters(parameters);
            try { socket.startHandshake(); throw new IllegalStateException("invalid_tls_identity_accepted"); }
            catch (javax.net.ssl.SSLHandshakeException | java.net.SocketException expected) { System.out.println("verified TLS handshake rejection"); }
        }
    }
}
