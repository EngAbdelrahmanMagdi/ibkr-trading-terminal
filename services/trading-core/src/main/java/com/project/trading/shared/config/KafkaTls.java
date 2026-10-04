package com.project.trading.shared.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/** Service-specific PEM identity. Configured TLS never falls back to plaintext. */
public final class KafkaTls {
    private KafkaTls() { }

    public static Map<String, Object> configure(Map<String, Object> original) {
        return configure(original, System.getenv());
    }

    static Map<String, Object> configure(Map<String, Object> original, Map<String, String> environment) {
        String ca = environment.getOrDefault("KAFKA_TLS_CA_FILE", "");
        String key = environment.getOrDefault("KAFKA_TLS_KEYSTORE_FILE", "");
        if (ca.isEmpty() && key.isEmpty()) return original; // Isolated test brokers may use plaintext.
        if (ca.isEmpty() || key.isEmpty() || !Files.isReadable(Path.of(ca)) || !Files.isReadable(Path.of(key))) {
            throw new IllegalStateException("Kafka TLS identity is incomplete or unreadable");
        }
        Map<String, Object> result = new HashMap<>(original);
        result.put("security.protocol", "SSL");
        result.put("ssl.keystore.type", "PEM");
        result.put("ssl.keystore.location", key);
        result.put("ssl.truststore.type", "PEM");
        result.put("ssl.truststore.location", ca);
        result.put("ssl.endpoint.identification.algorithm", "https");
        result.put("ssl.enabled.protocols", "TLSv1.2,TLSv1.3");
        return Map.copyOf(result);
    }
}
