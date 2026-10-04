package com.project.trading.shared.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaTlsTest {
    @TempDir Path directory;

    @Test void configuredTlsRequiresCompleteIdentityAndHostnameVerification() throws Exception {
        assertThatThrownBy(() -> KafkaTls.configure(Map.of(), Map.of("KAFKA_TLS_CA_FILE", "missing")))
                .isInstanceOf(IllegalStateException.class);
        Path ca = Files.writeString(directory.resolve("ca.pem"), "test");
        Path identity = Files.writeString(directory.resolve("identity.pem"), "test");
        var configured = KafkaTls.configure(Map.of("bootstrap.servers", "kafka:29092"), Map.of(
                "KAFKA_TLS_CA_FILE", ca.toString(), "KAFKA_TLS_KEYSTORE_FILE", identity.toString()));
        assertThat(configured).containsEntry("security.protocol", "SSL")
                .containsEntry("ssl.endpoint.identification.algorithm", "https")
                .containsEntry("ssl.enabled.protocols", "TLSv1.2,TLSv1.3");
        assertThat(KafkaTls.configure(Map.of("isolated", true), Map.of())).containsEntry("isolated", true);
    }
}
