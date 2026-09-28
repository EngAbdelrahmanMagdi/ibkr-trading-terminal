package com.project.trading.shared.config;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IBKR_PAPER runs only in a trusted private deployment, on a database bound to that mode and account. */
class RuntimeGuardsTest {

    private static final List<String> PRIVATE_ORIGINS = List.of("http://localhost:3000", "http://192.168.1.20:3000");

    @Test
    void ibkrPaperRequiresATrustedEnvironmentAndPrivateOrigins() {
        assertThatCode(() -> RuntimeModeGuard.check("MOCK", false, List.of("https://demo.example.com")))
                .doesNotThrowAnyException();
        assertThatCode(() -> RuntimeModeGuard.check("IBKR_PAPER", true, PRIVATE_ORIGINS)).doesNotThrowAnyException();

        assertThatThrownBy(() -> RuntimeModeGuard.check("IBKR_PAPER", false, PRIVATE_ORIGINS))
                .hasMessageContaining("APP_TRUSTED_ENVIRONMENT");
        assertThatThrownBy(() -> RuntimeModeGuard.check("IBKR_PAPER", true, List.of("https://demo.example.com")))
                .hasMessageContaining("demo.example.com");
        assertThatThrownBy(() -> RuntimeModeGuard.check("IBKR_PAPER", true, List.of("http://8.8.8.8:3000")))
                .hasMessageContaining("8.8.8.8");
        assertThatThrownBy(() -> RuntimeModeGuard.check("LIVE", true, PRIVATE_ORIGINS))
                .hasMessageContaining("Unsupported runtime mode");
    }

    @Test
    void theDatabaseStaysBoundToOneModeAndAccount() {
        RuntimeBindingGuard.Binding paper = new RuntimeBindingGuard.Binding("IBKR_PAPER", "DU1234567");
        RuntimeBindingGuard.Binding mock = new RuntimeBindingGuard.Binding("MOCK", "MOCK-ACCOUNT");

        assertThatCode(() -> RuntimeBindingGuard.check(paper, null, false)).doesNotThrowAnyException();
        assertThatCode(() -> RuntimeBindingGuard.check(mock, null, true)).doesNotThrowAnyException();
        assertThatCode(() -> RuntimeBindingGuard.check(paper, paper, false)).doesNotThrowAnyException();

        assertThatThrownBy(() -> RuntimeBindingGuard.check(paper, null, true)).hasMessageContaining("clean database");
        assertThatThrownBy(() -> RuntimeBindingGuard.check(paper, mock, false)).hasMessageContaining("belongs to runtime mode MOCK");
        assertThatThrownBy(() -> RuntimeBindingGuard.check(paper, new RuntimeBindingGuard.Binding("IBKR_PAPER", "DU7654321"), false))
                .isInstanceOf(IllegalStateException.class);
    }
}
