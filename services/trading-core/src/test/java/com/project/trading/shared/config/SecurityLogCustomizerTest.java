package com.project.trading.shared.config;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class SecurityLogCustomizerTest {
    @Test void credentialsAreRemovedFromHeadersAndExceptionMessages() {
        for (String text : new String[]{"Authorization: Bearer sentinel-auth", "Cookie: a=sentinel-cookie; b=private",
                "request failed ?token=sentinel-token&symbol=AAPL", "api_key=sentinel-key", "sessionId=sentinel-session"}) {
            assertThat(SecurityLogCustomizer.redact(text)).contains("[REDACTED]").doesNotContain("sentinel", "b=private");
        }
        assertThat(SecurityLogCustomizer.redact("order update category=TimeoutException")).isEqualTo("order update category=TimeoutException");
    }
}
