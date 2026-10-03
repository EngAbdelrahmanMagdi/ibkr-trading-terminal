package com.project.trading.news.infrastructure;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NewsNormalizerTest {
    private static final Instant NOW = Instant.parse("2026-10-03T12:00:00Z");
    private final NewsNormalizer normalizer = new NewsNormalizer();

    @Test void normalizesIdentityUrlSymbolsAndStableContentWithoutHeadlineDedup() {
        var first = normalizer.normalize("FINNHUB", "42", "NVDA", "NVDA,AMD,amd", "  Product\n update  ",
                "Publisher", "HTTPS://Example.com:443/story?id=7&utm_source=test#fragment", NOW, "Summary", NOW.minusSeconds(60), NOW);
        var repeated = normalizer.normalize("FINNHUB", "43", "AMD", "NVDA", "Product update", "Publisher",
                "https://example.com/story?id=7", NOW, "Summary", NOW.minusSeconds(60), NOW);
        assertThat(first.providerId()).isEqualTo("FINNHUB:42");
        assertThat(first.url()).isEqualTo("https://example.com/story?id=7");
        assertThat(first.symbols()).containsExactly("AMD", "NVDA");
        assertThat(first.contentHash()).isEqualTo(repeated.contentHash()).matches("[a-f0-9]{64}");
        assertThat(first.providerId()).isNotEqualTo(repeated.providerId());
    }
    @Test void rejectsUnsafeLinksInvalidTimesAndOversizedRequiredText() {
        for (String url : new String[]{"javascript:alert(1)", "http://127.0.0.1/private", "https://user:pass@example.com/a", "http://localhost/a", "http://[::1]/a"})
            assertThatThrownBy(() -> NewsNormalizer.canonicalUrl(url)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> normalizer.normalize("FINNHUB", "1", "NVDA", "", "x".repeat(501), "Publisher",
                "https://example.com/story", NOW, null, NOW.minusSeconds(60), NOW)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> normalizer.normalize("FINNHUB", "1", "NVDA", "", "Headline", "Publisher",
                "https://example.com/story", NOW.plusSeconds(301), null, NOW.minusSeconds(60), NOW)).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void usesStableUrlAndTimeFallbackNotHeadlineAlone() {
        var first = normalizer.normalize("FINNHUB", null, "NVDA", "", "Headline", "Publisher", "https://example.com/a", NOW, null, NOW.minusSeconds(60), NOW);
        var duplicate = normalizer.normalize("FINNHUB", null, "NVDA", "", "Changed headline", "Publisher", "https://example.com/a", NOW, null, NOW.minusSeconds(60), NOW);
        var different = normalizer.normalize("FINNHUB", null, "NVDA", "", "Headline", "Publisher", "https://example.com/b", NOW, null, NOW.minusSeconds(60), NOW);
        assertThat(first.providerId()).isEqualTo(duplicate.providerId()).isNotEqualTo(different.providerId());
    }
}
