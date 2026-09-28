package com.project.trading.broker.infrastructure.ibkr;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.project.trading.instrument.domain.Instrument;
import com.project.trading.instrument.domain.Shortability;
import com.project.trading.instrument.domain.ShortabilityStatus;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Clock;
import java.time.Duration;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.project.trading.broker.infrastructure.ibkr.IbkrStubs.API;
import static org.assertj.core.api.Assertions.assertThat;

/** Snapshot shortability data maps to exactly one of the three states; nothing is invented. */
class IbkrShortabilityTest {

    @RegisterExtension
    static WireMockExtension wm = WireMockExtension.newInstance().options(wireMockConfig().dynamicPort()).build();

    private static final Instrument NVDA = new Instrument("NVDA", 4815747, "NVIDIA CORP", "NASDAQ", "USD", "STK", 2);

    @ParameterizedTest(name = "HTTP {0} {1} -> {2}")
    @CsvSource(delimiter = '|', textBlock = """
            200 | [{"conid":4815747,"_updated":1790582400000,"7636":"0","7644":"Not shortable"}]      | NOT_SHORTABLE | 0     |
            200 | [{"conid":4815747,"_updated":1790582400000,"7636":"12000","7637":"0.25"}]           | SHORTABLE     | 12000 | 0.25
            200 | [{"conid":4815747,"_updated":1790582400000,"server_id":"q1"}]                        | UNAVAILABLE   |       |
            200 | [{"conid":4815747,"_updated":1790582400000,"7636":"1.2M"}]                          | UNAVAILABLE   |       |
            503 | {"error":"service unavailable"}                                                      | UNAVAILABLE   |       |
            """)
    void snapshotFieldsMapToTheThreeStates(int status, String body, ShortabilityStatus expected, Long quantity,
                                           String fee) {
        wm.stubFor(get(urlPathEqualTo(API + "/iserver/marketdata/snapshot")).willReturn(IbkrStubs.json(status, body)));
        IbkrShortability shortability = new IbkrShortability(IbkrStubs.http(wm, IbkrStubs.limiter()),
                new ReplyGate(Clock.systemUTC(), Duration.ofSeconds(30)), Clock.systemUTC(), Duration.ofMinutes(1));

        Shortability result = shortability.shortability(NVDA);

        assertThat(result.status()).isEqualTo(expected);
        assertThat(result.availableQuantity()).isEqualTo(quantity);
        assertThat(result.borrowFeeRate()).isEqualTo(fee == null ? null : new java.math.BigDecimal(fee));
        if (expected == ShortabilityStatus.UNAVAILABLE) {
            assertThat(result).isEqualTo(Shortability.unavailable());
        }
    }
}
