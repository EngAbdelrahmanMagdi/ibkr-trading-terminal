package com.project.trading.order.domain;

import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.instrument.domain.Shortability;
import com.project.trading.instrument.domain.ShortabilityStatus;
import com.project.trading.shared.domain.DomainException;
import com.project.trading.shared.domain.Quantity;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderPolicyTest {

    @Test
    void intentMapsToBrokerSide() {
        assertThat(OrderPolicy.brokerSide(OrderIntent.BUY)).isEqualTo(BrokerSide.BUY);
        assertThat(OrderPolicy.brokerSide(OrderIntent.SELL)).isEqualTo(BrokerSide.SELL);
        assertThat(OrderPolicy.brokerSide(OrderIntent.SHORT)).isEqualTo(BrokerSide.SELL);
    }

    @Test
    void sellIsLimitedToTheLongPosition() {
        assertThatCode(() -> OrderPolicy.checkPosition(OrderIntent.SELL, Quantity.of("10"), new BigDecimal("10")))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> OrderPolicy.checkPosition(OrderIntent.SELL, Quantity.of("11"), new BigDecimal("10")))
                .isInstanceOf(DomainException.class).extracting("status").isEqualTo(422);
        assertThatThrownBy(() -> OrderPolicy.checkPosition(OrderIntent.SELL, Quantity.of("1"), new BigDecimal("-5")))
                .isInstanceOf(DomainException.class);
    }

    @Test
    void shortRequiresNoLongPosition() {
        assertThatCode(() -> OrderPolicy.checkPosition(OrderIntent.SHORT, Quantity.of("5"), BigDecimal.ZERO))
                .doesNotThrowAnyException();
        assertThatCode(() -> OrderPolicy.checkPosition(OrderIntent.SHORT, Quantity.of("5"), new BigDecimal("-5")))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> OrderPolicy.checkPosition(OrderIntent.SHORT, Quantity.of("5"), new BigDecimal("1")))
                .isInstanceOf(DomainException.class);
        assertThatCode(() -> OrderPolicy.checkPosition(OrderIntent.BUY, Quantity.of("5"), new BigDecimal("-5")))
                .doesNotThrowAnyException();
    }

    @Test
    void shortabilityPolicyBlocksOnlyNotShortable() {
        assertThatThrownBy(() -> OrderPolicy.checkShortability(OrderIntent.SHORT, ShortabilityStatus.NOT_SHORTABLE))
                .isInstanceOf(DomainException.class).extracting("status").isEqualTo(422);
        assertThatCode(() -> OrderPolicy.checkShortability(OrderIntent.SHORT, ShortabilityStatus.UNAVAILABLE))
                .doesNotThrowAnyException();
        assertThatCode(() -> OrderPolicy.checkShortability(OrderIntent.SHORT, ShortabilityStatus.SHORTABLE))
                .doesNotThrowAnyException();
        assertThatCode(() -> OrderPolicy.checkShortability(OrderIntent.SELL, ShortabilityStatus.NOT_SHORTABLE))
                .doesNotThrowAnyException();
    }

    @Test
    void staleOrUndatedShortabilityIsUnavailable() {
        Instant now = Instant.parse("2026-09-28T10:00:00Z");
        Duration maxAge = Duration.ofHours(1);
        assertThat(new Shortability(ShortabilityStatus.NOT_SHORTABLE, null, null, now.minusSeconds(60))
                .effectiveStatus(now, maxAge)).isEqualTo(ShortabilityStatus.NOT_SHORTABLE);
        assertThat(new Shortability(ShortabilityStatus.NOT_SHORTABLE, null, null, now.minus(Duration.ofHours(2)))
                .effectiveStatus(now, maxAge)).isEqualTo(ShortabilityStatus.UNAVAILABLE);
        assertThat(new Shortability(ShortabilityStatus.SHORTABLE, null, null, null)
                .effectiveStatus(now, maxAge)).isEqualTo(ShortabilityStatus.UNAVAILABLE);
    }
}
