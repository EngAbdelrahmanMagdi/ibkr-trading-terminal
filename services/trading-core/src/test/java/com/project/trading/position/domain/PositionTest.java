package com.project.trading.position.domain;

import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class PositionTest {

    private static Position apply(Position p, BrokerSide side, String qty, String price) {
        return p.apply(side, Quantity.of(qty), Price.of(price));
    }

    private static void assertPosition(Position p, String qty, String avg, String realized) {
        assertThat(p.quantity()).isEqualByComparingTo(qty);
        assertThat(p.averageCost()).isEqualByComparingTo(avg);
        assertThat(p.realizedPnl()).isEqualByComparingTo(realized);
    }

    @Test
    void longPositionAveragesAndRealizesOnReduction() {
        Position p = Position.flat("NVDA", "USD");
        p = apply(p, BrokerSide.BUY, "10", "100");
        p = apply(p, BrokerSide.BUY, "10", "110");
        assertPosition(p, "20", "105", "0");
        p = apply(p, BrokerSide.SELL, "5", "120");
        assertPosition(p, "15", "105", "75");
        p = apply(p, BrokerSide.SELL, "15", "100");
        assertPosition(p, "0", "0", "0");
        assertThat(p.isFlat()).isTrue();
    }

    @Test
    void shortPositionRealizesWhenCovered() {
        Position p = Position.flat("TSLA", "USD");
        p = apply(p, BrokerSide.SELL, "10", "300");
        p = apply(p, BrokerSide.SELL, "10", "310");
        assertPosition(p, "-20", "305", "0");
        p = apply(p, BrokerSide.BUY, "5", "295");
        assertPosition(p, "-15", "305", "50");
        assertThat(p.unrealizedPnl(Price.of("300"))).isEqualByComparingTo("75");
        assertThat(p.marketValue(Price.of("300"))).isEqualByComparingTo("-4500");
    }

    @Test
    void crossingZeroOpensTheOtherSideAtTheFillPrice() {
        Position p = Position.flat("AMD", "USD");
        p = apply(p, BrokerSide.BUY, "10", "100");
        p = apply(p, BrokerSide.SELL, "15", "90");
        assertPosition(p, "-5", "90", "-100");
        p = apply(p, BrokerSide.BUY, "8", "80");
        assertPosition(p, "3", "80", "-50");
    }

    @Test
    void averageCostIsRoundedHalfEvenAtPriceScale() {
        Position p = Position.flat("AAPL", "USD");
        p = apply(p, BrokerSide.BUY, "1", "100");
        p = apply(p, BrokerSide.BUY, "2", "100.01");
        assertThat(p.averageCost()).isEqualTo(new BigDecimal("100.006667"));
    }
}
