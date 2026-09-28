package com.project.trading.broker.infrastructure.mock;

import com.project.trading.broker.domain.BrokerOrderUpdate;
import com.project.trading.marketdata.domain.QuoteReferencePort;
import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.shared.domain.Decimals;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

/**
 * How the simulated broker prices fills: against the fresh simulated quote only (real-time, not stale, not
 * halted). A buy trades at the ask, a sell at the bid. Commission is a flat per-share rate with a minimum.
 */
class MockMarket {

    private final QuoteReferencePort quotes;
    private final Clock clock;
    private final Duration quoteMaxAge;
    private final BigDecimal commissionPerShare;
    private final BigDecimal commissionMinimum;

    MockMarket(QuoteReferencePort quotes, Clock clock, Duration quoteMaxAge, MockBrokerProperties properties) {
        this.quotes = quotes;
        this.clock = clock;
        this.quoteMaxAge = quoteMaxAge;
        this.commissionPerShare = properties.commissionPerShare();
        this.commissionMinimum = properties.commissionMinimum();
    }

    Instant now() {
        return clock.instant();
    }

    /** The price a marketable order would trade at now, if a fresh quote has that side. */
    Optional<Price> executablePrice(String symbol, BrokerSide side) {
        Instant now = clock.instant();
        return quotes.latest(symbol)
                .filter(q -> q.isFresh(now, quoteMaxAge))
                .map(q -> side == BrokerSide.BUY ? q.ask() : q.bid());
    }

    /** The fill price of a limit order if it is marketable now (a buy when ask <= limit, a sell when bid >= limit). */
    Optional<Price> limitFillPrice(String symbol, BrokerSide side, Price limit) {
        return executablePrice(symbol, side).filter(p -> side == BrokerSide.BUY
                ? p.compareTo(limit) <= 0 : p.compareTo(limit) >= 0);
    }

    BrokerOrderUpdate.Fill fill(String brokerOrderId, BrokerSide side, Quantity quantity, Price price, String currency) {
        BigDecimal commission = Decimals.money(quantity.value().multiply(commissionPerShare).max(commissionMinimum));
        return new BrokerOrderUpdate.Fill(brokerOrderId, "MOCK-E-" + compactUuid(), side, quantity, price,
                commission, currency, clock.instant());
    }

    static String compactUuid() {
        return UUID.randomUUID().toString().replace("-", "").toUpperCase(Locale.ROOT);
    }
}
