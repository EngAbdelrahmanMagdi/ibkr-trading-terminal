package com.project.trading.position.application;

import com.project.trading.marketdata.domain.QuoteReferencePort;
import com.project.trading.marketdata.domain.ReferenceQuote;
import com.project.trading.position.domain.OpeningValuation;
import com.project.trading.position.domain.OpeningValuationRepository;
import com.project.trading.position.domain.Position;
import com.project.trading.shared.domain.Decimals;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Optional;

/** Execution cash flow plus marked inventory change, net of commissions. No cumulative P&L shortcut. */
public class TradingDayPnl {
    public static final ZoneId ZONE = ZoneId.of("America/New_York");
    private final TradingDaySnapshot snapshots;
    private final OpeningValuationRepository openings;
    private final QuoteReferencePort quotes;
    private final Clock clock;
    private final Duration quoteMaxAge;

    public TradingDayPnl(TradingDaySnapshot snapshots, OpeningValuationRepository openings,
                         QuoteReferencePort quotes, Clock clock, Duration quoteMaxAge) {
        this.snapshots = snapshots;
        this.openings = openings;
        this.quotes = quotes;
        this.clock = clock;
        this.quoteMaxAge = quoteMaxAge;
    }

    public Optional<BigDecimal> value() {
        Instant now = clock.instant();
        LocalDate day = now.atZone(ZONE).toLocalDate();
        Instant start = day.atStartOfDay(ZONE).toInstant();
        TradingDaySnapshot.View view = snapshots.read(day, start, day.plusDays(1).atStartOfDay(ZONE).toInstant());
        BigDecimal result = view.cashFlow();
        for (var entry : view.openingQuantities().entrySet()) {
            if (entry.getValue().signum() == 0) continue;
            OpeningValuation baseline = view.openings().get(entry.getKey());
            // Also fail closed when a late historical execution invalidates the captured opening quantity.
            if (baseline == null || baseline.quantity().compareTo(entry.getValue()) != 0) return Optional.empty();
            result = result.subtract(baseline.marketValue());
        }
        for (Position position : view.positions()) {
            if (position.isFlat()) continue;
            Optional<ReferenceQuote> quote = quotes.latest(position.symbol())
                    .filter(q -> usable(q, now));
            if (quote.isEmpty()) return Optional.empty();
            result = result.add(position.quantity().multiply(quote.get().last().value()));
        }
        return Optional.of(Decimals.money(result));
    }

    /** Only boundary-adjacent, fresh pre-boundary marks may establish the opening valuation. */
    public void captureOpening() {
        Instant now = clock.instant();
        LocalDate day = now.atZone(ZONE).toLocalDate();
        Instant start = day.atStartOfDay(ZONE).toInstant();
        if (Duration.between(start, now).compareTo(Duration.ofSeconds(15)) > 0) return;
        TradingDaySnapshot.View view = snapshots.read(day, start, day.plusDays(1).atStartOfDay(ZONE).toInstant());
        for (var entry : view.openingQuantities().entrySet()) {
            if (entry.getValue().signum() == 0 || view.openings().containsKey(entry.getKey())) continue;
            quotes.latest(entry.getKey()).filter(q -> usable(q, start)).ifPresent(q ->
                    openings.save(new OpeningValuation(entry.getKey(), day, entry.getValue(),
                            Decimals.money(entry.getValue().multiply(q.last().value())), q.timestamp())));
        }
    }

    private boolean usable(ReferenceQuote quote, Instant at) {
        return quote.last() != null && !quote.timestamp().isAfter(at) && quote.isFresh(at, quoteMaxAge);
    }
}
