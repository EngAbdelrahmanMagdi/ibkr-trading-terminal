package com.project.trading.portfolio.application;

import com.project.trading.execution.application.ExecutionLedger;
import com.project.trading.marketdata.domain.QuoteReferencePort;
import com.project.trading.marketdata.domain.ReferenceQuote;
import com.project.trading.position.application.PositionLedger;
import com.project.trading.position.domain.Position;
import com.project.trading.shared.config.AppProperties;
import com.project.trading.shared.domain.Decimals;
import com.project.trading.shared.domain.Price;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Positions and account metrics of the simulated cash account. Values that need a mark price use fresh
 * reference quotes only; when a quote is missing or stale the metric is reported as unavailable, never
 * estimated. Metrics that cannot be derived honestly (excess liquidity, day P&L) are always unavailable.
 */
@Service
public class PortfolioService {

    /** A metric that may be unavailable (value is null then). */
    public record Metric(boolean available, BigDecimal value, String currency) {

        static Metric of(BigDecimal value, String currency) {
            return new Metric(true, Decimals.money(value), currency);
        }

        static Metric unavailable(String currency) {
            return new Metric(false, null, currency);
        }
    }

    public record PositionView(String symbol, BigDecimal quantity, BigDecimal averageCost, Metric marketValue,
                               Metric unrealizedPnl, Metric realizedPnl) {
    }

    public record Summary(String accountMode, Metric netLiquidation, Metric cash, Metric buyingPower,
                          Metric excessLiquidity, Metric dayPnl, Metric unrealizedPnl, Metric realizedPnl,
                          Instant asOf) {
    }

    private final PositionLedger positions;
    private final ExecutionLedger executions;
    private final QuoteReferencePort quotes;
    private final Clock clock;
    private final AppProperties properties;
    private final Duration quoteMaxAge;

    public PortfolioService(PositionLedger positions, ExecutionLedger executions, QuoteReferencePort quotes,
                            Clock clock, AppProperties properties) {
        this.positions = positions;
        this.executions = executions;
        this.quotes = quotes;
        this.clock = clock;
        this.properties = properties;
        this.quoteMaxAge = properties.orders().quoteMaxAge();
    }

    /** Open (non-flat) positions with their mark-dependent metrics. */
    public List<PositionView> positions() {
        Instant now = clock.instant();
        List<PositionView> views = new ArrayList<>();
        for (Position p : positions.all()) {
            if (!p.isFlat()) {
                views.add(view(p, now));
            }
        }
        return views;
    }

    public Summary summary() {
        Instant now = clock.instant();
        String currency = properties.currency();
        List<Position> all = positions.all();

        BigDecimal realized = BigDecimal.ZERO;
        BigDecimal unrealized = BigDecimal.ZERO;
        BigDecimal marketValue = BigDecimal.ZERO;
        boolean allPriced = true;
        for (Position p : all) {
            realized = realized.add(p.realizedPnl());
            if (p.isFlat()) {
                continue;
            }
            Optional<Price> mark = mark(p.symbol(), now);
            if (mark.isEmpty()) {
                allPriced = false;
                continue;
            }
            unrealized = unrealized.add(p.unrealizedPnl(mark.get()));
            marketValue = marketValue.add(p.marketValue(mark.get()));
        }
        BigDecimal cash = properties.portfolio().startingCash().add(executions.netCashFlow());

        return new Summary(properties.runtimeMode().name(),
                allPriced ? Metric.of(cash.add(marketValue), currency) : Metric.unavailable(currency),
                Metric.of(cash, currency),
                Metric.of(cash, currency),
                Metric.unavailable(currency),
                Metric.unavailable(currency),
                allPriced ? Metric.of(unrealized, currency) : Metric.unavailable(currency),
                Metric.of(realized, currency),
                now);
    }

    private PositionView view(Position p, Instant now) {
        Optional<Price> mark = mark(p.symbol(), now);
        Metric marketValue = mark.map(m -> Metric.of(p.marketValue(m), p.currency()))
                .orElseGet(() -> Metric.unavailable(p.currency()));
        Metric unrealized = mark.map(m -> Metric.of(p.unrealizedPnl(m), p.currency()))
                .orElseGet(() -> Metric.unavailable(p.currency()));
        return new PositionView(p.symbol(), p.quantity(), p.averageCost(), marketValue, unrealized,
                Metric.of(p.realizedPnl(), p.currency()));
    }

    /** The last trade price of a fresh quote. */
    private Optional<Price> mark(String symbol, Instant now) {
        return quotes.latest(symbol)
                .filter(q -> q.isFresh(now, quoteMaxAge))
                .map(ReferenceQuote::last);
    }
}
