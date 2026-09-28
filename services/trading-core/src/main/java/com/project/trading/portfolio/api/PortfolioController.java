package com.project.trading.portfolio.api;

import com.project.trading.portfolio.application.PortfolioService;
import com.project.trading.shared.api.ApiFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1")
public class PortfolioController {

    public record MetricResponse(boolean available, String value, String currency) {
    }

    public record PositionResponse(String symbol, String quantity, String averageCost, MetricResponse marketValue,
                                   MetricResponse unrealizedPnl, MetricResponse realizedPnl) {
    }

    public record PortfolioResponse(String accountMode, MetricResponse netLiquidation, MetricResponse cash,
                                    MetricResponse buyingPower, MetricResponse excessLiquidity, MetricResponse dayPnl,
                                    MetricResponse unrealizedPnl, MetricResponse realizedPnl, String asOf) {
    }

    private final PortfolioService portfolio;

    public PortfolioController(PortfolioService portfolio) {
        this.portfolio = portfolio;
    }

    @GetMapping("/portfolio")
    public PortfolioResponse summary() {
        PortfolioService.Summary s = portfolio.summary();
        return new PortfolioResponse(s.accountMode(), metric(s.netLiquidation()), metric(s.cash()),
                metric(s.buyingPower()), metric(s.excessLiquidity()), metric(s.dayPnl()), metric(s.unrealizedPnl()),
                metric(s.realizedPnl()), ApiFormat.instant(s.asOf()));
    }

    @GetMapping("/positions")
    public List<PositionResponse> positions() {
        return portfolio.positions().stream()
                .map(p -> new PositionResponse(p.symbol(), ApiFormat.decimal(p.quantity(), 0),
                        ApiFormat.decimal(p.averageCost(), 2), metric(p.marketValue()), metric(p.unrealizedPnl()),
                        metric(p.realizedPnl())))
                .toList();
    }

    private static MetricResponse metric(PortfolioService.Metric m) {
        return new MetricResponse(m.available(), ApiFormat.decimal(m.value(), 2), m.currency());
    }
}
