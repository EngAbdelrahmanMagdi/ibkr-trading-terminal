package com.project.trading.broker.domain;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Account metrics reported by the broker, in the account currency. A null value is unavailable: it is never
 * estimated. netLiquidation may be null when the broker leaves it to be derived from cash and marked positions.
 */
public record AccountMetrics(BigDecimal cash, BigDecimal buyingPower, BigDecimal netLiquidation,
                             BigDecimal excessLiquidity, BigDecimal dayPnl, Instant asOf) {

    public static AccountMetrics unavailable(Instant asOf) {
        return new AccountMetrics(null, null, null, null, null, asOf);
    }
}
