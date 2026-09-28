package com.project.trading.broker.infrastructure.ibkr;

import java.time.Duration;

/**
 * The IBKR Web API endpoints this service calls, with the documented per-endpoint limits where one applies (all
 * others share the global session limit). Pacing limits checked 2026-09-28 against
 * https://www.interactivebrokers.com/docs/web-api/ ("Pacing Limitations").
 */
enum IbkrEndpoint {
    AUTH_STATUS("auth_status", null),
    ACCOUNTS("accounts", null),
    PLACE_ORDER("place_order", null),
    REPLY("reply", null),
    CANCEL_ORDER("cancel_order", null),
    LIVE_ORDERS("live_orders", Duration.ofSeconds(5)),
    TRADES("trades", Duration.ofSeconds(5)),
    PORTFOLIO_ACCOUNTS("portfolio_accounts", Duration.ofSeconds(5)),
    LEDGER("ledger", null),
    PNL("pnl", Duration.ofSeconds(5)),
    STOCKS("stocks", null),
    SECDEF("secdef", null),
    CONTRACT_RULES("contract_rules", null),
    SNAPSHOT("snapshot", null);

    private final String metricName;
    private final Duration minInterval;

    IbkrEndpoint(String metricName, Duration minInterval) {
        this.metricName = metricName;
        this.minInterval = minInterval;
    }

    String metricName() {
        return metricName;
    }

    /** The documented minimum interval between requests, or null when only the global limit applies. */
    Duration minInterval() {
        return minInterval;
    }
}
