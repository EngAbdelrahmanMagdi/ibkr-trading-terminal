package com.project.trading.broker.infrastructure.ibkr;

import com.project.trading.broker.domain.AccountMetrics;
import com.project.trading.broker.domain.BrokerAccountPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Account metrics of the paper account, only from documented fields:
 * <ul>
 *   <li>GET /portfolio/accounts (required before other /portfolio requests): the account's base currency. Metrics
 *   are reported only when it equals the application currency.</li>
 *   <li>GET /portfolio/{accountId}/ledger, entry BASE: {@code cashbalance} (cash).</li>
 *   <li>GET /iserver/account/pnl/partitioned, entry {accountId}.Core: {@code nl} (net liquidation), {@code el}
 *   (excess liquidity), {@code dpl} (day P&L).</li>
 * </ul>
 * Buying power has no documented field in these responses and is reported as unavailable. Any value the broker
 * does not provide is unavailable, never estimated. Results are reused for accountTtl; nothing is requested while
 * a confirmation is outstanding. Checked 2026-09-28 against https://www.interactivebrokers.com/docs/web-api/
 * (Portfolio Accounts, Portfolio Ledger, Account Profit and Loss).
 */
final class IbkrAccount implements BrokerAccountPort {

    private static final Logger log = LoggerFactory.getLogger(IbkrAccount.class);

    private final IbkrHttp http;
    private final ReplyGate replies;
    private final String accountId;
    private final String currency;
    private final Clock clock;
    private final Duration ttl;
    private AccountMetrics cached;
    private boolean currencyMismatchLogged;

    IbkrAccount(IbkrHttp http, ReplyGate replies, String accountId, String currency, Clock clock, Duration ttl) {
        this.http = http;
        this.replies = replies;
        this.accountId = accountId;
        this.currency = currency;
        this.clock = clock;
        this.ttl = ttl;
    }

    @Override
    public synchronized AccountMetrics metrics() {
        Instant now = clock.instant();
        if (cached != null && (replies.isOpen() || now.isBefore(cached.asOf().plus(ttl)))) {
            return cached;
        }
        if (replies.isOpen()) {
            return AccountMetrics.unavailable(now);
        }
        cached = fetch(now);
        return cached;
    }

    private AccountMetrics fetch(Instant now) {
        try {
            if (!baseCurrencyMatches()) {
                return AccountMetrics.unavailable(now);
            }
            BigDecimal cash = null;
            IbkrHttp.Response ledger = http.get(IbkrEndpoint.LEDGER, "/portfolio/" + accountId + "/ledger");
            if (ledger.ok()) {
                cash = IbkrJson.decimal(ledger.body().path("BASE").get("cashbalance"));
            }
            BigDecimal netLiquidation = null;
            BigDecimal excessLiquidity = null;
            BigDecimal dayPnl = null;
            IbkrHttp.Response pnl = http.get(IbkrEndpoint.PNL, "/iserver/account/pnl/partitioned");
            if (pnl.ok()) {
                JsonNode core = pnl.body().path("upnl").path(accountId + ".Core");
                netLiquidation = IbkrJson.decimal(core.get("nl"));
                excessLiquidity = IbkrJson.decimal(core.get("el"));
                dayPnl = IbkrJson.decimal(core.get("dpl"));
            }
            return new AccountMetrics(cash, null, netLiquidation, excessLiquidity, dayPnl, now);
        } catch (IbkrHttp.CallException e) {
            log.debug("IBKR account metrics unavailable: {}", e.getMessage());
            return AccountMetrics.unavailable(now);
        }
    }

    private boolean baseCurrencyMatches() throws IbkrHttp.CallException {
        IbkrHttp.Response accounts = http.get(IbkrEndpoint.PORTFOLIO_ACCOUNTS, "/portfolio/accounts");
        if (!accounts.ok()) {
            return false;
        }
        for (JsonNode account : accounts.body()) {
            String id = IbkrJson.id(account.get("id"));
            if (accountId.equals(id) || accountId.equals(IbkrJson.id(account.get("accountId")))) {
                boolean matches = currency.equals(IbkrJson.id(account.get("currency")));
                if (!matches && !currencyMismatchLogged) {
                    log.warn("IBKR account base currency differs from {}; account metrics are unavailable", currency);
                    currencyMismatchLogged = true;
                }
                return matches;
            }
        }
        return false;
    }
}
