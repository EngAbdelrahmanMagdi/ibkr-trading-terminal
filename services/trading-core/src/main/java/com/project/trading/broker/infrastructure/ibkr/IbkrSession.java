package com.project.trading.broker.infrastructure.ibkr;

import com.project.trading.broker.domain.BrokerConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * Broker readiness as seen by Trading Core. This service never initializes or keeps the IBKR session alive (the
 * Realtime Gateway does); it only checks it:
 * <ul>
 *   <li>POST /iserver/auth/status must report connected and authenticated, not competing, and established when
 *   that flag is present;</li>
 *   <li>GET /iserver/accounts (required before order commands) must list the configured account and report
 *   isPaper=true. There is no live trading.</li>
 * </ul>
 * The result is reused for readinessTtl, with at most one check in flight. While a confirmation request is
 * outstanding the last result is reused regardless of age, to avoid extra session traffic.
 * Endpoints checked 2026-09-28 against https://www.interactivebrokers.com/docs/web-api/.
 */
final class IbkrSession {

    private static final Logger log = LoggerFactory.getLogger(IbkrSession.class);

    /** Why an account check failed. */
    enum AccountCheck {
        OK,
        NOT_PAPER,
        ACCOUNT_NOT_FOUND,
        UNAVAILABLE
    }

    private record Snapshot(BrokerConnectionState state, long at) {
    }

    private final IbkrHttp http;
    private final String accountId;
    private final ReplyGate replies;
    private final long ttl;
    private final long lockWait;
    private final LongSupplier ticker;
    private final ReentrantLock refresh = new ReentrantLock();
    private volatile Snapshot cached;
    private volatile AccountCheck lastAccountCheck = AccountCheck.OK;
    private volatile Runnable onReady = () -> { };
    private BrokerConnectionState lastState = BrokerConnectionState.UNAVAILABLE;

    IbkrSession(IbkrHttp http, String accountId, ReplyGate replies, Duration readinessTtl, Duration lockWait,
                LongSupplier ticker) {
        this.http = http;
        this.accountId = accountId;
        this.replies = replies;
        this.ttl = readinessTtl.toNanos();
        this.lockWait = lockWait.toNanos();
        this.ticker = ticker;
    }

    BrokerConnectionState state() {
        Snapshot s = cached;
        if (s != null && (replies.isOpen() || ticker.getAsLong() - s.at() < ttl)) {
            return s.state();
        }
        boolean locked;
        try {
            locked = refresh.tryLock(lockWait, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            locked = false;
        }
        if (!locked) {
            return s == null ? BrokerConnectionState.UNAVAILABLE : s.state();
        }
        try {
            s = cached;
            if (s != null && ticker.getAsLong() - s.at() < ttl) {
                return s.state();
            }
            BrokerConnectionState state = check();
            cached = new Snapshot(state, ticker.getAsLong());
            boolean becameReady = state == BrokerConnectionState.READY && lastState != BrokerConnectionState.READY;
            lastState = state;
            if (becameReady) {
                onReady.run();
            }
            return state;
        } finally {
            refresh.unlock();
        }
    }

    /** Called each time the session becomes ready (for example to reconcile after an outage). Must not block. */
    void onReady(Runnable listener) {
        this.onReady = listener;
    }

    /** Forces the next state() call to check the broker again (for example after a 401). */
    void invalidate() {
        cached = null;
    }

    /**
     * Startup check: when the gateway is reachable and authenticated, the session must be a paper session with
     * access to the configured account, otherwise startup fails. An unreachable gateway is not an error (the
     * developer logs in interactively); orders are refused until it is ready.
     */
    void verifyAtStartup() {
        BrokerConnectionState state = state();
        AccountCheck check = lastAccountCheck;
        if (check == AccountCheck.NOT_PAPER) {
            throw new IllegalStateException("IBKR_PAPER startup refused: the IBKR session is not a paper trading session");
        }
        if (check == AccountCheck.ACCOUNT_NOT_FOUND) {
            throw new IllegalStateException("IBKR_PAPER startup refused: the IBKR session has no access to the configured account");
        }
        log.info("IBKR broker session at startup: {}", state);
    }

    private BrokerConnectionState check() {
        try {
            IbkrHttp.Response status = http.post(IbkrEndpoint.AUTH_STATUS, "/iserver/auth/status", null);
            if (status.status() == 429 || !status.ok() || !authenticated(status.body())) {
                return BrokerConnectionState.UNAVAILABLE;
            }
            AccountCheck accounts = checkAccounts();
            if (accounts != lastAccountCheck && accounts != AccountCheck.OK) {
                log.error("IBKR session refused for trading: account policy mismatch");
            }
            lastAccountCheck = accounts;
            return accounts == AccountCheck.OK ? BrokerConnectionState.READY : BrokerConnectionState.UNAVAILABLE;
        } catch (IbkrHttp.CallException e) {
            log.debug("IBKR readiness check failed: {}", e.getMessage());
            return BrokerConnectionState.UNAVAILABLE;
        }
    }

    private static boolean authenticated(JsonNode body) {
        boolean ready = IbkrJson.isTrue(body.path("connected")) && IbkrJson.isTrue(body.path("authenticated"))
                && !IbkrJson.isTrue(body.path("competing"));
        if (body.has("established")) {
            ready = ready && IbkrJson.isTrue(body.path("established"));
        }
        return ready;
    }

    private AccountCheck checkAccounts() throws IbkrHttp.CallException {
        IbkrHttp.Response response = http.get(IbkrEndpoint.ACCOUNTS, "/iserver/accounts");
        if (!response.ok()) {
            return AccountCheck.UNAVAILABLE;
        }
        JsonNode body = response.body();
        boolean listed = false;
        for (JsonNode account : body.path("accounts")) {
            if (account.isString() && account.stringValue().equals(accountId)) {
                listed = true;
            }
        }
        if (!IbkrJson.isTrue(body.path("isPaper"))) {
            return body.path("isPaper").isBoolean() ? AccountCheck.NOT_PAPER : AccountCheck.UNAVAILABLE;
        }
        return listed ? AccountCheck.OK : AccountCheck.ACCOUNT_NOT_FOUND;
    }
}
