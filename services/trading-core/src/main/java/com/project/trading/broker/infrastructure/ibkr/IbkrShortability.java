package com.project.trading.broker.infrastructure.ibkr;

import com.project.trading.instrument.domain.Instrument;
import com.project.trading.instrument.domain.Shortability;
import com.project.trading.instrument.domain.ShortabilityPort;
import com.project.trading.instrument.domain.ShortabilityStatus;
import com.project.trading.shared.domain.Decimals;
import tools.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shortability from the market data snapshot (GET /iserver/marketdata/snapshot) field 7636, "Shortable Shares":
 * <ul>
 *   <li>exactly 0: NOT_SHORTABLE (no shares available, so the SHORT is blocked locally);</li>
 *   <li>a whole number above 0: SHORTABLE, with the quantity and, when it is an exact decimal, the fee rate (field
 *   7637, "Fee Rate");</li>
 *   <li>anything else (missing, abbreviated, the empty answer of a first snapshot request, a failure or a timeout):
 *   UNAVAILABLE. Nothing is invented and the SHORT is not blocked; IBKR stays the final authority.</li>
 * </ul>
 * Field 7644 ("Shortable", the borrow difficulty) is requested but not classified: its values are not documented.
 * Explicit results are reused for shortabilityTtl; nothing is requested while a confirmation is outstanding.
 * Fields checked 2026-09-28 against https://www.interactivebrokers.com/docs/web-api/ (Market Data Fields).
 */
final class IbkrShortability implements ShortabilityPort {

    private static final int MAX_CACHED = 1000;
    private static final BigDecimal MAX_FEE_RATE = new BigDecimal("1000000");

    private record Entry(Shortability value, Instant fetchedAt) {
    }

    private final IbkrHttp http;
    private final ReplyGate replies;
    private final Clock clock;
    private final Duration ttl;
    private final ConcurrentHashMap<Long, Entry> cache = new ConcurrentHashMap<>();

    IbkrShortability(IbkrHttp http, ReplyGate replies, Clock clock, Duration ttl) {
        this.http = http;
        this.replies = replies;
        this.clock = clock;
        this.ttl = ttl;
    }

    @Override
    public Shortability shortability(Instrument instrument) {
        Instant now = clock.instant();
        Entry cached = cache.get(instrument.conid());
        if (cached != null && (replies.isOpen() || now.isBefore(cached.fetchedAt().plus(ttl)))) {
            return cached.value();
        }
        if (replies.isOpen()) {
            return Shortability.unavailable();
        }
        Shortability fetched = fetch(instrument.conid(), now);
        if (fetched.status() != ShortabilityStatus.UNAVAILABLE) {
            if (cache.size() >= MAX_CACHED) {
                cache.clear();
            }
            cache.put(instrument.conid(), new Entry(fetched, now));
        }
        return fetched;
    }

    private Shortability fetch(long conid, Instant now) {
        IbkrHttp.Response response;
        try {
            response = http.get(IbkrEndpoint.SNAPSHOT, "/iserver/marketdata/snapshot?conids=" + conid + "&fields=7636,7637,7644");
        } catch (IbkrHttp.CallException e) {
            return Shortability.unavailable();
        }
        if (!response.ok() || !response.body().isArray()) {
            return Shortability.unavailable();
        }
        for (JsonNode row : response.body()) {
            Long rowConid = IbkrJson.whole(row.get("conid"));
            if (rowConid == null || rowConid != conid) {
                continue;
            }
            Long shares = IbkrJson.whole(row.get("7636"));
            if (shares == null) {
                return Shortability.unavailable();
            }
            if (shares == 0) {
                return new Shortability(ShortabilityStatus.NOT_SHORTABLE, 0L, null, now);
            }
            return new Shortability(ShortabilityStatus.SHORTABLE, shares, feeRate(row.get("7637")), now);
        }
        return Shortability.unavailable();
    }

    /** The fee rate in percent when it is an exact decimal of reasonable size; otherwise not reported. */
    private static BigDecimal feeRate(JsonNode node) {
        BigDecimal fee = IbkrJson.decimal(node);
        if (fee == null || Decimals.significantScale(fee) > 6 || fee.abs().compareTo(MAX_FEE_RATE) >= 0) {
            return null;
        }
        return fee;
    }
}
