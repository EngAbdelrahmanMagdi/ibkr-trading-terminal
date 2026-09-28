package com.project.trading.broker.infrastructure.ibkr;

import com.project.trading.broker.domain.BrokerConnectionState;
import com.project.trading.broker.domain.BrokerTruthPort;
import com.project.trading.broker.domain.ObservedStatus;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * IBKR's view of the paper account for reconciliation:
 * <ul>
 *   <li>GET /iserver/account/orders: the current day's orders ({@code orderId}, {@code order_ref} = the cOID sent with
 *   the order, {@code status}, {@code filledQuantity}); at most one request per 5 s;</li>
 *   <li>GET /iserver/account/trades: recent executions ({@code execution_id}, {@code order_ref}, {@code size},
 *   {@code price}, {@code commission}, {@code trade_time_r}); at most one request per 5 s;</li>
 *   <li>GET /portfolio/accounts, then GET /portfolio/{accountId}/positions/0 ({@code conid}, {@code position}), the first
 *   page of up to 100 positions.</li>
 * </ul>
 * Orders and executions are required: without them the snapshot is empty and the run is skipped. Positions are
 * optional. Nothing is requested while the session is not ready or a confirmation is outstanding. Checked 2026-09-28
 * against https://www.interactivebrokers.com/docs/web-api/ (Live Orders, Trades, Positions).
 */
final class IbkrBrokerTruth implements BrokerTruthPort {

    private static final Logger log = LoggerFactory.getLogger(IbkrBrokerTruth.class);
    private static final Pattern IDENTIFIER = Pattern.compile("^[A-Za-z0-9._:-]{1,64}$");

    private final IbkrHttp http;
    private final IbkrSession session;
    private final ReplyGate replies;
    private final String accountId;
    private final Clock clock;

    IbkrBrokerTruth(IbkrHttp http, IbkrSession session, ReplyGate replies, String accountId, Clock clock) {
        this.http = http;
        this.session = session;
        this.replies = replies;
        this.accountId = accountId;
        this.clock = clock;
    }

    @Override
    public Optional<Snapshot> snapshot() {
        if (replies.isOpen() || session.state() != BrokerConnectionState.READY) {
            return Optional.empty();
        }
        try {
            List<OrderView> orders = orders();
            List<ExecutionView> executions = executions();
            return Optional.of(new Snapshot(orders, executions, positions(), clock.instant()));
        } catch (IbkrHttp.CallException e) {
            log.debug("IBKR reconciliation data unavailable: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private List<OrderView> orders() throws IbkrHttp.CallException {
        IbkrHttp.Response response = http.get(IbkrEndpoint.LIVE_ORDERS, "/iserver/account/orders");
        require(response);
        List<OrderView> out = new ArrayList<>();
        for (JsonNode row : response.body().path("orders")) {
            String orderId = IbkrJson.id(row.get("orderId"));
            String status = IbkrJson.id(row.get("status"));
            BigDecimal filled = IbkrJson.decimal(row.get("filledQuantity"));
            if (orderId == null || !IDENTIFIER.matcher(orderId).matches() || status == null) {
                continue;
            }
            Quantity filledQuantity = quantity(filled == null ? BigDecimal.ZERO : filled);
            if (filledQuantity == null) {
                continue;
            }
            out.add(new OrderView(orderId, reference(row.get("order_ref")), observed(status, filledQuantity),
                    filledQuantity, IbkrJson.sanitize(status, 64)));
        }
        return out;
    }

    private List<ExecutionView> executions() throws IbkrHttp.CallException {
        IbkrHttp.Response response = http.get(IbkrEndpoint.TRADES, "/iserver/account/trades");
        require(response);
        if (!response.body().isArray()) {
            throw new IbkrHttp.CallException(IbkrHttp.CallException.Kind.MAYBE_SENT, "unexpected trades response");
        }
        List<ExecutionView> out = new ArrayList<>();
        for (JsonNode trade : response.body()) {
            String executionId = IbkrJson.id(trade.get("execution_id"));
            BigDecimal size = IbkrJson.decimal(trade.get("size"));
            BigDecimal price = IbkrJson.decimal(trade.get("price"));
            Long time = IbkrJson.whole(trade.get("trade_time_r"));
            if (executionId == null || !IDENTIFIER.matcher(executionId).matches() || size == null || size.signum() <= 0
                    || price == null || price.signum() <= 0 || time == null) {
                continue;
            }
            Quantity quantity = quantity(size);
            Price fillPrice;
            try {
                fillPrice = new Price(price);
            } catch (IllegalArgumentException e) {
                continue;
            }
            if (quantity == null) {
                continue;
            }
            BigDecimal commission = IbkrJson.decimal(trade.get("commission"));
            String orderId = IbkrJson.id(trade.get("order_id"));
            out.add(new ExecutionView(executionId, orderId != null && IDENTIFIER.matcher(orderId).matches() ? orderId : null,
                    reference(trade.get("order_ref")), quantity, fillPrice,
                    commission == null || commission.signum() < 0 ? null : commission, Instant.ofEpochMilli(time)));
        }
        return out;
    }

    /** Positions by conid, or null when they cannot be read (positions are then not compared). */
    private Map<Long, BigDecimal> positions() {
        try {
            require(http.get(IbkrEndpoint.PORTFOLIO_ACCOUNTS, "/portfolio/accounts"));
            IbkrHttp.Response response = http.get(IbkrEndpoint.POSITIONS, "/portfolio/" + accountId + "/positions/0");
            require(response);
            Map<Long, BigDecimal> out = new HashMap<>();
            for (JsonNode p : response.body()) {
                Long conid = IbkrJson.whole(p.get("conid"));
                BigDecimal position = IbkrJson.decimal(p.get("position"));
                if (conid != null && position != null) {
                    out.merge(conid, position, BigDecimal::add);
                }
            }
            return out;
        } catch (IbkrHttp.CallException e) {
            log.debug("IBKR positions unavailable: {}", e.getMessage());
            return null;
        }
    }

    private static Quantity quantity(BigDecimal value) {
        try {
            return new Quantity(value);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static String reference(JsonNode node) {
        String ref = IbkrJson.id(node);
        return ref != null && IDENTIFIER.matcher(ref).matches() ? ref : null;
    }

    /** IBKR order status to the neutral status (Cancelled can also mean the destination rejected the order). */
    static ObservedStatus observed(String status, Quantity filled) {
        return switch (status.toLowerCase(Locale.ROOT)) {
            case "submitted", "presubmitted", "pendingsubmit", "pendingcancel", "precancelled" ->
                    filled.isZero() ? ObservedStatus.WORKING : ObservedStatus.PARTIALLY_FILLED;
            case "filled" -> ObservedStatus.FILLED;
            case "cancelled" -> ObservedStatus.CANCELLED;
            case "inactive" -> ObservedStatus.INACTIVE;
            default -> ObservedStatus.OTHER;
        };
    }

    private static void require(IbkrHttp.Response response) throws IbkrHttp.CallException {
        if (!response.ok()) {
            throw new IbkrHttp.CallException(IbkrHttp.CallException.Kind.MAYBE_SENT, "HTTP " + response.status());
        }
    }
}
