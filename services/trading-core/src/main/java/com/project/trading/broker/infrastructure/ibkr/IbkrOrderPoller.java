package com.project.trading.broker.infrastructure.ibkr;

import com.project.trading.broker.domain.BrokerConnectionState;
import com.project.trading.broker.domain.BrokerOrderUpdate;
import com.project.trading.broker.domain.BrokerOrderUpdateHandler;
import com.project.trading.broker.domain.OpenOrderSource;
import com.project.trading.broker.domain.WorkingBrokerOrder;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import tools.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Advances this application's own working IBKR orders by polling the broker, at most once per interval (IBKR
 * allows one live-orders and one trades request per 5 s). It only applies positive observations through the
 * common broker-update path:
 * <ul>
 *   <li>a fill, from GET /iserver/account/trades, identified by its unique {@code execution_id} and matched to the
 *   order by {@code order_ref} (the client order ID sent as {@code cOID});</li>
 *   <li>a cancellation, from GET /iserver/account/orders status {@code Cancelled}, only once every fill the broker
 *   reported for the order has been applied.</li>
 * </ul>
 * An order missing from the broker's list is never treated as cancelled or failed, orders placed outside this
 * application are ignored, and unknown outcomes are not resolved here: those need full reconciliation against
 * broker truth. Nothing is requested when there are no working orders or while a confirmation is outstanding.
 * Checked 2026-09-28 against https://www.interactivebrokers.com/docs/web-api/ (Live Orders, Trades, Order Status
 * Value).
 */
final class IbkrOrderPoller implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(IbkrOrderPoller.class);

    private static final int MAX_WORKING_ORDERS = 1000;
    private static final Pattern EXECUTION_ID = Pattern.compile("^[A-Za-z0-9.:_-]{1,64}$");

    /** One row of the broker's live-orders list that belongs to a working order of this application. */
    private record BrokerRow(WorkingBrokerOrder order, String status, BigDecimal filled) {
    }

    private final IbkrHttp http;
    private final IbkrSession session;
    private final ReplyGate replies;
    private final OpenOrderSource orders;
    private final BrokerOrderUpdateHandler updates;
    private final String currency;
    private final Duration interval;
    private final Clock clock;
    private ScheduledExecutorService executor;
    private volatile boolean running;

    IbkrOrderPoller(IbkrHttp http, IbkrSession session, ReplyGate replies, OpenOrderSource orders,
                    BrokerOrderUpdateHandler updates, String currency, Duration interval, Clock clock) {
        this.http = http;
        this.session = session;
        this.replies = replies;
        this.orders = orders;
        this.updates = updates;
        this.currency = currency;
        this.interval = interval;
        this.clock = clock;
    }

    @Override
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ibkr-order-poller");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::safePoll, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Stops after the web server has drained requests and before the datasource closes. */
    @Override
    public int getPhase() {
        return 0;
    }

    private void safePoll() {
        try {
            poll();
        } catch (RuntimeException e) {
            log.error("IBKR order poll failed", e);
        }
    }

    /** One polling round. */
    void poll() {
        List<WorkingBrokerOrder> working = orders.workingOrders(MAX_WORKING_ORDERS);
        if (working.isEmpty() || replies.isOpen() || session.state() != BrokerConnectionState.READY) {
            return;
        }
        List<BrokerRow> rows;
        try {
            rows = liveOrders(working);
        } catch (IbkrHttp.CallException e) {
            log.debug("IBKR live orders unavailable: {}", e.getMessage());
            return;
        }
        Map<String, BigDecimal> applied = new HashMap<>();
        boolean fillsReported = rows.stream().anyMatch(r -> r.filled().compareTo(r.order().filledQuantity().value()) > 0);
        if (fillsReported) {
            try {
                applied = applyTrades(working);
            } catch (IbkrHttp.CallException e) {
                log.debug("IBKR trades unavailable: {}", e.getMessage());
                return;
            }
        }
        for (BrokerRow row : rows) {
            if (!"Cancelled".equalsIgnoreCase(row.status())) {
                continue;
            }
            BigDecimal localFilled = row.order().filledQuantity().value()
                    .add(applied.getOrDefault(row.order().brokerOrderId(), BigDecimal.ZERO));
            if (row.filled().compareTo(localFilled) > 0) {
                continue;
            }
            handle(new BrokerOrderUpdate.Cancelled(row.order().brokerOrderId(), clock.instant()));
        }
    }

    private List<BrokerRow> liveOrders(List<WorkingBrokerOrder> working) throws IbkrHttp.CallException {
        IbkrHttp.Response response = http.get(IbkrEndpoint.LIVE_ORDERS, "/iserver/account/orders");
        if (!response.ok()) {
            throw new IbkrHttp.CallException(IbkrHttp.CallException.Kind.MAYBE_SENT, "HTTP " + response.status());
        }
        Map<String, WorkingBrokerOrder> byBrokerId = new HashMap<>();
        Map<String, WorkingBrokerOrder> byClientId = new HashMap<>();
        for (WorkingBrokerOrder o : working) {
            byBrokerId.put(o.brokerOrderId(), o);
            byClientId.put(o.clientOrderId(), o);
        }
        List<BrokerRow> rows = new ArrayList<>();
        for (JsonNode row : response.body().path("orders")) {
            WorkingBrokerOrder order = byBrokerId.get(IbkrJson.id(row.get("orderId")));
            if (order == null) {
                order = byClientId.get(IbkrJson.id(row.get("order_ref")));
            }
            BigDecimal filled = IbkrJson.decimal(row.get("filledQuantity"));
            String status = IbkrJson.id(row.get("status"));
            if (order != null && status != null) {
                rows.add(new BrokerRow(order, status, filled == null ? BigDecimal.ZERO : filled));
            }
        }
        return rows;
    }

    /** Applies this application's executions, oldest first; returns the quantity applied per broker order ID. */
    private Map<String, BigDecimal> applyTrades(List<WorkingBrokerOrder> working) throws IbkrHttp.CallException {
        IbkrHttp.Response response = http.get(IbkrEndpoint.TRADES, "/iserver/account/trades");
        if (!response.ok() || !response.body().isArray()) {
            throw new IbkrHttp.CallException(IbkrHttp.CallException.Kind.MAYBE_SENT, "HTTP " + response.status());
        }
        Map<String, WorkingBrokerOrder> byClientId = new HashMap<>();
        for (WorkingBrokerOrder o : working) {
            byClientId.put(o.clientOrderId(), o);
        }
        List<BrokerOrderUpdate.Fill> fills = new ArrayList<>();
        for (JsonNode trade : response.body()) {
            WorkingBrokerOrder order = byClientId.get(IbkrJson.id(trade.get("order_ref")));
            if (order != null) {
                BrokerOrderUpdate.Fill fill = fill(order, trade);
                if (fill != null) {
                    fills.add(fill);
                }
            }
        }
        fills.sort(Comparator.comparing(BrokerOrderUpdate.Fill::executedAt));
        Map<String, BigDecimal> applied = new HashMap<>();
        for (BrokerOrderUpdate.Fill fill : fills) {
            if (handle(fill) == BrokerOrderUpdateHandler.Outcome.APPLIED) {
                applied.merge(fill.brokerOrderId(), fill.quantity().value(), BigDecimal::add);
            }
        }
        return applied;
    }

    /** The execution as a fill, or null (logged) when a required field is missing or not exact. */
    private BrokerOrderUpdate.Fill fill(WorkingBrokerOrder order, JsonNode trade) {
        String executionId = IbkrJson.id(trade.get("execution_id"));
        BigDecimal size = IbkrJson.decimal(trade.get("size"));
        BigDecimal price = IbkrJson.decimal(trade.get("price"));
        Long time = IbkrJson.whole(trade.get("trade_time_r"));
        if (executionId == null || !EXECUTION_ID.matcher(executionId).matches() || size == null || size.signum() <= 0
                || price == null || price.signum() <= 0 || time == null) {
            log.warn("IBKR execution for broker order {} skipped: incomplete data", order.brokerOrderId());
            return null;
        }
        Quantity quantity;
        Price fillPrice;
        try {
            quantity = new Quantity(size);
            fillPrice = new Price(price);
        } catch (IllegalArgumentException e) {
            log.warn("IBKR execution for broker order {} skipped: {}", order.brokerOrderId(), e.getMessage());
            return null;
        }
        if (!quantity.isWhole()) {
            log.warn("IBKR execution for broker order {} skipped: fractional quantity", order.brokerOrderId());
            return null;
        }
        BigDecimal commission = IbkrJson.decimal(trade.get("commission"));
        return new BrokerOrderUpdate.Fill(order.brokerOrderId(), executionId, order.side(), quantity, fillPrice,
                commission == null || commission.signum() < 0 ? null : commission, currency, Instant.ofEpochMilli(time));
    }

    private BrokerOrderUpdateHandler.Outcome handle(BrokerOrderUpdate update) {
        try {
            return updates.handle(update);
        } catch (RuntimeException e) {
            log.warn("IBKR update for broker order {} not applied; retried on a later poll", update.brokerOrderId(), e);
            return BrokerOrderUpdateHandler.Outcome.NOT_READY;
        }
    }
}
