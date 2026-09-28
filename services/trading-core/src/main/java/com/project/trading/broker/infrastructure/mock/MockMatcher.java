package com.project.trading.broker.infrastructure.mock;

import com.project.trading.broker.domain.BrokerOrderUpdate;
import com.project.trading.broker.domain.BrokerOrderUpdateHandler;
import com.project.trading.broker.domain.OpenBrokerOrder;
import com.project.trading.broker.domain.OpenOrderSource;
import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * The simulated broker's book of resting limit orders. A single scheduled task re-evaluates a bounded batch of
 * orders against the fresh simulated quote on each tick: marketable orders fill in full at the quote, and
 * requested cancellations are confirmed. Market orders never rest here (they fill on submission). The book is
 * bounded and is rebuilt from the system of record on start.
 */
public class MockMatcher implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(MockMatcher.class);

    private static final class Entry {
        final String brokerOrderId;
        final String symbol;
        final BrokerSide side;
        final Quantity quantity;
        final Price limit;
        final String currency;
        volatile boolean cancelRequested;
        Instant notReadySince;

        Entry(String brokerOrderId, String symbol, BrokerSide side, Quantity quantity, Price limit, String currency,
              boolean cancelRequested) {
            this.brokerOrderId = brokerOrderId;
            this.symbol = symbol;
            this.side = side;
            this.quantity = quantity;
            this.limit = limit;
            this.currency = currency;
            this.cancelRequested = cancelRequested;
        }
    }

    private final Map<String, Entry> book = new LinkedHashMap<>();
    private final MockMarket market;
    private final BrokerOrderUpdateHandler updates;
    private final OpenOrderSource openOrders;
    private final Function<String, String> currencyOf;
    private final Duration interval;
    private final int batchSize;
    private final int maxOpenOrders;
    private final Duration notReadyTimeout;

    private ScheduledExecutorService executor;
    private volatile boolean running;
    private int cursor;

    MockMatcher(MockMarket market, BrokerOrderUpdateHandler updates, OpenOrderSource openOrders,
                Function<String, String> currencyOf, MockBrokerProperties properties) {
        this.market = market;
        this.updates = updates;
        this.openOrders = openOrders;
        this.currencyOf = currencyOf;
        this.interval = properties.matcherInterval();
        this.batchSize = properties.matcherBatchSize();
        this.maxOpenOrders = properties.maxOpenOrders();
        this.notReadyTimeout = properties.notReadyTimeout();
    }

    /** Adds a resting limit order; false when the book is full. */
    synchronized boolean add(String brokerOrderId, String symbol, BrokerSide side, Quantity quantity, Price limit) {
        if (book.size() >= maxOpenOrders) {
            return false;
        }
        book.put(brokerOrderId, new Entry(brokerOrderId, symbol, side, quantity, limit, currencyOf.apply(symbol), false));
        return true;
    }

    /** Marks a resting order for cancellation; false when the order is not in the book. */
    synchronized boolean requestCancel(String brokerOrderId) {
        Entry entry = book.get(brokerOrderId);
        if (entry == null) {
            return false;
        }
        entry.cancelRequested = true;
        return true;
    }

    /** Number of resting orders in the book. */
    public synchronized int openOrderCount() {
        return book.size();
    }

    @Override
    public void start() {
        List<OpenBrokerOrder> open = openOrders.openLimitOrders();
        synchronized (this) {
            book.clear();
            for (OpenBrokerOrder o : open) {
                if (book.size() >= maxOpenOrders) {
                    log.warn("simulated order book is full; {} open orders not restored", open.size() - book.size());
                    break;
                }
                book.put(o.brokerOrderId(), new Entry(o.brokerOrderId(), o.symbol(), o.side(), o.openQuantity(),
                        o.limitPrice(), currencyOf.apply(o.symbol()), o.cancelRequested()));
            }
        }
        log.info("simulated order book restored with {} open limit orders", openOrderCount());
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "mock-matcher");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::safeTick, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
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

    private void safeTick() {
        try {
            tick();
        } catch (RuntimeException e) {
            log.error("simulated matcher tick failed", e);
        }
    }

    /** Evaluates at most batchSize orders, rotating through the book across ticks. */
    void tick() {
        List<Entry> batch = nextBatch();
        for (Entry entry : batch) {
            if (!running && executor != null) {
                return;
            }
            evaluate(entry);
        }
    }

    private synchronized List<Entry> nextBatch() {
        List<Entry> all = new ArrayList<>(book.values());
        if (all.isEmpty()) {
            return List.of();
        }
        int n = Math.min(batchSize, all.size());
        List<Entry> batch = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            batch.add(all.get((cursor + i) % all.size()));
        }
        cursor = (cursor + n) % all.size();
        return batch;
    }

    private void evaluate(Entry entry) {
        BrokerOrderUpdate update;
        if (entry.cancelRequested) {
            update = new BrokerOrderUpdate.Cancelled(entry.brokerOrderId, market.now());
        } else {
            Optional<Price> price = market.limitFillPrice(entry.symbol, entry.side, entry.limit);
            if (price.isEmpty()) {
                return;
            }
            update = market.fill(entry.brokerOrderId, entry.side, entry.quantity, price.get(), entry.currency);
        }
        BrokerOrderUpdateHandler.Outcome outcome;
        try {
            outcome = updates.handle(update);
        } catch (RuntimeException e) {
            log.warn("simulated update for {} not applied; retrying on a later tick", entry.brokerOrderId, e);
            return;
        }
        switch (outcome) {
            case APPLIED, IGNORED, UNKNOWN_ORDER -> remove(entry.brokerOrderId);
            case NOT_READY -> notReady(entry);
        }
    }

    private synchronized void remove(String brokerOrderId) {
        book.remove(brokerOrderId);
    }

    private synchronized void notReady(Entry entry) {
        Instant now = market.now();
        if (entry.notReadySince == null) {
            entry.notReadySince = now;
        } else if (Duration.between(entry.notReadySince, now).compareTo(notReadyTimeout) > 0) {
            log.warn("simulated order {} was never acknowledged locally; dropped from the book", entry.brokerOrderId);
            book.remove(entry.brokerOrderId);
        }
    }
}
