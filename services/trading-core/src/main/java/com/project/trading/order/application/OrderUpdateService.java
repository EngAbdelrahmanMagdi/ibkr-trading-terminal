package com.project.trading.order.application;

import com.project.trading.broker.domain.BrokerOrderUpdate;
import com.project.trading.broker.domain.BrokerOrderUpdateHandler;
import com.project.trading.broker.domain.OpenBrokerOrder;
import com.project.trading.broker.domain.OpenOrderSource;
import com.project.trading.execution.application.ExecutionLedger;
import com.project.trading.execution.domain.Execution;
import com.project.trading.order.domain.InvalidTransitionException;
import com.project.trading.order.domain.Order;
import com.project.trading.order.domain.OrderRepository;
import com.project.trading.order.domain.OrderStatus;
import com.project.trading.position.application.PositionLedger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * The single path for broker order updates (fills, confirmed cancellations, rejections of working orders).
 * One transaction per update: the execution, the order transition and the position change commit together.
 * Duplicate and out-of-date updates are ignored, so redelivery is harmless.
 */
@Service
public class OrderUpdateService implements BrokerOrderUpdateHandler, OpenOrderSource {

    private static final Logger log = LoggerFactory.getLogger(OrderUpdateService.class);

    /** States in which the broker has not yet acknowledged the order locally. */
    private static final Set<OrderStatus> NOT_YET_ACKNOWLEDGED =
            EnumSet.of(OrderStatus.CREATED, OrderStatus.SUBMISSION_PENDING, OrderStatus.PENDING_CONFIRMATION);

    /** Upper bound of working limit orders loaded at once (the mock broker's book is bounded too). */
    private static final int MAX_OPEN_ORDERS = 10_000;

    private final OrderRepository orders;
    private final ExecutionLedger executions;
    private final PositionLedger positions;
    private final OrderMetrics metrics;
    private final TransactionTemplate tx;

    public OrderUpdateService(OrderRepository orders, ExecutionLedger executions, PositionLedger positions,
                              OrderMetrics metrics, PlatformTransactionManager transactionManager) {
        this.orders = orders;
        this.executions = executions;
        this.positions = positions;
        this.metrics = metrics;
        this.tx = new TransactionTemplate(transactionManager);
    }

    @Override
    public Outcome handle(BrokerOrderUpdate update) {
        Outcome outcome = tx.execute(status -> apply(update));
        if (outcome == Outcome.APPLIED) {
            log.info("broker update applied: {} for broker order {}", update.getClass().getSimpleName(),
                    update.brokerOrderId());
        }
        return outcome;
    }

    private Outcome apply(BrokerOrderUpdate update) {
        Optional<Order> found = orders.lockByBrokerOrderId(update.brokerOrderId());
        if (found.isEmpty()) {
            // The broker order ID is stored when the acknowledgement commits; until then the order is unknown.
            log.debug("broker update for broker order {} not yet known locally", update.brokerOrderId());
            return Outcome.NOT_READY;
        }
        Order order = found.get();
        if (NOT_YET_ACKNOWLEDGED.contains(order.status())) {
            return Outcome.NOT_READY;
        }
        try {
            return switch (update) {
                case BrokerOrderUpdate.Fill fill -> applyFill(order, fill);
                case BrokerOrderUpdate.Cancelled cancelled -> save(order, order.confirmCancelled(cancelled.at()));
                case BrokerOrderUpdate.Rejected rejected ->
                        save(order, order.brokerRejected(rejected.reason(), rejected.at()));
            };
        } catch (InvalidTransitionException e) {
            metrics.invalidTransition();
            log.warn("broker update refused for order {}: {}", order.id(), e.getMessage());
            return Outcome.IGNORED;
        }
    }

    private Outcome applyFill(Order order, BrokerOrderUpdate.Fill fill) {
        if (executions.isRecorded(fill.brokerExecutionId())) {
            return Outcome.IGNORED;
        }
        Order.UpdateResult result;
        try {
            result = order.applyFill(fill.quantity(), fill.price(), fill.executedAt());
        } catch (IllegalArgumentException e) {
            metrics.invalidTransition();
            log.error("fill for order {} refused: {}", order.id(), e.getMessage());
            return Outcome.IGNORED;
        }
        if (result == Order.UpdateResult.IGNORED) {
            log.warn("fill {} for terminal order {} ignored", fill.brokerExecutionId(), order.id());
            return Outcome.IGNORED;
        }
        orders.save(order);
        executions.record(new Execution(UUID.randomUUID(), order.id(), fill.brokerExecutionId(), order.symbol(),
                fill.side(), fill.quantity(), fill.price(), fill.commission(), fill.currency(), fill.executedAt()));
        positions.applyFill(order.symbol(), fill.currency(), fill.side(), fill.quantity(), fill.price(),
                fill.executedAt());
        return Outcome.APPLIED;
    }

    private Outcome save(Order order, Order.UpdateResult result) {
        if (result == Order.UpdateResult.IGNORED) {
            return Outcome.IGNORED;
        }
        orders.save(order);
        return Outcome.APPLIED;
    }

    @Override
    public List<OpenBrokerOrder> openLimitOrders() {
        return orders.findWorkingLimitOrders(MAX_OPEN_ORDERS).stream()
                .map(o -> new OpenBrokerOrder(o.brokerOrderId(), o.symbol(), o.brokerSide(),
                        o.quantity().minus(o.filledQuantity()), o.limitPrice(),
                        o.status() == OrderStatus.CANCEL_PENDING))
                .toList();
    }
}
