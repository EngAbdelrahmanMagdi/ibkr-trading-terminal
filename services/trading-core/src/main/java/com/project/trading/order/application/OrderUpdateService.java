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
import com.project.trading.outbox.application.OutboxAppender;
import com.project.trading.position.application.PositionLedger;
import com.project.trading.shared.domain.BrokerSide;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * The single path for broker order updates: fills, confirmed cancellations, rejections of working orders, and
 * observations of the broker's order state (from the order stream or reconciliation). One transaction per update
 * (joining the caller's transaction when there is one): the execution, the order transition, the position change and
 * their events commit together. Duplicate and out-of-date updates are ignored, so redelivery is harmless.
 * <p>
 * Rules:
 * <ul>
 *   <li>An update finds its order by broker order ID, else by the client order reference the broker echoes. An order
 *   whose outcome was unknown is acknowledged with the broker order ID the broker reports for it.</li>
 *   <li>Every previously unseen execution is recorded and applied to the position, even when the order already
 *   ended locally: the order's status never moves backward, and its filled quantity advances only while it stays
 *   within the order quantity. Such late executions are reported as drift (serious for rejected or failed orders, or
 *   when the order quantity would be exceeded).</li>
 *   <li>A cancellation is applied only once every fill the broker reported for the order is recorded.</li>
 * </ul>
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
    private final OrderEventFactory events;
    private final OutboxAppender outbox;
    private final DriftReporter drift;
    private final Clock clock;
    private final Counter unknownOrders;

    public OrderUpdateService(OrderRepository orders, ExecutionLedger executions, PositionLedger positions,
                              OrderMetrics metrics, PlatformTransactionManager transactionManager,
                              OrderEventFactory events, OutboxAppender outbox, DriftReporter drift, Clock clock,
                              MeterRegistry registry) {
        this.orders = orders;
        this.executions = executions;
        this.positions = positions;
        this.metrics = metrics;
        this.tx = new TransactionTemplate(transactionManager);
        this.events = events;
        this.outbox = outbox;
        this.drift = drift;
        this.clock = clock;
        this.unknownOrders = Counter.builder("broker.updates.unknown.orders")
                .description("Broker updates for orders placed outside this application (not imported)").register(registry);
    }

    @Override
    public Outcome handle(BrokerOrderUpdate update) {
        Outcome outcome = tx.execute(status -> apply(update));
        if (outcome == Outcome.APPLIED) {
            log.info("broker update applied: {} for broker order {}", update.getClass().getSimpleName(),
                    update.brokerOrderId());
        } else if (outcome == Outcome.UNKNOWN_ORDER) {
            unknownOrders.increment();
            log.info("broker update for an order placed outside this application ignored (broker order {})",
                    update.brokerOrderId());
        }
        return outcome;
    }

    private Outcome apply(BrokerOrderUpdate update) {
        Optional<Order> found = update.brokerOrderId() == null ? Optional.empty()
                : orders.lockByBrokerOrderId(update.brokerOrderId());
        if (found.isEmpty() && update.clientOrderRef() != null) {
            found = orders.lockByClientOrderId(update.clientOrderRef());
        }
        if (found.isEmpty()) {
            // Without a client reference the order may simply not be acknowledged locally yet (its broker order ID is
            // stored when the acknowledgement commits); with one, no local order has it.
            return update.clientOrderRef() != null || update instanceof BrokerOrderUpdate.Observed
                    ? Outcome.UNKNOWN_ORDER : Outcome.NOT_READY;
        }
        Order order = found.get();
        boolean resolved = false;
        if (order.brokerOrderId() == null) {
            // A process may die after sending but before saving the acknowledgement. Only a
            // positive broker observation with the matching client reference can recover it.
            if ((order.status() != OrderStatus.UNKNOWN && order.status() != OrderStatus.SUBMISSION_PENDING)
                    || update.brokerOrderId() == null) {
                return Outcome.NOT_READY;
            }
            order.acknowledge(update.brokerOrderId(), clock.instant());
            resolved = true;
            drift.report("order", DriftReporter.INFO, order.id(), "order awaiting acknowledgement found at the broker");
        } else if (update.brokerOrderId() != null && !order.brokerOrderId().equals(update.brokerOrderId())) {
            drift.report("order", DriftReporter.SERIOUS, order.id(),
                    "the broker reports another broker order ID for this client order reference");
            return Outcome.IGNORED;
        }
        if (NOT_YET_ACKNOWLEDGED.contains(order.status())) {
            return Outcome.NOT_READY;
        }
        Outcome outcome;
        try {
            outcome = switch (update) {
                case BrokerOrderUpdate.Fill fill -> applyFill(order, fill);
                case BrokerOrderUpdate.Cancelled cancelled -> save(order, order.confirmCancelled(cancelled.at()));
                case BrokerOrderUpdate.Rejected rejected ->
                        save(order, order.brokerRejected(rejected.reason(), rejected.at()));
                case BrokerOrderUpdate.Observed observed -> applyObserved(order, observed);
            };
        } catch (InvalidTransitionException e) {
            metrics.invalidTransition();
            log.warn("broker update refused for order {}: {}", order.id(), e.getMessage());
            outcome = Outcome.IGNORED;
        }
        if (resolved && outcome != Outcome.APPLIED && outcome != Outcome.NOT_READY) {
            orders.save(order);
            return Outcome.APPLIED;
        }
        return outcome;
    }

    private Outcome applyObserved(Order order, BrokerOrderUpdate.Observed observed) {
        return switch (observed.status()) {
            case CANCELLED -> {
                if (observed.filledQuantity() != null && observed.filledQuantity().compareTo(order.filledQuantity()) > 0) {
                    // Fills first: the cancellation is applied once every reported fill is recorded.
                    yield Outcome.NOT_READY;
                }
                yield save(order, order.confirmCancelled(observed.at()));
            }
            case REJECTED -> save(order, order.brokerRejected("rejected by the broker (" + observed.detail() + ")",
                    observed.at()));
            default -> Outcome.IGNORED;
        };
    }

    private Outcome applyFill(Order order, BrokerOrderUpdate.Fill fill) {
        if (executions.isRecorded(fill.brokerExecutionId())) {
            return Outcome.IGNORED;
        }
        BrokerSide side = fill.side() != null ? fill.side() : order.brokerSide();
        OrderStatus status = order.status();
        if (status.isTerminal()) {
            Order.LateFillResult late = order.recordLateFill(fill.quantity(), fill.price(), fill.executedAt());
            boolean serious = late == Order.LateFillResult.EXCEEDS_ORDER_QUANTITY
                    || status == OrderStatus.REJECTED || status == OrderStatus.FAILED;
            drift.report("execution", serious ? DriftReporter.SERIOUS : DriftReporter.INFO, order.id(),
                    "execution " + fill.brokerExecutionId() + " reported for an order already " + status
                            + (late == Order.LateFillResult.EXCEEDS_ORDER_QUANTITY ? "; it exceeds the order quantity" : ""));
            if (late == Order.LateFillResult.FILL_RECORDED) {
                orders.save(order);
            }
        } else if (!order.fits(fill.quantity())) {
            drift.report("execution", DriftReporter.SERIOUS, order.id(),
                    "execution " + fill.brokerExecutionId() + " exceeds the order quantity; the order is left unchanged");
        } else {
            order.applyFill(fill.quantity(), fill.price(), fill.executedAt());
            orders.save(order);
        }
        Execution execution = new Execution(UUID.randomUUID(), order.id(), fill.brokerExecutionId(), order.symbol(),
                side, fill.quantity(), fill.price(), fill.commission(), fill.currency(), fill.executedAt());
        executions.record(execution);
        outbox.append(events.executionRecorded(order, execution));
        positions.applyFill(order.symbol(), fill.currency(), side, fill.quantity(), fill.price(), fill.executedAt());
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
