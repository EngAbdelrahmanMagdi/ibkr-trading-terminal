package com.project.trading.order.application;

import com.project.trading.broker.domain.BrokerOrderUpdate;
import com.project.trading.broker.domain.BrokerOrderUpdateHandler;
import com.project.trading.broker.domain.SubmitResult;
import com.project.trading.order.domain.Order;
import com.project.trading.order.domain.OrderRepository;
import com.project.trading.order.domain.OrderStatus;
import com.project.trading.shared.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Applies a broker submission or confirmation result to an order in one transaction, then feeds updates the
 * broker returned immediately (for example the fill of a market order) through the broker update path.
 */
@Component
class SubmissionOutcomes {

    private static final Logger log = LoggerFactory.getLogger(SubmissionOutcomes.class);

    /** The order after the outcome was applied. */
    record Applied(Order order, boolean awaitingOutcome) {
    }

    private final OrderRepository orders;
    private final BrokerOrderUpdateHandler updates;
    private final OrderMetrics metrics;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final int maxReplyDepth;

    SubmissionOutcomes(OrderRepository orders, BrokerOrderUpdateHandler updates, OrderMetrics metrics,
                       PlatformTransactionManager transactionManager, Clock clock, AppProperties properties) {
        this.orders = orders;
        this.updates = updates;
        this.metrics = metrics;
        this.tx = new TransactionTemplate(transactionManager);
        this.clock = clock;
        this.maxReplyDepth = properties.orders().maxReplyDepth();
    }

    /**
     * Applies the result if the order is still in expectedStatus (and, for a confirmation, still waiting on
     * expectedReplyId); otherwise the result is stale and the current order is returned unchanged.
     * inTransaction runs inside the same transaction (for example to complete the idempotency record).
     */
    Applied apply(UUID orderId, OrderStatus expectedStatus, String expectedReplyId, SubmitResult result,
                  Consumer<Applied> inTransaction) {
        Applied applied = tx.execute(status -> {
            Order order = orders.lockById(orderId).orElseThrow();
            if (order.status() != expectedStatus
                    || (expectedReplyId != null && !expectedReplyId.equals(order.replyId()))) {
                log.warn("stale broker result for order {} ignored (status {})", orderId, order.status());
                Applied current = new Applied(order, order.status() == OrderStatus.PENDING_CONFIRMATION
                        || order.status() == OrderStatus.UNKNOWN);
                if (inTransaction != null) {
                    inTransaction.accept(current);
                }
                return current;
            }
            Instant now = clock.instant();
            boolean awaiting = switch (result) {
                case SubmitResult.Accepted accepted -> {
                    order.acknowledge(accepted.brokerOrderId(), now);
                    yield false;
                }
                case SubmitResult.ConfirmationRequired reply -> {
                    boolean pending = order.requireConfirmation(reply.replyId(), reply.message(), maxReplyDepth, now);
                    if (!pending) {
                        metrics.rejectedByBroker();
                    }
                    yield pending;
                }
                case SubmitResult.Rejected rejected -> {
                    order.reject(rejected.reason(), now);
                    metrics.rejectedByBroker();
                    yield false;
                }
                case SubmitResult.Failed failed -> {
                    if (order.status() == OrderStatus.SUBMISSION_PENDING) {
                        order.fail(failed.reason(), now);
                    } else {
                        order.reject(failed.reason(), now);
                    }
                    yield false;
                }
                case SubmitResult.Unknown unknown -> {
                    log.warn("broker outcome unknown for order {}: {}", orderId, unknown.reason());
                    order.markUnknown(now);
                    yield true;
                }
            };
            Applied saved = new Applied(orders.save(order), awaiting);
            if (inTransaction != null) {
                inTransaction.accept(saved);
            }
            return saved;
        });

        if (result instanceof SubmitResult.Accepted accepted && !accepted.immediateUpdates().isEmpty()
                && applied.order().status() == OrderStatus.SUBMITTED) {
            applyImmediate(orderId, accepted.immediateUpdates());
            return new Applied(orders.findById(orderId).orElseThrow(), false);
        }
        return applied;
    }

    private void applyImmediate(UUID orderId, List<BrokerOrderUpdate> immediate) {
        for (BrokerOrderUpdate update : immediate) {
            try {
                BrokerOrderUpdateHandler.Outcome outcome = updates.handle(update);
                if (outcome != BrokerOrderUpdateHandler.Outcome.APPLIED) {
                    log.warn("immediate broker update for order {} not applied: {}", orderId, outcome);
                }
            } catch (RuntimeException e) {
                log.error("immediate broker update for order {} failed", orderId, e);
            }
        }
    }
}
