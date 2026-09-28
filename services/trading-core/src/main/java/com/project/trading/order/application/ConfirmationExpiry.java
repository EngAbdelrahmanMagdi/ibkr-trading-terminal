package com.project.trading.order.application;

import com.project.trading.order.domain.Order;
import com.project.trading.order.domain.OrderRepository;
import com.project.trading.order.domain.OrderStatus;
import com.project.trading.shared.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Expires broker confirmation requests that were not answered in time. An unanswered confirmation means the
 * order was never transmitted, so expiring it rejects the order locally without calling the broker. It also
 * releases the rule that blocks new orders while a confirmation is pending.
 * <p>
 * An answer is refused once the confirmation TTL has passed. The background sweep waits an extra grace period
 * (longer than a broker request can take), so it can never expire an order whose answer is still in flight.
 */
@Component
class ConfirmationExpiry {

    private static final Logger log = LoggerFactory.getLogger(ConfirmationExpiry.class);
    private static final int SWEEP_BATCH = 100;

    static final String REASON = "the confirmation request expired before it was answered; nothing was sent";

    private final OrderRepository orders;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final Duration ttl;
    private final Duration sweepAfter;

    ConfirmationExpiry(OrderRepository orders, PlatformTransactionManager transactionManager, Clock clock,
                       AppProperties properties) {
        this.orders = orders;
        this.tx = new TransactionTemplate(transactionManager);
        this.clock = clock;
        this.ttl = properties.orders().confirmationTtl();
        this.sweepAfter = ttl.plus(properties.orders().confirmationSweepGrace());
    }

    /** True once the confirmation request of a PENDING_CONFIRMATION order may no longer be answered. */
    boolean isExpired(Order order, Instant now) {
        return order.status() == OrderStatus.PENDING_CONFIRMATION && now.isAfter(order.updatedAt().plus(ttl));
    }

    /** Rejects the order if it is still waiting on replyId; returns the current order either way. */
    Order expire(UUID orderId, String replyId) {
        return tx.execute(status -> {
            Order order = orders.lockById(orderId).orElseThrow();
            if (order.status() != OrderStatus.PENDING_CONFIRMATION || !replyId.equals(order.replyId())) {
                return order;
            }
            order.reject(REASON, clock.instant());
            log.info("order {} rejected: confirmation expired", orderId);
            return orders.save(order);
        });
    }

    @Scheduled(fixedDelay = 5, initialDelay = 5, timeUnit = TimeUnit.SECONDS)
    void sweep() {
        try {
            Instant now = clock.instant();
            for (Order order : orders.findRecent(EnumSet.of(OrderStatus.PENDING_CONFIRMATION), SWEEP_BATCH)) {
                if (now.isAfter(order.updatedAt().plus(sweepAfter))) {
                    expire(order.id(), order.replyId());
                }
            }
        } catch (RuntimeException e) {
            log.warn("confirmation expiry sweep failed: {}", e.getClass().getSimpleName());
        }
    }
}
