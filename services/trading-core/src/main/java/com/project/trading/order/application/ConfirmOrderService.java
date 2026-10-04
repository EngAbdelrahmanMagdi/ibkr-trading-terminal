package com.project.trading.order.application;

import com.project.trading.broker.domain.BrokerConnectionState;
import com.project.trading.broker.domain.BrokerTradingPort;
import com.project.trading.broker.domain.SubmitResult;
import com.project.trading.order.domain.Order;
import com.project.trading.order.domain.OrderRepository;
import com.project.trading.order.domain.OrderStatus;
import com.project.trading.shared.domain.DomainException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.UUID;

/**
 * Answers a broker confirmation request. Only valid in PENDING_CONFIRMATION. The broker may answer with another
 * confirmation request; the chain depth is bounded and exceeding it rejects the order locally. A confirmation
 * request expires after the configured TTL: it is then rejected locally and the broker is not called.
 */
@Service
public class ConfirmOrderService {

    private static final Logger log = LoggerFactory.getLogger(ConfirmOrderService.class);

    private final OrderRepository orders;
    private final BrokerTradingPort broker;
    private final SubmissionOutcomes outcomes;
    private final ConfirmationExpiry expiry;
    private final Clock clock;

    ConfirmOrderService(OrderRepository orders, BrokerTradingPort broker, SubmissionOutcomes outcomes,
                        ConfirmationExpiry expiry, Clock clock) {
        this.orders = orders;
        this.broker = broker;
        this.outcomes = outcomes;
        this.expiry = expiry;
        this.clock = clock;
    }

    public Order confirm(UUID orderId, boolean confirm) {
        Order order = orders.findById(orderId).orElseThrow(() -> DomainException.notFound("order " + orderId));
        if (order.status() != OrderStatus.PENDING_CONFIRMATION) {
            throw DomainException.conflict("the order is not waiting for confirmation (status " + order.status() + ")");
        }
        String replyId = order.replyId();
        if (expiry.isExpired(order, clock.instant())) {
            expiry.expire(orderId, replyId);
            throw DomainException.conflict("the confirmation request expired; the order was rejected and nothing was sent");
        }
        if (confirm && broker.connectionState() != BrokerConnectionState.READY) {
            throw DomainException.brokerUnavailable("the broker is not available; try again before the confirmation expires");
        }
        SubmitResult result;
        try {
            result = broker.confirmReply(replyId, confirm);
        } catch (RuntimeException e) {
            log.error("broker confirmation failed; outcome unknown for order {} category={}", orderId, e.getClass().getSimpleName());
            result = new SubmitResult.Unknown("broker call failed");
        }
        Order updated = outcomes.apply(orderId, OrderStatus.PENDING_CONFIRMATION, replyId, result, null).order();
        log.info("order {} confirmation answered ({}) -> {}", orderId, confirm ? "confirm" : "decline", updated.status());
        return updated;
    }
}
