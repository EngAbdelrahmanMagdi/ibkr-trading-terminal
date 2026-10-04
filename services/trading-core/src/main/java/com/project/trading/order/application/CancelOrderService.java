package com.project.trading.order.application;

import com.project.trading.broker.domain.BrokerConnectionState;
import com.project.trading.broker.domain.BrokerTradingPort;
import com.project.trading.broker.domain.CancelResult;
import com.project.trading.order.domain.Order;
import com.project.trading.order.domain.OrderRepository;
import com.project.trading.order.domain.OrderStatus;
import com.project.trading.shared.domain.DomainException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.util.UUID;

/**
 * Requests cancellation of a working order. The broker is asked first (outside any transaction); the order then
 * moves to CANCEL_PENDING until the broker confirms. Fills that arrive meanwhile are still applied.
 */
@Service
public class CancelOrderService {

    private static final Logger log = LoggerFactory.getLogger(CancelOrderService.class);

    private final OrderRepository orders;
    private final BrokerTradingPort broker;
    private final TransactionTemplate tx;
    private final Clock clock;

    CancelOrderService(OrderRepository orders, BrokerTradingPort broker, PlatformTransactionManager transactionManager,
                       Clock clock) {
        this.orders = orders;
        this.broker = broker;
        this.tx = new TransactionTemplate(transactionManager);
        this.clock = clock;
    }

    public Order cancel(UUID orderId) {
        Order order = orders.findById(orderId).orElseThrow(() -> DomainException.notFound("order " + orderId));
        if (order.status() == OrderStatus.CANCEL_PENDING) {
            return order;
        }
        if (order.status() != OrderStatus.SUBMITTED && order.status() != OrderStatus.PARTIALLY_FILLED) {
            throw DomainException.conflict("the order is not open (status " + order.status() + ")");
        }
        if (broker.connectionState() != BrokerConnectionState.READY) {
            throw DomainException.brokerUnavailable("the broker is not available for cancellations");
        }

        CancelResult result;
        try {
            result = broker.cancel(order.brokerOrderId());
        } catch (RuntimeException e) {
            log.error("broker cancel failed for order {} category={}", orderId, e.getClass().getSimpleName());
            result = new CancelResult.Unknown("broker call failed");
        }

        return switch (result) {
            case CancelResult.Requested requested -> tx.execute(status -> {
                Order current = orders.lockById(orderId).orElseThrow();
                if (!current.status().isWorking() || current.status() == OrderStatus.CANCEL_PENDING) {
                    return current;
                }
                current.requestCancel(clock.instant());
                log.info("order {} cancellation requested", orderId);
                return orders.save(current);
            });
            case CancelResult.Rejected rejected -> throw DomainException.conflict(
                    "the broker refused the cancellation: " + rejected.reason());
            case CancelResult.Unknown unknown -> throw DomainException.brokerUnavailable(
                    "the cancellation outcome is unknown; check the order status before retrying");
        };
    }
}
