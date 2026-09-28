package com.project.trading.order.domain;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Persistence port for orders. save uses optimistic locking on the version; the lock methods take a row lock
 * for the rest of the current transaction (serializing concurrent updates of one order).
 */
public interface OrderRepository {

    Order save(Order order);

    Optional<Order> findById(UUID id);

    Optional<Order> lockById(UUID id);

    Optional<Order> lockByBrokerOrderId(String brokerOrderId);

    /** Newest first; all statuses when statuses is empty. */
    List<Order> findRecent(Collection<OrderStatus> statuses, int limit);

    boolean existsWithStatus(OrderStatus status);

    /** Working LIMIT orders that have a broker order ID, oldest first. */
    List<Order> findWorkingLimitOrders(int limit);

    /** Working orders of any type that have a broker order ID, oldest first. */
    List<Order> findWorkingOrders(int limit);

    /** Remaining quantity of non-terminal SELL-side orders in a symbol (reserved against the long position). */
    BigDecimal openSellQuantity(String symbol);

    Map<OrderStatus, Long> countByStatus();
}
