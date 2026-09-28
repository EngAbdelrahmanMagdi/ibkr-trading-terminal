package com.project.trading.support;

import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.order.domain.Order;
import com.project.trading.order.domain.OrderRepository;
import com.project.trading.order.domain.OrderStatus;
import com.project.trading.shared.domain.OrderType;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Order repository in memory with the same version semantics as the database adapter. */
public class InMemoryOrderRepository implements OrderRepository {

    private final Map<UUID, Order> rows = new ConcurrentHashMap<>();

    @Override
    public synchronized Order save(Order order) {
        Order existing = rows.get(order.id());
        if (existing != null && existing.version() != order.version()) {
            throw new IllegalStateException("optimistic lock failure");
        }
        Order stored = copy(order, existing == null ? 0 : order.version() + 1);
        rows.put(order.id(), stored);
        return copy(stored, stored.version());
    }

    @Override
    public Optional<Order> findById(UUID id) {
        return Optional.ofNullable(rows.get(id)).map(o -> copy(o, o.version()));
    }

    @Override
    public Optional<Order> lockById(UUID id) {
        return findById(id);
    }

    @Override
    public Optional<Order> lockByBrokerOrderId(String brokerOrderId) {
        return rows.values().stream().filter(o -> brokerOrderId.equals(o.brokerOrderId())).findFirst()
                .map(o -> copy(o, o.version()));
    }

    @Override
    public Optional<Order> lockByClientOrderId(String clientOrderId) {
        return findByClientOrderId(clientOrderId);
    }

    @Override
    public Optional<Order> findByClientOrderId(String clientOrderId) {
        return rows.values().stream().filter(o -> clientOrderId.equals(o.clientOrderId())).findFirst()
                .map(o -> copy(o, o.version()));
    }

    @Override
    public Optional<Order> findByBrokerOrderId(String brokerOrderId) {
        return rows.values().stream().filter(o -> brokerOrderId.equals(o.brokerOrderId())).findFirst()
                .map(o -> copy(o, o.version()));
    }

    @Override
    public List<Order> findRecent(Collection<OrderStatus> statuses, int limit) {
        return rows.values().stream()
                .filter(o -> statuses.isEmpty() || statuses.contains(o.status()))
                .sorted(Comparator.comparing(Order::createdAt).reversed())
                .limit(limit)
                .toList();
    }

    @Override
    public boolean existsWithStatus(OrderStatus status) {
        return rows.values().stream().anyMatch(o -> o.status() == status);
    }

    @Override
    public List<Order> findWorkingLimitOrders(int limit) {
        return rows.values().stream()
                .filter(o -> o.orderType() == OrderType.LIMIT && o.brokerOrderId() != null && o.status().isWorking())
                .limit(limit)
                .toList();
    }

    @Override
    public List<Order> findWorkingOrders(int limit) {
        return rows.values().stream()
                .filter(o -> o.brokerOrderId() != null && o.status().isWorking())
                .limit(limit)
                .toList();
    }

    @Override
    public BigDecimal openSellQuantity(String symbol) {
        return rows.values().stream()
                .filter(o -> o.symbol().equals(symbol) && o.brokerSide() == BrokerSide.SELL && !o.status().isTerminal())
                .map(o -> o.quantity().value().subtract(o.filledQuantity().value()))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    @Override
    public Map<OrderStatus, Long> countByStatus() {
        Map<OrderStatus, Long> counts = new EnumMap<>(OrderStatus.class);
        rows.values().forEach(o -> counts.merge(o.status(), 1L, Long::sum));
        return counts;
    }

    private static Order copy(Order o, long version) {
        return new Order(o.id(), o.clientOrderId(), o.brokerOrderId(), o.accountId(), o.conid(), o.symbol(), o.intent(),
                o.brokerSide(), o.orderType(), o.quantity(), o.filledQuantity(), o.limitPrice(), o.averageFillPrice(),
                o.timeInForce(), o.status(), o.replyId(), o.replyMessage(), o.replyDepth(), o.rejectionReason(),
                o.createdAt(), o.submittedAt(), o.updatedAt(), version);
    }
}
