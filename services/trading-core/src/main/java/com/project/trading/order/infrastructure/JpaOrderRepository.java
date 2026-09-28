package com.project.trading.order.infrastructure;

import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.order.application.OrderEventFactory;
import com.project.trading.order.domain.Order;
import com.project.trading.order.domain.OrderIntent;
import com.project.trading.order.domain.OrderRepository;
import com.project.trading.order.domain.OrderStatus;
import com.project.trading.outbox.application.OutboxAppender;
import com.project.trading.shared.domain.OrderType;
import com.project.trading.shared.domain.TimeInForce;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import jakarta.persistence.EntityManager;
import org.springframework.data.domain.PageRequest;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * JPA adapter of the order repository port. Saving an order also records one event per status change in the outbox,
 * in the same transaction, so that no committed change is ever left without its event (and no event without it).
 */
@Repository
class JpaOrderRepository implements OrderRepository {

    private final OrderJpaRepository jpa;
    private final EntityManager entityManager;
    private final OrderEventFactory events;
    private final OutboxAppender outbox;

    JpaOrderRepository(OrderJpaRepository jpa, EntityManager entityManager, OrderEventFactory events,
                       OutboxAppender outbox) {
        this.jpa = jpa;
        this.entityManager = entityManager;
        this.events = events;
        this.outbox = outbox;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public Order save(Order order) {
        OrderEntity entity = jpa.findById(order.id()).orElse(null);
        if (entity == null) {
            entity = new OrderEntity(order.id());
            copy(order, entity);
            entityManager.persist(entity);
        } else {
            if (entity.version != order.version()) {
                throw new ObjectOptimisticLockingFailureException(OrderEntity.class, order.id());
            }
            copy(order, entity);
        }
        entityManager.flush();
        for (Order.StatusChange change : order.drainStatusChanges()) {
            outbox.append(events.orderEvent(order, change));
        }
        return toDomain(entity);
    }

    @Override
    public Optional<Order> findById(UUID id) {
        return jpa.findById(id).map(JpaOrderRepository::toDomain);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<Order> lockById(UUID id) {
        return jpa.lockById(id).map(JpaOrderRepository::toDomain);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<Order> lockByBrokerOrderId(String brokerOrderId) {
        return jpa.lockByBrokerOrderId(brokerOrderId).map(JpaOrderRepository::toDomain);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<Order> lockByClientOrderId(String clientOrderId) {
        return jpa.lockByClientOrderId(clientOrderId).map(JpaOrderRepository::toDomain);
    }

    @Override
    public Optional<Order> findByClientOrderId(String clientOrderId) {
        return jpa.findByClientOrderId(clientOrderId).map(JpaOrderRepository::toDomain);
    }

    @Override
    public Optional<Order> findByBrokerOrderId(String brokerOrderId) {
        return jpa.findByBrokerOrderId(brokerOrderId).map(JpaOrderRepository::toDomain);
    }

    @Override
    public List<Order> findRecent(Collection<OrderStatus> statuses, int limit) {
        PageRequest page = PageRequest.of(0, limit);
        List<OrderEntity> rows = statuses.isEmpty() ? jpa.findAllByOrderByCreatedAtDescIdDesc(page)
                : jpa.findByStatusInOrderByCreatedAtDescIdDesc(statuses.stream().map(Enum::name).toList(), page);
        return rows.stream().map(JpaOrderRepository::toDomain).toList();
    }

    @Override
    public boolean existsWithStatus(OrderStatus status) {
        return jpa.existsByStatus(status.name());
    }

    @Override
    public List<Order> findWorkingLimitOrders(int limit) {
        return jpa.findWorkingLimitOrders(PageRequest.of(0, limit)).stream().map(JpaOrderRepository::toDomain).toList();
    }

    @Override
    public List<Order> findWorkingOrders(int limit) {
        return jpa.findWorkingOrders(PageRequest.of(0, limit)).stream().map(JpaOrderRepository::toDomain).toList();
    }

    @Override
    public BigDecimal openSellQuantity(String symbol) {
        return jpa.openSellQuantity(symbol);
    }

    @Override
    public Map<OrderStatus, Long> countByStatus() {
        Map<OrderStatus, Long> counts = new EnumMap<>(OrderStatus.class);
        for (OrderStatus s : OrderStatus.values()) {
            counts.put(s, 0L);
        }
        for (Object[] row : jpa.countByStatus()) {
            counts.put(OrderStatus.valueOf((String) row[0]), (Long) row[1]);
        }
        return counts;
    }

    private static void copy(Order o, OrderEntity e) {
        e.clientOrderId = o.clientOrderId();
        e.brokerOrderId = o.brokerOrderId();
        e.accountId = o.accountId();
        e.conid = o.conid();
        e.symbol = o.symbol();
        e.intent = o.intent().name();
        e.brokerSide = o.brokerSide().name();
        e.orderType = o.orderType().name();
        e.quantity = o.quantity().value();
        e.filledQuantity = o.filledQuantity().value();
        e.limitPrice = o.limitPrice() == null ? null : o.limitPrice().value();
        e.averageFillPrice = o.averageFillPrice() == null ? null : o.averageFillPrice().value();
        e.timeInForce = o.timeInForce().name();
        e.status = o.status().name();
        e.replyId = o.replyId();
        e.replyMessage = o.replyMessage();
        e.replyDepth = o.replyDepth();
        e.rejectionReason = o.rejectionReason();
        e.createdAt = o.createdAt();
        e.submittedAt = o.submittedAt();
        e.updatedAt = o.updatedAt();
    }

    private static Order toDomain(OrderEntity e) {
        return new Order(e.id, e.clientOrderId, e.brokerOrderId, e.accountId, e.conid, e.symbol,
                OrderIntent.valueOf(e.intent), BrokerSide.valueOf(e.brokerSide), OrderType.valueOf(e.orderType),
                new Quantity(e.quantity), new Quantity(e.filledQuantity),
                e.limitPrice == null ? null : new Price(e.limitPrice),
                e.averageFillPrice == null ? null : new Price(e.averageFillPrice),
                TimeInForce.valueOf(e.timeInForce), OrderStatus.valueOf(e.status), e.replyId, e.replyMessage,
                e.replyDepth, e.rejectionReason, e.createdAt, e.submittedAt, e.updatedAt, e.version);
    }
}
