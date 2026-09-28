package com.project.trading.order.infrastructure;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

interface OrderJpaRepository extends JpaRepository<OrderEntity, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "3000"))
    @Query("select o from OrderEntity o where o.id = :id")
    Optional<OrderEntity> lockById(@Param("id") UUID id);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "3000"))
    @Query("select o from OrderEntity o where o.brokerOrderId = :brokerOrderId")
    Optional<OrderEntity> lockByBrokerOrderId(@Param("brokerOrderId") String brokerOrderId);

    List<OrderEntity> findAllByOrderByCreatedAtDescIdDesc(Pageable page);

    List<OrderEntity> findByStatusInOrderByCreatedAtDescIdDesc(Collection<String> statuses, Pageable page);

    boolean existsByStatus(String status);

    @Query("select o from OrderEntity o where o.orderType = 'LIMIT' and o.brokerOrderId is not null"
            + " and o.status in ('SUBMITTED', 'PARTIALLY_FILLED', 'CANCEL_PENDING') order by o.createdAt asc, o.id asc")
    List<OrderEntity> findWorkingLimitOrders(Pageable page);

    @Query("select o from OrderEntity o where o.brokerOrderId is not null"
            + " and o.status in ('SUBMITTED', 'PARTIALLY_FILLED', 'CANCEL_PENDING') order by o.createdAt asc, o.id asc")
    List<OrderEntity> findWorkingOrders(Pageable page);

    @Query("select coalesce(sum(o.quantity - o.filledQuantity), 0) from OrderEntity o where o.symbol = :symbol"
            + " and o.brokerSide = 'SELL' and o.status not in ('FILLED', 'CANCELLED', 'REJECTED', 'FAILED')")
    BigDecimal openSellQuantity(@Param("symbol") String symbol);

    @Query("select o.status, count(o) from OrderEntity o group by o.status")
    List<Object[]> countByStatus();
}
