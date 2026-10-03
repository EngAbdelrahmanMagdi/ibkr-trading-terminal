package com.project.trading.execution.infrastructure;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.time.Instant;

public interface ExecutionJpaRepository extends JpaRepository<ExecutionEntity, UUID> {

    boolean existsByBrokerExecutionId(String brokerExecutionId);

    List<ExecutionEntity> findByOrderIdOrderByExecutedAtDescIdDesc(UUID orderId, Pageable page);

    List<ExecutionEntity> findAllByOrderByExecutedAtDescIdDesc(Pageable page);

    /** Net cash flow of all executions: sales add, purchases subtract, commissions subtract. */
    @Query("select coalesce(sum(case when e.side = 'SELL' then e.quantity * e.price else -(e.quantity * e.price) end"
            + " - coalesce(e.commission, 0)), 0) from ExecutionEntity e")
    BigDecimal netCashFlow();

    @Query("select coalesce(sum(case when e.side = 'SELL' then e.quantity * e.price else -(e.quantity * e.price) end"
            + " - coalesce(e.commission, 0)), 0) from ExecutionEntity e where e.executedAt >= :start and e.executedAt < :end")
    BigDecimal netCashFlowBetween(Instant start, Instant end);

    interface OpeningQuantity {
        String getSymbol();
        BigDecimal getQuantity();
    }

    @Query("select e.symbol as symbol, sum(case when e.side = 'BUY' then e.quantity else -e.quantity end) as quantity"
            + " from ExecutionEntity e where e.executedAt < :start group by e.symbol")
    List<OpeningQuantity> openingQuantities(Instant start);
}
