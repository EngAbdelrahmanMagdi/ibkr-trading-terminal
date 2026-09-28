package com.project.trading.execution.infrastructure;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public interface ExecutionJpaRepository extends JpaRepository<ExecutionEntity, UUID> {

    boolean existsByBrokerExecutionId(String brokerExecutionId);

    List<ExecutionEntity> findByOrderIdOrderByExecutedAtDescIdDesc(UUID orderId, Pageable page);

    List<ExecutionEntity> findAllByOrderByExecutedAtDescIdDesc(Pageable page);

    /** Net cash flow of all executions: sales add, purchases subtract, commissions subtract. */
    @Query("select coalesce(sum(case when e.side = 'SELL' then e.quantity * e.price else -(e.quantity * e.price) end"
            + " - coalesce(e.commission, 0)), 0) from ExecutionEntity e")
    BigDecimal netCashFlow();
}
