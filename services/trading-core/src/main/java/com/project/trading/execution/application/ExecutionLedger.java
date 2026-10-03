package com.project.trading.execution.application;

import com.project.trading.execution.domain.Execution;
import com.project.trading.execution.infrastructure.ExecutionEntity;
import com.project.trading.execution.infrastructure.ExecutionJpaRepository;
import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.shared.domain.Decimals;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

/** The execution record: each broker execution is stored once. */
@Service
public class ExecutionLedger {

    private final ExecutionJpaRepository repository;

    public ExecutionLedger(ExecutionJpaRepository repository) {
        this.repository = repository;
    }

    @Transactional(readOnly = true)
    public boolean isRecorded(String brokerExecutionId) {
        return repository.existsByBrokerExecutionId(brokerExecutionId);
    }

    /** Records an execution; must run inside the caller's transaction (with the order and position update). */
    @Transactional(propagation = Propagation.MANDATORY)
    public void record(Execution e) {
        repository.save(new ExecutionEntity(e.id(), e.orderId(), e.brokerExecutionId(), e.symbol(), e.side().name(),
                e.quantity().value(), e.price().value(),
                e.commission() == null ? null : Decimals.money(e.commission()), e.currency(), e.executedAt()));
    }

    @Transactional(readOnly = true)
    public List<Execution> list(UUID orderId, int limit) {
        PageRequest page = PageRequest.of(0, limit);
        List<ExecutionEntity> rows = orderId == null ? repository.findAllByOrderByExecutedAtDescIdDesc(page)
                : repository.findByOrderIdOrderByExecutedAtDescIdDesc(orderId, page);
        return rows.stream().map(ExecutionLedger::toDomain).toList();
    }

    /** Net cash flow from all executions (money scale). */
    @Transactional(readOnly = true)
    public BigDecimal netCashFlow() {
        return Decimals.money(repository.netCashFlow());
    }

    @Transactional(readOnly = true)
    public BigDecimal netCashFlowBetween(Instant start, Instant end) {
        return Decimals.money(repository.netCashFlowBetween(start, end));
    }

    /** Full persisted ledger, independent of the paginated browser execution list. */
    @Transactional(readOnly = true)
    public Map<String, BigDecimal> openingQuantities(Instant start) {
        return repository.openingQuantities(start).stream().collect(Collectors.toMap(
                ExecutionJpaRepository.OpeningQuantity::getSymbol, ExecutionJpaRepository.OpeningQuantity::getQuantity));
    }

    private static Execution toDomain(ExecutionEntity e) {
        return new Execution(e.getId(), e.getOrderId(), e.getBrokerExecutionId(), e.getSymbol(),
                BrokerSide.valueOf(e.getSide()), new Quantity(e.getQuantity()), new Price(e.getPrice()),
                e.getCommission(), e.getCurrency(), e.getExecutedAt());
    }
}
