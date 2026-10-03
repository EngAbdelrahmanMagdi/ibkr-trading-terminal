package com.project.trading.position.application;

import com.project.trading.execution.application.ExecutionLedger;
import com.project.trading.position.domain.OpeningValuation;
import com.project.trading.position.domain.OpeningValuationRepository;
import com.project.trading.position.domain.Position;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/** One consistent ledger snapshot; reference quotes are read after this transaction completes. */
@Service
public class TradingDaySnapshot {
    public record View(List<Position> positions, Map<String, BigDecimal> openingQuantities,
                       Map<String, OpeningValuation> openings, BigDecimal cashFlow) {
    }

    private final PositionLedger positions;
    private final ExecutionLedger executions;
    private final OpeningValuationRepository openings;

    public TradingDaySnapshot(PositionLedger positions, ExecutionLedger executions, OpeningValuationRepository openings) {
        this.positions = positions;
        this.executions = executions;
        this.openings = openings;
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public View read(LocalDate day, Instant start, Instant end) {
        return new View(positions.all(), executions.openingQuantities(start), openings.find(day),
                executions.netCashFlowBetween(start, end));
    }
}
