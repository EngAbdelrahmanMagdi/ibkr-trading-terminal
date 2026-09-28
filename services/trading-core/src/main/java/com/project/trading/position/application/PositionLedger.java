package com.project.trading.position.application;

import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.position.domain.Position;
import com.project.trading.position.infrastructure.PositionEntity;
import com.project.trading.position.infrastructure.PositionJpaRepository;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** The position read model, updated in the same transaction as each execution. */
@Service
public class PositionLedger {

    private final PositionJpaRepository repository;

    public PositionLedger(PositionJpaRepository repository) {
        this.repository = repository;
    }

    /** Applies a fill to the symbol's position (row-locked); must run inside the execution's transaction. */
    @Transactional(propagation = Propagation.MANDATORY)
    public Position applyFill(String symbol, String currency, BrokerSide side, Quantity quantity, Price price, Instant at) {
        PositionEntity entity = repository.lockBySymbol(symbol).orElseGet(() -> new PositionEntity(symbol, currency));
        Position next = toDomain(entity).apply(side, quantity, price);
        entity.update(next.quantity(), next.averageCost(), next.realizedPnl(), at);
        repository.save(entity);
        return next;
    }

    /** Signed quantity held (zero when there is no position). */
    @Transactional(readOnly = true)
    public BigDecimal signedQuantity(String symbol) {
        return repository.findById(symbol).map(PositionEntity::getQuantity).orElse(BigDecimal.ZERO);
    }

    /** All positions, including flat ones that carry realized P&L. */
    @Transactional(readOnly = true)
    public List<Position> all() {
        return repository.findAllByOrderBySymbolAsc().stream().map(PositionLedger::toDomain).toList();
    }

    private static Position toDomain(PositionEntity e) {
        return new Position(e.getSymbol(), e.getQuantity(), e.getAverageCost(), e.getRealizedPnl(), e.getCurrency());
    }
}
