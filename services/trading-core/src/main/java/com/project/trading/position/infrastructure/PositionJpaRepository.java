package com.project.trading.position.infrastructure;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface PositionJpaRepository extends JpaRepository<PositionEntity, String> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "3000"))
    @Query("select p from PositionEntity p where p.symbol = :symbol")
    Optional<PositionEntity> lockBySymbol(@Param("symbol") String symbol);

    List<PositionEntity> findAllByOrderBySymbolAsc();
}
