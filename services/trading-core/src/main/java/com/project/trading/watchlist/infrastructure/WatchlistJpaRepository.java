package com.project.trading.watchlist.infrastructure;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface WatchlistJpaRepository extends JpaRepository<WatchlistEntity, UUID> {

    Optional<WatchlistEntity> findByName(String name);

    /** Serializes watchlist changes (position assignment) on the watchlist row. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select w from WatchlistEntity w where w.name = :name")
    Optional<WatchlistEntity> lockByName(@Param("name") String name);
}
