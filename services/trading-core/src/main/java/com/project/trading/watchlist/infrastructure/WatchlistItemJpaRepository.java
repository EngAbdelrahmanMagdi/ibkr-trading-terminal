package com.project.trading.watchlist.infrastructure;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface WatchlistItemJpaRepository extends JpaRepository<WatchlistItemEntity, WatchlistItemEntity.Key> {

    @Query("select i from WatchlistItemEntity i where i.key.watchlistId = :id order by i.position asc, i.key.symbol asc")
    List<WatchlistItemEntity> findItems(@Param("id") UUID watchlistId);

    @Query("select coalesce(max(i.position), -1) from WatchlistItemEntity i where i.key.watchlistId = :id")
    int maxPosition(@Param("id") UUID watchlistId);

    @Query("select count(i) from WatchlistItemEntity i where i.key.watchlistId = :id")
    long countItems(@Param("id") UUID watchlistId);
}
