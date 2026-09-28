package com.project.trading.order.infrastructure;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.UUID;

public interface IdempotencyJpaRepository extends JpaRepository<IdempotencyRecordEntity, UUID> {

    /** Claims a key; returns 0 when it already exists (the primary key arbitrates concurrent claims). */
    @Modifying
    @Query(value = "INSERT INTO idempotency_records (idempotency_key, request_fingerprint, created_at)"
            + " VALUES (:key, :fingerprint, :now) ON CONFLICT (idempotency_key) DO NOTHING", nativeQuery = true)
    int insertIfAbsent(@Param("key") UUID key, @Param("fingerprint") String fingerprint, @Param("now") Instant now);

    /**
     * Takes over an abandoned claim of the same request: one that never created an order (so nothing was sent
     * to the broker) and is older than staleBefore. Returns 1 when taken over.
     */
    @Modifying
    @Query(value = "UPDATE idempotency_records SET created_at = :now WHERE idempotency_key = :key"
            + " AND request_fingerprint = :fingerprint AND order_id IS NULL AND completed_at IS NULL"
            + " AND created_at < :staleBefore", nativeQuery = true)
    int takeOverStale(@Param("key") UUID key, @Param("fingerprint") String fingerprint, @Param("now") Instant now,
                      @Param("staleBefore") Instant staleBefore);
}
