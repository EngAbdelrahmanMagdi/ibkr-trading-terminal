package com.project.trading.order.infrastructure;

import com.project.trading.order.application.IdempotencyStore;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Repository
class JpaIdempotencyStore implements IdempotencyStore {

    private final IdempotencyJpaRepository jpa;

    JpaIdempotencyStore(IdempotencyJpaRepository jpa) {
        this.jpa = jpa;
    }

    @Override
    @Transactional
    public boolean insertIfAbsent(UUID key, String fingerprint, Instant now) {
        return jpa.insertIfAbsent(key, fingerprint, now) == 1;
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<Entry> find(UUID key) {
        return jpa.findById(key).map(r -> new Entry(r.getKey(), r.getFingerprint(), r.getOrderId(),
                r.getResponseStatus(), r.getProblemBody(), r.getCompletedAt()));
    }

    @Override
    @Transactional
    public boolean takeOverStale(UUID key, String fingerprint, Instant now, Instant staleBefore) {
        return jpa.takeOverStale(key, fingerprint, now, staleBefore) == 1;
    }

    @Override
    @Transactional
    public void linkOrder(UUID key, UUID orderId) {
        jpa.findById(key).orElseThrow(() -> new IllegalStateException("idempotency record missing")).linkOrder(orderId);
    }

    @Override
    @Transactional
    public void complete(UUID key, int responseStatus, String problemBody, Instant at) {
        jpa.findById(key).orElseThrow(() -> new IllegalStateException("idempotency record missing"))
                .complete(responseStatus, problemBody, at);
    }
}
