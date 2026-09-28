package com.project.trading.support;

import com.project.trading.order.application.IdempotencyStore;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryIdempotencyStore implements IdempotencyStore {

    private final Map<UUID, Entry> rows = new ConcurrentHashMap<>();
    private final Map<UUID, Instant> created = new ConcurrentHashMap<>();

    @Override
    public boolean insertIfAbsent(UUID key, String fingerprint, Instant now) {
        boolean inserted = rows.putIfAbsent(key, new Entry(key, fingerprint, null, null, null, null)) == null;
        if (inserted) {
            created.put(key, now);
        }
        return inserted;
    }

    @Override
    public Optional<Entry> find(UUID key) {
        return Optional.ofNullable(rows.get(key));
    }

    @Override
    public synchronized boolean takeOverStale(UUID key, String fingerprint, Instant now, Instant staleBefore) {
        Entry e = rows.get(key);
        if (e != null && e.fingerprint().equals(fingerprint) && e.orderId() == null && e.completedAt() == null
                && created.get(key).isBefore(staleBefore)) {
            created.put(key, now);
            return true;
        }
        return false;
    }

    @Override
    public void linkOrder(UUID key, UUID orderId) {
        rows.computeIfPresent(key, (k, e) -> new Entry(k, e.fingerprint(), orderId, null, null, null));
    }

    @Override
    public void complete(UUID key, int responseStatus, String problemBody, Instant at) {
        rows.computeIfPresent(key, (k, e) -> new Entry(k, e.fingerprint(), e.orderId(), responseStatus, problemBody, at));
    }
}
