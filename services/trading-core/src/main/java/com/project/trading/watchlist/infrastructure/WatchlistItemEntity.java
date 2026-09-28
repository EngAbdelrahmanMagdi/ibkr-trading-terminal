package com.project.trading.watchlist.infrastructure;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "watchlist_items")
public class WatchlistItemEntity {

    @Embeddable
    public record Key(@Column(name = "watchlist_id") UUID watchlistId,
                      @Column(name = "symbol", length = 12) String symbol) implements Serializable {
    }

    @EmbeddedId
    private Key key;

    @Column(name = "position", nullable = false)
    private int position;

    @Column(name = "added_at", nullable = false)
    private Instant addedAt;

    protected WatchlistItemEntity() {
    }

    public WatchlistItemEntity(Key key, int position, Instant addedAt) {
        this.key = key;
        this.position = position;
        this.addedAt = addedAt;
    }

    public Key getKey() {
        return key;
    }

    public int getPosition() {
        return position;
    }
}
