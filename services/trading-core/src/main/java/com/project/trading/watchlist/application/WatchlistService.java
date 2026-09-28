package com.project.trading.watchlist.application;

import com.project.trading.instrument.application.InstrumentService;
import com.project.trading.shared.config.AppProperties;
import com.project.trading.shared.domain.DomainException;
import com.project.trading.watchlist.infrastructure.WatchlistEntity;
import com.project.trading.watchlist.infrastructure.WatchlistItemEntity;
import com.project.trading.watchlist.infrastructure.WatchlistItemJpaRepository;
import com.project.trading.watchlist.infrastructure.WatchlistJpaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.util.List;
import java.util.UUID;

/**
 * The single persisted watchlist. Items keep a stable position (appended at the end; removal does not renumber).
 * Symbols are resolved through the instrument catalog before any transaction starts.
 */
@Service
public class WatchlistService {

    private static final Logger log = LoggerFactory.getLogger(WatchlistService.class);
    static final String NAME = "default";

    public record Item(String symbol, int position) {
    }

    public record Watchlist(UUID id, List<Item> items) {
    }

    private final WatchlistJpaRepository watchlists;
    private final WatchlistItemJpaRepository items;
    private final InstrumentService instruments;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final AppProperties.Watchlist settings;

    public WatchlistService(WatchlistJpaRepository watchlists, WatchlistItemJpaRepository items,
                            InstrumentService instruments, PlatformTransactionManager transactionManager, Clock clock,
                            AppProperties properties) {
        this.watchlists = watchlists;
        this.items = items;
        this.instruments = instruments;
        this.tx = new TransactionTemplate(transactionManager);
        this.clock = clock;
        this.settings = properties.watchlist();
    }

    /** Creates the watchlist with the configured default symbols on first start (idempotent). */
    @EventListener(ApplicationReadyEvent.class)
    public void seed() {
        if (watchlists.findByName(NAME).isPresent()) {
            return;
        }
        ensureWatchlist();
        for (String symbol : settings.defaultSymbols()) {
            try {
                add(symbol);
            } catch (DomainException e) {
                log.warn("default watchlist symbol skipped: {} ({})", symbol, e.getMessage());
            }
        }
        log.info("watchlist seeded with {} default symbols", settings.defaultSymbols().size());
    }

    public Watchlist get() {
        UUID id = ensureWatchlist();
        return tx.execute(status -> read(id));
    }

    public Watchlist add(String symbol) {
        instruments.resolve(symbol);
        UUID id = ensureWatchlist();
        try {
            return tx.execute(status -> {
                watchlists.lockByName(NAME);
                WatchlistItemEntity.Key key = new WatchlistItemEntity.Key(id, symbol);
                if (items.existsById(key)) {
                    throw DomainException.conflict(symbol + " is already in the watchlist");
                }
                if (items.countItems(id) >= settings.maxItems()) {
                    throw DomainException.invalid("the watchlist is full (at most " + settings.maxItems() + " symbols)");
                }
                items.save(new WatchlistItemEntity(key, items.maxPosition(id) + 1, clock.instant()));
                items.flush();
                return read(id);
            });
        } catch (DataIntegrityViolationException e) {
            throw DomainException.conflict(symbol + " is already in the watchlist");
        }
    }

    public void remove(String symbol) {
        UUID id = ensureWatchlist();
        tx.executeWithoutResult(status -> {
            watchlists.lockByName(NAME);
            WatchlistItemEntity.Key key = new WatchlistItemEntity.Key(id, symbol);
            if (!items.existsById(key)) {
                throw DomainException.notFound("watchlist item " + symbol);
            }
            items.deleteById(key);
        });
    }

    private Watchlist read(UUID id) {
        List<Item> list = items.findItems(id).stream()
                .map(i -> new Item(i.getKey().symbol(), i.getPosition()))
                .toList();
        return new Watchlist(id, list);
    }

    private UUID ensureWatchlist() {
        return watchlists.findByName(NAME).map(WatchlistEntity::getId).orElseGet(() -> {
            try {
                return watchlists.saveAndFlush(new WatchlistEntity(UUID.randomUUID(), NAME, clock.instant())).getId();
            } catch (DataIntegrityViolationException concurrent) {
                return watchlists.findByName(NAME).orElseThrow().getId();
            }
        });
    }
}
