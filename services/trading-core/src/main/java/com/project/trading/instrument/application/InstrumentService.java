package com.project.trading.instrument.application;

import com.project.trading.instrument.domain.Instrument;
import com.project.trading.instrument.domain.InstrumentCatalogPort;
import com.project.trading.instrument.domain.Shortability;
import com.project.trading.instrument.domain.ShortabilityPort;
import com.project.trading.instrument.infrastructure.InstrumentEntity;
import com.project.trading.instrument.infrastructure.InstrumentJpaRepository;
import com.project.trading.shared.config.AppProperties;
import com.project.trading.shared.domain.DomainException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Locale;

/**
 * Instrument resolution and search. A symbol is resolved through the broker catalog once and then persisted,
 * so later lookups are local. The catalog is never called inside a database transaction.
 */
@Service
public class InstrumentService {

    /** An instrument with its effective shortability. */
    public record InstrumentDetails(Instrument instrument, Shortability shortability) {
    }

    private final InstrumentJpaRepository repository;
    private final InstrumentCatalogPort catalog;
    private final ShortabilityPort shortabilityPort;
    private final Clock clock;
    private final Duration shortabilityMaxAge;

    public InstrumentService(InstrumentJpaRepository repository, InstrumentCatalogPort catalog,
                             ShortabilityPort shortabilityPort, Clock clock, AppProperties properties) {
        this.repository = repository;
        this.catalog = catalog;
        this.shortabilityPort = shortabilityPort;
        this.clock = clock;
        this.shortabilityMaxAge = properties.orders().shortabilityMaxAge();
    }

    /** Resolves a symbol, or throws INSTRUMENT_NOT_FOUND. */
    public Instrument resolve(String symbol) {
        return repository.findById(symbol).map(InstrumentService::toDomain).orElseGet(() -> {
            Instrument instrument = catalog.resolve(symbol).orElseThrow(() -> DomainException.instrumentNotFound(symbol));
            try {
                repository.saveAndFlush(new InstrumentEntity(instrument.symbol(), instrument.conid(), instrument.name(),
                        instrument.exchange(), instrument.currency(), instrument.assetType(), instrument.priceScale(),
                        clock.instant()));
            } catch (DataIntegrityViolationException concurrent) {
                // Resolved concurrently by another request: the stored row is equivalent.
            }
            return instrument;
        });
    }

    /**
     * Shortability with stale or undated data downgraded to UNAVAILABLE. Unavailable data carries no details:
     * nothing is invented.
     */
    public Shortability shortability(String symbol) {
        Shortability raw = shortabilityPort.shortability(symbol);
        if (raw.effectiveStatus(clock.instant(), shortabilityMaxAge) != raw.status()) {
            return Shortability.unavailable();
        }
        return raw;
    }

    public List<InstrumentDetails> search(String query, int limit) {
        return catalog.search(query.trim().toUpperCase(Locale.ROOT), limit).stream()
                .map(i -> new InstrumentDetails(i, shortability(i.symbol())))
                .toList();
    }

    private static Instrument toDomain(InstrumentEntity e) {
        return new Instrument(e.getSymbol(), e.getConid(), e.getName(), e.getExchange(), e.getCurrency(),
                e.getAssetType(), e.getPriceScale());
    }
}
