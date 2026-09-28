package com.project.trading.instrument.domain;

import java.util.List;
import java.util.Optional;

/** The broker's instrument catalog (symbol to broker contract). Implemented by the broker adapter. */
public interface InstrumentCatalogPort {

    Optional<Instrument> resolve(String symbol);

    /** Instruments whose symbol or name matches the query, best matches first. */
    List<Instrument> search(String query, int limit);
}
