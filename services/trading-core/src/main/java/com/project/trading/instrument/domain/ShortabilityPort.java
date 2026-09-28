package com.project.trading.instrument.domain;

/** Broker shortability data. Implementations return UNAVAILABLE on missing data or failure; never throw. */
public interface ShortabilityPort {

    Shortability shortability(Instrument instrument);
}
