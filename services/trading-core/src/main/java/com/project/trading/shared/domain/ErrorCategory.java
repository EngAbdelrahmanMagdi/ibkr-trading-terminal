package com.project.trading.shared.domain;

/** Stable, machine-readable error categories of the REST error model. */
public enum ErrorCategory {
    VALIDATION,
    INSTRUMENT_NOT_FOUND,
    CONFLICT,
    RATE_LIMITED,
    STALE_MARKET_DATA,
    BROKER_REJECTED,
    BROKER_UNAVAILABLE,
    SERVICE_UNAVAILABLE,
    INTERNAL
}
