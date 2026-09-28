package com.project.trading.instrument.domain;

/** Broker-reported shortability. UNAVAILABLE covers missing, failed and stale data; nothing is invented. */
public enum ShortabilityStatus {
    SHORTABLE,
    NOT_SHORTABLE,
    UNAVAILABLE
}
