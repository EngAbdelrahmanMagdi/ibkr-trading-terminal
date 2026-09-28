package com.project.trading.broker.domain;

/** Outcome of a cancel request: Requested only means the broker received it, not that the order is cancelled. */
public sealed interface CancelResult {

    record Requested() implements CancelResult {
    }

    record Rejected(String reason) implements CancelResult {
    }

    record Unknown(String reason) implements CancelResult {
    }
}
