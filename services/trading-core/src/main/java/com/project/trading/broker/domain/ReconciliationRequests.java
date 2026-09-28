package com.project.trading.broker.domain;

/** Asks for a reconciliation run soon (for example after the broker session or the update stream recovered). */
public interface ReconciliationRequests {

    void requestReconciliation(String reason);
}
