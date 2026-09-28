package com.project.trading.broker.domain;

/** Receives broker order updates (implemented by the trading application; one code path for every broker). */
public interface BrokerOrderUpdateHandler {

    /** Result of handling an update. */
    enum Outcome {
        APPLIED,
        /** A duplicate or out-of-date update. */
        IGNORED,
        /** The order is not acknowledged locally yet; deliver the update again later (bounded). */
        NOT_READY,
        /** No local order matches: an order placed outside this application. Logged and counted, never imported. */
        UNKNOWN_ORDER
    }

    Outcome handle(BrokerOrderUpdate update);
}
