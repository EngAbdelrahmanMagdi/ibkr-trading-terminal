package com.project.trading.broker.domain;

/** Receives broker order updates (implemented by the trading application; one code path for every broker). */
public interface BrokerOrderUpdateHandler {

    /** Result of handling an update. */
    enum Outcome {
        APPLIED,
        /** A duplicate or out-of-date update. */
        IGNORED,
        /** The order is not known locally or not acknowledged yet; deliver the update again later (bounded). */
        NOT_READY
    }

    Outcome handle(BrokerOrderUpdate update);
}
