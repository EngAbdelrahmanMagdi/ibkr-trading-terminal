package com.project.trading.broker.domain;

/**
 * The broker, as seen by the trading domain. Implementations: the mock broker (MOCK mode) and the IBKR adapter.
 * Calls may be slow network calls: callers never hold a database transaction while calling. A submission is
 * never retried automatically.
 */
public interface BrokerTradingPort {

    BrokerConnectionState connectionState();

    SubmitResult submit(BrokerOrderRequest request);

    /** Answers a confirmation request (confirm or decline). The broker may ask for another confirmation. */
    SubmitResult confirmReply(String replyId, boolean confirm);

    CancelResult cancel(String brokerOrderId);
}
