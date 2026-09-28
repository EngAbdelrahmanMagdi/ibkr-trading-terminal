package com.project.trading.broker.domain;

/** Account metrics from the broker. Never throws: unavailable metrics are null. */
public interface BrokerAccountPort {

    AccountMetrics metrics();
}
