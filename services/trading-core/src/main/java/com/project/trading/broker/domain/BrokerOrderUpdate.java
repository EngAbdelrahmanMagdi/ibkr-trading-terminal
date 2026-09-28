package com.project.trading.broker.domain;

import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;

import java.math.BigDecimal;
import java.time.Instant;

/** A broker-neutral order update (a fill, a confirmed cancellation or a rejection of a working order). */
public sealed interface BrokerOrderUpdate {

    String brokerOrderId();

    /** One execution; brokerExecutionId is unique per execution. commission may be null. */
    record Fill(String brokerOrderId, String brokerExecutionId, BrokerSide side, Quantity quantity, Price price,
                BigDecimal commission, String currency, Instant executedAt) implements BrokerOrderUpdate {
    }

    record Cancelled(String brokerOrderId, Instant at) implements BrokerOrderUpdate {
    }

    record Rejected(String brokerOrderId, String reason, Instant at) implements BrokerOrderUpdate {
    }
}
