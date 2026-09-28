package com.project.trading.broker.domain;

import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A broker-neutral order update: a fill, a confirmed cancellation, a rejection of a working order, or an observation
 * of the broker's order state. An update identifies its order by broker order ID and, when the broker echoes it, by
 * the client order reference (which also locates an order whose broker order ID was never received).
 */
public sealed interface BrokerOrderUpdate {

    String brokerOrderId();

    /** The client order reference sent with the order, or null when the broker did not echo it. */
    default String clientOrderRef() {
        return null;
    }

    /**
     * One execution; brokerExecutionId is unique per execution. commission may be null. side may be null when the
     * source does not report it; the order's broker side is used then.
     */
    record Fill(String brokerOrderId, String brokerExecutionId, BrokerSide side, Quantity quantity, Price price,
                BigDecimal commission, String currency, Instant executedAt, String clientOrderRef)
            implements BrokerOrderUpdate {

        public Fill(String brokerOrderId, String brokerExecutionId, BrokerSide side, Quantity quantity, Price price,
                    BigDecimal commission, String currency, Instant executedAt) {
            this(brokerOrderId, brokerExecutionId, side, quantity, price, commission, currency, executedAt, null);
        }
    }

    record Cancelled(String brokerOrderId, Instant at) implements BrokerOrderUpdate {
    }

    record Rejected(String brokerOrderId, String reason, Instant at) implements BrokerOrderUpdate {
    }

    /**
     * The broker's view of the order at a point in time (from the order stream or reconciliation). filledQuantity is
     * the broker's cumulative filled quantity; fills themselves arrive as {@link Fill}. detail is the broker's own
     * status label.
     */
    record Observed(String brokerOrderId, String clientOrderRef, ObservedStatus status, Quantity filledQuantity,
                    String detail, Instant at) implements BrokerOrderUpdate {
    }
}
