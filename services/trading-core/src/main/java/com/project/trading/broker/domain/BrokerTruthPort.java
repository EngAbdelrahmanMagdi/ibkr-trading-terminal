package com.project.trading.broker.domain;

import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** The broker's current view of orders, executions and positions, for reconciliation. Never throws. */
public interface BrokerTruthPort {

    /** An order as the broker reports it; clientOrderRef is null when the broker has none (for example an external order). */
    record OrderView(String brokerOrderId, String clientOrderRef, ObservedStatus status, Quantity filledQuantity,
                     String detail) {
    }

    /** An execution as the broker reports it; brokerOrderId may be null when the source does not report it. */
    record ExecutionView(String brokerExecutionId, String brokerOrderId, String clientOrderRef, Quantity quantity,
                         Price price, BigDecimal commission, Instant executedAt) {
    }

    /**
     * A snapshot. orders covers the broker's current-day orders, executions its recent executions. positionsByConid
     * is null when positions could not be read (then positions are not compared).
     */
    record Snapshot(List<OrderView> orders, List<ExecutionView> executions, Map<Long, BigDecimal> positionsByConid,
                    Instant asOf) {
    }

    /** The current snapshot, or empty when the broker is unavailable or reconciliation is not supported. */
    Optional<Snapshot> snapshot();
}
