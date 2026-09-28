package com.project.trading.broker.domain;

import java.util.List;

/** Outcome of submitting an order or answering a broker confirmation request. */
public sealed interface SubmitResult {

    /** The broker accepted the order as working. immediateUpdates are updates the broker returned at once. */
    record Accepted(String brokerOrderId, List<BrokerOrderUpdate> immediateUpdates) implements SubmitResult {
        public Accepted {
            immediateUpdates = List.copyOf(immediateUpdates);
        }
    }

    /** The broker asks for an explicit confirmation before accepting the order. */
    record ConfirmationRequired(String replyId, String message) implements SubmitResult {
    }

    /** The broker rejected the order (reason is sanitized, safe to show). */
    record Rejected(String reason) implements SubmitResult {
    }

    /** The request definitely did not reach the broker (nothing to reconcile). */
    record Failed(String reason) implements SubmitResult {
    }

    /** The outcome is unknown (for example a timeout after sending). Never resubmitted. */
    record Unknown(String reason) implements SubmitResult {
    }
}
