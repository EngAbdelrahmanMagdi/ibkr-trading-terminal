package com.project.trading.order.domain;

import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.instrument.domain.ShortabilityStatus;
import com.project.trading.shared.domain.DomainException;
import com.project.trading.shared.domain.Quantity;

import java.math.BigDecimal;

/** Intent, side and position rules for new orders. */
public final class OrderPolicy {

    private OrderPolicy() {
    }

    /** BUY is a BUY; SELL and SHORT are SELLs at the broker. */
    public static BrokerSide brokerSide(OrderIntent intent) {
        return intent == OrderIntent.BUY ? BrokerSide.BUY : BrokerSide.SELL;
    }

    /**
     * Position rules: SELL may only reduce a long position (at most the long quantity); SHORT opens or adds to a
     * short and requires no long position (sell the long first). BUY is always allowed (it covers a short).
     */
    public static void checkPosition(OrderIntent intent, Quantity quantity, BigDecimal signedPosition) {
        switch (intent) {
            case SELL -> {
                if (signedPosition.signum() <= 0 || quantity.value().compareTo(signedPosition) > 0) {
                    throw DomainException.invalidField("quantity",
                            "SELL quantity exceeds the long position; use SHORT to open a short position");
                }
            }
            case SHORT -> {
                if (signedPosition.signum() > 0) {
                    throw DomainException.invalid("a long position is held; SELL it before opening a short position");
                }
            }
            case BUY -> {
                // Always allowed.
            }
        }
    }

    /** Shortability policy: NOT_SHORTABLE blocks a SHORT; UNAVAILABLE (including stale data) does not. */
    public static void checkShortability(OrderIntent intent, ShortabilityStatus effectiveStatus) {
        if (intent == OrderIntent.SHORT && effectiveStatus == ShortabilityStatus.NOT_SHORTABLE) {
            throw DomainException.invalid("instrument is not shortable per broker data");
        }
    }
}
