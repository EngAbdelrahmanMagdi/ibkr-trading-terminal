package com.project.trading.broker.infrastructure.mock;

import com.project.trading.instrument.domain.Shortability;
import com.project.trading.instrument.domain.ShortabilityPort;
import com.project.trading.instrument.domain.ShortabilityStatus;

import java.time.Clock;
import java.util.Map;

/**
 * Configured shortability of the simulated broker. Only the status is simulated: availability and borrow fees
 * are not reported (null), so nothing is invented.
 */
public class MockShortability implements ShortabilityPort {

    private final Map<String, ShortabilityStatus> configured;
    private final Clock clock;

    public MockShortability(Map<String, ShortabilityStatus> configured, Clock clock) {
        this.configured = Map.copyOf(configured);
        this.clock = clock;
    }

    @Override
    public Shortability shortability(String symbol) {
        ShortabilityStatus status = configured.getOrDefault(symbol, ShortabilityStatus.SHORTABLE);
        if (status == ShortabilityStatus.UNAVAILABLE) {
            return Shortability.unavailable();
        }
        return new Shortability(status, null, null, clock.instant());
    }
}
