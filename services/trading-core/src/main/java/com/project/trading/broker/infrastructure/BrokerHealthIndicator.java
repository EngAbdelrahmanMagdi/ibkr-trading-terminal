package com.project.trading.broker.infrastructure;

import com.project.trading.broker.domain.BrokerConnectionState;
import com.project.trading.broker.domain.BrokerTradingPort;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/** Broker availability, reported separately from readiness (a broker outage must not take the API down). */
@Component("broker")
public class BrokerHealthIndicator implements HealthIndicator {

    private final BrokerTradingPort broker;

    public BrokerHealthIndicator(BrokerTradingPort broker) {
        this.broker = broker;
    }

    @Override
    public Health health() {
        return broker.connectionState() == BrokerConnectionState.READY ? Health.up().build()
                : Health.outOfService().build();
    }
}
