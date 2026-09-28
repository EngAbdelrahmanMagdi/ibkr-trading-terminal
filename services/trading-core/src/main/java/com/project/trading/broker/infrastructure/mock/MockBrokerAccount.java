package com.project.trading.broker.infrastructure.mock;

import com.project.trading.broker.domain.AccountMetrics;
import com.project.trading.broker.domain.BrokerAccountPort;
import com.project.trading.execution.application.ExecutionLedger;

import java.math.BigDecimal;
import java.time.Clock;

/**
 * The simulated cash account: cash is the starting cash plus the net cash flow of all simulated executions, and
 * buying power equals cash (no margin). Net liquidation is left to be derived from cash and marked positions;
 * excess liquidity and day P&L cannot be derived honestly and are unavailable.
 */
public class MockBrokerAccount implements BrokerAccountPort {

    private final ExecutionLedger executions;
    private final BigDecimal startingCash;
    private final Clock clock;

    public MockBrokerAccount(ExecutionLedger executions, BigDecimal startingCash, Clock clock) {
        this.executions = executions;
        this.startingCash = startingCash;
        this.clock = clock;
    }

    @Override
    public AccountMetrics metrics() {
        BigDecimal cash = startingCash.add(executions.netCashFlow());
        return new AccountMetrics(cash, cash, null, null, null, clock.instant());
    }
}
