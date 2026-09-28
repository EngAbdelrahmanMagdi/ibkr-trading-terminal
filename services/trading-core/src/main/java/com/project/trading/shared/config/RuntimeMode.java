package com.project.trading.shared.config;

/** Runtime mode, chosen once at startup; it only selects adapters. There is no live-money mode. */
public enum RuntimeMode {
    MOCK,
    IBKR_PAPER
}
