package com.project.trading.broker.infrastructure.mock;

import com.project.trading.position.application.TradingDayPnl;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;

/** Opening snapshots are independent of browser visits and survive Core restarts. */
public class MockDayPnlCapture {
    private final TradingDayPnl pnl;

    public MockDayPnlCapture(TradingDayPnl pnl) {
        this.pnl = pnl;
    }

    @Scheduled(cron = "0 0 0 * * *", zone = "America/New_York")
    @EventListener(ApplicationReadyEvent.class)
    public void capture() {
        pnl.captureOpening();
    }
}
