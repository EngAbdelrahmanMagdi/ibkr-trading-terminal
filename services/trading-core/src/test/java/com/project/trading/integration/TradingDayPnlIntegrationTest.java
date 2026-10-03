package com.project.trading.integration;

import com.project.trading.execution.application.ExecutionLedger;
import com.project.trading.execution.domain.Execution;
import com.project.trading.marketdata.domain.QuoteReferencePort;
import com.project.trading.marketdata.domain.ReferenceQuote;
import com.project.trading.position.application.PositionLedger;
import com.project.trading.position.application.TradingDayPnl;
import com.project.trading.position.application.TradingDaySnapshot;
import com.project.trading.position.domain.OpeningValuation;
import com.project.trading.position.domain.OpeningValuationRepository;
import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import com.project.trading.support.TradingInfrastructure;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/** Isolated real PostgreSQL fixtures; no reused application database or live quote provider. */
@SpringBootTest(properties = {"management.server.port=0", "SECRETS_DIR=/nonexistent/"})
class TradingDayPnlIntegrationTest {
    static final TradingInfrastructure INFRASTRUCTURE = TradingInfrastructure.start();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        INFRASTRUCTURE.register(registry);
    }

    static class TestClock extends Clock {
        private Instant now = Instant.parse("2026-10-04T14:00:00Z");
        @Override public Instant instant() { return now; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return Clock.fixed(now, zone); }
    }

    @TestConfiguration
    static class Clocks {
        @Bean @Primary TestClock testClock() { return new TestClock(); }
    }

    @Autowired TestClock clock;
    @Autowired TradingDayPnl pnl;
    @Autowired TradingDaySnapshot snapshots;
    @Autowired PositionLedger positions;
    @Autowired ExecutionLedger executions;
    @Autowired OpeningValuationRepository openings;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactions;
    @MockitoBean QuoteReferencePort quotes;

    @BeforeEach
    void reset() {
        jdbc.update("delete from position_day_openings");
        jdbc.update("delete from executions");
        jdbc.update("delete from orders");
        jdbc.update("delete from positions");
        clock.now = Instant.parse("2026-10-04T14:00:00Z");
        when(quotes.latest("NVDA")).thenReturn(Optional.empty());
    }

    private void fill(String side, String quantity, String price, String commission, String at) {
        UUID id = UUID.randomUUID();
        Instant timestamp = Instant.parse(at);
        new TransactionTemplate(transactions).executeWithoutResult(status -> {
            jdbc.update("insert into orders (id, client_order_id, account_id, conid, symbol, intent, broker_side, order_type,"
                            + " quantity, filled_quantity, time_in_force, status, reply_depth, created_at, updated_at, version)"
                            + " values (?, ?, 'MOCK', 1, 'NVDA', ?, ?, 'MARKET', ?, ?, 'DAY', 'FILLED', 0, ?, ?, 0)",
                    id, id.toString(), side, side, new BigDecimal(quantity), new BigDecimal(quantity),
                    Timestamp.from(timestamp), Timestamp.from(timestamp));
            executions.record(new Execution(UUID.randomUUID(), id, id.toString(), "NVDA", BrokerSide.valueOf(side),
                    new Quantity(new BigDecimal(quantity)), new Price(new BigDecimal(price)), new BigDecimal(commission), "USD", timestamp));
            positions.applyFill("NVDA", "USD", BrokerSide.valueOf(side), new Quantity(new BigDecimal(quantity)),
                    new Price(new BigDecimal(price)), timestamp);
        });
    }

    private void quote(String price, Instant at) {
        Price mark = new Price(new BigDecimal(price));
        when(quotes.latest("NVDA")).thenReturn(Optional.of(new ReferenceQuote("NVDA", mark, mark, mark, at, false, true, false)));
    }

    @Test
    void sameDayRoundTripIncludesCommissionsAndIgnoresPreviousRealizedProfit() {
        fill("BUY", "1", "50", "0", "2026-10-02T15:00:00Z");
        fill("SELL", "1", "75", "0", "2026-10-02T16:00:00Z");
        fill("BUY", "10", "100", "1", "2026-10-04T12:00:00Z");
        fill("SELL", "10", "110", "1", "2026-10-04T13:00:00Z");
        assertThat(pnl.value()).hasValue(new BigDecimal("98.0000"));
        assertThat(executions.list(null, 1)).hasSize(1);
        assertThat(positions.all().getFirst().realizedPnl()).isEqualByComparingTo("125");
    }

    @Test
    void openIntradayPositionRequiresAFreshMark() {
        fill("BUY", "10", "100", "1", "2026-10-04T12:00:00Z");
        assertThat(pnl.value()).isEmpty();
        quote("102", clock.instant());
        assertThat(pnl.value()).hasValue(new BigDecimal("19.0000"));
        quote("102", clock.instant().minusSeconds(60));
        assertThat(pnl.value()).isEmpty();
        quote("102", clock.instant().plusSeconds(1));
        assertThat(pnl.value()).isEmpty();
    }

    @Test
    void carriedPositionNeedsDurableOpeningEvenWhenClosedToday() {
        fill("BUY", "2", "100", "1", "2026-10-03T15:00:00Z");
        fill("SELL", "2", "105", "1", "2026-10-04T13:00:00Z");
        assertThat(pnl.value()).isEmpty();
        Instant boundary = Instant.parse("2026-10-04T04:00:00Z");
        clock.now = boundary;
        quote("99.99", boundary.minusMillis(500));
        pnl.captureOpening();
        clock.now = Instant.parse("2026-10-04T14:00:00Z");
        assertThat(pnl.value()).hasValue(new BigDecimal("9.0200"));
        TradingDayPnl restarted = new TradingDayPnl(snapshots, openings, quotes, clock, Duration.ofSeconds(10));
        assertThat(restarted.value()).hasValue(new BigDecimal("9.0200"));
        // A new reader uses persisted state, not process memory, and same-day writes cannot replace it.
        openings.save(new OpeningValuation("NVDA", LocalDate.parse("2026-10-04"), new BigDecimal("2"),
                new BigDecimal("1000"), boundary));
        assertThat(pnl.value()).hasValue(new BigDecimal("9.0200"));
        assertThat(jdbc.queryForObject("select count(*) from position_day_openings", Integer.class)).isEqualTo(1);
    }

    @Test
    void carriedShortUsesSignedOpeningValueAndPartialCoverCashFlow() {
        fill("SELL", "2", "100", "0", "2026-10-03T15:00:00Z");
        clock.now = Instant.parse("2026-10-04T04:00:00Z");
        quote("102", clock.instant().minusMillis(500));
        pnl.captureOpening();
        clock.now = Instant.parse("2026-10-04T14:00:00Z");
        fill("BUY", "1", "99", "0.25", "2026-10-04T13:00:00Z");
        quote("98", clock.instant());
        assertThat(pnl.value()).hasValue(new BigDecimal("6.7500"));
    }

    @Test
    void openingCaptureRejectsStaleOrPostBoundaryMarksAndBoundsStoredState() {
        fill("BUY", "1", "100", "0", "2026-10-03T15:00:00Z");
        Instant boundary = Instant.parse("2026-10-04T04:00:00Z");
        clock.now = boundary;
        quote("100", boundary.minusSeconds(60));
        pnl.captureOpening();
        assertThat(openings.find(LocalDate.parse("2026-10-04"))).isEmpty();
        quote("100", boundary.plusMillis(1));
        pnl.captureOpening();
        assertThat(openings.find(LocalDate.parse("2026-10-04"))).isEmpty();
        quote("100", boundary.minusMillis(500));
        pnl.captureOpening();
        clock.now = Instant.parse("2026-10-05T04:00:00Z");
        quote("101", clock.instant().minusMillis(500));
        pnl.captureOpening();
        assertThat(openings.find(LocalDate.parse("2026-10-04"))).isEmpty();
        assertThat(openings.find(LocalDate.parse("2026-10-05"))).containsKey("NVDA");
        assertThat(jdbc.queryForObject("select count(*) from position_day_openings", Integer.class)).isEqualTo(1);
    }

    @Test
    void lateHistoricalFillInvalidatesCapturedOpeningQuantity() {
        fill("BUY", "2", "100", "0", "2026-10-03T15:00:00Z");
        clock.now = Instant.parse("2026-10-04T04:00:00Z");
        quote("100", clock.instant().minusMillis(500));
        pnl.captureOpening();
        fill("BUY", "1", "100", "0", "2026-10-03T16:00:00Z");
        clock.now = Instant.parse("2026-10-04T14:00:00Z");
        quote("101", clock.instant());
        assertThat(pnl.value()).isEmpty();
    }

    @Test
    void utcMidnightDoesNotResetNewYorkDayAndLateVisitCannotInventOpening() {
        clock.now = Instant.parse("2026-10-04T01:00:00Z");
        fill("BUY", "1", "100", "0", "2026-10-03T22:00:00Z");
        fill("SELL", "1", "103", "0", "2026-10-04T00:30:00Z");
        assertThat(pnl.value()).hasValue(new BigDecimal("3.0000"));
        fill("BUY", "1", "100", "0", "2026-10-03T21:00:00Z");
        clock.now = Instant.parse("2026-10-04T14:00:00Z");
        quote("110", clock.instant());
        pnl.captureOpening();
        assertThat(openings.find(LocalDate.parse("2026-10-04"))).isEmpty();
        assertThat(pnl.value()).isEmpty();
    }
}
