package com.project.trading.position;

import com.project.trading.marketdata.domain.QuoteReferencePort;
import com.project.trading.position.application.TradingDayPnl;
import com.project.trading.position.application.TradingDaySnapshot;
import com.project.trading.position.domain.OpeningValuationRepository;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TradingDayPnlTest {
    @Test
    void daylightSavingDaysUseActualNewYorkBoundaries() {
        for (String[] bounds : new String[][]{
                {"2026-03-08", "2026-03-08T05:00:00Z", "2026-03-09T04:00:00Z"},
                {"2026-11-01", "2026-11-01T04:00:00Z", "2026-11-02T05:00:00Z"}}) {
            TradingDaySnapshot snapshots = mock(TradingDaySnapshot.class);
            when(snapshots.read(LocalDate.parse(bounds[0]), Instant.parse(bounds[1]), Instant.parse(bounds[2])))
                    .thenReturn(new TradingDaySnapshot.View(List.of(), Map.of(), Map.of(), BigDecimal.ZERO));
            TradingDayPnl pnl = new TradingDayPnl(snapshots, mock(OpeningValuationRepository.class),
                    mock(QuoteReferencePort.class), Clock.fixed(Instant.parse(bounds[1]).plusSeconds(3600), ZoneOffset.UTC),
                    Duration.ofSeconds(5));
            assertThat(pnl.value()).hasValue(new BigDecimal("0.0000"));
        }
    }
}
