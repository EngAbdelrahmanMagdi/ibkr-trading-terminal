package com.project.trading.broker.infrastructure.mock;

import com.project.trading.instrument.domain.Instrument;
import com.project.trading.instrument.domain.InstrumentCatalogPort;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * The simulated broker's instrument catalog: the instruments of the market-data simulator, with synthetic
 * contract IDs. Illustrative data for the demo, not broker data.
 */
public class MockInstrumentCatalog implements InstrumentCatalogPort {

    static final String EXCHANGE = "MOCK";

    private static final List<Instrument> INSTRUMENTS = List.of(
            stock("NVDA", 900_001, "NVIDIA Corporation"),
            stock("AAPL", 900_002, "Apple Inc."),
            stock("META", 900_003, "Meta Platforms, Inc."),
            stock("AMD", 900_004, "Advanced Micro Devices, Inc."),
            stock("IONQ", 900_005, "IonQ, Inc."),
            stock("MSFT", 900_006, "Microsoft Corporation"),
            stock("TSLA", 900_007, "Tesla, Inc."),
            stock("SPY", 900_008, "SPDR S&P 500 ETF Trust"));

    private static Instrument stock(String symbol, long conid, String name) {
        return new Instrument(symbol, conid, name, EXCHANGE, "USD", "STK", 2);
    }

    @Override
    public Optional<Instrument> resolve(String symbol) {
        return INSTRUMENTS.stream().filter(i -> i.symbol().equals(symbol)).findFirst();
    }

    /** Symbol prefix matches first, then name matches; alphabetical within each group. */
    @Override
    public List<Instrument> search(String query, int limit) {
        String q = query.toUpperCase(Locale.ROOT);
        return INSTRUMENTS.stream()
                .filter(i -> i.symbol().startsWith(q) || i.name().toUpperCase(Locale.ROOT).contains(q))
                .sorted(Comparator.comparing((Instrument i) -> !i.symbol().startsWith(q))
                        .thenComparing(Instrument::symbol))
                .limit(limit)
                .toList();
    }
}
