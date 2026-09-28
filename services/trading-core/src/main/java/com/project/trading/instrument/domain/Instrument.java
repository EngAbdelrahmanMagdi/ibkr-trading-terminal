package com.project.trading.instrument.domain;

import java.util.Objects;

/**
 * A resolved, tradable instrument. priceScale is the number of decimals a limit price may have (the tick).
 */
public record Instrument(String symbol, long conid, String name, String exchange, String currency, String assetType,
                         int priceScale) {

    public Instrument {
        Objects.requireNonNull(symbol, "symbol");
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(currency, "currency");
        Objects.requireNonNull(assetType, "assetType");
    }
}
