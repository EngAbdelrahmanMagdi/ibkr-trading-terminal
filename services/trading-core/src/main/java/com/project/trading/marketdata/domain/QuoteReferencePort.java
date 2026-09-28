package com.project.trading.marketdata.domain;

import java.util.Optional;

/** Reads the latest quote of a symbol. Never throws: a missing or unreadable quote is empty. */
public interface QuoteReferencePort {

    Optional<ReferenceQuote> latest(String symbol);
}
