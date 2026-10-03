package com.project.trading.position.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

/** A durable New York day-opening mark, not a position cost basis. */
public record OpeningValuation(String symbol, LocalDate day, BigDecimal quantity, BigDecimal marketValue,
                               Instant markAt) {
}
