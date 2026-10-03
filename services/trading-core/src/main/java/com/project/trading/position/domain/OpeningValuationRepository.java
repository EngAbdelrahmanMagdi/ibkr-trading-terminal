package com.project.trading.position.domain;

import java.time.LocalDate;
import java.util.Map;

public interface OpeningValuationRepository {
    Map<String, OpeningValuation> find(LocalDate day);

    /** First valid mark wins; a later trading day replaces the single retained row per symbol. */
    void save(OpeningValuation valuation);
}
