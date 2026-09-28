package com.project.trading.instrument.infrastructure;

import org.springframework.data.jpa.repository.JpaRepository;

public interface InstrumentJpaRepository extends JpaRepository<InstrumentEntity, String> {
}
