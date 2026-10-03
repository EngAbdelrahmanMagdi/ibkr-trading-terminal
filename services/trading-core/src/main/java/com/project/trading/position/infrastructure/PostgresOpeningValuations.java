package com.project.trading.position.infrastructure;

import com.project.trading.position.domain.OpeningValuation;
import com.project.trading.position.domain.OpeningValuationRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

@Repository
public class PostgresOpeningValuations implements OpeningValuationRepository {
    private final JdbcTemplate jdbc;

    public PostgresOpeningValuations(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Map<String, OpeningValuation> find(LocalDate day) {
        Map<String, OpeningValuation> result = new HashMap<>();
        jdbc.query("select symbol, trading_day, quantity, market_value, mark_at from position_day_openings where trading_day = ?",
                (org.springframework.jdbc.core.RowCallbackHandler) row -> {
                    OpeningValuation value = new OpeningValuation(row.getString("symbol"),
                            row.getDate("trading_day").toLocalDate(), row.getBigDecimal("quantity"),
                            row.getBigDecimal("market_value"), row.getTimestamp("mark_at").toInstant());
                    result.put(value.symbol(), value);
                }, Date.valueOf(day));
        return result;
    }

    @Override
    public void save(OpeningValuation value) {
        jdbc.update("insert into position_day_openings (symbol, trading_day, quantity, market_value, mark_at) values (?, ?, ?, ?, ?)"
                        + " on conflict (symbol) do update set trading_day = excluded.trading_day, quantity = excluded.quantity,"
                        + " market_value = excluded.market_value, mark_at = excluded.mark_at"
                        + " where position_day_openings.trading_day < excluded.trading_day",
                value.symbol(), Date.valueOf(value.day()), value.quantity(), value.marketValue(), Timestamp.from(value.markAt()));
    }
}
