package com.project.trading.shared.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;

/**
 * Binds the database to one runtime mode and account, and refuses to start when the configured mode or account
 * differs from the stored binding: simulated instruments, orders and positions must never be traded against a
 * broker account (and the reverse). Runs after the schema migrations and before any background work starts.
 * Switching mode or account requires a separate or clean database.
 */
@Component
class RuntimeBindingGuard implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(RuntimeBindingGuard.class);

    /** The stored binding. */
    record Binding(String runtimeMode, String accountId) {
    }

    private final JdbcTemplate jdbc;
    private final AppProperties properties;
    private final Clock clock;

    RuntimeBindingGuard(JdbcTemplate jdbc, AppProperties properties, Clock clock) {
        this.jdbc = jdbc;
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public void afterSingletonsInstantiated() {
        Binding configured = new Binding(properties.runtimeMode().name(), properties.accountId());
        Binding stored = read();
        if (stored == null) {
            boolean hasData = Boolean.TRUE.equals(jdbc.queryForObject(
                    "select exists(select 1 from instruments) or exists(select 1 from orders)", Boolean.class));
            check(configured, null, hasData);
            jdbc.update("insert into runtime_binding (singleton, runtime_mode, account_id, bound_at)"
                            + " values (true, ?, ?, ?) on conflict (singleton) do nothing",
                    configured.runtimeMode(), configured.accountId(), Timestamp.from(clock.instant()));
            stored = read();
            log.info("database bound to runtime mode {}", stored == null ? null : stored.runtimeMode());
        }
        check(configured, stored, false);
    }

    private Binding read() {
        List<Binding> rows = jdbc.query("select runtime_mode, account_id from runtime_binding",
                (rs, n) -> new Binding(rs.getString(1), rs.getString(2)));
        return rows.isEmpty() ? null : rows.getFirst();
    }

    /**
     * The binding rules. An unbound database with existing data may only be bound to MOCK: such data predates
     * the binding and was created by the simulated broker.
     */
    static void check(Binding configured, Binding stored, boolean unboundDatabaseHasData) {
        if (stored == null) {
            if (unboundDatabaseHasData && !RuntimeMode.MOCK.name().equals(configured.runtimeMode())) {
                throw new IllegalStateException("Startup refused: the database contains data that is not bound to "
                        + configured.runtimeMode() + "; use a separate or clean database for this runtime mode");
            }
            return;
        }
        if (!stored.equals(configured)) {
            throw new IllegalStateException("Startup refused: the database belongs to runtime mode "
                    + stored.runtimeMode() + " and a different account or mode than configured ("
                    + configured.runtimeMode() + "); use a separate or clean database when switching");
        }
    }
}
