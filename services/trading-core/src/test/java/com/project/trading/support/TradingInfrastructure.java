package com.project.trading.support;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.util.Map;

/**
 * Real PostgreSQL (with the production role setup: Flyway as the schema owner, the application as a DML-only
 * role) and real Redis (ACL user) for application-level tests. Each started instance is a separate database, so
 * tests of different runtime modes never share one.
 */
public final class TradingInfrastructure {

    private static final String OWNER_PASSWORD = "test-owner-password";
    private static final String APP_PASSWORD = "test-app-password";
    private static final String REDIS_PASSWORD = "test-redis-password";

    private final PostgreSQLContainer postgres;
    private final GenericContainer<?> redis;

    private TradingInfrastructure() {
        postgres = new PostgreSQLContainer("postgres:18.6")
                .withEnv(Map.of("TRADING_OWNER_ROLE", "trading_owner", "TRADING_APP_ROLE", "trading_app",
                        "TRADING_SCHEMA", "trading"))
                .withCopyToContainer(MountableFile.forHostPath(Path.of(System.getProperty("infrastructure.dir",
                        "../../infrastructure"), "postgres", "init", "10-create-roles.sh"), 0755),
                        "/docker-entrypoint-initdb.d/10-create-roles.sh")
                .withCopyToContainer(Transferable.of(OWNER_PASSWORD), "/run/secrets/trading_owner_password")
                .withCopyToContainer(Transferable.of(APP_PASSWORD), "/run/secrets/trading_app_password");
        redis = new GenericContainer<>("redis:8.10.2")
                .withExposedPorts(6379)
                .withCopyToContainer(Transferable.of("protected-mode no\nuser default off\nuser app on >" + REDIS_PASSWORD
                        + " ~* &* +@all\n"), "/usr/local/etc/redis/redis.conf")
                .withCommand("redis-server", "/usr/local/etc/redis/redis.conf");
    }

    public static TradingInfrastructure start() {
        TradingInfrastructure infrastructure = new TradingInfrastructure();
        infrastructure.postgres.start();
        infrastructure.redis.start();
        return infrastructure;
    }

    public void register(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> "jdbc:postgresql://" + postgres.getHost() + ":"
                + postgres.getMappedPort(5432) + "/" + postgres.getDatabaseName() + "?currentSchema=trading");
        registry.add("spring.datasource.username", () -> "trading_app");
        registry.add("spring.datasource.password", () -> APP_PASSWORD);
        registry.add("spring.flyway.user", () -> "trading_owner");
        registry.add("spring.flyway.password", () -> OWNER_PASSWORD);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("spring.data.redis.username", () -> "app");
        registry.add("spring.data.redis.password", () -> REDIS_PASSWORD);
    }
}
