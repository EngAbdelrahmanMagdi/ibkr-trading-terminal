package com.project.trading.order.application;

import com.project.trading.broker.domain.ReconciliationRequests;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Schedules reconciliation on one thread: periodically, shortly after startup, and on request (when the broker
 * session becomes ready again, or the broker-update stream is assigned or recovers). Requests are coalesced: runs are
 * at least minSpacing apart, and at most one requested run is pending.
 */
@Component
public class Reconciler implements ReconciliationRequests, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Reconciler.class);

    /** Registers the settings; kept here so the order module owns its configuration. */
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(ReconciliationSettings.class)
    static class Settings {
    }

    private final ReconciliationService reconciliation;
    private final ReconciliationSettings settings;
    private final AtomicBoolean requested = new AtomicBoolean();
    private final Counter runs;
    private final Counter failures;
    private ScheduledExecutorService executor;
    private volatile boolean running;
    private long lastRunNanos;
    private boolean ranOnce;

    public Reconciler(ReconciliationService reconciliation, ReconciliationSettings settings, MeterRegistry registry) {
        this.reconciliation = reconciliation;
        this.settings = settings;
        this.runs = Counter.builder("reconciliation.runs").description("Completed reconciliation runs").register(registry);
        this.failures = Counter.builder("reconciliation.failures").description("Reconciliation runs that failed")
                .register(registry);
    }

    @Override
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "reconciliation");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(() -> runNow("periodic"), settings.initialDelay().toMillis(),
                settings.interval().toMillis(), TimeUnit.MILLISECONDS);
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Stops after the web server has drained requests and before the datasource closes. */
    @Override
    public int getPhase() {
        return 0;
    }

    @Override
    public void requestReconciliation(String reason) {
        ScheduledExecutorService ex = executor;
        if (ex == null || ex.isShutdown() || !requested.compareAndSet(false, true)) {
            return;
        }
        log.debug("reconciliation requested: {}", reason);
        long delay;
        synchronized (this) {
            long spacing = settings.minSpacing().toNanos();
            delay = ranOnce ? Math.max(0, lastRunNanos + spacing - System.nanoTime()) : 0;
        }
        ex.schedule(() -> {
            requested.set(false);
            runNow(reason);
        }, delay, TimeUnit.NANOSECONDS);
    }

    private void runNow(String reason) {
        synchronized (this) {
            if (ranOnce && System.nanoTime() - lastRunNanos < settings.minSpacing().toNanos() && "periodic".equals(reason)) {
                return;
            }
            lastRunNanos = System.nanoTime();
            ranOnce = true;
        }
        try {
            if (reconciliation.run().ran()) {
                runs.increment();
            }
        } catch (RuntimeException e) {
            failures.increment();
            log.warn("reconciliation run failed ({}): {}", reason, e.getClass().getSimpleName());
        }
    }
}
