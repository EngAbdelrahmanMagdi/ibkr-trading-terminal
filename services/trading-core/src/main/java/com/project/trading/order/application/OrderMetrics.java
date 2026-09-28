package com.project.trading.order.application;

import com.project.trading.order.domain.OrderRepository;
import com.project.trading.order.domain.OrderStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Order metrics. The per-status gauge is refreshed periodically from PostgreSQL. */
@Component
public class OrderMetrics {

    private static final Logger log = LoggerFactory.getLogger(OrderMetrics.class);

    private final Counter submitted;
    private final Counter rejectedByValidation;
    private final Counter rejectedByBroker;
    private final Counter invalidTransitions;
    private final Timer submission;
    private final Map<OrderStatus, AtomicLong> byStatus = new EnumMap<>(OrderStatus.class);
    private final OrderRepository orders;

    public OrderMetrics(MeterRegistry registry, OrderRepository orders) {
        this.orders = orders;
        this.submitted = Counter.builder("orders.submitted").description("Orders sent to the broker").register(registry);
        this.rejectedByValidation = Counter.builder("orders.rejected").tag("source", "validation")
                .description("Orders rejected").register(registry);
        this.rejectedByBroker = Counter.builder("orders.rejected").tag("source", "broker")
                .description("Orders rejected").register(registry);
        this.invalidTransitions = Counter.builder("orders.transitions.invalid")
                .description("Order updates refused by the lifecycle state machine").register(registry);
        this.submission = Timer.builder("order.submission").description("Order placement latency")
                .register(registry);
        for (OrderStatus status : OrderStatus.values()) {
            AtomicLong value = new AtomicLong();
            byStatus.put(status, value);
            registry.gauge("orders.by.status", Tags.of("status", status.name()), value);
        }
    }

    public void submitted() {
        submitted.increment();
    }

    public void rejectedByValidation() {
        rejectedByValidation.increment();
    }

    public void rejectedByBroker() {
        rejectedByBroker.increment();
    }

    public void invalidTransition() {
        invalidTransitions.increment();
    }

    public Timer submissionTimer() {
        return submission;
    }

    @Scheduled(fixedDelayString = "${app.metrics.status-refresh-interval}", initialDelayString = "${app.metrics.status-refresh-interval}")
    void refreshStatusGauge() {
        try {
            orders.countByStatus().forEach((status, count) -> byStatus.get(status).set(count));
        } catch (RuntimeException e) {
            log.warn("order status gauge refresh failed: {}", e.getClass().getSimpleName());
        }
    }
}
