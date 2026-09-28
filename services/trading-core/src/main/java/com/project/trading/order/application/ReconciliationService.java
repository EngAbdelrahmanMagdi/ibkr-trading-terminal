package com.project.trading.order.application;

import com.project.trading.broker.domain.BrokerConnectionState;
import com.project.trading.broker.domain.BrokerOrderUpdate;
import com.project.trading.broker.domain.BrokerOrderUpdateHandler;
import com.project.trading.broker.domain.BrokerTradingPort;
import com.project.trading.broker.domain.BrokerTruthPort;
import com.project.trading.execution.application.ExecutionLedger;
import com.project.trading.instrument.application.InstrumentService;
import com.project.trading.order.domain.Order;
import com.project.trading.order.domain.OrderRepository;
import com.project.trading.order.domain.OrderStatus;
import com.project.trading.position.application.PositionLedger;
import com.project.trading.position.domain.Position;
import com.project.trading.shared.config.AppProperties;
import com.project.trading.shared.domain.DomainException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Brings local order state to broker truth after missed updates (a lost stream message, a Kafka outage, a restart or
 * a network failure). Every repair goes through the normal update path, so it records executions, positions and
 * outbox events exactly as a live update would. Nothing is invented:
 * <ol>
 *   <li>broker executions of this application's orders that are not recorded yet are applied;</li>
 *   <li>orders the broker reports as cancelled or rejected are moved (only after their reported fills), and orders
 *   whose outcome was unknown are acknowledged when the broker knows them;</li>
 *   <li>an order whose outcome stays unknown is never resolved by absence: after a configured time (counted only while
 *   the broker session is ready) it is reported for manual resolution and stays UNKNOWN;</li>
 *   <li>position differences that remain are reported as drift and never overwritten;</li>
 *   <li>orders and executions placed outside this application are counted, not imported.</li>
 * </ol>
 * Runs are not concurrent and hold no database transaction while the broker is called.
 */
@Service
public class ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);
    private static final int MAX_UNKNOWN_ORDERS = 1000;
    private static final int MAX_SEEN_EXTERNAL = 10_000;

    /** Result of one run. */
    public record Result(boolean ran, int applied, int external) {
        static Result skipped() {
            return new Result(false, 0, 0);
        }
    }

    private final BrokerTradingPort broker;
    private final BrokerTruthPort truth;
    private final BrokerOrderUpdateHandler updates;
    private final OrderRepository orders;
    private final ExecutionLedger executions;
    private final PositionLedger positions;
    private final InstrumentService instruments;
    private final DriftReporter drift;
    private final Clock clock;
    private final String currency;
    private final Duration unknownAlertAfter;
    private final Counter external;
    private final AtomicLong unresolvedUnknown = new AtomicLong();

    /** Time the broker session was ready while an UNKNOWN order stayed unmatched (in memory; resets on restart). */
    private final Map<UUID, Duration> unmatchedReadyTime = new HashMap<>();
    private final Set<UUID> reportedUnresolved = new HashSet<>();
    private final Map<Long, String> reportedPositionDrift = new HashMap<>();
    private final Set<UUID> reportedMissingAtBroker = new HashSet<>();
    private final Set<String> seenExternal = new HashSet<>();
    private Instant lastRun;

    public ReconciliationService(BrokerTradingPort broker, BrokerTruthPort truth, BrokerOrderUpdateHandler updates,
                                 OrderRepository orders, ExecutionLedger executions, PositionLedger positions,
                                 InstrumentService instruments, DriftReporter drift, Clock clock, AppProperties app,
                                 ReconciliationSettings settings, MeterRegistry registry) {
        this.broker = broker;
        this.truth = truth;
        this.updates = updates;
        this.orders = orders;
        this.executions = executions;
        this.positions = positions;
        this.instruments = instruments;
        this.drift = drift;
        this.clock = clock;
        this.currency = app.currency();
        this.unknownAlertAfter = settings.unknownOrderAlertAfter();
        this.external = Counter.builder("reconciliation.external")
                .description("Broker orders and executions placed outside this application (not imported)").register(registry);
        registry.gauge("orders.unknown.unresolved", unresolvedUnknown);
    }

    /** One run. Skipped while the broker is not ready or reconciliation is not supported. */
    public synchronized Result run() {
        Instant now = clock.instant();
        if (broker.connectionState() != BrokerConnectionState.READY) {
            lastRun = null; // the time without a ready session never counts toward the UNKNOWN alert
            return Result.skipped();
        }
        Optional<BrokerTruthPort.Snapshot> found = truth.snapshot();
        if (found.isEmpty()) {
            lastRun = null;
            return Result.skipped();
        }
        BrokerTruthPort.Snapshot snapshot = found.get();
        Duration readyTime = lastRun == null ? Duration.ZERO : Duration.between(lastRun, now);
        lastRun = now;

        int applied = 0;
        int externalCount = 0;
        Map<String, String> brokerIdByRef = new HashMap<>();
        for (BrokerTruthPort.OrderView o : snapshot.orders()) {
            if (o.clientOrderRef() != null) {
                brokerIdByRef.put(o.clientOrderRef(), o.brokerOrderId());
            }
        }

        for (BrokerTruthPort.ExecutionView e : snapshot.executions()) {
            Optional<Order> local = e.clientOrderRef() == null ? Optional.empty() : orders.findByClientOrderId(e.clientOrderRef());
            if (local.isEmpty()) {
                externalCount += firstSighting("execution:" + e.brokerExecutionId());
                continue;
            }
            if (executions.isRecorded(e.brokerExecutionId())) {
                continue;
            }
            String brokerOrderId = e.brokerOrderId() != null ? e.brokerOrderId()
                    : local.get().brokerOrderId() != null ? local.get().brokerOrderId() : brokerIdByRef.get(e.clientOrderRef());
            BrokerOrderUpdateHandler.Outcome outcome = updates.handle(new BrokerOrderUpdate.Fill(brokerOrderId,
                    e.brokerExecutionId(), null, e.quantity(), e.price(), e.commission(), currency, e.executedAt(),
                    e.clientOrderRef()));
            if (outcome == BrokerOrderUpdateHandler.Outcome.APPLIED) {
                applied++;
                drift.report("execution", DriftReporter.INFO, local.get().id(),
                        "missing execution " + e.brokerExecutionId() + " recovered from the broker");
            }
        }

        Set<String> brokerRefs = new HashSet<>(brokerIdByRef.keySet());
        for (BrokerTruthPort.OrderView o : snapshot.orders()) {
            Optional<Order> local = o.clientOrderRef() != null ? orders.findByClientOrderId(o.clientOrderRef())
                    : Optional.empty();
            if (local.isEmpty() && o.brokerOrderId() != null) {
                local = orders.findByBrokerOrderId(o.brokerOrderId());
            }
            if (local.isEmpty()) {
                externalCount += firstSighting("order:" + o.brokerOrderId());
                continue;
            }
            if (local.get().status().isTerminal()) {
                continue; // nothing to move; late executions arrive as executions
            }
            BrokerOrderUpdateHandler.Outcome outcome = updates.handle(new BrokerOrderUpdate.Observed(o.brokerOrderId(),
                    o.clientOrderRef(), o.status(), o.filledQuantity(), o.detail(), now));
            if (outcome == BrokerOrderUpdateHandler.Outcome.APPLIED) {
                applied++;
                drift.report("order", DriftReporter.INFO, local.get().id(),
                        "broker order state (" + o.status() + ") applied");
            }
        }

        reviewUnknownOrders(brokerRefs, readyTime);
        reportWorkingOrdersMissingAtBroker(brokerRefs);
        if (snapshot.positionsByConid() != null) {
            comparePositions(snapshot.positionsByConid());
        }
        external.increment(externalCount);
        if (applied > 0) {
            log.info("reconciliation applied {} broker updates", applied);
        }
        return new Result(true, applied, externalCount);
    }

    /** 1 the first time an external order or execution is seen (bounded memory), else 0. */
    private int firstSighting(String key) {
        if (seenExternal.size() >= MAX_SEEN_EXTERNAL) {
            seenExternal.clear();
        }
        return seenExternal.add(key) ? 1 : 0;
    }

    private void reviewUnknownOrders(Set<String> brokerRefs, Duration readyTime) {
        Set<UUID> stillUnknown = new HashSet<>();
        for (Order order : orders.findRecent(EnumSet.of(OrderStatus.UNKNOWN), MAX_UNKNOWN_ORDERS)) {
            stillUnknown.add(order.id());
            if (brokerRefs.contains(order.clientOrderId())) {
                continue; // known at the broker; resolved by its updates
            }
            Duration waited = unmatchedReadyTime.merge(order.id(), readyTime, Duration::plus);
            if (waited.compareTo(unknownAlertAfter) >= 0 && reportedUnresolved.add(order.id())) {
                log.atWarn().addKeyValue("event", "unknown_order_unresolved").addKeyValue("orderId", order.id().toString())
                        .log("order {} still has an unknown outcome and the broker does not report it; manual resolution"
                                + " is needed (it stays UNKNOWN)", order.id());
            }
        }
        unmatchedReadyTime.keySet().retainAll(stillUnknown);
        reportedUnresolved.retainAll(stillUnknown);
        unresolvedUnknown.set(reportedUnresolved.size());
    }

    /** The broker's order list covers the current day only, so an absent order proves nothing: reported once. */
    private void reportWorkingOrdersMissingAtBroker(Set<String> brokerRefs) {
        Set<UUID> working = new HashSet<>();
        for (Order order : orders.findWorkingOrders(MAX_UNKNOWN_ORDERS)) {
            working.add(order.id());
            if (!brokerRefs.contains(order.clientOrderId()) && reportedMissingAtBroker.add(order.id())) {
                drift.report("order", DriftReporter.INFO, order.id(),
                        "working order not in the broker's current-day order list (reported only)");
            }
        }
        reportedMissingAtBroker.retainAll(working);
    }

    /** Reports position differences that remain after applying broker executions; positions are never overwritten. */
    private void comparePositions(Map<Long, BigDecimal> brokerPositions) {
        Map<Long, BigDecimal> local = new HashMap<>();
        Map<Long, String> symbols = new HashMap<>();
        for (Position p : positions.all()) {
            try {
                long conid = instruments.resolve(p.symbol()).conid();
                local.put(conid, p.quantity());
                symbols.put(conid, p.symbol());
            } catch (DomainException e) {
                log.debug("position {} has no instrument: {}", p.symbol(), e.getMessage());
            }
        }
        Set<Long> conids = new HashSet<>(local.keySet());
        conids.addAll(brokerPositions.keySet());
        Map<Long, String> current = new HashMap<>();
        for (Long conid : conids) {
            BigDecimal mine = local.getOrDefault(conid, BigDecimal.ZERO);
            BigDecimal theirs = brokerPositions.getOrDefault(conid, BigDecimal.ZERO);
            if (mine.compareTo(theirs) == 0) {
                continue;
            }
            String state = mine.stripTrailingZeros().toPlainString() + "/" + theirs.stripTrailingZeros().toPlainString();
            current.put(conid, state);
            if (!state.equals(reportedPositionDrift.get(conid))) {
                drift.report("position", DriftReporter.SERIOUS, null, "position " + symbols.getOrDefault(conid, "conid " + conid)
                        + ": local " + mine.stripTrailingZeros().toPlainString() + ", broker "
                        + theirs.stripTrailingZeros().toPlainString() + " (reported only; positions are not overwritten)");
            }
        }
        reportedPositionDrift.clear();
        reportedPositionDrift.putAll(current);
    }
}
