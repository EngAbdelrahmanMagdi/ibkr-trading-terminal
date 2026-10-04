package com.project.trading.order.application;

import com.project.trading.broker.domain.BrokerOrderRequest;
import com.project.trading.broker.domain.BrokerTradingPort;
import com.project.trading.broker.domain.SubmitResult;
import com.project.trading.order.domain.Order;
import com.project.trading.order.domain.OrderRepository;
import com.project.trading.order.domain.OrderStatus;
import com.project.trading.shared.config.AppProperties;
import com.project.trading.shared.domain.DomainException;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.Locale;
import java.util.UUID;

/**
 * Places an order: claim the Idempotency-Key, validate, record the order (transaction 1), submit it to the
 * broker exactly once with no transaction open, then record the outcome (transaction 2). A submission is never
 * retried; an uncertain outcome becomes UNKNOWN.
 */
@Service
public class PlaceOrderService {

    private static final Logger log = LoggerFactory.getLogger(PlaceOrderService.class);

    /** The HTTP-level outcome: 201 for a definite result, 202 while waiting on confirmation or verification. */
    public record Placed(int status, Order order) {
    }

    private final IdempotencyService idempotency;
    private final OrderValidator validator;
    private final OrderRepository orders;
    private final BrokerTradingPort broker;
    private final SubmissionOutcomes outcomes;
    private final OrderMetrics metrics;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final String accountId;

    PlaceOrderService(IdempotencyService idempotency, OrderValidator validator, OrderRepository orders,
                      BrokerTradingPort broker, SubmissionOutcomes outcomes, OrderMetrics metrics,
                      PlatformTransactionManager transactionManager, Clock clock, AppProperties properties) {
        this.idempotency = idempotency;
        this.validator = validator;
        this.orders = orders;
        this.broker = broker;
        this.outcomes = outcomes;
        this.metrics = metrics;
        this.tx = new TransactionTemplate(transactionManager);
        this.clock = clock;
        this.accountId = properties.accountId();
    }

    public Placed place(UUID idempotencyKey, PlaceOrderCommand command) {
        IdempotencyService.Claim claim = idempotency.claim(idempotencyKey, IdempotencyService.fingerprint(command));
        switch (claim) {
            case IdempotencyService.Claim.Completed done -> {
                return new Placed(done.status(), orders.findById(done.orderId()).orElseThrow());
            }
            case IdempotencyService.Claim.InProgress inProgress -> {
                if (inProgress.orderId().isPresent()) {
                    return new Placed(202, orders.findById(inProgress.orderId().get()).orElseThrow());
                }
                throw DomainException.conflict("the request with this Idempotency-Key is still in progress; retry later");
            }
            case IdempotencyService.Claim.Claimed claimed -> {
                // First request with this key: continue below.
            }
        }

        Timer.Sample sample = Timer.start();
        try {
            return submitNew(idempotencyKey, command);
        } finally {
            sample.stop(metrics.submissionTimer());
        }
    }

    private Placed submitNew(UUID key, PlaceOrderCommand command) {
        OrderValidator.ValidatedOrder valid;
        try {
            valid = validator.validate(command);
        } catch (DomainException e) {
            if (e.status() < 500 && e.status() != 429) {
                idempotency.completeWithProblem(key, e);
            }
            metrics.rejectedByValidation();
            throw e;
        }

        Order order = tx.execute(status -> {
            UUID id = UUID.randomUUID();
            Instant now = clock.instant();
            Order created = Order.create(id, clientOrderId(id), accountId, valid.instrument().conid(),
                    valid.instrument().symbol(), valid.intent(), valid.orderType(), valid.quantity(),
                    valid.limitPrice(), command.timeInForce(), now);
            created.markSubmissionPending(now);
            Order saved = orders.save(created);
            idempotency.linkOrder(key, saved.id());
            return saved;
        });

        SubmitResult result;
        try {
            metrics.submitted();
            result = broker.submit(new BrokerOrderRequest(order.id(), order.clientOrderId(), order.accountId(),
                    order.conid(), order.symbol(), order.brokerSide(), order.orderType(), order.quantity(),
                    order.limitPrice(), order.timeInForce()));
        } catch (RuntimeException e) {
            log.error("broker submission failed; outcome unknown for order {} category={}", order.id(), e.getClass().getSimpleName());
            result = new SubmitResult.Unknown("broker call failed");
        }

        SubmissionOutcomes.Applied applied = outcomes.apply(order.id(), OrderStatus.SUBMISSION_PENDING, null, result,
                a -> idempotency.complete(key, a.awaitingOutcome() ? 202 : 201));
        log.info("order {} placed: {} {} {} -> {}", order.id(), order.intent(), order.quantity(), order.symbol(),
                applied.order().status());
        return new Placed(applied.awaitingOutcome() ? 202 : 201, applied.order());
    }

    /** Deterministic client order ID derived from the order ID (fits the broker identifier rules). */
    static String clientOrderId(UUID orderId) {
        return "TC-" + orderId.toString().replace("-", "").toUpperCase(Locale.ROOT);
    }
}
