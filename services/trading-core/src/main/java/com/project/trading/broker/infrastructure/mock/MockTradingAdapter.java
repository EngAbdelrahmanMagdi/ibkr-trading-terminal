package com.project.trading.broker.infrastructure.mock;

import com.project.trading.broker.domain.BrokerConnectionState;
import com.project.trading.broker.domain.BrokerOrderRequest;
import com.project.trading.broker.domain.BrokerTradingPort;
import com.project.trading.broker.domain.CancelResult;
import com.project.trading.broker.domain.SubmitResult;
import com.project.trading.shared.domain.OrderType;
import com.project.trading.shared.domain.Price;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * The simulated broker. Market orders fill immediately and in full at the fresh simulated quote (a buy at the
 * ask, a sell at the bid), also right after a final confirmation; without a fresh quote they are rejected rather
 * than filled at an old price. Limit orders rest in {@link MockMatcher}. Large orders need confirmation: one
 * above the first notional threshold, a chained second one above the second threshold.
 */
public class MockTradingAdapter implements BrokerTradingPort {

    private static final Logger log = LoggerFactory.getLogger(MockTradingAdapter.class);

    private record PendingReply(BrokerOrderRequest request, String brokerOrderId, int remaining, Instant createdAt) {
    }

    private final MockMarket market;
    private final MockMatcher matcher;
    private final Function<String, String> currencyOf;
    private final BigDecimal firstThreshold;
    private final BigDecimal secondThreshold;
    private final Duration replyTtl;
    private final int maxPendingReplies;
    private final Set<String> rejectSymbols;
    private final Map<String, PendingReply> pendingReplies = new ConcurrentHashMap<>();

    MockTradingAdapter(MockMarket market, MockMatcher matcher, Function<String, String> currencyOf,
                       MockBrokerProperties properties) {
        this.market = market;
        this.matcher = matcher;
        this.currencyOf = currencyOf;
        this.firstThreshold = properties.replyNotionalThreshold();
        this.secondThreshold = properties.secondReplyNotionalThreshold();
        this.replyTtl = properties.replyTtl();
        this.maxPendingReplies = properties.maxPendingReplies();
        this.rejectSymbols = Set.copyOf(properties.rejectSymbols());
    }

    @Override
    public BrokerConnectionState connectionState() {
        return BrokerConnectionState.READY;
    }

    @Override
    public SubmitResult submit(BrokerOrderRequest request) {
        if (rejectSymbols.contains(request.symbol())) {
            return new SubmitResult.Rejected("rejected by the simulated broker (configured rejection rule)");
        }
        Optional<BigDecimal> notional = notional(request);
        if (notional.isEmpty()) {
            return new SubmitResult.Rejected("no live market quote to fill the market order");
        }
        String brokerOrderId = "MOCK-" + MockMarket.compactUuid();
        int replies = notional.get().compareTo(secondThreshold) > 0 ? 2
                : notional.get().compareTo(firstThreshold) > 0 ? 1 : 0;
        if (replies == 0) {
            return accept(request, brokerOrderId);
        }
        return askConfirmation(new PendingReply(request, brokerOrderId, replies - 1, market.now()), false);
    }

    @Override
    public SubmitResult confirmReply(String replyId, boolean confirm) {
        PendingReply pending = pendingReplies.remove(replyId);
        if (pending == null || Duration.between(pending.createdAt(), market.now()).compareTo(replyTtl) > 0) {
            return new SubmitResult.Rejected("the confirmation request expired");
        }
        if (!confirm) {
            return new SubmitResult.Rejected("declined by the user");
        }
        if (pending.remaining() > 0) {
            return askConfirmation(new PendingReply(pending.request(), pending.brokerOrderId(), pending.remaining() - 1,
                    market.now()), true);
        }
        return accept(pending.request(), pending.brokerOrderId());
    }

    @Override
    public CancelResult cancel(String brokerOrderId) {
        if (matcher.requestCancel(brokerOrderId)) {
            return new CancelResult.Requested();
        }
        return new CancelResult.Rejected("the order is not open at the simulated broker");
    }

    private SubmitResult askConfirmation(PendingReply pending, boolean chained) {
        Instant now = market.now();
        pendingReplies.values().removeIf(p -> Duration.between(p.createdAt(), now).compareTo(replyTtl) > 0);
        if (pendingReplies.size() >= maxPendingReplies) {
            return new SubmitResult.Rejected("too many confirmations pending at the simulated broker");
        }
        String replyId = "MOCK-R-" + MockMarket.compactUuid();
        pendingReplies.put(replyId, pending);
        String message = chained
                ? "Large order: the order value exceeds " + secondThreshold.toPlainString() + ". Confirm again to submit."
                : "The order value exceeds " + firstThreshold.toPlainString() + ". Confirm to submit.";
        return new SubmitResult.ConfirmationRequired(replyId, message);
    }

    private SubmitResult accept(BrokerOrderRequest request, String brokerOrderId) {
        if (request.orderType() == OrderType.MARKET) {
            Optional<Price> price = market.executablePrice(request.symbol(), request.side());
            if (price.isEmpty()) {
                return new SubmitResult.Rejected("no live market quote to fill the market order");
            }
            log.info("simulated market order {} filled at {}", brokerOrderId, price.get());
            return new SubmitResult.Accepted(brokerOrderId, List.of(market.fill(brokerOrderId, request.side(),
                    request.quantity(), price.get(), currencyOf.apply(request.symbol()))));
        }
        if (!matcher.add(brokerOrderId, request.symbol(), request.side(), request.quantity(), request.limitPrice())) {
            return new SubmitResult.Rejected("the simulated order book is full");
        }
        return new SubmitResult.Accepted(brokerOrderId, List.of());
    }

    /** The order value: at the limit price, or at the executable side of the fresh quote for a market order. */
    private Optional<BigDecimal> notional(BrokerOrderRequest request) {
        Optional<Price> price = request.orderType() == OrderType.LIMIT ? Optional.of(request.limitPrice())
                : market.executablePrice(request.symbol(), request.side());
        return price.map(p -> p.value().multiply(request.quantity().value()));
    }
}
