package com.project.trading.order.application;

import com.project.trading.broker.domain.BrokerConnectionState;
import com.project.trading.broker.domain.BrokerTradingPort;
import com.project.trading.instrument.application.InstrumentService;
import com.project.trading.instrument.domain.Instrument;
import com.project.trading.marketdata.domain.QuoteReferencePort;
import com.project.trading.marketdata.domain.ReferenceQuote;
import com.project.trading.order.domain.OrderIntent;
import com.project.trading.order.domain.OrderPolicy;
import com.project.trading.order.domain.OrderRepository;
import com.project.trading.order.domain.OrderStatus;
import com.project.trading.shared.domain.OrderType;
import com.project.trading.position.application.PositionLedger;
import com.project.trading.shared.config.AppProperties;
import com.project.trading.shared.domain.Decimals;
import com.project.trading.shared.domain.DomainException;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;

/**
 * Business validation of a new order. User-entered prices and quantities are checked, never rounded. Runs
 * outside any transaction because it may consult the broker catalog and the quote cache.
 */
@Component
public class OrderValidator {

    /** A validated order, ready to be recorded. */
    public record ValidatedOrder(Instrument instrument, OrderIntent intent, OrderType orderType, Quantity quantity,
                                 Price limitPrice, PlaceOrderCommand command) {
    }

    private final InstrumentService instruments;
    private final PositionLedger positions;
    private final OrderRepository orders;
    private final QuoteReferencePort quotes;
    private final BrokerTradingPort broker;
    private final AppProperties.Orders limits;
    private final Clock clock;

    public OrderValidator(InstrumentService instruments, PositionLedger positions, OrderRepository orders,
                          QuoteReferencePort quotes, BrokerTradingPort broker, AppProperties properties, Clock clock) {
        this.instruments = instruments;
        this.positions = positions;
        this.orders = orders;
        this.quotes = quotes;
        this.broker = broker;
        this.limits = properties.orders();
        this.clock = clock;
    }

    public ValidatedOrder validate(PlaceOrderCommand c) {
        if (c.orderType() == OrderType.LIMIT && c.limitPrice() == null) {
            throw DomainException.malformedField("limitPrice", "limitPrice is required for LIMIT orders");
        }
        if (c.orderType() == OrderType.MARKET && c.limitPrice() != null) {
            throw DomainException.malformedField("limitPrice", "limitPrice must not be sent for MARKET orders");
        }
        Quantity quantity = quantity(c.quantity());
        Price limitPrice = c.limitPrice() == null ? null : limitPrice(c.limitPrice());

        if (broker.connectionState() != BrokerConnectionState.READY) {
            throw DomainException.brokerUnavailable("the broker is not available for new orders");
        }
        if (orders.existsWithStatus(OrderStatus.PENDING_CONFIRMATION)) {
            throw DomainException.conflict("another order is waiting for confirmation; confirm or decline it first");
        }

        Instrument instrument = instruments.resolve(c.symbol());
        if (limitPrice != null && Decimals.significantScale(limitPrice.value()) > instrument.priceScale()) {
            throw DomainException.invalidField("limitPrice",
                    "limitPrice has more decimals than the instrument allows (" + instrument.priceScale() + ")");
        }

        BigDecimal held = positions.signedQuantity(c.symbol());
        if (c.intent() == OrderIntent.SELL) {
            BigDecimal available = held.max(BigDecimal.ZERO).subtract(orders.openSellQuantity(c.symbol()));
            OrderPolicy.checkPosition(c.intent(), quantity, available);
        } else {
            OrderPolicy.checkPosition(c.intent(), quantity, held);
        }
        if (c.intent() == OrderIntent.SHORT) {
            OrderPolicy.checkShortability(c.intent(), instruments.shortability(c.symbol()).status());
        }

        BigDecimal referencePrice = limitPrice != null ? limitPrice.value() : marketReference(c.symbol(), c.intent());
        if (quantity.value().multiply(referencePrice).compareTo(limits.maxNotional()) > 0) {
            throw DomainException.invalid("the order value exceeds the configured maximum of " + limits.maxNotional().toPlainString());
        }
        return new ValidatedOrder(instrument, c.intent(), c.orderType(), quantity, limitPrice, c);
    }

    private Quantity quantity(BigDecimal value) {
        if (value.signum() <= 0) {
            throw DomainException.invalidField("quantity", "quantity must be greater than zero");
        }
        if (Decimals.significantScale(value) > 0) {
            throw DomainException.invalidField("quantity", "fractional share quantities are not supported");
        }
        if (value.compareTo(limits.maxQuantity()) > 0) {
            throw DomainException.invalidField("quantity",
                    "quantity exceeds the configured maximum of " + limits.maxQuantity().toPlainString());
        }
        return new Quantity(value);
    }

    private static Price limitPrice(BigDecimal value) {
        if (value.signum() <= 0) {
            throw DomainException.invalidField("limitPrice", "limitPrice must be greater than zero");
        }
        return new Price(value);
    }

    /** The side a market order would trade against: the ask for a buy, the bid for a sell or short. */
    private BigDecimal marketReference(String symbol, OrderIntent intent) {
        Instant now = clock.instant();
        ReferenceQuote quote = quotes.latest(symbol)
                .filter(q -> q.isFresh(now, limits.quoteMaxAge()))
                .orElseThrow(() -> DomainException.staleMarketData("no fresh quote for " + symbol + "; market orders need live market data"));
        Price side = intent == OrderIntent.BUY ? quote.ask() : quote.bid();
        if (side == null) {
            throw DomainException.staleMarketData("no " + (intent == OrderIntent.BUY ? "ask" : "bid") + " price for " + symbol);
        }
        return side.value();
    }
}
