package com.project.trading.order.application;

import com.project.trading.broker.domain.BrokerConnectionState;
import com.project.trading.broker.domain.BrokerTradingPort;
import com.project.trading.instrument.application.InstrumentService;
import com.project.trading.instrument.domain.Instrument;
import com.project.trading.instrument.domain.Shortability;
import com.project.trading.instrument.domain.ShortabilityStatus;
import com.project.trading.marketdata.domain.QuoteReferencePort;
import com.project.trading.marketdata.domain.ReferenceQuote;
import com.project.trading.order.domain.Order;
import com.project.trading.order.domain.OrderIntent;
import com.project.trading.shared.domain.OrderType;
import com.project.trading.shared.domain.TimeInForce;
import com.project.trading.position.application.PositionLedger;
import com.project.trading.shared.domain.DomainException;
import com.project.trading.shared.domain.ErrorCategory;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import com.project.trading.support.InMemoryOrderRepository;
import com.project.trading.support.TestProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrderValidatorTest {

    private static final Instant NOW = Instant.parse("2026-09-28T10:00:00Z");

    private final InstrumentService instruments = mock(InstrumentService.class);
    private final PositionLedger positions = mock(PositionLedger.class);
    private final QuoteReferencePort quotes = mock(QuoteReferencePort.class);
    private final BrokerTradingPort broker = mock(BrokerTradingPort.class);
    private final InMemoryOrderRepository orders = new InMemoryOrderRepository();
    private OrderValidator validator;

    @BeforeEach
    void setUp() {
        validator = new OrderValidator(instruments, positions, orders, quotes, broker,
                TestProperties.app(3), Clock.fixed(NOW, ZoneOffset.UTC));
        when(broker.connectionState()).thenReturn(BrokerConnectionState.READY);
        when(instruments.resolve("NVDA")).thenReturn(new Instrument("NVDA", 1, "NVIDIA", "MOCK", "USD", "STK", 2));
        when(instruments.shortability(any(Instrument.class))).thenReturn(Shortability.unavailable());
        when(positions.signedQuantity("NVDA")).thenReturn(BigDecimal.ZERO);
        quote(NOW.minusSeconds(1), false);
    }

    private void quote(Instant at, boolean stale) {
        when(quotes.latest("NVDA")).thenReturn(Optional.of(new ReferenceQuote("NVDA", Price.of("179.90"),
                Price.of("180.10"), Price.of("180.00"), at, stale, true, false)));
    }

    private static PlaceOrderCommand order(OrderIntent intent, OrderType type, String qty, String limit) {
        return new PlaceOrderCommand("NVDA", intent, type, new BigDecimal(qty),
                limit == null ? null : new BigDecimal(limit), TimeInForce.DAY);
    }

    private static void assertRejected(Runnable call, int status, ErrorCategory category, String field) {
        assertThatThrownBy(call::run).isInstanceOfSatisfying(DomainException.class, e -> {
            assertThat(e.status()).isEqualTo(status);
            assertThat(e.category()).isEqualTo(category);
            if (field != null) {
                assertThat(e.fieldErrors()).extracting("field").containsExactly(field);
            }
        });
    }

    @Test
    void acceptsAValidMarketAndLimitOrder() {
        assertThatCode(() -> validator.validate(order(OrderIntent.BUY, OrderType.MARKET, "10", null))).doesNotThrowAnyException();
        assertThat(validator.validate(order(OrderIntent.BUY, OrderType.LIMIT, "10", "180.05")).limitPrice())
                .isEqualTo(Price.of("180.05"));
    }

    @Test
    void quantityMustBePositiveWholeAndWithinTheLimit() {
        assertRejected(() -> validator.validate(order(OrderIntent.BUY, OrderType.MARKET, "0", null)), 422, ErrorCategory.VALIDATION, "quantity");
        assertRejected(() -> validator.validate(order(OrderIntent.BUY, OrderType.MARKET, "1.5", null)), 422, ErrorCategory.VALIDATION, "quantity");
        assertRejected(() -> validator.validate(order(OrderIntent.BUY, OrderType.MARKET, "10001", null)), 422, ErrorCategory.VALIDATION, "quantity");
    }

    @Test
    void limitPriceMustMatchTheTypeAndTheInstrumentScaleWithoutRounding() {
        assertRejected(() -> validator.validate(order(OrderIntent.BUY, OrderType.LIMIT, "1", null)), 400, ErrorCategory.VALIDATION, "limitPrice");
        assertRejected(() -> validator.validate(order(OrderIntent.BUY, OrderType.MARKET, "1", "10")), 400, ErrorCategory.VALIDATION, "limitPrice");
        assertRejected(() -> validator.validate(order(OrderIntent.BUY, OrderType.LIMIT, "1", "180.005")), 422, ErrorCategory.VALIDATION, "limitPrice");
        assertRejected(() -> validator.validate(order(OrderIntent.BUY, OrderType.LIMIT, "1", "0")), 422, ErrorCategory.VALIDATION, "limitPrice");
        assertThatCode(() -> validator.validate(order(OrderIntent.BUY, OrderType.LIMIT, "1", "180.100000"))).doesNotThrowAnyException();
    }

    @Test
    void notionalIsCheckedAtTheLimitPriceOrTheExecutableQuoteSide() {
        assertRejected(() -> validator.validate(order(OrderIntent.BUY, OrderType.LIMIT, "5001", "200")), 422, ErrorCategory.VALIDATION, null);
        // 5553 * 180.10 (ask) > 1,000,000; the same quantity at the bid for a short is 998,984.70.
        assertRejected(() -> validator.validate(order(OrderIntent.BUY, OrderType.MARKET, "5553", null)), 422, ErrorCategory.VALIDATION, null);
        assertThatCode(() -> validator.validate(order(OrderIntent.SHORT, OrderType.MARKET, "5553", null))).doesNotThrowAnyException();
    }

    @Test
    void marketOrdersNeedAFreshQuote() {
        quote(NOW.minusSeconds(30), false);
        assertRejected(() -> validator.validate(order(OrderIntent.BUY, OrderType.MARKET, "1", null)), 409, ErrorCategory.STALE_MARKET_DATA, null);
        quote(NOW, true);
        assertRejected(() -> validator.validate(order(OrderIntent.BUY, OrderType.MARKET, "1", null)), 409, ErrorCategory.STALE_MARKET_DATA, null);
        assertThatCode(() -> validator.validate(order(OrderIntent.BUY, OrderType.LIMIT, "1", "180"))).doesNotThrowAnyException();
    }

    @Test
    void shortIsBlockedOnlyWhenNotShortable() {
        when(instruments.shortability(any(Instrument.class))).thenReturn(new Shortability(ShortabilityStatus.NOT_SHORTABLE, null, null, NOW));
        assertRejected(() -> validator.validate(order(OrderIntent.SHORT, OrderType.LIMIT, "1", "180")), 422, ErrorCategory.VALIDATION, null);
    }

    @Test
    void sellIsLimitedToTheUnreservedLongPosition() {
        when(positions.signedQuantity("NVDA")).thenReturn(new BigDecimal("5"));
        assertThatCode(() -> validator.validate(order(OrderIntent.SELL, OrderType.LIMIT, "5", "180"))).doesNotThrowAnyException();
        assertRejected(() -> validator.validate(order(OrderIntent.SELL, OrderType.LIMIT, "6", "180")), 422, ErrorCategory.VALIDATION, "quantity");
    }

    @Test
    void noNewOrderWhileAnotherWaitsForConfirmation() {
        Order pending = Order.create(UUID.randomUUID(), "TC-1", "ACC", 1, "NVDA", OrderIntent.BUY, OrderType.LIMIT,
                Quantity.of("1"), Price.of("180"), TimeInForce.DAY, NOW);
        pending.markSubmissionPending(NOW);
        pending.requireConfirmation("R1", "confirm?", 3, NOW);
        orders.save(pending);
        assertRejected(() -> validator.validate(order(OrderIntent.BUY, OrderType.LIMIT, "1", "180")), 409, ErrorCategory.CONFLICT, null);
    }

    @Test
    void anUnavailableBrokerRejectsNewOrders() {
        when(broker.connectionState()).thenReturn(BrokerConnectionState.UNAVAILABLE);
        assertRejected(() -> validator.validate(order(OrderIntent.BUY, OrderType.LIMIT, "1", "180")), 503, ErrorCategory.BROKER_UNAVAILABLE, null);
    }
}
