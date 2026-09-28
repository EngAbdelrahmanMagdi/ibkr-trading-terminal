package com.project.trading.broker.infrastructure.ibkr;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.project.trading.broker.domain.BrokerOrderUpdate;
import com.project.trading.broker.domain.BrokerOrderUpdateHandler;
import com.project.trading.broker.domain.OpenBrokerOrder;
import com.project.trading.broker.domain.OpenOrderSource;
import com.project.trading.broker.domain.WorkingBrokerOrder;
import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.project.trading.broker.infrastructure.ibkr.IbkrStubs.ACCOUNT;
import static com.project.trading.broker.infrastructure.ibkr.IbkrStubs.API;
import static org.assertj.core.api.Assertions.assertThat;

/** Polling advances only this application's working orders, with positive observations only. */
class IbkrOrderPollerTest {

    @RegisterExtension
    static WireMockExtension wm = WireMockExtension.newInstance().options(wireMockConfig().dynamicPort()).build();

    private static final String LIVE_ORDERS = API + "/iserver/account/orders";
    private static final String TRADES = API + "/iserver/account/trades";

    /** The system of record, reduced to what the poller reads; fills applied by the handler update it. */
    private final List<WorkingBrokerOrder> working = new ArrayList<>();
    private final List<BrokerOrderUpdate> applied = new ArrayList<>();
    private final Set<String> executions = new HashSet<>();
    /** Simulated time between polls (the live-orders endpoint allows one request per 5 s). */
    private final AtomicLong elapsed = new AtomicLong();
    private IbkrOrderPoller poller;

    @BeforeEach
    void setUp() {
        IbkrRateLimiter limiter = new IbkrRateLimiter(50, 16, Duration.ofSeconds(2), Duration.ofMinutes(15),
                () -> System.nanoTime() + elapsed.get(), new SimpleMeterRegistry());
        IbkrHttp http = IbkrStubs.http(wm, limiter);
        ReplyGate replies = new ReplyGate(Clock.systemUTC(), Duration.ofSeconds(30));
        IbkrSession session = new IbkrSession(http, ACCOUNT, replies, Duration.ofMinutes(1), Duration.ofSeconds(2),
                System::nanoTime);
        OpenOrderSource source = new OpenOrderSource() {
            @Override
            public List<OpenBrokerOrder> openLimitOrders() {
                return List.of();
            }

            @Override
            public List<WorkingBrokerOrder> workingOrders(int limit) {
                return List.copyOf(working);
            }
        };
        BrokerOrderUpdateHandler handler = update -> {
            if (update instanceof BrokerOrderUpdate.Fill fill) {
                if (!executions.add(fill.brokerExecutionId())) {
                    return BrokerOrderUpdateHandler.Outcome.IGNORED;
                }
                WorkingBrokerOrder o = working.removeFirst();
                working.add(new WorkingBrokerOrder(o.brokerOrderId(), o.clientOrderId(), o.symbol(), o.side(),
                        o.quantity(), o.filledQuantity().plus(fill.quantity())));
            }
            applied.add(update);
            return BrokerOrderUpdateHandler.Outcome.APPLIED;
        };
        poller = new IbkrOrderPoller(http, session, replies, source, handler, "USD", Duration.ofSeconds(5), Clock.systemUTC());
        IbkrStubs.readySession(wm);
    }

    private void pollAfterInterval() {
        elapsed.addAndGet(Duration.ofSeconds(5).toNanos());
        poller.poll();
    }

    private void workingOrder(String brokerOrderId, String clientOrderId, String qty) {
        working.add(new WorkingBrokerOrder(brokerOrderId, clientOrderId, "NVDA", BrokerSide.BUY, Quantity.of(qty), Quantity.ZERO));
    }

    private static void liveOrder(long orderId, String orderRef, String status, String filled) {
        wm.stubFor(get(urlEqualTo(LIVE_ORDERS)).willReturn(okJson("""
                {"orders":[{"acct":"%s","conid":4815747,"orderId":%d,"ticker":"NVDA","side":"BUY","status":"%s",
                 "filledQuantity":%s,"remainingQuantity":0.0,"orderType":"Limit","order_ref":"%s"},
                 {"acct":"%s","conid":265598,"orderId":42,"ticker":"AAPL","status":"Cancelled","filledQuantity":0.0,
                 "order_ref":"placed-elsewhere"}],"snapshot":true}""".formatted(ACCOUNT, orderId, status, filled, orderRef, ACCOUNT))));
    }

    private static void trades(String json) {
        wm.stubFor(get(urlEqualTo(TRADES)).willReturn(okJson(json)));
    }

    private static final String ONE_TRADE = """
            [{"execution_id":"0000e0d5.6576a7f5.01.01","symbol":"NVDA","side":"B","order_ref":"TC-1","size":10.0,
              "price":"185.25","commission":"1.00","trade_time_r":1790582400000,"account":"DU1234567","conid":4815747},
             {"execution_id":"0000e0d5.1111.01.01","symbol":"AAPL","side":"B","order_ref":"placed-elsewhere",
              "size":5.0,"price":"230.00","commission":"1.00","trade_time_r":1790582400000}]""";

    @Test
    void aFillIsAppliedOnceAcrossPolls() {
        workingOrder("1001", "TC-1", "10");
        liveOrder(1001, "TC-1", "Filled", "10.0");
        trades(ONE_TRADE);

        poller.poll();
        pollAfterInterval();

        assertThat(applied).containsExactly(new BrokerOrderUpdate.Fill("1001", "0000e0d5.6576a7f5.01.01",
                BrokerSide.BUY, Quantity.of("10"), Price.of("185.25"), new java.math.BigDecimal("1.00"), "USD",
                Instant.ofEpochMilli(1790582400000L)));
        wm.verify(1, getRequestedFor(urlEqualTo(TRADES)));
    }

    @Test
    void aCancellationIsAppliedOnlyAfterTheReportedFills() {
        workingOrder("1001", "TC-1", "20");
        liveOrder(1001, "TC-1", "Cancelled", "10.0");
        trades("[]");

        poller.poll();
        assertThat(applied).isEmpty();

        trades(ONE_TRADE);
        pollAfterInterval();
        assertThat(applied).hasSize(2);
        assertThat(applied.get(0)).isInstanceOf(BrokerOrderUpdate.Fill.class);
        assertThat(applied.get(1)).isInstanceOf(BrokerOrderUpdate.Cancelled.class);
    }

    @Test
    void anOrderMissingFromTheBrokerListIsNeverTreatedAsCancelled() {
        workingOrder("1001", "TC-1", "10");
        wm.stubFor(get(urlEqualTo(LIVE_ORDERS)).willReturn(okJson("{\"orders\":[],\"snapshot\":true}")));

        poller.poll();

        assertThat(applied).isEmpty();
        wm.verify(0, getRequestedFor(urlEqualTo(TRADES)));
    }

    @Test
    void withoutWorkingOrdersNothingIsRequested() {
        poller.poll();

        assertThat(wm.findAll(anyRequestedFor(anyUrl()))).isEmpty();
    }
}
