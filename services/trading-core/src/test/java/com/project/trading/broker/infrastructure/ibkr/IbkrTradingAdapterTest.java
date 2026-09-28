package com.project.trading.broker.infrastructure.ibkr;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.project.trading.broker.domain.BrokerConnectionState;
import com.project.trading.broker.domain.BrokerOrderRequest;
import com.project.trading.broker.domain.CancelResult;
import com.project.trading.broker.domain.SubmitResult;
import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.shared.domain.OrderType;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import com.project.trading.shared.domain.TimeInForce;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Clock;
import java.time.Duration;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.project.trading.broker.infrastructure.ibkr.IbkrStubs.ACCOUNT;
import static com.project.trading.broker.infrastructure.ibkr.IbkrStubs.API;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Order placement, reply chains, rejections, uncertain outcomes and cancellation against IBKR fixtures. */
class IbkrTradingAdapterTest {

    @RegisterExtension
    static WireMockExtension wm = WireMockExtension.newInstance().options(wireMockConfig().dynamicPort()).build();

    private static final String ORDERS = API + "/iserver/account/" + ACCOUNT + "/orders";

    private IbkrSession session;
    private IbkrTradingAdapter adapter;

    @BeforeEach
    void setUp() {
        build(Duration.ofSeconds(5));
    }

    private void build(Duration requestTimeout) {
        IbkrHttp http = IbkrStubs.http(wm, IbkrStubs.limiter(), requestTimeout);
        ReplyGate replies = new ReplyGate(Clock.systemUTC(), Duration.ofSeconds(30));
        session = new IbkrSession(http, ACCOUNT, replies, Duration.ofMinutes(1), Duration.ofSeconds(2), System::nanoTime);
        adapter = new IbkrTradingAdapter(http, session, replies, ACCOUNT, Duration.ofSeconds(2));
    }

    private static BrokerOrderRequest limit(BrokerSide side, String qty, String price) {
        return new BrokerOrderRequest(UUID.randomUUID(), "TC-ABC123", ACCOUNT, 4815747, "NVDA", side, OrderType.LIMIT,
                Quantity.of(qty), Price.of(price), TimeInForce.DAY);
    }

    @Test
    void anAcknowledgedOrderIsAcceptedAndTheTicketMatchesTheDocumentedShape() {
        IbkrStubs.readySession(wm);
        wm.stubFor(post(urlEqualTo(ORDERS)).willReturn(okJson(
                "[{\"order_id\":\"1234567890\",\"order_status\":\"Submitted\",\"encrypt_message\":\"1\"}]")));

        SubmitResult result = adapter.submit(limit(BrokerSide.BUY, "10", "185.50"));

        assertThat(result).isEqualTo(new SubmitResult.Accepted("1234567890", java.util.List.of()));
        wm.verify(postRequestedFor(urlEqualTo(ORDERS)).withRequestBody(equalToJson("""
                {"orders":[{"acctId":"DU1234567","conid":4815747,"cOID":"TC-ABC123","orderType":"LMT",
                 "side":"BUY","tif":"DAY","quantity":10,"price":185.5}]}""")));
    }

    @Test
    void aChainedReplyIsConfirmedStepByStepAndBlocksOtherOrdersMeanwhile() {
        IbkrStubs.readySession(wm);
        wm.stubFor(post(urlEqualTo(ORDERS)).willReturn(okJson("""
                [{"id":"07a13a5a-4a48-44a5-bb25-5ab37b79186c","message":["The following order <b>BUY 10 NVDA</b> price exceeds \\nthe Percentage constraint of 3%.\\nAre you sure you want to submit this order?"],"isSuppressed":false,"messageIds":["o163"]}]""")));
        wm.stubFor(post(urlEqualTo(API + "/iserver/reply/07a13a5a-4a48-44a5-bb25-5ab37b79186c"))
                .withRequestBody(equalToJson("{\"confirmed\":true}"))
                .willReturn(okJson("[{\"id\":\"r-2\",\"message\":[\"Order value exceeds the limit.\"],\"isSuppressed\":false,\"messageIds\":[\"o354\"]}]")));
        wm.stubFor(post(urlEqualTo(API + "/iserver/reply/r-2")).willReturn(okJson(
                "[{\"order_id\":\"1234567891\",\"order_status\":\"PreSubmitted\"}]")));

        SubmitResult first = adapter.submit(limit(BrokerSide.BUY, "10", "185.50"));
        assertThat(first).isInstanceOf(SubmitResult.ConfirmationRequired.class);
        assertThat(((SubmitResult.ConfirmationRequired) first).message())
                .contains("BUY 10 NVDA").contains("Percentage constraint").doesNotContain("<b>");

        assertThat(adapter.submit(limit(BrokerSide.BUY, "1", "185.50"))).isInstanceOf(SubmitResult.Failed.class);
        assertThat(adapter.cancel("999")).isInstanceOf(CancelResult.Rejected.class);

        SubmitResult second = adapter.confirmReply("07a13a5a-4a48-44a5-bb25-5ab37b79186c", true);
        assertThat(second).isEqualTo(new SubmitResult.ConfirmationRequired("r-2", "Order value exceeds the limit."));
        assertThat(adapter.confirmReply("r-2", true)).isEqualTo(new SubmitResult.Accepted("1234567891", java.util.List.of()));
        wm.verify(1, postRequestedFor(urlEqualTo(ORDERS)));
    }

    @Test
    void declineSendsNothingAndReleasesTheNextOrder() {
        IbkrStubs.readySession(wm);
        wm.stubFor(post(urlEqualTo(ORDERS)).willReturn(okJson(
                "[{\"id\":\"r-1\",\"message\":[\"Confirm?\"],\"isSuppressed\":false,\"messageIds\":[\"o163\"]}]")));
        assertThat(adapter.submit(limit(BrokerSide.BUY, "10", "185.50"))).isInstanceOf(SubmitResult.ConfirmationRequired.class);

        assertThat(adapter.confirmReply("r-1", false)).isEqualTo(new SubmitResult.Rejected("declined by the user"));

        wm.verify(0, postRequestedFor(urlEqualTo(API + "/iserver/reply/r-1")));
        assertThat(adapter.submit(limit(BrokerSide.BUY, "10", "185.50"))).isInstanceOf(SubmitResult.ConfirmationRequired.class);
    }

    @Test
    void aBrokerRejectionReturnedWithStatus200IsRejectedWithTheSanitizedReason() {
        IbkrStubs.readySession(wm);
        wm.stubFor(post(urlEqualTo(ORDERS)).willReturn(okJson(
                "{\"error\":\"Order rejected - reason: <i>Short sale</i> is not available for this contract.\"}")));

        SubmitResult result = adapter.submit(limit(BrokerSide.SELL, "5", "190.00"));

        assertThat(result).isEqualTo(new SubmitResult.Rejected(
                "Order rejected - reason: Short sale is not available for this contract."));
    }

    @Test
    void aTimeoutAfterSendingIsUnknownAndTheOrderIsSentExactlyOnce() {
        IbkrStubs.readySession(wm);
        session.state(); // warms up the stub server so only the order request can exceed the short timeout
        build(Duration.ofMillis(500));
        wm.stubFor(post(urlEqualTo(ORDERS)).willReturn(okJson("[{\"order_id\":\"1\"}]").withFixedDelay(1500)));

        SubmitResult result = adapter.submit(limit(BrokerSide.BUY, "10", "185.50"));

        assertThat(result).isInstanceOf(SubmitResult.Unknown.class);
        wm.verify(1, postRequestedFor(urlEqualTo(ORDERS)));
    }

    @Test
    void anUnreadableAnswerAfterSendingIsUnknownNotFailed() {
        IbkrStubs.readySession(wm);
        wm.stubFor(post(urlEqualTo(ORDERS)).willReturn(IbkrStubs.json(200, "[{\"order_id\":")));

        assertThat(adapter.submit(limit(BrokerSide.BUY, "10", "185.50"))).isInstanceOf(SubmitResult.Unknown.class);
    }

    @Test
    void aLostSessionRefusesOrdersBeforeAnythingIsSent() {
        wm.stubFor(post(urlEqualTo(API + "/iserver/auth/status")).willReturn(okJson(
                "{\"authenticated\":false,\"competing\":false,\"connected\":false,\"message\":\"\"}")));

        assertThat(adapter.connectionState()).isEqualTo(BrokerConnectionState.UNAVAILABLE);
        assertThat(adapter.submit(limit(BrokerSide.BUY, "10", "185.50"))).isInstanceOf(SubmitResult.Failed.class);
        wm.verify(0, postRequestedFor(urlEqualTo(ORDERS)));
    }

    @Test
    void a429IsNotSentAndPausesEveryFurtherRequest() {
        IbkrStubs.readySession(wm);
        session.state();
        wm.stubFor(post(urlEqualTo(ORDERS)).willReturn(IbkrStubs.json(429, "")));

        assertThat(adapter.submit(limit(BrokerSide.BUY, "10", "185.50"))).isInstanceOf(SubmitResult.Failed.class);
        int requests = wm.findAll(anyRequestedFor(anyUrl())).size();

        assertThat(adapter.connectionState()).isEqualTo(BrokerConnectionState.UNAVAILABLE);
        session.invalidate();
        assertThat(adapter.submit(limit(BrokerSide.BUY, "10", "185.50"))).isInstanceOf(SubmitResult.Failed.class);
        assertThat(wm.findAll(anyRequestedFor(anyUrl()))).hasSize(requests);
    }

    @Test
    void cancellationIsOnlyRequestedAndABrokerErrorIsARejection() {
        IbkrStubs.readySession(wm);
        wm.stubFor(delete(urlEqualTo(API + "/iserver/account/" + ACCOUNT + "/order/1001")).willReturn(okJson(
                "{\"msg\":\"Request was submitted\",\"order_id\":1001,\"conid\":4815747,\"account\":\"" + ACCOUNT + "\"}")));
        wm.stubFor(delete(urlEqualTo(API + "/iserver/account/" + ACCOUNT + "/order/1")).willReturn(okJson(
                "{\"error\":\"OrderID 1 doesn't exist\"}")));

        assertThat(adapter.cancel("1001")).isEqualTo(new CancelResult.Requested());
        assertThat(adapter.cancel("1")).isEqualTo(new CancelResult.Rejected("OrderID 1 doesn't exist"));
    }

    @Test
    void onlyAPaperSessionWithTheConfiguredAccountIsReady() {
        IbkrStubs.readySession(wm, false, ACCOUNT);
        assertThat(adapter.connectionState()).isEqualTo(BrokerConnectionState.UNAVAILABLE);
        assertThatThrownBy(session::verifyAtStartup).hasMessageContaining("not a paper trading session");
        wm.verify(0, postRequestedFor(urlEqualTo(ORDERS)));

        IbkrStubs.readySession(wm, true, "DU7654321");
        session.invalidate();
        assertThat(adapter.connectionState()).isEqualTo(BrokerConnectionState.UNAVAILABLE);
        assertThat(adapter.submit(limit(BrokerSide.BUY, "10", "185.50"))).isInstanceOf(SubmitResult.Failed.class);
        wm.verify(0, postRequestedFor(urlEqualTo(ORDERS)));
    }
}
