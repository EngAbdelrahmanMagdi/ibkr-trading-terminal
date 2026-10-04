package com.project.trading.broker.infrastructure.ibkr;

import com.project.trading.broker.domain.BrokerConnectionState;
import com.project.trading.broker.domain.BrokerOrderRequest;
import com.project.trading.broker.domain.BrokerTradingPort;
import com.project.trading.broker.domain.CancelResult;
import com.project.trading.broker.domain.SubmitResult;
import com.project.trading.shared.domain.OrderType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

/**
 * Places, confirms and cancels IBKR paper orders through the Client Portal Gateway.
 * <ul>
 *   <li>POST /iserver/account/{accountId}/orders: the answer is an acknowledgement ({@code order_id}), an order
 *   reply that needs explicit confirmation ({@code id}, {@code message}), or {@code {"error": ...}} with HTTP 200
 *   for a rejection.</li>
 *   <li>POST /iserver/reply/{replyId} with {@code {"confirmed": true}}: may answer with another reply (a chain).
 *   Declining sends nothing: an unconfirmed order is never transmitted.</li>
 *   <li>DELETE /iserver/account/{accountId}/order/{orderId}: success only means the request was received.</li>
 * </ul>
 * Order commands are serialized (IBKR requires each order to be fully acknowledged before the next one) and a new
 * order is refused while a reply is outstanding. A request that may have reached IBKR but has no readable answer is
 * reported as unknown and never sent again. Checked 2026-09-28 against https://www.interactivebrokers.com/docs/web-api/
 * (Place Order, Place Order Reply Confirmation, Cancel Order, Order Error Details).
 */
final class IbkrTradingAdapter implements BrokerTradingPort {

    private static final Logger log = LoggerFactory.getLogger(IbkrTradingAdapter.class);

    private static final Pattern REPLY_ID = Pattern.compile("^[A-Za-z0-9-]{1,128}$");
    private static final Pattern BROKER_ORDER_ID = Pattern.compile("^[A-Za-z0-9-]{1,64}$");
    private static final int MAX_REPLY_MESSAGE = 2000;
    private static final int MAX_REASON = 500;

    private final IbkrHttp http;
    private final IbkrSession session;
    private final ReplyGate replies;
    private final String accountId;
    private final long lockTimeout;
    private final ReentrantLock orderCommands = new ReentrantLock(true);

    IbkrTradingAdapter(IbkrHttp http, IbkrSession session, ReplyGate replies, String accountId, Duration lockTimeout) {
        this.http = http;
        this.session = session;
        this.replies = replies;
        this.accountId = accountId;
        this.lockTimeout = lockTimeout.toNanos();
    }

    @Override
    public BrokerConnectionState connectionState() {
        return http.isCoolingDown() ? BrokerConnectionState.UNAVAILABLE : session.state();
    }

    @Override
    public SubmitResult submit(BrokerOrderRequest request) {
        if (!lock()) {
            return new SubmitResult.Failed("another order command is in progress; the order was not sent");
        }
        try {
            if (replies.isOpen()) {
                return new SubmitResult.Failed("another order is waiting for confirmation; the order was not sent");
            }
            if (!accountId.equals(request.accountId())) {
                return new SubmitResult.Failed("the order belongs to a different account; the order was not sent");
            }
            if (session.state() != BrokerConnectionState.READY) {
                return new SubmitResult.Failed("the broker is not available; the order was not sent");
            }
            IbkrHttp.Response response;
            try {
                response = http.post(IbkrEndpoint.PLACE_ORDER, "/iserver/account/" + accountId + "/orders",
                        orderTicket(request));
            } catch (IbkrHttp.CallException e) {
                return notAnswered(e, "order " + request.orderId());
            }
            SubmitResult result = interpret(response);
            log.info("IBKR order submission for order {}: {} (HTTP {})", request.orderId(),
                    result.getClass().getSimpleName(), response.status());
            return result;
        } finally {
            orderCommands.unlock();
        }
    }

    @Override
    public SubmitResult confirmReply(String replyId, boolean confirm) {
        if (!confirm) {
            replies.close(replyId);
            return new SubmitResult.Rejected("declined by the user");
        }
        if (replyId == null || !REPLY_ID.matcher(replyId).matches()) {
            return new SubmitResult.Rejected("the confirmation request is not valid");
        }
        if (!lock()) {
            return new SubmitResult.Failed("another order command is in progress; the confirmation was not sent");
        }
        try {
            ObjectNode body = IbkrJson.MAPPER.createObjectNode().put("confirmed", true);
            IbkrHttp.Response response;
            try {
                response = http.post(IbkrEndpoint.REPLY, "/iserver/reply/" + replyId, body);
            } catch (IbkrHttp.CallException e) {
                replies.close(replyId);
                return notAnswered(e, "reply");
            }
            replies.close(replyId);
            SubmitResult result = interpret(response);
            log.info("IBKR confirmation answered: {} (HTTP {})", result.getClass().getSimpleName(), response.status());
            return result;
        } finally {
            orderCommands.unlock();
        }
    }

    @Override
    public CancelResult cancel(String brokerOrderId) {
        if (brokerOrderId == null || !BROKER_ORDER_ID.matcher(brokerOrderId).matches()) {
            return new CancelResult.Rejected("the broker order ID is not valid");
        }
        if (!lock()) {
            return new CancelResult.Rejected("another order command is in progress; try again");
        }
        try {
            if (replies.isOpen()) {
                return new CancelResult.Rejected("another order is waiting for confirmation; answer it first");
            }
            if (session.state() != BrokerConnectionState.READY) {
                return new CancelResult.Rejected("the broker is not available; the cancellation was not sent");
            }
            IbkrHttp.Response response;
            try {
                response = http.delete(IbkrEndpoint.CANCEL_ORDER, "/iserver/account/" + accountId + "/order/" + brokerOrderId);
            } catch (IbkrHttp.CallException e) {
                return e.kind() == IbkrHttp.CallException.Kind.NOT_SENT
                        ? new CancelResult.Rejected("the cancellation could not be sent: " + e.getMessage())
                        : new CancelResult.Unknown(e.getMessage());
            }
            JsonNode body = response.body();
            if (response.ok() && body.has("msg") && !body.has("error")) {
                return new CancelResult.Requested();
            }
            if (response.status() == 429 || response.status() == 401 || response.status() == 403) {
                if (response.status() != 429) {
                    session.invalidate();
                }
                return new CancelResult.Rejected("the broker did not accept the request (HTTP " + response.status() + ")");
            }
            if (body.path("error").isString()) {
                return new CancelResult.Rejected(IbkrJson.sanitize(body.path("error").stringValue(), MAX_REASON));
            }
            return new CancelResult.Unknown("unexpected broker response (HTTP " + response.status() + ")");
        } finally {
            orderCommands.unlock();
        }
    }

    private boolean lock() {
        try {
            return orderCommands.tryLock(lockTimeout, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private SubmitResult notAnswered(IbkrHttp.CallException e, String what) {
        if (e.kind() == IbkrHttp.CallException.Kind.NOT_SENT) {
            return new SubmitResult.Failed(e.getMessage() + "; nothing was sent");
        }
        log.warn("IBKR outcome unknown for {} category={}", what, e.kind());
        return new SubmitResult.Unknown(e.getMessage());
    }

    /** The order ticket: {"orders": [{acctId, conid, cOID, orderType, side, tif, quantity, price?}]}. */
    private static ObjectNode orderTicket(BrokerOrderRequest r) {
        ObjectNode order = IbkrJson.MAPPER.createObjectNode()
                .put("acctId", r.accountId())
                .put("conid", r.conid())
                .put("cOID", r.clientOrderId())
                .put("orderType", r.orderType() == OrderType.LIMIT ? "LMT" : "MKT")
                .put("side", r.side().name())
                .put("tif", r.timeInForce().name())
                .put("quantity", r.quantity().value().toBigIntegerExact());
        if (r.limitPrice() != null) {
            order.put("price", r.limitPrice().value().stripTrailingZeros());
        }
        ObjectNode ticket = IbkrJson.MAPPER.createObjectNode();
        ArrayNode orders = ticket.putArray("orders");
        orders.add(order);
        return ticket;
    }

    /** Maps an answer of the order or reply endpoint. Opens the reply gate when IBKR asks for a confirmation. */
    private SubmitResult interpret(IbkrHttp.Response response) {
        int status = response.status();
        JsonNode body = response.body();
        if (status == 429) {
            return new SubmitResult.Failed("the broker rate limit was reached; nothing was sent");
        }
        if (status == 401 || status == 403) {
            session.invalidate();
            return new SubmitResult.Failed("the broker session is not authenticated; nothing was sent");
        }
        if (status == 400) {
            String error = body.path("error").isString() ? body.path("error").stringValue() : null;
            return new SubmitResult.Rejected(error == null ? "the broker refused the order request"
                    : IbkrJson.sanitize(error, MAX_REASON));
        }
        if (!response.ok()) {
            return new SubmitResult.Unknown("unexpected broker status " + status);
        }
        if (body.isObject() && body.path("error").isString()) {
            return new SubmitResult.Rejected(IbkrJson.sanitize(body.path("error").stringValue(), MAX_REASON));
        }
        if (!body.isArray()) {
            return new SubmitResult.Unknown("unexpected broker response");
        }
        for (JsonNode element : body) {
            String replyId = IbkrJson.id(element.get("id"));
            if (replyId != null && element.has("message")) {
                if (!REPLY_ID.matcher(replyId).matches()) {
                    return new SubmitResult.Unknown("unexpected broker confirmation request");
                }
                replies.open(replyId);
                log.info("IBKR asks for confirmation (message IDs {})", messageIds(element));
                return new SubmitResult.ConfirmationRequired(replyId, replyMessage(element.get("message")));
            }
        }
        for (JsonNode element : body) {
            String orderId = IbkrJson.id(element.get("order_id"));
            if (orderId != null) {
                return BROKER_ORDER_ID.matcher(orderId).matches() ? new SubmitResult.Accepted(orderId, List.of())
                        : new SubmitResult.Unknown("unexpected broker order ID");
            }
        }
        for (JsonNode element : body) {
            if (element.path("error").isString()) {
                return new SubmitResult.Rejected(IbkrJson.sanitize(element.path("error").stringValue(), MAX_REASON));
            }
        }
        return new SubmitResult.Unknown("unexpected broker response");
    }

    private static String replyMessage(JsonNode message) {
        List<String> parts = new ArrayList<>();
        if (message != null && message.isArray()) {
            for (JsonNode m : message) {
                if (m.isString()) {
                    parts.add(m.stringValue());
                }
            }
        } else if (message != null && message.isString()) {
            parts.add(message.stringValue());
        }
        String text = IbkrJson.sanitize(String.join("\n", parts), MAX_REPLY_MESSAGE);
        return text == null || text.isEmpty() ? "The broker asks for confirmation of this order." : text;
    }

    private static List<String> messageIds(JsonNode element) {
        List<String> ids = new ArrayList<>();
        for (JsonNode id : element.path("messageIds")) {
            if (id.isString() && ids.size() < 10) {
                ids.add(IbkrJson.sanitize(id.stringValue(), 16));
            }
        }
        return ids;
    }
}
