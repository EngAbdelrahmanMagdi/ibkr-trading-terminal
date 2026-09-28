package com.project.trading.order.application;

import com.project.trading.execution.domain.Execution;
import com.project.trading.order.domain.Order;
import com.project.trading.order.domain.OrderStatus;
import com.project.trading.outbox.application.OutboxMessage;
import com.project.trading.shared.api.ApiFormat;
import com.project.trading.shared.api.Correlation;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;

/**
 * Builds the order and execution domain events (standard envelope, decimal strings, UTC). Events are keyed by
 * {@code accountId:orderId}, so all events of one order stay in order on their topic. They carry no broker data
 * beyond the sanitized reason already stored on the order.
 */
@Component
public class OrderEventFactory {

    public static final String ORDER_EVENTS_TOPIC = "trading.order-events.v1";
    public static final String EXECUTION_EVENTS_TOPIC = "trading.execution-events.v1";
    static final String AGGREGATE_TYPE = "ORDER";
    static final String SOURCE = "trading-core";
    static final int EVENT_VERSION = 1;

    private static final Map<OrderStatus, String> EVENT_TYPES = new EnumMap<>(Map.ofEntries(
            Map.entry(OrderStatus.CREATED, "ORDER_CREATED"),
            Map.entry(OrderStatus.SUBMISSION_PENDING, "ORDER_SUBMISSION_PENDING"),
            Map.entry(OrderStatus.PENDING_CONFIRMATION, "ORDER_CONFIRMATION_REQUIRED"),
            Map.entry(OrderStatus.SUBMITTED, "ORDER_SUBMITTED"),
            Map.entry(OrderStatus.PARTIALLY_FILLED, "ORDER_PARTIALLY_FILLED"),
            Map.entry(OrderStatus.FILLED, "ORDER_FILLED"),
            Map.entry(OrderStatus.CANCEL_PENDING, "ORDER_CANCEL_REQUESTED"),
            Map.entry(OrderStatus.CANCELLED, "ORDER_CANCELLED"),
            Map.entry(OrderStatus.REJECTED, "ORDER_REJECTED"),
            Map.entry(OrderStatus.FAILED, "ORDER_FAILED"),
            Map.entry(OrderStatus.UNKNOWN, "ORDER_STATUS_UNKNOWN")));

    private static final JsonMapper JSON = JsonMapper.builder().build();

    /** The event for one persisted status change of the order (the order holds the state after the change). */
    public OutboxMessage orderEvent(Order order, Order.StatusChange change) {
        String eventType = EVENT_TYPES.get(change.next());
        UUID eventId = UUID.randomUUID();
        ObjectNode envelope = envelope(eventId, eventType, order, change.at());
        ObjectNode payload = envelope.putObject("payload");
        payload.put("clientOrderId", order.clientOrderId());
        payload.put("brokerOrderId", order.brokerOrderId());
        payload.put("status", change.next().name());
        payload.put("previousStatus", change.previous() == null ? null : change.previous().name());
        payload.put("intent", order.intent().name());
        payload.put("brokerSide", order.brokerSide().name());
        payload.put("orderType", order.orderType().name());
        payload.put("quantity", ApiFormat.decimal(order.quantity().value(), 0));
        payload.put("filledQuantity", ApiFormat.decimal(order.filledQuantity().value(), 0));
        payload.put("limitPrice", order.limitPrice() == null ? null : ApiFormat.decimal(order.limitPrice().value(), 2));
        payload.put("averageFillPrice", order.averageFillPrice() == null ? null
                : ApiFormat.decimal(order.averageFillPrice().value(), 2));
        payload.put("timeInForce", order.timeInForce().name());
        payload.put("reason", reason(order, change.next()));
        return message(eventId, ORDER_EVENTS_TOPIC, eventType, order, envelope);
    }

    /** The event for an execution recorded for the order. */
    public OutboxMessage executionRecorded(Order order, Execution execution) {
        UUID eventId = UUID.randomUUID();
        String eventType = "EXECUTION_RECORDED";
        ObjectNode envelope = envelope(eventId, eventType, order, execution.executedAt());
        ObjectNode payload = envelope.putObject("payload");
        payload.put("executionId", execution.id().toString());
        payload.put("brokerExecutionId", execution.brokerExecutionId());
        payload.put("side", execution.side().name());
        payload.put("quantity", ApiFormat.decimal(execution.quantity().value(), 0));
        payload.put("price", ApiFormat.decimal(execution.price().value(), 2));
        payload.put("commission", ApiFormat.decimal(execution.commission(), 2));
        payload.put("currency", execution.currency());
        payload.put("executedAt", ApiFormat.instant(execution.executedAt()));
        return message(eventId, EXECUTION_EVENTS_TOPIC, eventType, order, envelope);
    }

    private static ObjectNode envelope(UUID eventId, String eventType, Order order, java.time.Instant occurredAt) {
        ObjectNode envelope = JSON.createObjectNode();
        envelope.put("eventId", eventId.toString());
        envelope.put("eventType", eventType);
        envelope.put("eventVersion", EVENT_VERSION);
        envelope.put("occurredAt", ApiFormat.instant(occurredAt));
        envelope.put("source", SOURCE);
        envelope.put("correlationId", Correlation.current());
        envelope.put("accountId", order.accountId());
        envelope.put("orderId", order.id().toString());
        envelope.put("symbol", order.symbol());
        return envelope;
    }

    private static OutboxMessage message(UUID eventId, String topic, String eventType, Order order, ObjectNode envelope) {
        return new OutboxMessage(eventId, AGGREGATE_TYPE, order.id(), topic, order.accountId() + ":" + order.id(),
                eventType, EVENT_VERSION, JSON.writeValueAsString(envelope));
    }

    private static String reason(Order order, OrderStatus status) {
        return switch (status) {
            case PENDING_CONFIRMATION -> order.replyMessage();
            case REJECTED, FAILED -> order.rejectionReason();
            default -> null;
        };
    }
}
