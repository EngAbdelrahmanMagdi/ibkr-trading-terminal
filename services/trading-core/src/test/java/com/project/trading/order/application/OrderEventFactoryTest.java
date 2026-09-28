package com.project.trading.order.application;

import com.project.trading.execution.domain.Execution;
import com.project.trading.order.domain.Order;
import com.project.trading.order.domain.OrderIntent;
import com.project.trading.order.domain.OrderStatus;
import com.project.trading.outbox.application.OutboxMessage;
import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.shared.domain.OrderType;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import com.project.trading.shared.domain.TimeInForce;
import com.project.trading.support.ContractSchemas;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Every event the order module can emit conforms to the published event contracts. */
class OrderEventFactoryTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Instant AT = Instant.parse("2026-09-28T10:00:00.123456Z");
    private final OrderEventFactory factory = new OrderEventFactory();

    private static Order order(OrderStatus status) {
        boolean filled = status == OrderStatus.FILLED || status == OrderStatus.PARTIALLY_FILLED;
        return new Order(UUID.randomUUID(), "TC-0A1B2C3D", status == OrderStatus.SUBMISSION_PENDING ? null : "B-77",
                "DU1234567", 4815747, "NVDA", OrderIntent.SHORT, BrokerSide.SELL, OrderType.LIMIT, Quantity.of("10"),
                filled ? Quantity.of(status == OrderStatus.FILLED ? "10" : "4") : Quantity.ZERO, Price.of("185.5"),
                filled ? Price.of("185.51") : null, TimeInForce.GTC, status,
                status == OrderStatus.PENDING_CONFIRMATION ? "R-1" : null,
                status == OrderStatus.PENDING_CONFIRMATION ? "Price exceeds the percentage constraint" : null, 1,
                status == OrderStatus.REJECTED || status == OrderStatus.FAILED ? "rejected by the broker" : null,
                AT, AT, AT, 3);
    }

    @ParameterizedTest
    @EnumSource(value = OrderStatus.class, names = "CREATED", mode = EnumSource.Mode.EXCLUDE)
    void orderEventsConformToTheContract(OrderStatus status) {
        Order order = order(status);

        OutboxMessage message = factory.orderEvent(order, new Order.StatusChange(OrderStatus.SUBMITTED, status, AT));

        JsonNode event = JSON.readTree(message.envelopeJson());
        ContractSchemas.assertValid("events/trading-order-event.schema.json", event);
        assertThat(message.topic()).isEqualTo("trading.order-events.v1");
        assertThat(message.key()).isEqualTo("DU1234567:" + order.id());
        assertThat(event.get("eventId").stringValue()).isEqualTo(message.eventId().toString());
        assertThat(event.get("payload").get("status").stringValue()).isEqualTo(status.name());
    }

    @Test
    void executionEventsConformToTheContract() {
        Order order = order(OrderStatus.FILLED);
        Execution execution = new Execution(UUID.randomUUID(), order.id(), "0000e0d5.6576a7f5.01.01", "NVDA",
                BrokerSide.SELL, Quantity.of("10"), Price.of("185.51"), new BigDecimal("1.0000"), "USD", AT);

        OutboxMessage message = factory.executionRecorded(order, execution);

        ContractSchemas.assertValid("events/trading-execution-event.schema.json", JSON.readTree(message.envelopeJson()));
        assertThat(message.topic()).isEqualTo("trading.execution-events.v1");
        assertThat(message.key()).isEqualTo("DU1234567:" + order.id());
    }
}
