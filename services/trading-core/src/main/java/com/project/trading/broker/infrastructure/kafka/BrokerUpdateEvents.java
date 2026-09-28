package com.project.trading.broker.infrastructure.kafka;

import com.project.trading.broker.domain.BrokerOrderUpdate;
import com.project.trading.broker.domain.ObservedStatus;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Reads broker order observations (topic broker.order-updates.v1, contract events/broker-order-update.schema.json)
 * into broker-neutral updates. Unknown fields are ignored; anything that does not fit the contract is rejected with a
 * reason, never guessed.
 */
final class BrokerUpdateEvents {

    /** A readable event: its ID and the updates it carries. */
    record Parsed(UUID eventId, List<BrokerOrderUpdate> updates) {
    }

    /** Rejection reasons (metric label values). */
    static final String INVALID = "invalid";
    static final String OTHER_ACCOUNT = "other_account";
    static final String UNKNOWN_TYPE = "unknown_type";

    /** An event that is skipped, with the reason. */
    static final class Rejected extends Exception {
        private static final long serialVersionUID = 1L;
        private final String reason;

        Rejected(String reason, String detail) {
            super(detail);
            this.reason = reason;
        }

        String reason() {
            return reason;
        }
    }

    private static final JsonMapper JSON = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).build();
    private static final Pattern IDENTIFIER = Pattern.compile("^[A-Za-z0-9._:-]{1,64}$");

    private final String accountId;
    private final String currency;

    BrokerUpdateEvents(String accountId, String currency) {
        this.accountId = accountId;
        this.currency = currency;
    }

    Parsed parse(String value) throws Rejected {
        JsonNode event;
        try {
            event = JSON.readTree(value);
        } catch (JacksonException e) {
            throw new Rejected(INVALID, "not JSON");
        }
        UUID eventId = uuid(event.path("eventId"));
        if (!"realtime-gateway".equals(text(event.path("source"))) || event.path("eventVersion").asInt(0) != 1) {
            throw new Rejected(INVALID, "unexpected source or version");
        }
        String type = text(event.path("eventType"));
        if (!accountId.equals(text(event.path("accountId")))) {
            throw new Rejected(OTHER_ACCOUNT, "event for another account");
        }
        JsonNode p = event.path("payload");
        String brokerOrderId = identifier(p.path("brokerOrderId"), true);
        String clientOrderRef = identifier(p.path("clientOrderRef"), false);
        ObservedStatus status = status(p.path("observedStatus"));
        Quantity filled = quantity(p.path("filledQuantity"));
        String raw = text(p.path("brokerStatusRaw"));
        Instant sourceTime = instant(p.path("sourceTimestamp"));
        if (raw == null || raw.isBlank() || raw.length() > 64) {
            throw new Rejected(INVALID, "brokerStatusRaw");
        }
        List<BrokerOrderUpdate> updates = new ArrayList<>(2);
        switch (type == null ? "" : type) {
            case "BROKER_EXECUTION_OBSERVED" -> {
                JsonNode e = p.path("execution");
                if (!e.isObject()) {
                    throw new Rejected(INVALID, "execution missing");
                }
                Quantity quantity = quantity(e.path("quantity"));
                if (quantity.isZero()) {
                    throw new Rejected(INVALID, "execution quantity");
                }
                updates.add(new BrokerOrderUpdate.Fill(brokerOrderId, identifier(e.path("brokerExecutionId"), true),
                        null, quantity, price(e.path("price")), null, currency, instant(e.path("executedAt")),
                        clientOrderRef));
            }
            case "BROKER_ORDER_STATUS_OBSERVED" -> {
                // the observation below
            }
            default -> throw new Rejected(UNKNOWN_TYPE, "event type " + type);
        }
        updates.add(new BrokerOrderUpdate.Observed(brokerOrderId, clientOrderRef, status, filled, raw, sourceTime));
        return new Parsed(eventId, updates);
    }

    private static String text(JsonNode node) {
        return node.isString() ? node.stringValue() : null;
    }

    private static UUID uuid(JsonNode node) throws Rejected {
        try {
            return UUID.fromString(text(node));
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new Rejected(INVALID, "eventId");
        }
    }

    private static String identifier(JsonNode node, boolean required) throws Rejected {
        String value = text(node);
        if (value == null && !required) {
            return null;
        }
        if (value == null || !IDENTIFIER.matcher(value).matches()) {
            throw new Rejected(INVALID, "identifier");
        }
        return value;
    }

    private static ObservedStatus status(JsonNode node) throws Rejected {
        try {
            return ObservedStatus.valueOf(text(node));
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new Rejected(INVALID, "observedStatus");
        }
    }

    private static Quantity quantity(JsonNode node) throws Rejected {
        try {
            return new Quantity(new BigDecimal(text(node)));
        } catch (RuntimeException e) {
            throw new Rejected(INVALID, "quantity");
        }
    }

    private static Price price(JsonNode node) throws Rejected {
        try {
            return new Price(new BigDecimal(text(node)));
        } catch (RuntimeException e) {
            throw new Rejected(INVALID, "price");
        }
    }

    private static Instant instant(JsonNode node) throws Rejected {
        try {
            return Instant.parse(text(node));
        } catch (DateTimeParseException | NullPointerException e) {
            throw new Rejected(INVALID, "timestamp");
        }
    }
}
