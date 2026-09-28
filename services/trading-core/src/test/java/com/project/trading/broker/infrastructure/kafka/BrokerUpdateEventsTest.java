package com.project.trading.broker.infrastructure.kafka;

import com.project.trading.broker.domain.BrokerOrderUpdate;
import com.project.trading.broker.domain.ObservedStatus;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;
import com.project.trading.support.ContractSchemas;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Contract events are read into broker-neutral updates; anything else is rejected with a reason. */
class BrokerUpdateEventsTest {

    private static final String EXECUTION = """
            {"eventId":"0c9d8e7f-6a5b-4c3d-8e1f-a2b3c4d5e6f7","eventType":"BROKER_EXECUTION_OBSERVED","eventVersion":1,
             "occurredAt":"2026-09-27T14:03:11.123Z","source":"realtime-gateway",
             "correlationId":"3f1c2a9e-4b7d-4c8e-9f0a-1b2c3d4e5f60","accountId":"DU000000",
             "payload":{"brokerOrderId":"987654","clientOrderRef":"c-20260927-0001","observedStatus":"PARTIALLY_FILLED",
              "brokerStatusRaw":"PreSubmitted","filledQuantity":"4","remainingQuantity":"6","averagePrice":"184.26",
              "execution":{"brokerExecutionId":"0000e0d5.6512a3b1.01.01","quantity":"4","price":"184.26",
               "executedAt":"2026-09-27T14:03:11.123Z"},
              "sourceTimestamp":"2026-09-27T14:03:11.123Z"}}""";

    private final BrokerUpdateEvents events = new BrokerUpdateEvents("DU000000", "USD");

    @Test
    void anExecutionEventBecomesAFillFollowedByTheObservedOrderState() throws Exception {
        ContractSchemas.assertValid("events/broker-order-update.schema.json", JsonMapper.builder().build().readTree(EXECUTION));

        BrokerUpdateEvents.Parsed parsed = events.parse(EXECUTION);

        assertThat(parsed.eventId()).isEqualTo(UUID.fromString("0c9d8e7f-6a5b-4c3d-8e1f-a2b3c4d5e6f7"));
        assertThat(parsed.updates()).hasSize(2);
        BrokerOrderUpdate.Fill fill = (BrokerOrderUpdate.Fill) parsed.updates().get(0);
        assertThat(fill.brokerOrderId()).isEqualTo("987654");
        assertThat(fill.clientOrderRef()).isEqualTo("c-20260927-0001");
        assertThat(fill.quantity()).isEqualTo(Quantity.of("4"));
        assertThat(fill.price()).isEqualTo(Price.of("184.26"));
        assertThat(fill.commission()).as("the stream carries no commission").isNull();
        BrokerOrderUpdate.Observed observed = (BrokerOrderUpdate.Observed) parsed.updates().get(1);
        assertThat(observed.status()).isEqualTo(ObservedStatus.PARTIALLY_FILLED);
        assertThat(observed.filledQuantity()).isEqualTo(Quantity.of("4"));
    }

    @Test
    void eventsThatDoNotFitAreRejectedWithAReason() {
        assertThatThrownBy(() -> events.parse(EXECUTION.replace("\"DU000000\"", "\"DU999999\"")))
                .isInstanceOf(BrokerUpdateEvents.Rejected.class).extracting("reason").isEqualTo(BrokerUpdateEvents.OTHER_ACCOUNT);
        assertThatThrownBy(() -> events.parse(EXECUTION.replace("BROKER_EXECUTION_OBSERVED", "BROKER_POSITION_OBSERVED")))
                .isInstanceOf(BrokerUpdateEvents.Rejected.class).extracting("reason").isEqualTo(BrokerUpdateEvents.UNKNOWN_TYPE);
        assertThatThrownBy(() -> events.parse(EXECUTION.replace("\"price\":\"184.26\"", "\"price\":\"abc\"")))
                .isInstanceOf(BrokerUpdateEvents.Rejected.class).extracting("reason").isEqualTo(BrokerUpdateEvents.INVALID);
        assertThatThrownBy(() -> events.parse(EXECUTION.replace("realtime-gateway", "somewhere-else")))
                .isInstanceOf(BrokerUpdateEvents.Rejected.class).extracting("reason").isEqualTo(BrokerUpdateEvents.INVALID);
        assertThatThrownBy(() -> events.parse("not json"))
                .isInstanceOf(BrokerUpdateEvents.Rejected.class).extracting("reason").isEqualTo(BrokerUpdateEvents.INVALID);
    }
}
