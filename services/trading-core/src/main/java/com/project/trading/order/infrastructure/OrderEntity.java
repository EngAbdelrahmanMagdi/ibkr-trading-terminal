package com.project.trading.order.infrastructure;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Persistent form of an order. Enums are stored as their names (the table has CHECK constraints). */
@Entity
@Table(name = "orders")
public class OrderEntity {

    @Id
    @Column(name = "id")
    UUID id;

    @Column(name = "client_order_id", nullable = false, length = 64)
    String clientOrderId;

    @Column(name = "broker_order_id", length = 64)
    String brokerOrderId;

    @Column(name = "account_id", nullable = false, length = 64)
    String accountId;

    @Column(name = "conid", nullable = false)
    long conid;

    @Column(name = "symbol", nullable = false, length = 12)
    String symbol;

    @Column(name = "intent", nullable = false, length = 8)
    String intent;

    @Column(name = "broker_side", nullable = false, length = 4)
    String brokerSide;

    @Column(name = "order_type", nullable = false, length = 8)
    String orderType;

    @Column(name = "quantity", nullable = false, precision = 19, scale = 4)
    BigDecimal quantity;

    @Column(name = "filled_quantity", nullable = false, precision = 19, scale = 4)
    BigDecimal filledQuantity;

    @Column(name = "limit_price", precision = 19, scale = 6)
    BigDecimal limitPrice;

    @Column(name = "average_fill_price", precision = 19, scale = 6)
    BigDecimal averageFillPrice;

    @Column(name = "time_in_force", nullable = false, length = 4)
    String timeInForce;

    @Column(name = "status", nullable = false, length = 24)
    String status;

    @Column(name = "reply_id", length = 128)
    String replyId;

    @Column(name = "reply_message", length = 2000)
    String replyMessage;

    @Column(name = "reply_depth", nullable = false)
    int replyDepth;

    @Column(name = "rejection_reason", length = 500)
    String rejectionReason;

    @Column(name = "created_at", nullable = false)
    Instant createdAt;

    @Column(name = "submitted_at")
    Instant submittedAt;

    @Column(name = "updated_at", nullable = false)
    Instant updatedAt;

    @Version
    @Column(name = "version", nullable = false)
    long version;

    protected OrderEntity() {
    }

    OrderEntity(UUID id) {
        this.id = id;
    }
}
