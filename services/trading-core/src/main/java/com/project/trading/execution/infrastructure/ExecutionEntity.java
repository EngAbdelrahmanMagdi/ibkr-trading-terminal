package com.project.trading.execution.infrastructure;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "executions")
public class ExecutionEntity {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "broker_execution_id", nullable = false, length = 64)
    private String brokerExecutionId;

    @Column(name = "symbol", nullable = false, length = 12)
    private String symbol;

    @Column(name = "side", nullable = false, length = 4)
    private String side;

    @Column(name = "quantity", nullable = false, precision = 19, scale = 4)
    private BigDecimal quantity;

    @Column(name = "price", nullable = false, precision = 19, scale = 6)
    private BigDecimal price;

    @Column(name = "commission", precision = 19, scale = 4)
    private BigDecimal commission;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Column(name = "executed_at", nullable = false)
    private Instant executedAt;

    protected ExecutionEntity() {
    }

    public ExecutionEntity(UUID id, UUID orderId, String brokerExecutionId, String symbol, String side,
                           BigDecimal quantity, BigDecimal price, BigDecimal commission, String currency,
                           Instant executedAt) {
        this.id = id;
        this.orderId = orderId;
        this.brokerExecutionId = brokerExecutionId;
        this.symbol = symbol;
        this.side = side;
        this.quantity = quantity;
        this.price = price;
        this.commission = commission;
        this.currency = currency;
        this.executedAt = executedAt;
    }

    public UUID getId() {
        return id;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public String getBrokerExecutionId() {
        return brokerExecutionId;
    }

    public String getSymbol() {
        return symbol;
    }

    public String getSide() {
        return side;
    }

    public BigDecimal getQuantity() {
        return quantity;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public BigDecimal getCommission() {
        return commission;
    }

    public String getCurrency() {
        return currency;
    }

    public Instant getExecutedAt() {
        return executedAt;
    }
}
