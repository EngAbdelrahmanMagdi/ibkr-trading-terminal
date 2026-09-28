package com.project.trading.position.infrastructure;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "positions")
public class PositionEntity {

    @Id
    @Column(name = "symbol", length = 12)
    private String symbol;

    @Column(name = "quantity", nullable = false, precision = 19, scale = 4)
    private BigDecimal quantity;

    @Column(name = "average_cost", nullable = false, precision = 19, scale = 6)
    private BigDecimal averageCost;

    @Column(name = "realized_pnl", nullable = false, precision = 19, scale = 4)
    private BigDecimal realizedPnl;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    @Column(name = "version", nullable = false)
    private long version;

    protected PositionEntity() {
    }

    public PositionEntity(String symbol, String currency) {
        this.symbol = symbol;
        this.currency = currency;
        this.quantity = BigDecimal.ZERO;
        this.averageCost = BigDecimal.ZERO;
        this.realizedPnl = BigDecimal.ZERO;
    }

    public void update(BigDecimal quantity, BigDecimal averageCost, BigDecimal realizedPnl, Instant updatedAt) {
        this.quantity = quantity;
        this.averageCost = averageCost;
        this.realizedPnl = realizedPnl;
        this.updatedAt = updatedAt;
    }

    public String getSymbol() {
        return symbol;
    }

    public BigDecimal getQuantity() {
        return quantity;
    }

    public BigDecimal getAverageCost() {
        return averageCost;
    }

    public BigDecimal getRealizedPnl() {
        return realizedPnl;
    }

    public String getCurrency() {
        return currency;
    }
}
