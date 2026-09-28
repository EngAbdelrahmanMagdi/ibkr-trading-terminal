package com.project.trading.instrument.infrastructure;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "instruments")
public class InstrumentEntity {

    @Id
    @Column(name = "symbol", length = 12)
    private String symbol;

    @Column(name = "conid", nullable = false)
    private long conid;

    @Column(name = "name", length = 200)
    private String name;

    @Column(name = "exchange", nullable = false, length = 32)
    private String exchange;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Column(name = "asset_type", nullable = false, length = 8)
    private String assetType;

    @Column(name = "price_scale", nullable = false)
    private int priceScale;

    @Column(name = "resolved_at", nullable = false)
    private Instant resolvedAt;

    protected InstrumentEntity() {
    }

    public InstrumentEntity(String symbol, long conid, String name, String exchange, String currency, String assetType,
                            int priceScale, Instant resolvedAt) {
        this.symbol = symbol;
        this.conid = conid;
        this.name = name;
        this.exchange = exchange;
        this.currency = currency;
        this.assetType = assetType;
        this.priceScale = priceScale;
        this.resolvedAt = resolvedAt;
    }

    public String getSymbol() {
        return symbol;
    }

    public long getConid() {
        return conid;
    }

    public String getName() {
        return name;
    }

    public String getExchange() {
        return exchange;
    }

    public String getCurrency() {
        return currency;
    }

    public String getAssetType() {
        return assetType;
    }

    public int getPriceScale() {
        return priceScale;
    }
}
