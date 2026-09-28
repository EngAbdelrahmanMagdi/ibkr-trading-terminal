package com.project.trading.position.domain;

import com.project.trading.shared.domain.BrokerSide;
import com.project.trading.shared.domain.Decimals;
import com.project.trading.shared.domain.Price;
import com.project.trading.shared.domain.Quantity;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * A position under the average-cost method. quantity is signed (negative is short). Adding to a position
 * re-averages the cost; reducing it realizes P&L against the average cost; crossing zero closes the old side
 * and opens the new one at the fill price. Commissions are not part of the average cost or realized P&L.
 */
public record Position(String symbol, BigDecimal quantity, BigDecimal averageCost, BigDecimal realizedPnl,
                       String currency) {

    public Position {
        Objects.requireNonNull(symbol, "symbol");
        Objects.requireNonNull(currency, "currency");
        quantity = Decimals.quantity(quantity);
        averageCost = Decimals.price(averageCost);
        realizedPnl = Decimals.money(realizedPnl);
        if (averageCost.signum() < 0) {
            throw new IllegalArgumentException("average cost must not be negative");
        }
    }

    public static Position flat(String symbol, String currency) {
        return new Position(symbol, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, currency);
    }

    public boolean isFlat() {
        return quantity.signum() == 0;
    }

    /** Returns the position after a fill. */
    public Position apply(BrokerSide side, Quantity fillQuantity, Price fillPrice) {
        BigDecimal q = fillQuantity.value();
        BigDecimal price = fillPrice.value();
        BigDecimal signedFill = side == BrokerSide.BUY ? q : q.negate();
        BigDecimal newQuantity = quantity.add(signedFill);

        if (quantity.signum() == 0 || quantity.signum() == signedFill.signum()) {
            BigDecimal held = quantity.abs();
            BigDecimal cost = held.multiply(averageCost).add(q.multiply(price));
            BigDecimal average = cost.divide(held.add(q), Decimals.PRICE_SCALE, Decimals.ROUNDING);
            return new Position(symbol, newQuantity, average, realizedPnl, currency);
        }

        BigDecimal closed = quantity.abs().min(q);
        BigDecimal perShare = quantity.signum() > 0 ? price.subtract(averageCost) : averageCost.subtract(price);
        BigDecimal realized = realizedPnl.add(closed.multiply(perShare));
        BigDecimal average;
        if (newQuantity.signum() == 0) {
            average = BigDecimal.ZERO;
        } else if (newQuantity.signum() != quantity.signum()) {
            average = price;
        } else {
            average = averageCost;
        }
        return new Position(symbol, newQuantity, average, realized, currency);
    }

    /** Unrealized P&L at a mark price. */
    public BigDecimal unrealizedPnl(Price mark) {
        return Decimals.money(quantity.multiply(mark.value().subtract(averageCost)));
    }

    /** Signed market value at a mark price (negative for shorts). */
    public BigDecimal marketValue(Price mark) {
        return Decimals.money(quantity.multiply(mark.value()));
    }
}
