package com.project.trading.order.api;

import com.project.trading.order.application.CancelOrderService;
import com.project.trading.order.application.ConfirmOrderService;
import com.project.trading.order.application.OrderQueries;
import com.project.trading.order.application.PlaceOrderCommand;
import com.project.trading.order.application.PlaceOrderService;
import com.project.trading.order.domain.Order;
import com.project.trading.order.domain.OrderIntent;
import com.project.trading.order.domain.OrderStatus;
import com.project.trading.shared.domain.OrderType;
import com.project.trading.shared.domain.TimeInForce;
import com.project.trading.shared.api.ApiFormat;
import com.project.trading.shared.api.ApiLimits;
import com.project.trading.shared.domain.DomainException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

/** Order endpoints. Syntax is checked here; every business rule lives in the application layer. */
@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    static final String IDEMPOTENCY_KEY = "Idempotency-Key";

    public record PlaceOrderRequest(
            @NotNull @Pattern(regexp = "^[A-Z][A-Z0-9.-]{0,11}$") String symbol,
            @NotNull @Pattern(regexp = "^(BUY|SELL|SHORT)$") String intent,
            @NotNull @Pattern(regexp = "^(MARKET|LIMIT)$") String orderType,
            @NotNull @Pattern(regexp = "^[0-9]{1,15}(\\.[0-9]{1,4})?$") String quantity,
            @Pattern(regexp = "^[0-9]{1,13}(\\.[0-9]{1,6})?$") String limitPrice,
            @NotNull @Pattern(regexp = "^(DAY|GTC)$") String timeInForce) {
    }

    public record ConfirmationRequest(@NotNull Boolean confirm) {
    }

    public record PendingConfirmation(String message) {
    }

    public record OrderResponse(String id, String clientOrderId, String brokerOrderId, String symbol, String intent,
                                String brokerSide, String orderType, String quantity, String filledQuantity,
                                String limitPrice, String averageFillPrice, String timeInForce, String status,
                                PendingConfirmation pendingConfirmation, String rejectionReason, String createdAt,
                                String submittedAt, String updatedAt) {
    }

    private final PlaceOrderService placeOrders;
    private final ConfirmOrderService confirmations;
    private final CancelOrderService cancellations;
    private final OrderQueries queries;
    private final ApiLimits limits;

    public OrderController(PlaceOrderService placeOrders, ConfirmOrderService confirmations,
                           CancelOrderService cancellations, OrderQueries queries, ApiLimits limits) {
        this.placeOrders = placeOrders;
        this.confirmations = confirmations;
        this.cancellations = cancellations;
        this.queries = queries;
        this.limits = limits;
    }

    @GetMapping
    public List<OrderResponse> list(@RequestParam(name = "status", required = false) List<OrderStatus> statuses,
                                    @RequestParam(name = "limit", required = false) Integer limit) {
        return queries.recent(statuses == null ? List.of() : statuses, limits.resolve(limit)).stream()
                .map(OrderController::toResponse)
                .toList();
    }

    @PostMapping
    public ResponseEntity<OrderResponse> place(@RequestHeader(IDEMPOTENCY_KEY) String idempotencyKey,
                                               @Valid @RequestBody PlaceOrderRequest request) {
        UUID key = parseKey(idempotencyKey);
        PlaceOrderCommand command = new PlaceOrderCommand(request.symbol(), OrderIntent.valueOf(request.intent()),
                OrderType.valueOf(request.orderType()), new BigDecimal(request.quantity()),
                request.limitPrice() == null ? null : new BigDecimal(request.limitPrice()),
                TimeInForce.valueOf(request.timeInForce()));
        PlaceOrderService.Placed placed = placeOrders.place(key, command);
        return ResponseEntity.status(placed.status()).body(toResponse(placed.order()));
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public OrderResponse cancel(@PathVariable("id") UUID id) {
        return toResponse(cancellations.cancel(id));
    }

    @PostMapping("/{id}/confirmation")
    public OrderResponse confirm(@PathVariable("id") UUID id, @Valid @RequestBody ConfirmationRequest request) {
        return toResponse(confirmations.confirm(id, request.confirm()));
    }

    /** Accepts only a canonical UUID. */
    static UUID parseKey(String value) {
        try {
            UUID key = UUID.fromString(value);
            if (key.toString().equalsIgnoreCase(value)) {
                return key;
            }
        } catch (IllegalArgumentException ignored) {
            // Reported below.
        }
        throw DomainException.malformedField(IDEMPOTENCY_KEY, "the Idempotency-Key header must be a UUID");
    }

    static OrderResponse toResponse(Order o) {
        return new OrderResponse(o.id().toString(), o.clientOrderId(), o.brokerOrderId(), o.symbol(), o.intent().name(),
                o.brokerSide().name(), o.orderType().name(), ApiFormat.decimal(o.quantity().value(), 0),
                ApiFormat.decimal(o.filledQuantity().value(), 0),
                o.limitPrice() == null ? null : ApiFormat.decimal(o.limitPrice().value(), 2),
                o.averageFillPrice() == null ? null : ApiFormat.decimal(o.averageFillPrice().value(), 2),
                o.timeInForce().name(), o.status().name(),
                o.status() == OrderStatus.PENDING_CONFIRMATION && o.replyMessage() != null
                        ? new PendingConfirmation(o.replyMessage()) : null,
                o.rejectionReason(), ApiFormat.instant(o.createdAt()), ApiFormat.instant(o.submittedAt()),
                ApiFormat.instant(o.updatedAt()));
    }
}
