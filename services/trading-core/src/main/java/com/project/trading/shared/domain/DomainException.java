package com.project.trading.shared.domain;

import java.util.List;

/**
 * A business rule violation with its error category and HTTP status. Messages are safe to return to clients:
 * they never contain internal details.
 */
public class DomainException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** A field-level error. */
    public record FieldError(String field, String message) {
    }

    private final ErrorCategory category;
    private final int status;
    private final String title;
    private final transient List<FieldError> fieldErrors;

    public DomainException(ErrorCategory category, int status, String title, String detail, List<FieldError> fieldErrors) {
        super(detail);
        this.category = category;
        this.status = status;
        this.title = title;
        this.fieldErrors = List.copyOf(fieldErrors);
    }

    public DomainException(ErrorCategory category, int status, String title, String detail) {
        this(category, status, title, detail, List.of());
    }

    public ErrorCategory category() {
        return category;
    }

    public int status() {
        return status;
    }

    public String title() {
        return title;
    }

    public List<FieldError> fieldErrors() {
        return fieldErrors;
    }

    /** 422: the request is well-formed but violates a business rule. */
    public static DomainException invalid(String detail) {
        return new DomainException(ErrorCategory.VALIDATION, 422, "Order rejected by validation", detail);
    }

    /** 422 with a field error. */
    public static DomainException invalidField(String field, String detail) {
        return new DomainException(ErrorCategory.VALIDATION, 422, "Order rejected by validation", detail,
                List.of(new FieldError(field, detail)));
    }

    /** 400: malformed input. */
    public static DomainException malformed(String detail) {
        return new DomainException(ErrorCategory.VALIDATION, 400, "Validation failed", detail);
    }

    /** 400 with a field error. */
    public static DomainException malformedField(String field, String detail) {
        return new DomainException(ErrorCategory.VALIDATION, 400, "Validation failed", detail,
                List.of(new FieldError(field, detail)));
    }

    public static DomainException instrumentNotFound(String symbol) {
        return new DomainException(ErrorCategory.INSTRUMENT_NOT_FOUND, 404, "Instrument not found",
                "unknown symbol " + symbol);
    }

    public static DomainException notFound(String what) {
        return new DomainException(ErrorCategory.VALIDATION, 404, "Not found", what + " not found");
    }

    public static DomainException conflict(String detail) {
        return new DomainException(ErrorCategory.CONFLICT, 409, "Conflict", detail);
    }

    public static DomainException staleMarketData(String detail) {
        return new DomainException(ErrorCategory.STALE_MARKET_DATA, 409, "Market data is stale", detail);
    }

    public static DomainException brokerUnavailable(String detail) {
        return new DomainException(ErrorCategory.BROKER_UNAVAILABLE, 503, "Broker unavailable", detail);
    }
}
