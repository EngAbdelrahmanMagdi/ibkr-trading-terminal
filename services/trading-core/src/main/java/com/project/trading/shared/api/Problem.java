package com.project.trading.shared.api;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/** The REST error body. Never carries stack traces or internal details. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Problem(String type, String title, int status, String category, String detail, String correlationId,
                      List<FieldError> errors) {

    public record FieldError(String field, String message) {
    }
}
