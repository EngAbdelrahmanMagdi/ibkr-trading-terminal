package com.project.trading.shared.api;

import com.project.trading.shared.domain.DomainException;
import com.project.trading.shared.domain.ErrorCategory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionTimedOutException;
import org.springframework.web.HttpMediaTypeNotAcceptableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.util.List;

/**
 * Maps every error to the problem body with a stable category. Details returned to clients never include
 * stack traces, SQL, class names or parser internals; unexpected errors are logged server-side only.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    static ResponseEntity<Problem> respond(Problem problem) {
        return ResponseEntity.status(problem.status()).contentType(ProblemWriter.PROBLEM_JSON).body(problem);
    }

    private static ResponseEntity<Problem> badRequest(String detail, List<Problem.FieldError> errors) {
        return respond(ProblemWriter.of(ErrorCategory.VALIDATION, 400, "Validation failed", detail, errors));
    }

    @ExceptionHandler(DomainException.class)
    ResponseEntity<Problem> domain(DomainException e) {
        return respond(ProblemWriter.of(e));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<Problem> invalidBody(MethodArgumentNotValidException e) {
        List<Problem.FieldError> errors = e.getBindingResult().getFieldErrors().stream()
                .map(f -> new Problem.FieldError(f.getField(), f.getDefaultMessage() == null ? "invalid value"
                        : f.getDefaultMessage()))
                .toList();
        return badRequest("the request body is invalid", errors);
    }

    @ExceptionHandler(HandlerMethodValidationException.class)
    ResponseEntity<Problem> invalidParameters(HandlerMethodValidationException e) {
        List<Problem.FieldError> errors = e.getParameterValidationResults().stream()
                .flatMap(r -> r.getResolvableErrors().stream()
                        .map(err -> new Problem.FieldError(r.getMethodParameter().getParameterName() == null
                                ? "parameter" : r.getMethodParameter().getParameterName(),
                                err.getDefaultMessage() == null ? "invalid value" : err.getDefaultMessage())))
                .toList();
        return badRequest("request parameters are invalid", errors);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<Problem> unreadable(HttpMessageNotReadableException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof RequestSizeLimitFilter.RequestTooLargeException) {
                return respond(ProblemWriter.tooLarge());
            }
        }
        return badRequest("the request body is missing or is not valid JSON for this operation", List.of());
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    ResponseEntity<Problem> missingHeader(MissingRequestHeaderException e) {
        return badRequest("the " + e.getHeaderName() + " header is required",
                List.of(new Problem.FieldError(e.getHeaderName(), "required")));
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    ResponseEntity<Problem> missingParameter(MissingServletRequestParameterException e) {
        return badRequest("the " + e.getParameterName() + " parameter is required",
                List.of(new Problem.FieldError(e.getParameterName(), "required")));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    ResponseEntity<Problem> typeMismatch(MethodArgumentTypeMismatchException e) {
        return badRequest("invalid value for " + e.getName(), List.of(new Problem.FieldError(e.getName(), "invalid value")));
    }

    @ExceptionHandler(NoResourceFoundException.class)
    ResponseEntity<Problem> noResource(NoResourceFoundException e) {
        return respond(ProblemWriter.of(ErrorCategory.VALIDATION, 404, "Not found", "no such resource", List.of()));
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    ResponseEntity<Problem> method(HttpRequestMethodNotSupportedException e) {
        return respond(ProblemWriter.of(ErrorCategory.VALIDATION, 405, "Method not allowed",
                "method " + e.getMethod() + " is not supported for this resource", List.of()));
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    ResponseEntity<Problem> mediaType(HttpMediaTypeNotSupportedException e) {
        return respond(ProblemWriter.of(ErrorCategory.VALIDATION, 415, "Unsupported media type",
                "the request body must be application/json", List.of()));
    }

    @ExceptionHandler(HttpMediaTypeNotAcceptableException.class)
    ResponseEntity<Problem> notAcceptable(HttpMediaTypeNotAcceptableException e) {
        return respond(ProblemWriter.of(ErrorCategory.VALIDATION, 406, "Not acceptable",
                "responses are application/json", List.of()));
    }

    @ExceptionHandler(ConcurrencyFailureException.class)
    ResponseEntity<Problem> concurrency(ConcurrencyFailureException e) {
        log.info("concurrent update rejected: {}", e.getClass().getSimpleName());
        return respond(ProblemWriter.of(ErrorCategory.CONFLICT, 409, "Conflict",
                "the resource was changed concurrently; reload and retry", List.of()));
    }

    @ExceptionHandler({DataAccessResourceFailureException.class, CannotCreateTransactionException.class,
            QueryTimeoutException.class, TransactionTimedOutException.class})
    ResponseEntity<Problem> unavailable(Exception e) {
        log.warn("database unavailable: {}", e.getClass().getSimpleName());
        return respond(ProblemWriter.of(ErrorCategory.SERVICE_UNAVAILABLE, 503, "Service unavailable",
                "the service is temporarily unavailable", List.of()));
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<Problem> unexpected(Exception e) {
        log.error("unexpected error category={}", e.getClass().getSimpleName());
        return respond(ProblemWriter.of(ErrorCategory.INTERNAL, 500, "Internal error",
                "an unexpected error occurred", List.of()));
    }
}
