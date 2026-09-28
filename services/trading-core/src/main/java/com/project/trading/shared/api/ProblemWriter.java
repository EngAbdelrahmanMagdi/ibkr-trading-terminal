package com.project.trading.shared.api;

import com.project.trading.shared.domain.DomainException;
import com.project.trading.shared.domain.ErrorCategory;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/** Builds problem bodies and writes them outside Spring MVC (for example from a servlet filter). */
@Component
public class ProblemWriter {

    public static final MediaType PROBLEM_JSON = MediaType.APPLICATION_PROBLEM_JSON;

    private final ObjectMapper mapper;

    public ProblemWriter(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public static Problem of(ErrorCategory category, int status, String title, String detail,
                             List<Problem.FieldError> errors) {
        String type = "urn:problem-type:trading:" + category.name().toLowerCase(Locale.ROOT).replace('_', '-');
        return new Problem(type, title, status, category.name(), truncate(detail, 2000), Correlation.current(),
                errors == null || errors.isEmpty() ? null : List.copyOf(errors));
    }

    public static Problem of(DomainException e) {
        List<Problem.FieldError> errors = e.fieldErrors().stream()
                .map(f -> new Problem.FieldError(f.field(), truncate(f.message(), 500)))
                .toList();
        return of(e.category(), e.status(), e.title(), e.getMessage(), errors);
    }

    public static Problem tooLarge() {
        return of(ErrorCategory.VALIDATION, 413, "Request too large", "the request body exceeds the allowed size",
                List.of());
    }

    public void write(HttpServletResponse response, Problem problem) throws IOException {
        response.setStatus(problem.status());
        response.setContentType(PROBLEM_JSON.toString());
        mapper.writeValue(response.getOutputStream(), problem);
    }

    private static String truncate(String text, int max) {
        if (text == null) {
            return null;
        }
        return text.length() <= max ? text : text.substring(0, max);
    }
}
