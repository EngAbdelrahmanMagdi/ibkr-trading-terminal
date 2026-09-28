package com.project.trading.shared.api;

import org.slf4j.MDC;

import java.util.UUID;

/** The correlation ID of the current request (kept in the logging MDC). */
public final class Correlation {

    public static final String HEADER = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";

    private Correlation() {
    }

    /** The current correlation ID, or a new one when none is set (for example on a scheduler thread). */
    public static String current() {
        String id = MDC.get(MDC_KEY);
        return id != null ? id : UUID.randomUUID().toString();
    }

    /** Accepts a client-supplied ID only when it is a canonical UUID. */
    static String acceptOrGenerate(String candidate) {
        if (candidate != null && candidate.length() == 36) {
            try {
                UUID parsed = UUID.fromString(candidate);
                if (parsed.toString().equalsIgnoreCase(candidate)) {
                    return parsed.toString();
                }
            } catch (IllegalArgumentException ignored) {
                // Malformed: a new ID is generated below.
            }
        }
        return UUID.randomUUID().toString();
    }
}
