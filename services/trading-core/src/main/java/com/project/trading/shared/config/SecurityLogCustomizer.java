package com.project.trading.shared.config;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import org.springframework.boot.json.JsonWriter;
import org.springframework.boot.logging.structured.StructuredLoggingJsonMembersCustomizer;

/** Final structured-log boundary; application errors still use fixed categories. */
public final class SecurityLogCustomizer implements StructuredLoggingJsonMembersCustomizer<Object> {
    private static final Set<String> SECRET_KEYS = Set.of("authorization", "cookie", "setcookie", "password",
            "token", "apikey", "session", "sessionid", "ibkrsession", "accountid");
    private static final Pattern HEADER = Pattern.compile("(?i)(authorization|cookie|set-cookie)[\"']?\\s*[:=]\\s*[^\\r\\n]+");
    private static final Pattern VALUE = Pattern.compile("(?i)(api[-_]?key|password|token|session(?:id)?)[\"']?\\s*[:=]\\s*[\"']?[^\\s\"',;&}\\]]+");

    @Override public void customize(JsonWriter.Members<Object> members) {
        members.applyingValueProcessor((path, value) -> {
            String key = path.name() == null ? "" : path.name().replaceAll("[^A-Za-z]", "").toLowerCase(Locale.ROOT);
            if (SECRET_KEYS.contains(key)) return "[REDACTED]";
            return value instanceof String text ? redact(text) : value;
        });
    }

    static String redact(String value) {
        return VALUE.matcher(HEADER.matcher(value).replaceAll("$1=[REDACTED]")).replaceAll("$1=[REDACTED]");
    }
}
