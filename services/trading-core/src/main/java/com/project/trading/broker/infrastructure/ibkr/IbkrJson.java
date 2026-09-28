package com.project.trading.broker.infrastructure.ibkr;

import tools.jackson.core.StreamWriteFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.util.regex.Pattern;

/**
 * JSON handling for IBKR payloads. Numbers are read as exact decimals (never double) and decimals are written in
 * plain notation. Broker text shown to users is sanitized: markup and control characters removed, length capped.
 */
final class IbkrJson {

    static final JsonMapper MAPPER = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN)
            .build();

    private static final Pattern DECIMAL = Pattern.compile("^-?\\d{1,19}(\\.\\d{1,12})?$");
    private static final Pattern WHOLE = Pattern.compile("^\\d{1,15}$");
    private static final Pattern MARKUP = Pattern.compile("<[^<>]{0,200}>");
    private static final Pattern CONTROL = Pattern.compile("[\\p{Cntrl}&&[^\n]]");
    private static final Pattern SPACES = Pattern.compile("[ \\t]+");

    private IbkrJson() {
    }

    /** An exact decimal from a JSON number or a plain decimal string; null when absent or not a plain decimal. */
    static BigDecimal decimal(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.decimalValue();
        }
        if (node.isString()) {
            String text = node.stringValue().trim();
            return DECIMAL.matcher(text).matches() ? new BigDecimal(text) : null;
        }
        return null;
    }

    /** A non-negative whole number written as digits only (no suffixes such as K or M); null otherwise. */
    static Long whole(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (node.isIntegralNumber() && node.canConvertToLong() && node.longValue() >= 0) {
            return node.longValue();
        }
        if (node.isString() && WHOLE.matcher(node.stringValue().trim()).matches()) {
            return Long.parseLong(node.stringValue().trim());
        }
        return null;
    }

    /** A string or integral identifier as text; null otherwise. */
    static String id(JsonNode node) {
        if (node == null) {
            return null;
        }
        if (node.isString()) {
            return node.stringValue();
        }
        if (node.isIntegralNumber()) {
            return node.bigIntegerValue().toString();
        }
        return null;
    }

    static boolean isTrue(JsonNode node) {
        return node != null && node.isBoolean() && node.booleanValue();
    }

    /** Broker text made safe to store and show: no markup or control characters, at most max characters. */
    static String sanitize(String text, int max) {
        if (text == null) {
            return null;
        }
        String clean = MARKUP.matcher(text).replaceAll(" ");
        clean = CONTROL.matcher(clean).replaceAll(" ");
        clean = SPACES.matcher(clean).replaceAll(" ").strip();
        return clean.length() <= max ? clean : clean.substring(0, max);
    }
}
