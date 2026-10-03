package com.project.trading.news.infrastructure;

import com.project.trading.news.domain.NewsArticle;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.Normalizer;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Locale;
import java.util.TreeSet;
import java.util.UUID;

public class NewsNormalizer {
    public NewsArticle normalize(String provider, String providerId, String requestedSymbol, String related,
                                 String headline, String source, String url, Instant published, String summary,
                                 Instant from, Instant now) {
        String normalizedUrl = canonicalUrl(url);
        String title = text(headline);
        String publisher = text(source);
        if (title.isEmpty() || title.length() > 500 || publisher.isEmpty() || publisher.length() > 200
                || published == null || published.isBefore(from) || published.isAfter(now.plusSeconds(300)))
            throw new IllegalArgumentException("Invalid news record");
        var symbols = new TreeSet<String>();
        symbols.add(requestedSymbol);
        if (related != null) Arrays.stream(related.split(",", 101)).limit(100).map(String::trim)
                .map(s -> s.toUpperCase(Locale.ROOT)).filter(s -> s.matches("^[A-Z][A-Z0-9.-]{0,11}$"))
                .forEach(symbols::add);
        if (symbols.size() > 50 || !requestedSymbol.matches("^[A-Z][A-Z0-9.-]{0,11}$"))
            throw new IllegalArgumentException("Invalid news symbols");
        String snippet = summary == null ? null : text(summary);
        if (snippet != null && snippet.length() > 8000) snippet = snippet.substring(0, 8000);
        String identity = provider + ":" + (providerId == null || providerId.isBlank()
                ? hash(normalizedUrl + "\n" + published) : providerId);
        if (identity.length() > 200) throw new IllegalArgumentException("Invalid news identity");
        // Length-prefixed fields avoid delimiter ambiguity in the content digest.
        String content = "v1" + field(title) + field(publisher) + field(normalizedUrl) + field(published.toString()) + field(snippet);
        return new NewsArticle(UUID.randomUUID(), provider, identity, symbols.first(), symbols.stream().toList(),
                title, publisher, normalizedUrl, published, snippet, hash(content));
    }
    private static String field(String value) { return value == null ? "-1:" : value.length() + ":" + value; }
    private static String text(String value) {
        if (value == null) return "";
        return Normalizer.normalize(value, Normalizer.Form.NFC).replaceAll("[\\p{Cntrl}&&[^\\n\\t]]", "")
                .replaceAll("\\s+", " ").trim();
    }
    public static String canonicalUrl(String value) {
        try {
            if (value == null || value.length() > 2048) throw new IllegalArgumentException("Invalid news URL");
            URI uri = URI.create(value);
            String host = uri.getHost();
            String scheme = uri.getScheme();
            if (host == null || scheme == null || !scheme.matches("(?i)https?") || uri.getUserInfo() != null)
                throw new IllegalArgumentException("Invalid news URL");
            host = host.toLowerCase(Locale.ROOT);
            if (host.endsWith(".")) host = host.substring(0, host.length() - 1);
            if (!host.contains(".") || host.endsWith(".local") || host.endsWith(".localhost")
                    || host.matches("[0-9.]+") || host.contains(":")) throw new IllegalArgumentException("Invalid news URL");
            String query = uri.getRawQuery();
            if (query != null) {
                query = String.join("&", Arrays.stream(query.split("&"))
                        .filter(p -> !p.matches("(?i)(utm_[^=]*|fbclid|gclid)=.*")).toList());
                if (query.isEmpty()) query = null;
            }
            int port = uri.getPort();
            if (port > 65535) throw new IllegalArgumentException("Invalid news URL");
            scheme = scheme.toLowerCase(Locale.ROOT);
            if ((scheme.equals("https") && port == 443) || (scheme.equals("http") && port == 80)) port = -1;
            String result = scheme + "://" + host + (port == -1 ? "" : ":" + port)
                    + (uri.getRawPath().isEmpty() ? "/" : uri.getRawPath()) + (query == null ? "" : "?" + query);
            if (result.length() > 2048) throw new IllegalArgumentException("Invalid news URL");
            return result;
        } catch (IllegalArgumentException exception) { throw new IllegalArgumentException("Invalid news URL"); }
    }
    public static String hash(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException exception) { throw new IllegalStateException("SHA-256 unavailable", exception); }
    }
}
