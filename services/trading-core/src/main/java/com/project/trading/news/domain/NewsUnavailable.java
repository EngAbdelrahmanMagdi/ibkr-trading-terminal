package com.project.trading.news.domain;

/** A bounded, sanitized provider failure. Never contains a request URI or response body. */
public class NewsUnavailable extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final String classification;
    public NewsUnavailable(String classification) {
        super("News refresh unavailable");
        this.classification = classification;
    }
    public String classification() { return classification; }
}
