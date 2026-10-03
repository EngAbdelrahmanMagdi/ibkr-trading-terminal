"use client";

import { useEffect, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api, errorMessage } from "@/lib/api";
import { Icon } from "@/components/Icon";
import styles from "./terminal.module.css";

export function articleLink(url: string): string | null {
  try {
    const parsed = new URL(url);
    if (
      !["https:", "http:"].includes(parsed.protocol) ||
      parsed.username ||
      parsed.password ||
      parsed.hostname.endsWith(".invalid") ||
      parsed.hostname === "localhost" ||
      parsed.hostname.endsWith(".local") ||
      parsed.hostname.endsWith(".localhost") ||
      parsed.hostname.includes(":") ||
      /^[\d.]+$/.test(parsed.hostname)
    )
      return null;
    return parsed.href;
  } catch {
    return null;
  }
}

export function NewsPanel({ symbol }: { symbol: string }) {
  const queryClient = useQueryClient();
  const [limit, setLimit] = useState(20);
  const news = useQuery({
    queryKey: ["news", symbol, limit],
    queryFn: ({ signal }) => api.news(symbol, limit, signal),
    staleTime: 300000,
    gcTime: 120000,
    refetchInterval: 120000,
    refetchIntervalInBackground: false,
    retry: false,
  });
  useEffect(() => {
    const inactive = queryClient
      .getQueryCache()
      .findAll({ queryKey: ["news"], type: "inactive" })
      .sort((a, b) => b.state.dataUpdatedAt - a.state.dataUpdatedAt);
    inactive
      .slice(19)
      .forEach((query) =>
        queryClient.removeQueries({ queryKey: query.queryKey, exact: true }),
      );
  }, [queryClient, symbol, limit]);
  const status = news.isError
    ? "UNAVAILABLE"
    : (news.data?.status ?? "UNKNOWN");
  return (
    <section aria-label={`${symbol} news`} className={styles.newsPanel}>
      <div className={styles.newsToolbar}>
        <strong>{symbol} NEWS</strong>
        <span className={status === "FRESH" ? styles.muted : styles.warning}>
          {news.isPending
            ? "Loading…"
            : status === "FRESH"
              ? "Up to date"
              : status === "STALE"
                ? "News delayed"
                : status === "UNKNOWN"
                  ? "Freshness unavailable"
                  : "Provider unavailable"}
        </span>
        {news.data?.lastRefreshedAt && (
          <time
            dateTime={news.data.lastRefreshedAt}
            title={news.data.lastRefreshedAt}
          >
            Refreshed{" "}
            {new Date(news.data.lastRefreshedAt).toLocaleTimeString([], {
              hour: "2-digit",
              minute: "2-digit",
            })}
          </time>
        )}
        <button disabled={news.isFetching} onClick={() => void news.refetch()}>
          {news.isFetching ? "Refreshing…" : "Refresh news"}
        </button>
      </div>
      {news.isError && (
        <p role="status" className={styles.inlineError}>
          {errorMessage(news.error)} Previous articles may be delayed.
        </p>
      )}
      {news.isPending ? (
        <div className={styles.panelState}>Loading {symbol} news…</div>
      ) : news.data?.articles.length === 0 ? (
        <div className={styles.panelState}>
          {status === "FRESH"
            ? `No recent news for ${symbol}.`
            : `No saved news for ${symbol}. Refresh ${status === "STALE" ? "is delayed" : "is unavailable"}.`}
        </div>
      ) : (
        <ul className={styles.newsList}>
          {news.data?.articles.map((article) => {
            const link = articleLink(article.url);
            return (
              <li key={article.id}>
                <div className={styles.newsMeta}>
                  <span>{article.source}</span>
                  <time dateTime={article.publishedAt}>
                    {new Date(article.publishedAt).toLocaleString([], {
                      month: "short",
                      day: "numeric",
                      hour: "2-digit",
                      minute: "2-digit",
                    })}
                  </time>
                  <span>{article.symbols.join(" · ")}</span>
                </div>
                {link ? (
                  <a href={link} target="_blank" rel="noopener noreferrer">
                    {article.headline}
                    <Icon name="arrow" width={12} height={12} />
                  </a>
                ) : (
                  <strong>{article.headline}</strong>
                )}
                {article.rawSummary && <p>{article.rawSummary}</p>}
                {article.enrichment ? (
                  <div className={styles.newsInsight}>
                    <div className={styles.newsInsightMeta}>
                      <strong>
                        {article.enrichment.model === "synthetic-news.v1"
                          ? "Synthetic insight"
                          : "AI insight"}
                      </strong>
                      <span>{article.enrichment.insight.sentiment}</span>
                      <span title="Model confidence is not a calibrated probability">
                        Model confidence{" "}
                        {Math.round(
                          article.enrichment.insight.confidence * 100,
                        )}
                        %
                      </span>
                    </div>
                    <p>{article.enrichment.insight.summary}</p>
                    {article.enrichment.insight.flags.length > 0 && (
                      <span className={styles.warning}>
                        {article.enrichment.insight.flags
                          .map((flag) =>
                            flag.replaceAll("_", " ").toLowerCase(),
                          )
                          .join(" · ")}
                      </span>
                    )}
                    <details>
                      <summary>Insight details</summary>
                      <span>
                        Relevance{" "}
                        {Math.round(
                          article.enrichment.insight.relevanceScore * 100,
                        )}
                        % · Sentiment score{" "}
                        {article.enrichment.insight.sentimentScore.toFixed(2)}
                      </span>
                      {article.enrichment.insight.catalysts.map(
                        (catalyst, index) => (
                          <p key={`${catalyst.type}-${index}`}>
                            {catalyst.type.replaceAll("_", " ")}:{" "}
                            {catalyst.description}
                          </p>
                        ),
                      )}
                      <p>
                        Evidence: this article · {article.enrichment.model} ·{" "}
                        {article.enrichment.modelVersion ??
                          "Version unavailable"}{" "}
                        · {article.enrichment.promptVersion}
                      </p>
                      <time dateTime={article.enrichment.enrichedAt}>
                        Interpreted{" "}
                        {new Date(
                          article.enrichment.enrichedAt,
                        ).toLocaleString()}
                      </time>
                    </details>
                  </div>
                ) : (
                  <span className={styles.muted}>No AI insight available</span>
                )}
              </li>
            );
          })}
        </ul>
      )}
      {news.data && news.data.articles.length >= limit && limit < 100 && (
        <button
          className={styles.textAction}
          onClick={() => setLimit(limit === 20 ? 50 : 100)}
        >
          Show more
        </button>
      )}
    </section>
  );
}
