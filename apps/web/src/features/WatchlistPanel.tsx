"use client";

import { memo, useEffect, useState } from "react";
import {
  useMutation,
  useQuery,
  useQueryClient,
  type UseQueryResult,
} from "@tanstack/react-query";
import { api, errorMessage, type Watchlist } from "@/lib/api";
import { change, price } from "@/lib/format";
import { useQuote } from "@/lib/realtime";
import { Icon } from "@/components/Icon";
import styles from "./terminal.module.css";

export function WatchlistPanel({
  active,
  onSelect,
  onClose,
  watchlist,
}: {
  active: string;
  onSelect: (symbol: string) => void;
  onClose: () => void;
  watchlist: UseQueryResult<Watchlist>;
}) {
  const queryClient = useQueryClient();
  const [search, setSearch] = useState("");
  const [term, setTerm] = useState("");
  const [focused, setFocused] = useState(0);
  useEffect(() => {
    const timer = setTimeout(() => setTerm(search.trim().toUpperCase()), 250);
    return () => clearTimeout(timer);
  }, [search]);
  const results = useQuery({
    queryKey: ["search", term],
    queryFn: () => api.search(term),
    enabled: term.length > 0,
    staleTime: 300000,
  });
  const add = useMutation({
    mutationFn: api.addWatch,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["watchlist"] });
      setSearch("");
      setTerm("");
    },
  });
  const remove = useMutation({
    mutationFn: api.removeWatch,
    onSuccess: () =>
      void queryClient.invalidateQueries({ queryKey: ["watchlist"] }),
  });
  const items = watchlist.data?.items ?? [];

  return (
    <aside className={styles.sidebar} aria-label="Watchlist">
      <div className={styles.panelHeading}>
        <div>
          <span className={styles.eyebrow}>YOUR MARKETS</span>
          <h2>
            Watchlist <em>{items.length}</em>
          </h2>
        </div>
        <button
          className={styles.mobileClose}
          aria-label="Close watchlist"
          onClick={onClose}
        >
          <Icon name="close" />
        </button>
      </div>
      <div className={styles.searchWrap}>
        <Icon name="search" width={17} height={17} />
        <input
          role="combobox"
          aria-label="Find symbol"
          aria-expanded={term.length > 0}
          aria-controls="instrument-results"
          aria-activedescendant={term ? `instrument-${focused}` : undefined}
          value={search}
          onChange={(event) => {
            setSearch(event.target.value.slice(0, 32));
            setFocused(0);
          }}
          onKeyDown={(event) => {
            if (event.key === "ArrowDown") {
              event.preventDefault();
              setFocused((value) =>
                Math.min(value + 1, (results.data?.length ?? 1) - 1),
              );
            }
            if (event.key === "ArrowUp") {
              event.preventDefault();
              setFocused((value) => Math.max(0, value - 1));
            }
            if (event.key === "Enter" && results.data?.[focused]) {
              onSelect(results.data[focused].symbol);
              setSearch("");
            }
            if (event.key === "Escape") setSearch("");
          }}
          placeholder="Search markets"
          autoComplete="off"
        />
        <kbd>/</kbd>
      </div>
      {term && (
        <div
          id="instrument-results"
          role="listbox"
          className={styles.searchResults}
        >
          {results.isPending ? (
            <p>Searching…</p>
          ) : results.isError ? (
            <p>{errorMessage(results.error)}</p>
          ) : results.data?.length ? (
            results.data.map((item, index) => (
              <div
                id={`instrument-${index}`}
                role="option"
                aria-selected={focused === index}
                key={item.symbol}
                className={styles.searchResult}
              >
                <button
                  onClick={() => {
                    onSelect(item.symbol);
                    setSearch("");
                  }}
                >
                  <strong>{item.symbol}</strong>
                  <span>{item.name ?? item.exchange}</span>
                </button>
                {!items.some((row) => row.symbol === item.symbol) && (
                  <button
                    aria-label={`Add ${item.symbol} to watchlist`}
                    onClick={() => add.mutate(item.symbol)}
                  >
                    <Icon name="plus" width={15} height={15} />
                  </button>
                )}
              </div>
            ))
          ) : (
            <p>No instruments found</p>
          )}
        </div>
      )}
      <div className={styles.watchColumnHeads}>
        <span>SYMBOL</span>
        <span>LAST</span>
        <span>CHG %</span>
      </div>
      <div
        className={styles.watchRows}
        role="listbox"
        aria-label="Watchlist symbols"
        onKeyDown={(event) => {
          if (event.key !== "ArrowDown" && event.key !== "ArrowUp") return;
          event.preventDefault();
          const current = items.findIndex((item) => item.symbol === active);
          const next =
            event.key === "ArrowDown"
              ? Math.min(items.length - 1, current + 1)
              : Math.max(0, current - 1);
          const ticker = items[next]?.symbol;
          if (ticker) onSelect(ticker);
        }}
      >
        {watchlist.isPending && (
          <div className={styles.panelState}>Loading watchlist…</div>
        )}
        {watchlist.isError && (
          <div className={styles.panelState}>
            {errorMessage(watchlist.error)}{" "}
            <button onClick={() => void watchlist.refetch()}>Retry</button>
          </div>
        )}
        {!watchlist.isPending && !watchlist.isError && items.length === 0 && (
          <div className={styles.panelState}>
            No symbols yet. Search above to add one.
          </div>
        )}
        {items.map((item, index) => (
          <WatchRow
            key={item.symbol}
            symbol={item.symbol}
            active={item.symbol === active}
            onSelect={onSelect}
            onRemove={() => remove.mutate(item.symbol)}
            fetchBaseline={index < 20}
          />
        ))}
      </div>
      {add.isError && (
        <p className={styles.inlineError}>{errorMessage(add.error)}</p>
      )}
      {remove.isError && (
        <p className={styles.inlineError}>{errorMessage(remove.error)}</p>
      )}
      <div className={styles.sidebarFooter}>
        <span className={styles.tinyDot} /> MARKET DATA VIA GATEWAY
      </div>
    </aside>
  );
}

const WatchRow = memo(function WatchRow({
  symbol,
  active,
  onSelect,
  onRemove,
  fetchBaseline,
}: {
  symbol: string;
  active: boolean;
  onSelect: (symbol: string) => void;
  onRemove: () => void;
  fetchBaseline: boolean;
}) {
  const quote = useQuote(symbol);
  const bars = useQuery({
    queryKey: ["baseline", symbol],
    queryFn: () => api.bars(symbol, "1d", "1mo"),
    enabled: fetchBaseline,
    staleTime: 3600000,
    refetchOnWindowFocus: false,
  });
  const today = new Date().toISOString().slice(0, 10);
  const completed = bars.data?.bars.filter(
    (bar) => bar.time.slice(0, 10) < today,
  );
  const previous = completed?.at(-1)?.close ?? null;
  const delta = change(quote?.last ?? null, previous);
  const stale = !quote || quote.stale || quote.awaitingSnapshot;
  return (
    <div
      className={`${styles.watchRow} ${active ? styles.watchActive : ""}`}
      role="option"
      aria-selected={active}
    >
      <button
        className={styles.watchSelect}
        onClick={() => onSelect(symbol)}
        tabIndex={active ? 0 : -1}
      >
        <span className={styles.watchSymbol}>
          <strong>{symbol}</strong>
          <small>
            {stale
              ? "STALE"
              : quote.dataMode === "REALTIME"
                ? "LIVE"
                : quote.dataMode}
          </small>
        </span>
        <span className={stale ? styles.muted : ""}>{price(quote?.last)}</span>
        <span
          className={
            delta
              ? delta.amount.startsWith("-")
                ? styles.negative
                : styles.positive
              : styles.muted
          }
        >
          {delta?.percent ?? "—"}
        </span>
      </button>
      <button
        className={styles.watchRemove}
        aria-label={`Remove ${symbol} from watchlist`}
        onClick={onRemove}
      >
        <Icon name="close" width={13} height={13} />
      </button>
    </div>
  );
});
