"use client";

import dynamic from "next/dynamic";
import { useEffect, useMemo, useState } from "react";
import {
  QueryClient,
  QueryClientProvider,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { api, errorMessage } from "@/lib/api";
import { metric, price, signed } from "@/lib/format";
import { stream, useConnectionStatus, useQuote } from "@/lib/realtime";
import { Icon } from "@/components/Icon";
import { WatchlistPanel } from "./WatchlistPanel";
import { OrderTicket } from "./OrderTicket";
import { TradeDeck } from "./TradeDeck";
import styles from "./terminal.module.css";

const ChartPanel = dynamic(
  () => import("./ChartPanel").then((module) => module.ChartPanel),
  {
    ssr: false,
    loading: () => (
      <div className={styles.chartLoading}>Preparing market chart…</div>
    ),
  },
);

const client = new QueryClient({
  defaultOptions: {
    queries: { retry: 1, staleTime: 10000, refetchOnWindowFocus: true },
  },
});

export function Terminal() {
  return (
    <QueryClientProvider client={client}>
      <Workspace />
    </QueryClientProvider>
  );
}

function Workspace() {
  const queryClient = useQueryClient();
  const [symbol, setSymbol] = useState("NVDA");
  const [watchOpen, setWatchOpen] = useState(false);
  const [ticketOpen, setTicketOpen] = useState(false);
  const watchlist = useQuery({
    queryKey: ["watchlist"],
    queryFn: api.watchlist,
    staleTime: 60000,
  });
  const portfolio = useQuery({
    queryKey: ["portfolio"],
    queryFn: api.portfolio,
    refetchInterval: 60000,
  });
  const instrument = useQuery({
    queryKey: ["instrument", symbol],
    queryFn: () => api.search(symbol),
    staleTime: 300000,
  });
  const metadata = instrument.data?.find((item) => item.symbol === symbol);
  const watchSymbols = useMemo(
    () => watchlist.data?.items.map((item) => item.symbol) ?? [],
    [watchlist.data],
  );
  const status = useConnectionStatus();

  useEffect(() => {
    stream.start();
    const unsubscribe = stream.subscribeHint(() => {
      for (const key of ["orders", "executions", "positions", "portfolio"]) {
        void queryClient.invalidateQueries({ queryKey: [key] });
      }
    });
    return () => {
      unsubscribe();
      stream.stop();
    };
  }, [queryClient]);

  useEffect(() => {
    stream.setSymbols(watchSymbols, [symbol]);
  }, [watchSymbols, symbol]);

  useEffect(() => {
    if (status !== "LIVE") return;
    for (const key of ["orders", "executions", "positions", "portfolio"]) {
      void queryClient.invalidateQueries({ queryKey: [key] });
    }
  }, [status, queryClient]);

  return (
    <main className={styles.terminal}>
      <header className={styles.header}>
        <div className={styles.brand}>
          <span className={styles.brandMark}>
            <Icon name="bars" width={20} height={20} />
          </span>
          <span className={styles.brandName}>
            APERTURE<span> / TERMINAL</span>
          </span>
        </div>
        <nav className={styles.headerNav} aria-label="Workspace">
          <span className={styles.navActive}>Trading workspace</span>
          <span className={styles.navMeta}>
            MARKETS <span className={styles.navDot}>/</span> EQUITIES
          </span>
        </nav>
        <div className={styles.headerActions}>
          <span className={styles.modeBadge}>
            {portfolio.data?.accountMode ?? "ACCOUNT MODE UNKNOWN"}
          </span>
          <ConnectionBadge status={status} />
          <button
            className={styles.mobileMenu}
            aria-label="Open watchlist"
            onClick={() => setWatchOpen(true)}
          >
            <Icon name="menu" />
          </button>
        </div>
      </header>
      <section className={styles.accountStrip} aria-label="Account summary">
        <div className={styles.accountIdentity}>
          <span className={styles.accountEyebrow}>ACCOUNT OVERVIEW</span>
          <strong>Paper portfolio</strong>
        </div>
        {portfolio.isError ? (
          <div className={styles.accountError}>
            {errorMessage(portfolio.error)}{" "}
            <button onClick={() => void portfolio.refetch()}>Retry</button>
          </div>
        ) : (
          <div className={styles.accountMetrics}>
            <Metric
              label="NET LIQUIDATION"
              value={metric(portfolio.data?.netLiquidation)}
              loading={portfolio.isPending}
            />
            <Metric
              label="BUYING POWER"
              value={metric(portfolio.data?.buyingPower)}
              loading={portfolio.isPending}
            />
            <Metric
              label="DAY P&L"
              value={metric(portfolio.data?.dayPnl)}
              tone={signed(portfolio.data?.dayPnl.value)}
              loading={portfolio.isPending}
            />
            <Metric
              label="UNREALIZED"
              value={metric(portfolio.data?.unrealizedPnl)}
              tone={signed(portfolio.data?.unrealizedPnl.value)}
              loading={portfolio.isPending}
            />
          </div>
        )}
        <span className={styles.accountAsOf}>
          {portfolio.data
            ? `AS OF ${new Date(portfolio.data.asOf).toLocaleTimeString("en-GB", { timeZone: "UTC" })} UTC`
            : "CORE DATA"}
        </span>
      </section>
      <div className={styles.grid}>
        <div
          className={`${styles.sidebarWrap} ${watchOpen ? styles.mobileOpen : ""}`}
        >
          <WatchlistPanel
            active={symbol}
            onSelect={(next) => {
              setSymbol(next);
              setWatchOpen(false);
            }}
            onClose={() => setWatchOpen(false)}
            watchlist={watchlist}
          />
        </div>
        <section className={styles.center} aria-label="Symbol workspace">
          <SymbolHeader
            symbol={symbol}
            name={metadata?.name ?? null}
            exchange={metadata?.exchange ?? null}
          />
          <ChartPanel symbol={symbol} />
          <TradeDeck />
        </section>
        <div
          className={`${styles.ticketWrap} ${ticketOpen ? styles.mobileOpen : ""}`}
        >
          <OrderTicket
            symbol={symbol}
            instrument={metadata}
            onClose={() => setTicketOpen(false)}
          />
        </div>
      </div>
      <button
        className={styles.mobileTrade}
        onClick={() => setTicketOpen(true)}
      >
        Trade {symbol} <Icon name="arrow" />
      </button>
    </main>
  );
}

function Metric({
  label,
  value,
  tone = "neutral",
  loading,
}: {
  label: string;
  value: string;
  tone?: string;
  loading: boolean;
}) {
  return (
    <div className={styles.accountMetric}>
      <span>{label}</span>
      <strong
        className={
          tone === "positive"
            ? styles.positive
            : tone === "negative"
              ? styles.negative
              : ""
        }
      >
        {loading ? "—" : value}
      </strong>
    </div>
  );
}

function ConnectionBadge({
  status,
}: {
  status: ReturnType<typeof useConnectionStatus>;
}) {
  return (
    <div
      className={`${styles.connectionBadge} ${status === "LIVE" ? styles.live : status === "STALE" ? styles.stale : ""}`}
      role="status"
    >
      <span className={styles.connectionDot} />
      {status}
      {status === "DISCONNECTED" && (
        <button
          aria-label="Retry market connection"
          onClick={() => stream.retry()}
        >
          <Icon name="refresh" width={14} height={14} />
        </button>
      )}
    </div>
  );
}

function SymbolHeader({
  symbol,
  name,
  exchange,
}: {
  symbol: string;
  name: string | null;
  exchange: string | null;
}) {
  const quote = useQuote(symbol);
  const status = useConnectionStatus();
  const live =
    !!quote && !quote.stale && !quote.awaitingSnapshot && status === "LIVE";
  return (
    <div className={styles.symbolHeader}>
      <div className={styles.symbolIdentity}>
        <div className={styles.symbolMonogram}>{symbol.slice(0, 1)}</div>
        <div>
          <div className={styles.symbolTitle}>
            <h1>{symbol}</h1>
            <span>{exchange ?? "EQUITY"}</span>
          </div>
          <p>{name ?? "Instrument"}</p>
        </div>
      </div>
      <div className={styles.symbolPrice}>
        <strong>{price(quote?.last)}</strong>
        <span className={live ? styles.quoteLive : styles.quoteStale}>
          {live ? quote.dataMode : quote ? "STALE PRICE" : "AWAITING QUOTE"}
        </span>
      </div>
      <div className={styles.bidAsk}>
        <div>
          <span>BID</span>
          <strong>{price(quote?.bid)}</strong>
        </div>
        <div>
          <span>ASK</span>
          <strong>{price(quote?.ask)}</strong>
        </div>
      </div>
    </div>
  );
}
