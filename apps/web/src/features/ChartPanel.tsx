"use client";

import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  CandlestickSeries,
  ColorType,
  createChart,
  HistogramSeries,
  type IChartApi,
  type IPriceLine,
  type ISeriesApi,
  type UTCTimestamp,
} from "lightweight-charts";
import { api, errorMessage } from "@/lib/api";
import { price } from "@/lib/format";
import { stream, useConnectionStatus } from "@/lib/realtime";
import { Icon } from "@/components/Icon";
import styles from "./terminal.module.css";

const intervals = ["1m", "5m", "15m", "1h", "1d"] as const;
type Interval = (typeof intervals)[number];
const ranges: Record<Interval, string> = {
  "1m": "1d",
  "5m": "5d",
  "15m": "5d",
  "1h": "5d",
  "1d": "1mo",
};
const durations: Record<Interval, number> = {
  "1m": 60000,
  "5m": 300000,
  "15m": 900000,
  "1h": 3600000,
  "1d": 86400000,
};

export function ChartPanel({ symbol }: { symbol: string }) {
  const [interval, setInterval] = useState<Interval>("5m");
  const status = useConnectionStatus();
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  const priceLineRef = useRef<IPriceLine | null>(null);
  const dataKeyRef = useRef("");
  const bars = useQuery({
    queryKey: ["bars", symbol, interval],
    queryFn: () => api.bars(symbol, interval, ranges[interval]),
    staleTime: Math.min(durations[interval], 60000),
  });

  useEffect(() => {
    const element = containerRef.current;
    if (!element) return;
    const chart = createChart(element, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: "#101820" },
        textColor: "#82909d",
        attributionLogo: true,
        fontFamily: "Arial, sans-serif",
        fontSize: 11,
      },
      grid: {
        vertLines: { color: "#1d2933" },
        horzLines: { color: "#1d2933" },
      },
      crosshair: {
        vertLine: { color: "#617586", labelBackgroundColor: "#263b4a" },
        horzLine: { color: "#617586", labelBackgroundColor: "#263b4a" },
      },
      rightPriceScale: {
        borderColor: "#25343e",
        scaleMargins: { top: 0.07, bottom: 0.2 },
      },
      timeScale: {
        borderColor: "#25343e",
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 7,
      },
    });
    chartRef.current = chart;
    candleRef.current = chart.addSeries(CandlestickSeries, {
      upColor: "#2ac6a0",
      downColor: "#e46f75",
      borderVisible: false,
      wickUpColor: "#2ac6a0",
      wickDownColor: "#e46f75",
      priceLineVisible: false,
    });
    volumeRef.current = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "",
      lastValueVisible: false,
      priceLineVisible: false,
    });
    volumeRef.current
      .priceScale()
      .applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });
    const observer = new ResizeObserver(() =>
      chart.resize(element.clientWidth, element.clientHeight),
    );
    observer.observe(element);
    return () => {
      observer.disconnect();
      chart.remove();
      chartRef.current = null;
      candleRef.current = null;
      volumeRef.current = null;
      priceLineRef.current = null;
    };
  }, []);

  useEffect(() => {
    const candle = candleRef.current;
    const volume = volumeRef.current;
    if (!candle || !volume) return;
    const key = `${symbol}:${interval}`;
    const changed = dataKeyRef.current !== key;
    dataKeyRef.current = key;
    const source = bars.data?.bars.slice(-1000) ?? [];
    const candles = source
      .map((bar) => ({
        time: Math.floor(Date.parse(bar.time) / 1000) as UTCTimestamp,
        open: Number(bar.open),
        high: Number(bar.high),
        low: Number(bar.low),
        close: Number(bar.close),
      }))
      .filter(
        (bar) =>
          Number.isFinite(bar.time) &&
          [bar.open, bar.high, bar.low, bar.close].every(Number.isFinite),
      );
    const volumes = source
      .map((bar) => ({
        time: Math.floor(Date.parse(bar.time) / 1000) as UTCTimestamp,
        value: bar.volume,
        color:
          Number(bar.close) >= Number(bar.open) ? "#21685d65" : "#75424b65",
      }))
      .filter((bar) => Number.isFinite(bar.time) && Number.isFinite(bar.value));
    candle.setData(candles);
    volume.setData(volumes);
    if (changed && candles.length) chartRef.current?.timeScale().fitContent();
  }, [bars.data, symbol, interval]);

  useEffect(() => {
    const candle = candleRef.current;
    if (!candle) return;
    const update = () => {
      const quote = stream.getQuote(symbol);
      const numeric = quote?.last ? Number(quote.last) : NaN;
      const visible =
        !!quote &&
        !quote.stale &&
        !quote.awaitingSnapshot &&
        Number.isFinite(numeric) &&
        status === "LIVE";
      if (visible && !priceLineRef.current) {
        priceLineRef.current = candle.createPriceLine({
          price: numeric,
          color: "#50a9d6",
          lineWidth: 1,
          lineStyle: 2,
          axisLabelVisible: true,
          title: "LAST",
        });
      } else if (priceLineRef.current) {
        priceLineRef.current.applyOptions({
          price: Number.isFinite(numeric) ? numeric : 0,
          lineVisible: visible,
          axisLabelVisible: visible,
        });
      }
    };
    update();
    return stream.subscribeQuote(symbol, update);
  }, [symbol, status]);

  useEffect(() => {
    if (status === "LIVE") void bars.refetch();
    // Refetch on a connection transition; the query handles repeated requests.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [status]);

  useEffect(() => {
    let timer: ReturnType<typeof setTimeout>;
    const schedule = () => {
      const duration = durations[interval];
      const next = duration - (Date.now() % duration) + 2000;
      timer = setTimeout(() => {
        if (document.visibilityState === "visible") void bars.refetch();
        schedule();
      }, next);
    };
    schedule();
    return () => clearTimeout(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [interval, symbol]);

  const latest = bars.data?.bars.at(-1);
  return (
    <section className={styles.chartPanel} aria-label="Price chart">
      <div className={styles.chartToolbar}>
        <div className={styles.chartTitle}>
          <Icon name="bars" width={16} height={16} />
          <strong>PRICE ACTION</strong>
          <span>{symbol} / USD</span>
        </div>
        <div
          className={styles.chartControls}
          role="group"
          aria-label="Chart interval"
        >
          {intervals.map((item) => (
            <button
              key={item}
              className={interval === item ? styles.intervalActive : ""}
              aria-pressed={interval === item}
              onClick={() => setInterval(item)}
            >
              {item}
            </button>
          ))}
        </div>
        <button
          className={styles.iconButton}
          aria-label="Refresh chart"
          onClick={() => void bars.refetch()}
        >
          <Icon name="refresh" width={16} height={16} />
        </button>
      </div>
      <div className={styles.chartMeta}>
        <span>
          O <strong>{price(latest?.open)}</strong>
        </span>
        <span>
          H <strong>{price(latest?.high)}</strong>
        </span>
        <span>
          L <strong>{price(latest?.low)}</strong>
        </span>
        <span>
          C <strong>{price(latest?.close)}</strong>
        </span>
        <span className={styles.chartSource}>
          {bars.data?.source ?? "—"} BARS ·{" "}
          {status === "LIVE" ? "LIVE PRICE" : status}
        </span>
      </div>
      <div className={styles.chartCanvas} ref={containerRef} />
      {(bars.isPending || bars.isError || bars.data?.bars.length === 0) && (
        <div className={styles.chartOverlay}>
          {bars.isPending ? (
            "Loading historical bars…"
          ) : bars.isError ? (
            <>
              {errorMessage(bars.error)}{" "}
              <button onClick={() => void bars.refetch()}>Retry</button>
            </>
          ) : (
            "No chart data for this range."
          )}
        </div>
      )}
      <div className={styles.chartFootnote}>
        Candles: Gateway REST · Price marker: WebSocket{" "}
        <a
          href="https://www.tradingview.com/"
          target="_blank"
          rel="noopener noreferrer"
        >
          Charting by TradingView
        </a>
      </div>
    </section>
  );
}
