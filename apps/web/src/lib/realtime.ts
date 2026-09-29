"use client";

import { useSyncExternalStore } from "react";
import { z } from "zod";
import { streamUrl } from "@/lib/api";

const symbol = z.string().regex(/^[A-Z0-9.]{1,16}$/);
const quoteSchema = z.object({
  type: z.enum(["snapshot", "quote"]),
  symbol,
  bid: z.string().nullable(),
  ask: z.string().nullable(),
  last: z.string().nullable(),
  bidSize: z.number().int().nonnegative().nullable(),
  askSize: z.number().int().nonnegative().nullable(),
  volume: z.number().int().nonnegative().nullable(),
  sequence: z.number().int().nonnegative(),
  timestamp: z.string(),
  stale: z.boolean(),
  dataMode: z.enum(["REALTIME", "DELAYED", "FROZEN", "FROZEN_DELAYED"]),
  halted: z.boolean().nullable(),
});
const connectionSchema = z.object({
  type: z.literal("connection"),
  state: z.enum([
    "DISCONNECTED",
    "CONNECTING",
    "AUTHENTICATING",
    "READY",
    "DEGRADED",
    "RECONNECTING",
  ]),
  source: z.enum(["IBKR", "MOCK"]),
  limits: z.object({
    maxSymbolsPerSubscribe: z.number().int().positive(),
    maxSubscribedSymbols: z.number().int().positive(),
    maxInboundMessageBytes: z.number().int().positive(),
    heartbeatIntervalMs: z.number().int().positive(),
  }),
});
const staleSchema = z.object({
  type: z.literal("stale"),
  symbols: z.array(symbol).max(100),
  reason: z.string(),
});
const orderHintSchema = z.object({
  type: z.literal("order-update"),
  orderId: z.uuid(),
  status: z.string(),
});
const errorSchema = z.object({
  type: z.literal("error"),
  code: z.string(),
  message: z.string().max(500),
  symbols: z.array(symbol).optional(),
});

export type Quote = z.infer<typeof quoteSchema> & {
  awaitingSnapshot?: boolean;
};
export type ConnectionStatus =
  "CONNECTING" | "LIVE" | "STALE" | "RECONNECTING" | "DISCONNECTED";
type Listener = () => void;

export class StreamClient {
  private socket: WebSocket | null = null;
  private desired = new Set<string>();
  private subscribed = new Set<string>();
  private quotes = new Map<string, Quote>();
  private quoteListeners = new Map<string, Set<Listener>>();
  private statusListeners = new Set<Listener>();
  private hintListeners = new Set<Listener>();
  private pending = new Set<string>();
  private raf = 0;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private heartbeat: ReturnType<typeof setInterval> | undefined;
  private lastMessage = 0;
  private heartbeatMs = 15000;
  private maxPerFrame = 50;
  private maxSymbols = 100;
  private attempts = 0;
  private enabled = false;
  private status: ConnectionStatus = "DISCONNECTED";
  private rejected = 0;

  start() {
    if (this.enabled) return;
    this.enabled = true;
    window.addEventListener("online", this.retry);
    document.addEventListener("visibilitychange", this.visible);
    this.connect();
  }

  stop() {
    this.enabled = false;
    window.removeEventListener("online", this.retry);
    document.removeEventListener("visibilitychange", this.visible);
    clearTimeout(this.timer);
    clearInterval(this.heartbeat);
    this.socket?.close(1000);
    this.socket = null;
    this.setStatus("DISCONNECTED");
  }

  retry = () => {
    if (!this.enabled) return;
    clearTimeout(this.timer);
    this.attempts = 0;
    if (
      this.socket?.readyState === WebSocket.OPEN ||
      this.socket?.readyState === WebSocket.CONNECTING
    )
      return;
    this.connect();
  };

  private visible = () => {
    if (document.visibilityState === "visible") this.retry();
  };

  getStatus = () => this.status;
  getRejectedCount = () => this.rejected;
  subscribeStatus = (listener: Listener) => {
    this.statusListeners.add(listener);
    return () => this.statusListeners.delete(listener);
  };
  subscribeHint = (listener: Listener) => {
    this.hintListeners.add(listener);
    return () => this.hintListeners.delete(listener);
  };
  getQuote = (ticker: string) => this.quotes.get(ticker) ?? null;
  subscribeQuote = (ticker: string, listener: Listener) => {
    let listeners = this.quoteListeners.get(ticker);
    if (!listeners) {
      listeners = new Set();
      this.quoteListeners.set(ticker, listeners);
    }
    listeners.add(listener);
    return () => {
      listeners?.delete(listener);
      if (listeners?.size === 0) this.quoteListeners.delete(ticker);
    };
  };

  setSymbols(symbols: string[], priorities: string[]) {
    const ordered = [
      ...new Set(
        [...priorities, ...symbols].filter(
          (item) => symbol.safeParse(item).success,
        ),
      ),
    ].slice(0, Math.min(this.maxSymbols, 100));
    this.desired = new Set(ordered);
    for (const ticker of this.quotes.keys()) {
      if (!this.desired.has(ticker)) {
        this.quotes.delete(ticker);
        this.queue(ticker);
      }
    }
    this.syncSubscriptions();
  }

  private setStatus(status: ConnectionStatus) {
    if (this.status === status) return;
    this.status = status;
    this.statusListeners.forEach((listener) => listener());
  }

  private queue(symbol: string) {
    this.pending.add(symbol);
    if (this.raf) return;
    this.raf = requestAnimationFrame(() => {
      this.raf = 0;
      const pending = this.pending;
      this.pending = new Set();
      pending.forEach((ticker) =>
        this.quoteListeners.get(ticker)?.forEach((listener) => listener()),
      );
    });
  }

  private markAllStale() {
    this.quotes.forEach((quote, ticker) => {
      this.quotes.set(ticker, {
        ...quote,
        stale: true,
        awaitingSnapshot: true,
      });
      this.queue(ticker);
    });
  }

  private connect() {
    if (!this.enabled) return;
    this.setStatus(this.attempts ? "RECONNECTING" : "CONNECTING");
    this.markAllStale();
    this.subscribed.clear();
    const socket = new WebSocket(streamUrl);
    this.socket = socket;
    socket.onopen = () => {
      this.lastMessage = Date.now();
      this.attempts = 0;
      this.syncSubscriptions();
      clearInterval(this.heartbeat);
      this.heartbeat = setInterval(() => {
        if (Date.now() - this.lastMessage > this.heartbeatMs * 3)
          socket.close();
      }, 5000);
    };
    socket.onmessage = (event) => {
      if (typeof event.data !== "string" || event.data.length > 65536) {
        this.rejected++;
        return;
      }
      this.lastMessage = Date.now();
      this.handle(event.data);
    };
    socket.onerror = () => socket.close();
    socket.onclose = () => {
      if (this.socket !== socket) return;
      clearInterval(this.heartbeat);
      this.socket = null;
      this.markAllStale();
      if (!this.enabled) return;
      if (this.attempts >= 8) {
        this.setStatus("DISCONNECTED");
        return;
      }
      this.attempts++;
      this.setStatus("RECONNECTING");
      const max = Math.min(30000, 500 * 2 ** this.attempts);
      this.timer = setTimeout(() => this.connect(), Math.random() * max);
    };
  }

  private syncSubscriptions() {
    if (this.socket?.readyState !== WebSocket.OPEN) return;
    const removed = [...this.subscribed].filter(
      (ticker) => !this.desired.has(ticker),
    );
    const added = [...this.desired].filter(
      (ticker) => !this.subscribed.has(ticker),
    );
    for (const [type, values] of [
      ["unsubscribe", removed],
      ["subscribe", added],
    ] as const) {
      for (let index = 0; index < values.length; index += this.maxPerFrame) {
        this.socket.send(
          JSON.stringify({
            type,
            symbols: values.slice(index, index + this.maxPerFrame),
          }),
        );
      }
    }
    removed.forEach((ticker) => this.subscribed.delete(ticker));
    added.forEach((ticker) => this.subscribed.add(ticker));
  }

  private handle(raw: string) {
    let value: unknown;
    try {
      value = JSON.parse(raw);
    } catch {
      this.rejected++;
      return;
    }
    if (typeof value !== "object" || value === null || !("type" in value)) {
      this.rejected++;
      return;
    }
    const type = value.type;
    if (type === "heartbeat") return;
    if (type === "connection") {
      const message = connectionSchema.safeParse(value);
      if (!message.success) {
        this.rejected++;
        return;
      }
      this.heartbeatMs = message.data.limits.heartbeatIntervalMs;
      this.maxPerFrame = Math.min(
        message.data.limits.maxSymbolsPerSubscribe,
        50,
      );
      this.maxSymbols = Math.min(message.data.limits.maxSubscribedSymbols, 100);
      this.setStatus(
        message.data.state === "READY"
          ? "LIVE"
          : message.data.state === "DEGRADED"
            ? "STALE"
            : "RECONNECTING",
      );
      this.setSymbols([...this.desired], []);
      return;
    }
    if (type === "snapshot" || type === "quote") {
      const message = quoteSchema.safeParse(value);
      if (!message.success) {
        this.rejected++;
        return;
      }
      const ticker = message.data.symbol;
      if (!this.desired.has(ticker)) return;
      const previous = this.quotes.get(ticker);
      if (type === "quote" && (previous?.awaitingSnapshot || !previous)) return;
      if (
        type === "quote" &&
        previous &&
        message.data.sequence <= previous.sequence
      )
        return;
      this.quotes.set(ticker, { ...message.data, awaitingSnapshot: false });
      this.queue(ticker);
      return;
    }
    if (type === "stale") {
      const message = staleSchema.safeParse(value);
      if (!message.success) {
        this.rejected++;
        return;
      }
      message.data.symbols.forEach((ticker) => {
        const quote = this.quotes.get(ticker);
        if (quote) {
          this.quotes.set(ticker, { ...quote, stale: true });
          this.queue(ticker);
        }
      });
      return;
    }
    if (type === "order-update") {
      if (!orderHintSchema.safeParse(value).success) {
        this.rejected++;
        return;
      }
      this.hintListeners.forEach((listener) => listener());
      return;
    }
    if (type === "error") {
      const message = errorSchema.safeParse(value);
      if (!message.success) {
        this.rejected++;
        return;
      }
      if (message.data.code === "SOURCE_UNAVAILABLE") {
        message.data.symbols?.forEach((ticker) => {
          const quote = this.quotes.get(ticker);
          if (quote) {
            this.quotes.set(ticker, { ...quote, stale: true });
            this.queue(ticker);
          }
        });
      }
      return;
    }
    this.rejected++;
  }
}

export const stream = new StreamClient();

export function useQuote(symbol: string): Quote | null {
  return useSyncExternalStore(
    (listener) => stream.subscribeQuote(symbol, listener),
    () => stream.getQuote(symbol),
    () => null,
  );
}

export function useConnectionStatus(): ConnectionStatus {
  return useSyncExternalStore(
    stream.subscribeStatus,
    stream.getStatus,
    () => "DISCONNECTED",
  );
}
