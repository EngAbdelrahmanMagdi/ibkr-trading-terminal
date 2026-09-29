"use client";

import { useSyncExternalStore } from "react";
import { z } from "zod";
import type { OrderRequest } from "@/lib/api";

const storageKey = "trading-terminal:order-attempts:v1";
const attemptSchema = z.object({
  key: z.uuid(),
  createdAt: z.string(),
  orderId: z.uuid().optional(),
  body: z.object({
    symbol: z.string().regex(/^[A-Z0-9.]{1,16}$/),
    intent: z.enum(["BUY", "SELL", "SHORT"]),
    orderType: z.enum(["MARKET", "LIMIT"]),
    quantity: z.string().regex(/^\d{1,15}(\.\d{1,4})?$/),
    limitPrice: z
      .string()
      .regex(/^\d{1,13}(\.\d{1,6})?$/)
      .optional(),
    timeInForce: z.enum(["DAY", "GTC"]),
  }),
});
export type Attempt = z.infer<typeof attemptSchema>;
const listeners = new Set<() => void>();
const empty: Attempt[] = [];
let snapshot: Attempt[] | null = null;

export function loadAttempts(): Attempt[] {
  if (typeof window === "undefined") return [];
  try {
    const raw = sessionStorage.getItem(storageKey);
    if (!raw || raw.length > 10000) return [];
    const parsed = z.array(attemptSchema).max(5).safeParse(JSON.parse(raw));
    return parsed.success ? parsed.data : [];
  } catch {
    return [];
  }
}

export function saveAttempts(attempts: Attempt[]) {
  if (typeof window === "undefined") return;
  const valid = z.array(attemptSchema).max(5).safeParse(attempts.slice(-5));
  if (valid.success) {
    snapshot = valid.data;
    sessionStorage.setItem(storageKey, JSON.stringify(valid.data));
    listeners.forEach((listener) => listener());
  }
}

export function useAttempts(): Attempt[] {
  return useSyncExternalStore(
    (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    () => {
      snapshot ??= loadAttempts();
      return snapshot;
    },
    () => empty,
  );
}

export function newAttempt(body: OrderRequest): Attempt {
  return {
    key: crypto.randomUUID(),
    createdAt: new Date().toISOString(),
    body,
  };
}
