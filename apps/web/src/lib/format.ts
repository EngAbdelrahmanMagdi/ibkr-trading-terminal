import Decimal from "decimal.js";
import type { OptionalMetric } from "@/lib/api";

export function price(value: string | null | undefined, digits = 2): string {
  if (value == null) return "—";
  try {
    return new Decimal(value).toFixed(digits);
  } catch {
    return "—";
  }
}

export function signed(
  value: string | null | undefined,
): "positive" | "negative" | "neutral" {
  if (value == null) return "neutral";
  try {
    const number = new Decimal(value);
    return number.isPositive() && !number.isZero()
      ? "positive"
      : number.isNegative()
        ? "negative"
        : "neutral";
  } catch {
    return "neutral";
  }
}

export function metric(value: OptionalMetric | undefined): string {
  if (!value?.available || value.value == null) return "Unavailable";
  return `${price(value.value)}${value.currency ? ` ${value.currency}` : ""}`;
}

export function change(
  current: string | null,
  baseline: string | null,
): { amount: string; percent: string } | null {
  if (!current || !baseline) return null;
  try {
    const now = new Decimal(current);
    const previous = new Decimal(baseline);
    if (previous.isZero()) return null;
    const delta = now.minus(previous);
    return {
      amount: `${delta.isPositive() ? "+" : ""}${delta.toFixed(2)}`,
      percent: `${delta.isPositive() ? "+" : ""}${delta.div(previous).times(100).toFixed(2)}%`,
    };
  } catch {
    return null;
  }
}

export function time(value: string | null | undefined): string {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.valueOf())
    ? "—"
    : `${date.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", timeZone: "UTC" })} UTC`;
}
