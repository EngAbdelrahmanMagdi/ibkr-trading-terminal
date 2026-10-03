import type { Execution, Order, OrderRequest } from "./api";
import { price } from "./format";

export type Notice = {
  id: string;
  text: string;
  detail: string;
  tone: "info" | "success" | "warning" | "error";
  at: string;
  unread: boolean;
  toast: boolean;
};
type Tracked = {
  order: Order;
  fingerprint: string | null;
  announced: string | null;
};
const actionName = (intent: Order["intent"]) =>
  intent === "BUY" ? "Buy" : intent === "SELL" ? "Sell" : "Short";
const fingerprint = (order: Order) => `${order.status}:${order.filledQuantity}`;

export class Notifications {
  private listeners = new Set<() => void>();
  private items: readonly Notice[] = [];
  private orders = new Map<string, Tracked>();
  private executions = new Set<string>();
  private attempts = new Map<string, string>();
  private hydrated = false;
  private inFlight = new Set<string>();
  private held: [readonly Order[], readonly Execution[]] | null = null;
  begin(key: string) {
    this.inFlight.add(key);
  }
  end(key: string) {
    this.inFlight.delete(key);
    if (!this.inFlight.size && this.held) {
      const held = this.held;
      this.held = null;
      this.observe(...held);
    }
  }
  private capacityReported = false;
  constructor(private readonly now = () => new Date().toISOString()) {}
  snapshot = () => this.items;
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  private changed() {
    this.listeners.forEach((listener) => listener());
  }
  private add(
    id: string,
    text: string,
    detail: string,
    tone: Notice["tone"],
    at = this.now(),
    toast = true,
  ) {
    if (this.items.some((item) => item.id === id)) return;
    const next = [
      ...this.items,
      {
        id,
        text: text.slice(0, 240),
        detail: detail.slice(0, 500),
        tone,
        at,
        unread: true,
        toast,
      },
    ].slice(-100);
    const visible = next.filter((item) => item.toast).slice(-3);
    this.items = next.map((item) => ({
      ...item,
      toast: visible.includes(item),
    }));
    this.changed();
  }
  private capacity() {
    if (this.capacityReported) return;
    this.capacityReported = true;
    this.add(
      "capacity",
      "Notification tracking capacity reached",
      "New activity may not appear here. Core orders and executions remain available.",
      "info",
    );
  }
  private remember(order: Order, local = false) {
    if (!this.orders.has(order.id)) {
      if (this.orders.size >= 1000) {
        this.capacity();
        return;
      }
      this.orders.set(order.id, {
        order,
        fingerprint: local ? null : fingerprint(order),
        announced: null,
      });
    }
  }
  private attempt(key: string, outcome: string) {
    if (this.attempts.get(key) === outcome) return false;
    if (this.attempts.has(key) || this.attempts.size < 1000)
      this.attempts.set(key, outcome);
    else this.capacity();
    return true;
  }
  submitted(key: string, order: Order) {
    this.remember(order, true);
    const accepted = order.status === "SUBMITTED";
    if (this.attempt(key, "submitted"))
      this.add(
        `submit:${key}`,
        `${actionName(order.intent)} order ${accepted ? "accepted / open" : "submitted"}`,
        `${order.quantity} ${order.symbol}`,
        "info",
      );
    if (order.status === "UNKNOWN") this.uncertain(key, order);
    if (accepted || order.status === "UNKNOWN") {
      const tracked = this.orders.get(order.id);
      if (tracked) tracked.announced = fingerprint(order);
    }
    // No fill or execution claim is derived from a successful submission response.
  }
  uncertain(
    key: string,
    body: Pick<OrderRequest, "intent" | "quantity" | "symbol">,
  ) {
    if (this.attempt(key, "uncertain"))
      this.add(
        `uncertain:${key}`,
        "Order status uncertain — do not resubmit",
        `${actionName(body.intent)} · ${body.quantity} ${body.symbol}`,
        "warning",
      );
  }
  action(order: Order, kind: "cancel" | "confirm" | "decline") {
    this.remember(order, true);
    this.add(
      `action:${order.id}:${kind}:${order.status}`,
      kind === "cancel"
        ? "Cancellation requested"
        : kind === "confirm"
          ? "Confirmation submitted"
          : "Decline submitted",
      `${actionName(order.intent)} · ${order.symbol}`,
      "info",
    );
  }
  failure(key: string, text: string, detail: string) {
    if (this.attempt(key, text))
      this.add(`error:${key}:${text}`, text, detail, "error");
  }
  observe(orders: readonly Order[], executions: readonly Execution[]) {
    if (!this.hydrated && this.inFlight.size) {
      this.held = [orders, executions];
      return;
    }
    if (!this.hydrated) {
      orders.forEach((order) => this.remember(order));
      executions.forEach((execution) => {
        // Locally registered orders can complete while initial queries are hydrating.
        if (this.orders.get(execution.orderId)?.fingerprint === null) return;
        if (this.executions.size < 1000) this.executions.add(execution.id);
        else this.capacity();
      });
      this.hydrated = true;
      orders = orders.filter(
        (order) => this.orders.get(order.id)?.fingerprint === null,
      );
    }
    const executionOrders = new Set<string>();
    for (const execution of executions) {
      if (this.executions.has(execution.id)) continue;
      if (this.executions.size >= 1000) {
        this.capacity();
        continue;
      }
      this.executions.add(execution.id);
      executionOrders.add(execution.orderId);
      const order =
        orders.find((item) => item.id === execution.orderId) ??
        this.orders.get(execution.orderId)?.order;
      const verb =
        order?.intent === "SHORT"
          ? "Short sold"
          : order?.intent === "SELL"
            ? "Sold"
            : order?.intent === "BUY"
              ? "Bought"
              : `${execution.side === "BUY" ? "Buy" : "Sell"}-side execution`;
      this.add(
        `execution:${execution.id}`,
        `${verb} ${execution.quantity} ${execution.symbol} @ ${price(execution.price)}`,
        "Core execution",
        "success",
        execution.executedAt,
      );
    }
    for (const order of orders) {
      if (!this.orders.has(order.id)) this.remember(order, true);
      const tracked = this.orders.get(order.id);
      if (!tracked) continue;
      const next = fingerprint(order);
      if (tracked.fingerprint === next) {
        tracked.order = order;
        continue;
      }
      tracked.order = order;
      tracked.fingerprint = next;
      if (tracked.announced === next) continue;
      const [text, tone] = lifecycle(order);
      this.add(
        `state:${order.id}:${next}`,
        text,
        `${actionName(order.intent)} · ${order.quantity} ${order.symbol}${order.status === "PENDING_CONFIRMATION" && order.pendingConfirmation?.message ? ` · ${order.pendingConfirmation.message}` : order.status === "REJECTED" && order.rejectionReason ? ` · ${order.rejectionReason}` : ""}`,
        tone,
        order.updatedAt,
        !(
          executionOrders.has(order.id) &&
          ["FILLED", "PARTIALLY_FILLED"].includes(order.status)
        ),
      );
    }
  }
  dismiss(id: string) {
    this.items = this.items.filter((item) => item.id !== id);
    this.changed();
  }
  hideToast(id: string) {
    this.items = this.items.map((item) =>
      item.id === id ? { ...item, toast: false } : item,
    );
    this.changed();
  }
  read() {
    this.items = this.items.map((item) => ({ ...item, unread: false }));
    this.changed();
  }
}
function lifecycle(order: Order): [string, Notice["tone"]] {
  switch (order.status) {
    case "CREATED":
    case "SUBMISSION_PENDING":
      return [`${actionName(order.intent)} order submission pending`, "info"];
    case "SUBMITTED":
      return [`${actionName(order.intent)} order accepted / open`, "info"];
    case "PENDING_CONFIRMATION":
      return ["Order confirmation required", "warning"];
    case "PARTIALLY_FILLED":
      return [
        `Partially filled — ${order.filledQuantity} / ${order.quantity} shares`,
        "success",
      ];
    case "FILLED":
      return ["Order fully filled", "success"];
    case "CANCEL_PENDING":
      return ["Cancellation pending", "info"];
    case "CANCELLED":
      return ["Order cancelled", "info"];
    case "REJECTED":
      return ["Order rejected", "error"];
    case "FAILED":
      return ["Order failed", "error"];
    case "UNKNOWN":
      return ["Order status uncertain — do not resubmit", "warning"];
  }
}
