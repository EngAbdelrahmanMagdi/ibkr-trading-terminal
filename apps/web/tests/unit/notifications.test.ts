import { Notifications } from "@/lib/notifications";
import type { Execution, Order } from "@/lib/api";
const at = "2026-10-03T12:00:00Z";
export const order = (changes: Partial<Order> = {}): Order => ({
  id: "order-1",
  clientOrderId: "client-1",
  brokerOrderId: null,
  symbol: "NVDA",
  intent: "BUY",
  brokerSide: "BUY",
  orderType: "MARKET",
  quantity: "100",
  filledQuantity: "0",
  limitPrice: null,
  averageFillPrice: null,
  timeInForce: "DAY",
  status: "SUBMITTED",
  pendingConfirmation: null,
  rejectionReason: null,
  createdAt: at,
  submittedAt: at,
  updatedAt: at,
  ...changes,
});
const execution = (changes: Partial<Execution> = {}): Execution => ({
  id: "execution-1",
  orderId: "order-1",
  symbol: "NVDA",
  side: "BUY",
  quantity: "25",
  price: "180.10",
  commission: null,
  currency: "USD",
  executedAt: at,
  ...changes,
});

test("submission success does not imply acceptance or fill; Core SUBMITTED alone establishes open", () => {
  for (const status of ["CREATED", "SUBMISSION_PENDING", "FILLED"] as const) {
    const store = new Notifications();
    store.submitted(
      status,
      order({ status, filledQuantity: status === "FILLED" ? "100" : "0" }),
    );
    expect(store.snapshot().map((n) => n.text)).toEqual([
      "Buy order submitted",
    ]);
  }
  const store = new Notifications();
  store.submitted("key", order());
  expect(store.snapshot()[0]?.text).toBe("Buy order accepted / open");
});
test.each([
  ["BUY", "Bought"],
  ["SELL", "Sold"],
  ["SHORT", "Short sold"],
] as const)(
  "%s execution wording is authoritative and deduplicated",
  (intent, verb) => {
    const store = new Notifications();
    store.observe([], []);
    store.submitted("key", order({ intent }));
    const partial = order({
      intent,
      status: "PARTIALLY_FILLED",
      filledQuantity: "25",
    });
    store.observe([partial], [execution()]);
    store.observe([partial], [execution()]);
    expect(
      store.snapshot().filter((n) => n.id.startsWith("execution:")),
    ).toHaveLength(1);
    expect(
      store.snapshot().find((n) => n.id.startsWith("execution:"))?.text,
    ).toBe(`${verb} 25 NVDA @ 180.10`);
    expect(
      store.snapshot().find((n) => n.text.startsWith("Partially"))?.toast,
    ).toBe(false);
    store.observe(
      [order({ intent, status: "FILLED", filledQuantity: "100" })],
      [execution(), execution({ id: "execution-2", quantity: "75" })],
    );
    expect(
      store.snapshot().filter((n) => n.id.startsWith("execution:")),
    ).toHaveLength(2);
  },
);
test("hydration is silent, including refresh; local action during hydration still receives its fill", () => {
  const historical = order({ status: "FILLED", filledQuantity: "100" });
  const store = new Notifications();
  store.observe([historical], [execution()]);
  store.observe([historical], [execution()]);
  expect(store.snapshot()).toHaveLength(0);
  const fresh = new Notifications();
  fresh.begin("key");
  fresh.observe([historical], [execution()]);
  fresh.submitted("key", historical);
  fresh.end("key");
  expect(fresh.snapshot().some((n) => n.text.startsWith("Bought"))).toBe(true);
});
test("confirmation, cancellation and UNKNOWN remain distinct from successful execution", () => {
  const store = new Notifications();
  store.observe([], []);
  store.submitted("key", order());
  store.observe([order({ status: "PENDING_CONFIRMATION" })], []);
  store.action(order({ status: "CANCEL_PENDING" }), "cancel");
  for (const status of [
    "CANCEL_PENDING",
    "CANCELLED",
    "REJECTED",
    "FAILED",
    "UNKNOWN",
  ] as const)
    store.observe([order({ status })], []);
  expect(store.snapshot().map((n) => n.text)).toEqual(
    expect.arrayContaining([
      "Order confirmation required",
      "Cancellation requested",
      "Cancellation pending",
      "Order cancelled",
      "Order rejected",
      "Order failed",
      "Order status uncertain — do not resubmit",
    ]),
  );
  expect(
    store
      .snapshot()
      .filter((n) => ["Order failed", "Order rejected"].includes(n.text))
      .every((n) => n.tone === "error"),
  ).toBe(true);
  expect(store.snapshot().at(-1)?.tone).toBe("warning");
});
test("ambiguity is deduplicated and history dismissal does not replay executions", () => {
  const store = new Notifications();
  store.observe([], []);
  const body = {
    symbol: "NVDA",
    intent: "BUY" as const,
    quantity: "1",
    orderType: "MARKET" as const,
    timeInForce: "DAY" as const,
  };
  store.uncertain("same-key", body);
  store.uncertain("same-key", body);
  expect(store.snapshot()).toHaveLength(1);
  store.observe([order()], [execution()]);
  store.dismiss("execution:execution-1");
  store.observe([order()], [execution()]);
  expect(store.snapshot().some((n) => n.id === "execution:execution-1")).toBe(
    false,
  );
});
test("history/toasts and identity state are bounded without eviction or invented ordering", () => {
  const store = new Notifications();
  store.observe([], []);
  for (let i = 0; i < 1005; i++)
    store.observe(
      [order({ id: `order-${i}` })],
      [execution({ id: `execution-${i}`, orderId: `order-${i}` })],
    );
  expect(store.snapshot()).toHaveLength(100);
  expect(store.snapshot().filter((n) => n.toast).length).toBeLessThanOrEqual(3);
  expect(store.snapshot().filter((n) => n.id === "capacity")).toHaveLength(1);
  expect(
    store.snapshot().some((n) => n.id === "execution:execution-1004"),
  ).toBe(false);
  store.observe(
    [order({ id: "order-0", status: "CANCELLED" })],
    [execution({ id: "execution-0", orderId: "order-0" })],
  );
  expect(store.snapshot().at(-1)?.text).toBe("Order cancelled");
});

test("timestamp-only updates are silent and an unassociated sell is never called a short", () => {
  const store = new Notifications();
  store.observe([order()], []);
  store.observe([order({ updatedAt: "2026-10-03T13:00:00Z" })], []);
  expect(store.snapshot()).toHaveLength(0);
  store.observe([], [execution({ orderId: "unassociated", side: "SELL" })]);
  expect(store.snapshot()[0]?.text).toBe(
    "Sell-side execution 25 NVDA @ 180.10",
  );
});
