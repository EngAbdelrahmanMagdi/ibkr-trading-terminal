import { loadAttempts, newAttempt, saveAttempts } from "@/lib/order-attempt";
import { ticketSchema } from "@/features/OrderTicket";

beforeEach(() => sessionStorage.clear());

test("one order intent retains its key and exact payload across storage reload", () => {
  const body = {
    symbol: "NVDA",
    intent: "BUY" as const,
    orderType: "LIMIT" as const,
    quantity: "2",
    limitPrice: "150.00",
    timeInForce: "GTC" as const,
  };
  const attempt = newAttempt(body);
  saveAttempts([attempt]);
  expect(loadAttempts()).toEqual([attempt]);
  expect(loadAttempts()[0]?.key).toBe(attempt.key);
  expect(newAttempt(body).key).not.toBe(attempt.key);
});

test("corrupt or oversized tab storage never becomes a retryable order", () => {
  sessionStorage.setItem(
    "trading-terminal:order-attempts:v1",
    JSON.stringify([{ key: "bad", body: { symbol: "NVDA" } }]),
  );
  expect(loadAttempts()).toEqual([]);
  sessionStorage.setItem(
    "trading-terminal:order-attempts:v1",
    "x".repeat(10001),
  );
  expect(loadAttempts()).toEqual([]);
});

test("ticket accepts valid limit fields and rejects missing or nonpositive values", () => {
  const valid = {
    intent: "BUY",
    orderType: "LIMIT",
    quantity: "1.5",
    limitPrice: "123.45",
    timeInForce: "DAY",
  };
  expect(ticketSchema.safeParse(valid).success).toBe(true);
  expect(ticketSchema.safeParse({ ...valid, quantity: "0" }).success).toBe(
    false,
  );
  expect(ticketSchema.safeParse({ ...valid, limitPrice: "" }).success).toBe(
    false,
  );
});
