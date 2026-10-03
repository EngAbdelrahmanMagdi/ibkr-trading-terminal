"use client";

import { useEffect, useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm, useWatch } from "react-hook-form";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import {
  ApiError,
  api,
  errorMessage,
  type Instrument,
  type Order,
  type OrderRequest,
} from "@/lib/api";
import { price } from "@/lib/format";
import {
  newAttempt,
  saveAttempts,
  useAttempts,
  type Attempt,
} from "@/lib/order-attempt";
import { useConnectionStatus, useQuote } from "@/lib/realtime";
import { Icon } from "@/components/Icon";
import styles from "./terminal.module.css";
import { useNotifications } from "./Notifications";

const decimalQuantity = z
  .string()
  .regex(
    /^\d{1,15}(\.\d{1,4})?$/,
    "Enter a quantity with up to 4 decimal places.",
  )
  .refine((value) => Number(value) > 0, "Quantity must be greater than zero.");
export const ticketSchema = z
  .object({
    intent: z.enum(["BUY", "SELL", "SHORT"]),
    orderType: z.enum(["MARKET", "LIMIT"]),
    quantity: decimalQuantity,
    limitPrice: z.string(),
    timeInForce: z.enum(["DAY", "GTC"]),
  })
  .superRefine((value, context) => {
    if (
      value.orderType === "LIMIT" &&
      (!/^\d{1,13}(\.\d{1,6})?$/.test(value.limitPrice) ||
        Number(value.limitPrice) <= 0)
    ) {
      context.addIssue({
        code: "custom",
        path: ["limitPrice"],
        message: "Enter a positive limit price with up to 6 decimal places.",
      });
    }
  });
type TicketValues = z.infer<typeof ticketSchema>;

const refreshKeys = ["orders", "executions", "positions", "portfolio"];

export function OrderTicket({
  symbol,
  instrument,
  onClose,
}: {
  symbol: string;
  instrument: Instrument | undefined;
  onClose: () => void;
}) {
  const queryClient = useQueryClient();
  const notifications = useNotifications();
  const quote = useQuote(symbol);
  const connection = useConnectionStatus();
  const attempts = useAttempts();
  const [working, setWorking] = useState(false);
  const [latest, setLatest] = useState<Order | null>(null);
  const [message, setMessage] = useState("");
  const orders = useQuery({
    queryKey: ["orders"],
    queryFn: api.orders,
    refetchInterval: 60000,
  });
  const form = useForm<TicketValues>({
    resolver: zodResolver(ticketSchema),
    defaultValues: {
      intent: "BUY",
      orderType: "MARKET",
      quantity: "1",
      limitPrice: "",
      timeInForce: "DAY",
    },
  });
  const intent = useWatch({ control: form.control, name: "intent" });
  const orderType = useWatch({ control: form.control, name: "orderType" });
  const quantity = useWatch({ control: form.control, name: "quantity" });
  const limitPrice = useWatch({ control: form.control, name: "limitPrice" });
  const tif = useWatch({ control: form.control, name: "timeInForce" });
  const live =
    !!quote &&
    !quote.stale &&
    !quote.awaitingSnapshot &&
    connection === "LIVE" &&
    quote.last !== null;
  const shortBlocked =
    intent === "SHORT" && instrument?.shortability.status === "NOT_SHORTABLE";
  const selectedLatest =
    (latest && orders.data?.find((order) => order.id === latest.id)) || latest;

  useEffect(() => {
    if (!orders.data) return;
    const resolved = attempts.filter((attempt) => {
      if (!attempt.orderId) return true;
      const found = orders.data.find((order) => order.id === attempt.orderId);
      return !found || found.status === "UNKNOWN";
    });
    if (resolved.length !== attempts.length) saveAttempts(resolved);
  }, [orders.data, attempts]);

  function updateAttempts(next: Attempt[]) {
    saveAttempts(next.slice(-5));
  }

  async function send(attempt: Attempt, isRetry: boolean) {
    if (working) return;
    setWorking(true);
    notifications.begin(attempt.key);
    setMessage("");
    if (!isRetry) updateAttempts([...attempts, attempt]);
    try {
      const order = await api.submit(attempt.body as OrderRequest, attempt.key);
      setLatest(order);
      notifications.submitted(attempt.key, order);
      if (order.status === "UNKNOWN") {
        updateAttempts(
          (isRetry ? attempts : [...attempts, attempt]).map((item) =>
            item.key === attempt.key ? { ...item, orderId: order.id } : item,
          ),
        );
      } else {
        updateAttempts(
          (isRetry ? attempts : [...attempts, attempt]).filter(
            (item) => item.key !== attempt.key,
          ),
        );
      }
      refreshKeys.forEach(
        (key) => void queryClient.invalidateQueries({ queryKey: [key] }),
      );
      setMessage(
        order.status === "UNKNOWN"
          ? "Broker outcome is being verified. Do not place this order again with a new key."
          : order.status === "SUBMITTED"
            ? "Order accepted / open."
            : "Order submitted.",
      );
    } catch (error) {
      if (
        error instanceof ApiError &&
        error.status >= 400 &&
        error.status < 500
      ) {
        updateAttempts(
          (isRetry ? attempts : [...attempts, attempt]).filter(
            (item) => item.key !== attempt.key,
          ),
        );
        error.fields.forEach(({ field, message: fieldMessage }) => {
          if (
            field === "quantity" ||
            field === "limitPrice" ||
            field === "orderType" ||
            field === "timeInForce" ||
            field === "intent"
          )
            form.setError(field, { message: fieldMessage });
        });
      }
      if (
        error instanceof ApiError &&
        error.status >= 400 &&
        error.status < 500
      )
        notifications.failure(
          attempt.key,
          "Order request failed",
          errorMessage(error),
        );
      else notifications.uncertain(attempt.key, attempt.body as OrderRequest);
      setMessage(errorMessage(error));
    } finally {
      notifications.end(attempt.key);
      setWorking(false);
    }
  }

  async function respond(confirm: boolean) {
    if (!selectedLatest || working) return;
    setWorking(true);
    notifications.action(selectedLatest, confirm ? "confirm" : "decline");
    try {
      const order = await api.confirm(selectedLatest.id, confirm);
      setLatest(order);
      refreshKeys.forEach(
        (key) => void queryClient.invalidateQueries({ queryKey: [key] }),
      );
      setMessage(`Order ${order.status.toLowerCase().replaceAll("_", " ")}.`);
    } catch (error) {
      notifications.failure(
        `confirmation:${selectedLatest.id}`,
        "Confirmation request failed",
        errorMessage(error),
      );
      setMessage(errorMessage(error));
    } finally {
      setWorking(false);
    }
  }

  const onSubmit = form.handleSubmit((values) => {
    if (attempts.length >= 5) {
      setMessage(
        "Resolve or review the five outstanding submissions before placing another order.",
      );
      return;
    }
    const body: OrderRequest = {
      symbol,
      intent: values.intent,
      orderType: values.orderType,
      quantity: values.quantity,
      timeInForce: values.timeInForce,
      ...(values.orderType === "LIMIT"
        ? { limitPrice: values.limitPrice }
        : {}),
    };
    void send(newAttempt(body), false);
  });

  return (
    <aside className={styles.ticket} aria-label="Order ticket">
      <div className={styles.ticketHeading}>
        <div>
          <span className={styles.eyebrow}>TRADE EXECUTION</span>
          <h2>Order ticket</h2>
        </div>
        <button
          className={styles.mobileClose}
          aria-label="Close order ticket"
          onClick={onClose}
        >
          <Icon name="close" />
        </button>
      </div>
      <div className={styles.ticketSymbol}>
        <div className={styles.ticketSymbolIcon}>{symbol.slice(0, 1)}</div>
        <div>
          <strong>{symbol}</strong>
          <span>{instrument?.name ?? "Selected instrument"}</span>
        </div>
        <Icon name="chevron" width={15} height={15} />
      </div>
      <div className={styles.ticketQuote}>
        <span>LAST TRADED</span>
        <strong>{price(quote?.last)}</strong>
        <small>{live ? quote.dataMode : "STALE / UNAVAILABLE"}</small>
      </div>
      <form onSubmit={onSubmit} noValidate>
        <div className={styles.fieldHeading}>DIRECTION</div>
        <div
          className={styles.sideTabs}
          role="group"
          aria-label="Order direction"
        >
          {(["BUY", "SELL", "SHORT"] as const).map((side) => (
            <button
              type="button"
              key={side}
              aria-pressed={intent === side}
              className={
                intent === side
                  ? side === "BUY"
                    ? styles.sideBuy
                    : styles.sideSell
                  : ""
              }
              onClick={() => form.setValue("intent", side)}
            >
              {side}
            </button>
          ))}
        </div>
        {intent === "SHORT" && (
          <p className={shortBlocked ? styles.inlineError : styles.inlineNote}>
            {instrument?.shortability.status === "SHORTABLE"
              ? `Shortable${instrument.shortability.availableQuantity != null ? ` · ${instrument.shortability.availableQuantity} available` : ""}${instrument.shortability.borrowFeeRate ? ` · fee ${instrument.shortability.borrowFeeRate}%` : ""}`
              : shortBlocked
                ? "Not shortable. Broker reports this instrument cannot be shorted."
                : "Shortability unavailable. The broker makes the final decision."}
          </p>
        )}
        <div className={styles.formDivider} />
        <div className={styles.fieldHeading}>ORDER TYPE</div>
        <div className={styles.typeTabs} role="group" aria-label="Order type">
          {(["MARKET", "LIMIT"] as const).map((type) => (
            <button
              type="button"
              key={type}
              className={orderType === type ? styles.typeActive : ""}
              aria-pressed={orderType === type}
              onClick={() => form.setValue("orderType", type)}
            >
              {type === "MARKET" ? "Market" : "Limit"}
            </button>
          ))}
        </div>
        <label className={styles.fieldLabel} htmlFor="quantity">
          QUANTITY <span>SHARES</span>
        </label>
        <div className={styles.inputGroup}>
          <input
            id="quantity"
            inputMode="decimal"
            autoComplete="off"
            {...form.register("quantity")}
            aria-invalid={!!form.formState.errors.quantity}
            aria-describedby={
              form.formState.errors.quantity ? "quantity-error" : undefined
            }
          />
          <span>SH</span>
        </div>
        {form.formState.errors.quantity && (
          <p id="quantity-error" className={styles.inlineError}>
            {form.formState.errors.quantity.message}
          </p>
        )}
        {orderType === "LIMIT" && (
          <>
            <label className={styles.fieldLabel} htmlFor="limitPrice">
              LIMIT PRICE <span>USD</span>
            </label>
            <div className={styles.inputGroup}>
              <input
                id="limitPrice"
                inputMode="decimal"
                autoComplete="off"
                {...form.register("limitPrice")}
                aria-invalid={!!form.formState.errors.limitPrice}
                aria-describedby={
                  form.formState.errors.limitPrice ? "limit-error" : undefined
                }
              />
              <span>USD</span>
            </div>
            {form.formState.errors.limitPrice && (
              <p id="limit-error" className={styles.inlineError}>
                {form.formState.errors.limitPrice.message}
              </p>
            )}
          </>
        )}
        <label className={styles.fieldLabel} htmlFor="timeInForce">
          TIME IN FORCE
        </label>
        <select id="timeInForce" {...form.register("timeInForce")}>
          <option value="DAY">Day</option>
          <option value="GTC">Good till cancelled</option>
        </select>
        <div className={styles.ticketSummary}>
          <div>
            <span>ORDER</span>
            <strong>
              {intent} {quantity || "—"} {symbol}
            </strong>
          </div>
          <div>
            <span>PRICE</span>
            <strong>
              {orderType === "LIMIT" ? price(limitPrice) : "Market"}
            </strong>
          </div>
          <div>
            <span>DURATION</span>
            <strong>{tif}</strong>
          </div>
        </div>
        {!live && orderType === "MARKET" && (
          <p className={styles.inlineWarning}>
            Waiting for a fresh live quote before market submission.
          </p>
        )}
        <button
          className={`${styles.submitButton} ${intent !== "BUY" ? styles.submitSell : ""}`}
          type="submit"
          disabled={
            working || shortBlocked || (orderType === "MARKET" && !live)
          }
        >
          {working ? "Submitting…" : `Place ${intent.toLowerCase()} order`}{" "}
          <Icon name="arrow" width={17} height={17} />
        </button>
        <p className={styles.ticketDisclaimer}>
          MOCK PAPER TRADING · CORE VALIDATES EVERY ORDER
        </p>
      </form>
      {message && (
        <div className={styles.ticketMessage} role="status">
          {message}
        </div>
      )}
      {selectedLatest && (
        <div className={styles.latestOrder}>
          <span>LATEST ORDER · {selectedLatest.id.slice(0, 8)}</span>
          <strong
            className={
              selectedLatest.status === "UNKNOWN" ? styles.warning : ""
            }
          >
            {selectedLatest.status.replaceAll("_", " ")}
          </strong>
          {selectedLatest.status === "PENDING_CONFIRMATION" && (
            <>
              <p>{selectedLatest.pendingConfirmation?.message}</p>
              <div className={styles.confirmActions}>
                <button onClick={() => void respond(false)} disabled={working}>
                  Decline
                </button>
                <button onClick={() => void respond(true)} disabled={working}>
                  Confirm
                </button>
              </div>
            </>
          )}
        </div>
      )}
      {attempts.length > 0 && (
        <div className={styles.uncertainAttempts}>
          <span>UNRESOLVED SUBMISSIONS</span>
          {attempts.map((attempt) => (
            <div key={attempt.key}>
              <strong>
                {attempt.body.intent} {attempt.body.quantity}{" "}
                {attempt.body.symbol}
              </strong>
              <small>
                {attempt.orderId
                  ? "Broker status unknown"
                  : "Network outcome unknown"}
              </small>
              <button
                disabled={working}
                onClick={() => void send(attempt, true)}
              >
                Retry same intent
              </button>
            </div>
          ))}
        </div>
      )}
    </aside>
  );
}
