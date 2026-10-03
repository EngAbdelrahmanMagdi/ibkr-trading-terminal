"use client";

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, errorMessage, type Order } from "@/lib/api";
import { metric, price, signed, time } from "@/lib/format";
import { Icon } from "@/components/Icon";
import { NewsPanel } from "./NewsPanel";
import styles from "./terminal.module.css";

type Tab = "positions" | "orders" | "executions" | "news";
const openStatuses = new Set<Order["status"]>([
  "CREATED",
  "SUBMISSION_PENDING",
  "PENDING_CONFIRMATION",
  "SUBMITTED",
  "PARTIALLY_FILLED",
  "CANCEL_PENDING",
  "UNKNOWN",
]);

export function TradeDeck({ symbol }: { symbol: string }) {
  const queryClient = useQueryClient();
  const [tab, setTab] = useState<Tab>("positions");
  const [allOrders, setAllOrders] = useState(false);
  const [actionError, setActionError] = useState("");
  const positions = useQuery({
    queryKey: ["positions"],
    queryFn: api.positions,
    refetchInterval: 60000,
  });
  const orders = useQuery({
    queryKey: ["orders"],
    queryFn: api.orders,
    refetchInterval: 60000,
  });
  const executions = useQuery({
    queryKey: ["executions"],
    queryFn: api.executions,
    refetchInterval: 60000,
  });
  const invalidate = () => {
    ["orders", "positions", "executions", "portfolio"].forEach(
      (key) => void queryClient.invalidateQueries({ queryKey: [key] }),
    );
  };
  const cancel = useMutation({
    mutationFn: api.cancel,
    onSuccess: invalidate,
    onError: (error) => setActionError(errorMessage(error)),
  });
  const confirm = useMutation({
    mutationFn: ({ id, answer }: { id: string; answer: boolean }) =>
      api.confirm(id, answer),
    onSuccess: invalidate,
    onError: (error) => setActionError(errorMessage(error)),
  });
  const displayedOrders =
    orders.data?.filter(
      (order) => allOrders || openStatuses.has(order.status),
    ) ?? [];

  return (
    <section className={styles.deck} aria-label="Trading activity">
      <div className={styles.deckHeader}>
        <div
          className={styles.deckTabs}
          role="tablist"
          aria-label="Trading data"
        >
          {(["positions", "orders", "executions", "news"] as const).map(
            (name) => (
              <button
                key={name}
                role="tab"
                aria-selected={tab === name}
                className={tab === name ? styles.deckActive : ""}
                onClick={() => setTab(name)}
              >
                {name === "orders"
                  ? "Open orders"
                  : name === "executions"
                    ? "Executions"
                    : name === "news"
                      ? "News"
                      : "Positions"}
                {name !== "news" && (
                  <span>
                    {name === "positions"
                      ? (positions.data?.length ?? 0)
                      : name === "orders"
                        ? (orders.data?.filter((order) =>
                            openStatuses.has(order.status),
                          ).length ?? 0)
                        : (executions.data?.length ?? 0)}
                  </span>
                )}
              </button>
            ),
          )}
        </div>
        <div className={styles.deckActions}>
          {tab === "orders" && (
            <button onClick={() => setAllOrders(!allOrders)}>
              {allOrders ? "Open only" : "All orders"}
            </button>
          )}
          {tab !== "news" && (
            <button
              className={styles.iconButton}
              aria-label="Refresh trading data"
              onClick={() => {
                void positions.refetch();
                void orders.refetch();
                void executions.refetch();
              }}
            >
              <Icon name="refresh" width={15} height={15} />
            </button>
          )}
        </div>
      </div>
      {actionError && <p className={styles.inlineError}>{actionError}</p>}
      <div className={styles.tableScroll} role="tabpanel">
        {tab === "news" && <NewsPanel key={symbol} symbol={symbol} />}
        {tab === "positions" && (
          <>
            {positions.isPending ? (
              <PanelState text="Loading positions…" />
            ) : positions.isError ? (
              <PanelState
                text={errorMessage(positions.error)}
                retry={() => void positions.refetch()}
              />
            ) : positions.data?.length === 0 ? (
              <PanelState text="No positions yet. Filled orders will appear here." />
            ) : (
              <table>
                <thead>
                  <tr>
                    <th>SYMBOL</th>
                    <th>QTY</th>
                    <th>AVG COST</th>
                    <th>MARKET VALUE</th>
                    <th>UNREALIZED P&L</th>
                    <th>REALIZED P&L</th>
                  </tr>
                </thead>
                <tbody>
                  {positions.data?.map((position) => (
                    <tr key={position.symbol}>
                      <td className={styles.tableSymbol}>{position.symbol}</td>
                      <td>{position.quantity}</td>
                      <td>{price(position.averageCost)}</td>
                      <td>{metric(position.marketValue)}</td>
                      <td
                        className={
                          signed(position.unrealizedPnl.value) === "positive"
                            ? styles.positive
                            : signed(position.unrealizedPnl.value) ===
                                "negative"
                              ? styles.negative
                              : ""
                        }
                      >
                        {metric(position.unrealizedPnl)}
                      </td>
                      <td
                        className={
                          signed(position.realizedPnl.value) === "positive"
                            ? styles.positive
                            : signed(position.realizedPnl.value) === "negative"
                              ? styles.negative
                              : ""
                        }
                      >
                        {metric(position.realizedPnl)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </>
        )}
        {tab === "orders" && (
          <>
            {orders.isPending ? (
              <PanelState text="Loading orders…" />
            ) : orders.isError ? (
              <PanelState
                text={errorMessage(orders.error)}
                retry={() => void orders.refetch()}
              />
            ) : displayedOrders.length === 0 ? (
              <PanelState
                text={
                  allOrders
                    ? "No orders yet."
                    : "No open orders. Select All orders for completed activity."
                }
              />
            ) : (
              <table>
                <thead>
                  <tr>
                    <th>TIME</th>
                    <th>SYMBOL</th>
                    <th>SIDE</th>
                    <th>TYPE</th>
                    <th>QTY / FILLED</th>
                    <th>PRICE</th>
                    <th>STATUS</th>
                    <th>ACTION</th>
                  </tr>
                </thead>
                <tbody>
                  {displayedOrders.map((order) => (
                    <tr key={order.id}>
                      <td>{time(order.createdAt)}</td>
                      <td className={styles.tableSymbol}>{order.symbol}</td>
                      <td
                        className={
                          order.intent === "BUY"
                            ? styles.positive
                            : styles.negative
                        }
                      >
                        {order.intent}
                      </td>
                      <td>{order.orderType}</td>
                      <td>
                        {order.quantity} / {order.filledQuantity}
                      </td>
                      <td>
                        {price(order.limitPrice ?? order.averageFillPrice)}
                      </td>
                      <td>
                        <span
                          className={`${styles.statusPill} ${order.status === "UNKNOWN" || order.status === "PENDING_CONFIRMATION" ? styles.pillWarning : order.status === "FILLED" ? styles.pillSuccess : ""}`}
                        >
                          {order.status.replaceAll("_", " ")}
                        </span>
                        {order.status === "UNKNOWN" && (
                          <small className={styles.tableNote}>
                            Verifying broker outcome
                          </small>
                        )}
                        {order.status === "PENDING_CONFIRMATION" && (
                          <small className={styles.tableNote}>
                            {order.pendingConfirmation?.message}
                          </small>
                        )}
                      </td>
                      <td>
                        {order.status === "PENDING_CONFIRMATION" ? (
                          <div className={styles.rowActions}>
                            <button
                              disabled={confirm.isPending}
                              onClick={() =>
                                confirm.mutate({ id: order.id, answer: false })
                              }
                            >
                              Decline
                            </button>
                            <button
                              disabled={confirm.isPending}
                              onClick={() =>
                                confirm.mutate({ id: order.id, answer: true })
                              }
                            >
                              Confirm
                            </button>
                          </div>
                        ) : ["SUBMITTED", "PARTIALLY_FILLED"].includes(
                            order.status,
                          ) ? (
                          <button
                            className={styles.textAction}
                            disabled={cancel.isPending}
                            onClick={() => cancel.mutate(order.id)}
                          >
                            Cancel
                          </button>
                        ) : (
                          "—"
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </>
        )}
        {tab === "executions" && (
          <>
            {executions.isPending ? (
              <PanelState text="Loading executions…" />
            ) : executions.isError ? (
              <PanelState
                text={errorMessage(executions.error)}
                retry={() => void executions.refetch()}
              />
            ) : executions.data?.length === 0 ? (
              <PanelState text="No executions yet. Fills will appear here." />
            ) : (
              <table>
                <thead>
                  <tr>
                    <th>TIME</th>
                    <th>SYMBOL</th>
                    <th>SIDE</th>
                    <th>QUANTITY</th>
                    <th>PRICE</th>
                    <th>COMMISSION</th>
                    <th>EXECUTION ID</th>
                  </tr>
                </thead>
                <tbody>
                  {executions.data?.map((execution) => (
                    <tr key={execution.id}>
                      <td>{time(execution.executedAt)}</td>
                      <td className={styles.tableSymbol}>{execution.symbol}</td>
                      <td
                        className={
                          execution.side === "BUY"
                            ? styles.positive
                            : styles.negative
                        }
                      >
                        {execution.side}
                      </td>
                      <td>{execution.quantity}</td>
                      <td>{price(execution.price)}</td>
                      <td>
                        {execution.commission == null
                          ? "Unavailable"
                          : `${price(execution.commission)} ${execution.currency}`}
                      </td>
                      <td className={styles.muted}>
                        {execution.id.slice(0, 8)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </>
        )}
      </div>
    </section>
  );
}

function PanelState({ text, retry }: { text: string; retry?: () => void }) {
  return (
    <div className={styles.panelState}>
      {text}
      {retry && <button onClick={retry}>Retry</button>}
    </div>
  );
}
