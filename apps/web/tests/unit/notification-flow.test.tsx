import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { Terminal } from "@/features/Terminal";
import { api } from "@/lib/api";
import { stream } from "@/lib/realtime";
import type { Order, Execution } from "@/lib/api";

jest.mock(
  "next/dynamic",
  () => () =>
    function Chart() {
      return <div>Chart</div>;
    },
);
jest.mock("@/features/WatchlistPanel", () => ({ WatchlistPanel: () => null }));
jest.mock("@/features/OrderTicket", () => ({ OrderTicket: () => null }));
jest.mock("@/features/TradeDeck", () => ({ TradeDeck: () => null }));
jest.mock("@/lib/api", () => ({
  api: {
    orders: jest.fn(),
    executions: jest.fn(),
    portfolio: jest.fn(),
    watchlist: jest.fn(),
    search: jest.fn(),
  },
  errorMessage: () => "unavailable",
}));
jest.mock("@/lib/realtime", () => ({
  stream: {
    start: jest.fn(),
    stop: jest.fn(),
    setSymbols: jest.fn(),
    subscribeHint: jest.fn(),
  },
  useConnectionStatus: () => "LIVE",
  useQuote: () => null,
}));

test("Gateway hint only refetches; authoritative execution creates one notification", async () => {
  const orders = jest.mocked(api.orders);
  const executions = jest.mocked(api.executions);
  orders.mockResolvedValue([]);
  executions.mockResolvedValue([]);
  jest.mocked(api.portfolio).mockRejectedValue(new Error("unavailable"));
  jest.mocked(api.watchlist).mockResolvedValue({ id: "watchlist", items: [] });
  jest.mocked(api.search).mockResolvedValue([]);
  let hint = () => {};
  jest.mocked(stream.subscribeHint).mockImplementation((listener) => {
    hint = listener;
    return () => true;
  });
  render(<Terminal />);
  await waitFor(() => expect(executions).toHaveBeenCalled());
  fireEvent.click(screen.getByRole("button", { name: /^Notifications/ }));
  expect(screen.getByText("No new activity this session.")).toBeVisible();
  act(() => hint());
  await waitFor(() => expect(executions.mock.calls.length).toBeGreaterThan(1));
  expect(screen.getByText("No new activity this session.")).toBeVisible();
  const at = "2026-10-03T12:00:00Z";
  const order: Order = {
    id: "o",
    clientOrderId: "c",
    brokerOrderId: null,
    symbol: "NVDA",
    intent: "SHORT",
    brokerSide: "SELL",
    orderType: "MARKET",
    quantity: "3",
    filledQuantity: "3",
    limitPrice: null,
    averageFillPrice: "180.10",
    timeInForce: "DAY",
    status: "FILLED",
    pendingConfirmation: null,
    rejectionReason: null,
    createdAt: at,
    submittedAt: at,
    updatedAt: at,
  };
  const fill: Execution = {
    id: "e",
    orderId: "o",
    symbol: "NVDA",
    side: "SELL",
    quantity: "3",
    price: "180.10",
    commission: null,
    currency: "USD",
    executedAt: at,
  };
  orders.mockResolvedValue([order]);
  let finishExecutions!: (value: Execution[]) => void;
  executions.mockReturnValue(
    new Promise((resolve) => {
      finishExecutions = resolve;
    }),
  );
  act(() => hint());
  await waitFor(() => expect(orders.mock.calls.length).toBeGreaterThan(2));
  expect(screen.queryByText("Order fully filled")).not.toBeInTheDocument();
  await act(async () => finishExecutions([fill]));
  expect(await screen.findByText("Short sold 3 NVDA @ 180.10")).toBeVisible();
  fireEvent.click(screen.getByRole("button", { name: "Close notifications" }));
  expect(screen.getByLabelText("Recent notifications")).not.toHaveTextContent(
    "Order fully filled",
  );
  fireEvent.click(screen.getByRole("button", { name: /^Notifications/ }));
  executions.mockResolvedValue([fill]);
  act(() => hint());
  await waitFor(() => expect(executions.mock.calls.length).toBeGreaterThan(3));
  expect(screen.getAllByText("Short sold 3 NVDA @ 180.10")).toHaveLength(1);
});
