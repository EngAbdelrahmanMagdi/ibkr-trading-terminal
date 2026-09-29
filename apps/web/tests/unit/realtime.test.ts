import { StreamClient } from "@/lib/realtime";

class FakeSocket {
  static instances: FakeSocket[] = [];
  static readonly OPEN = 1;
  static readonly CONNECTING = 0;
  readyState = FakeSocket.CONNECTING;
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  sent: string[] = [];
  constructor(url: string) {
    void url;
    FakeSocket.instances.push(this);
  }
  send(value: string) {
    this.sent.push(value);
  }
  close() {
    this.readyState = 3;
    this.onclose?.();
  }
  open() {
    this.readyState = FakeSocket.OPEN;
    this.onopen?.();
  }
  message(value: object) {
    this.onmessage?.({ data: JSON.stringify(value) });
  }
}

const quote = (
  symbol: string,
  sequence: number,
  type: "snapshot" | "quote" = "quote",
) => ({
  type,
  symbol,
  bid: "100.00",
  ask: "100.02",
  last: "100.01",
  bidSize: 2,
  askSize: 4,
  volume: 100,
  sequence,
  timestamp: "2026-09-29T10:00:00Z",
  stale: false,
  dataMode: "REALTIME",
  halted: false,
});

beforeEach(() => {
  FakeSocket.instances = [];
  Object.defineProperty(globalThis, "WebSocket", {
    configurable: true,
    value: FakeSocket,
  });
  Object.defineProperty(globalThis, "requestAnimationFrame", {
    configurable: true,
    value: (callback: FrameRequestCallback) => setTimeout(() => callback(0), 0),
  });
});

test("one connection partitions quotes, requires snapshots and drops old sequences", async () => {
  const client = new StreamClient();
  const nvda = jest.fn();
  const aapl = jest.fn();
  client.subscribeQuote("NVDA", nvda);
  client.subscribeQuote("AAPL", aapl);
  client.setSymbols(["NVDA", "AAPL"], ["NVDA"]);
  client.start();
  const socket = FakeSocket.instances[0];
  expect(socket).toBeDefined();
  socket?.open();
  expect(FakeSocket.instances).toHaveLength(1);
  expect(socket?.sent).toContain(
    JSON.stringify({ type: "subscribe", symbols: ["NVDA", "AAPL"] }),
  );
  socket?.message(quote("NVDA", 2));
  expect(client.getQuote("NVDA")).toBeNull();
  socket?.message(quote("NVDA", 2, "snapshot"));
  socket?.message(quote("NVDA", 1));
  socket?.message(quote("NVDA", 3));
  await new Promise((resolve) => setTimeout(resolve, 10));
  expect(client.getQuote("NVDA")?.sequence).toBe(3);
  expect(nvda).toHaveBeenCalledTimes(1);
  expect(aapl).not.toHaveBeenCalled();
  client.setSymbols(["AAPL"], ["AAPL"]);
  expect(client.getQuote("NVDA")).toBeNull();
  expect(socket?.sent).toContain(
    JSON.stringify({ type: "unsubscribe", symbols: ["NVDA"] }),
  );
  client.stop();
});
