import type {
  components as Core,
  operations as CoreOperations,
} from "@/lib/contracts/trading";
import type { components as Gateway } from "@/lib/contracts/gateway";

export type Order = Core["schemas"]["Order"];
export type OrderRequest = Core["schemas"]["PlaceOrderRequest"];
export type Execution = Core["schemas"]["Execution"];
export type Position = Core["schemas"]["Position"];
export type Portfolio = Core["schemas"]["PortfolioSummary"];
export type Watchlist = Core["schemas"]["Watchlist"];
export type Instrument = Core["schemas"]["Instrument"];
export type OptionalMetric = Core["schemas"]["optional-metric.schema"];
export type Bars = Gateway["schemas"]["BarsResponse"];
export type NewsArticle = Core["schemas"]["NewsArticle"];
export type NewsResult = {
  articles: NewsArticle[];
  status:
    | NonNullable<
        CoreOperations["listNews"]["responses"][200]["headers"]["X-News-Status"]
      >
    | "UNKNOWN";
  lastRefreshedAt: string | null;
};

const coreOrigin =
  process.env.NEXT_PUBLIC_CORE_ORIGIN ?? "http://localhost:18080";
const gatewayOrigin =
  process.env.NEXT_PUBLIC_GATEWAY_ORIGIN ?? "http://localhost:18090";
export const streamUrl =
  process.env.NEXT_PUBLIC_GATEWAY_WS_URL ?? "ws://localhost:18090/ws";

type SafeProblem = {
  category: string | undefined;
  title: string | undefined;
  detail: string | undefined;
  errors: { field: string; message: string }[] | undefined;
};

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly category: string,
    message: string,
    readonly fields: { field: string; message: string }[] = [],
  ) {
    super(message);
    this.name = "ApiError";
  }
}

function safeProblem(value: unknown): SafeProblem {
  if (typeof value !== "object" || value === null)
    return {
      category: undefined,
      title: undefined,
      detail: undefined,
      errors: undefined,
    };
  const obj = value as Record<string, unknown>;
  return {
    category: typeof obj.category === "string" ? obj.category : undefined,
    title: typeof obj.title === "string" ? obj.title : undefined,
    detail: typeof obj.detail === "string" ? obj.detail : undefined,
    errors: Array.isArray(obj.errors)
      ? obj.errors.filter(
          (entry): entry is { field: string; message: string } =>
            typeof entry === "object" &&
            entry !== null &&
            typeof entry.field === "string" &&
            typeof entry.message === "string",
        )
      : undefined,
  };
}

async function request<T>(
  origin: string,
  path: string,
  options: {
    method?: string;
    body?: unknown;
    idempotencyKey?: string;
    root?: "array" | "object";
    onHeaders?: (headers: Headers) => void;
    signal?: AbortSignal;
  } = {},
): Promise<T> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10000);
  try {
    const response = await fetch(`${origin}${path}`, {
      method: options.method ?? "GET",
      cache: "no-store",
      signal: options.signal
        ? AbortSignal.any([controller.signal, options.signal])
        : controller.signal,
      headers: {
        Accept: "application/json, application/problem+json",
        "X-Correlation-Id": crypto.randomUUID(),
        ...(options.body !== undefined
          ? { "Content-Type": "application/json" }
          : {}),
        ...(options.idempotencyKey
          ? { "Idempotency-Key": options.idempotencyKey }
          : {}),
      },
      ...(options.body !== undefined
        ? { body: JSON.stringify(options.body) }
        : {}),
    });
    if (response.status === 204) return undefined as T;
    const value: unknown = await response.json().catch(() => null);
    if (!response.ok) {
      const problem = safeProblem(value);
      throw new ApiError(
        response.status,
        problem.category ?? "NETWORK",
        problem.detail ??
          problem.title ??
          `Request failed (${response.status})`,
        problem.errors ?? [],
      );
    }
    if (
      value === null ||
      typeof value !== "object" ||
      (options.root === "array" ? !Array.isArray(value) : Array.isArray(value))
    ) {
      throw new ApiError(
        response.status,
        "INVALID_RESPONSE",
        "The service returned an invalid response.",
      );
    }
    // OpenAPI-generated types own REST shapes. The runtime check only rejects unusable JSON roots.
    options.onHeaders?.(response.headers);
    return value as T;
  } catch (error) {
    if (error instanceof ApiError) throw error;
    throw new ApiError(
      0,
      "NETWORK",
      "The service is unavailable. Check the connection and retry.",
    );
  } finally {
    clearTimeout(timeout);
  }
}

export const api = {
  news: async (
    symbol: string,
    limit = 20,
    signal?: AbortSignal,
  ): Promise<NewsResult> => {
    let status: NewsResult["status"] = "UNKNOWN";
    let lastRefreshedAt: string | null = null;
    const articles = await request<NewsArticle[]>(
      coreOrigin,
      `/api/v1/news?symbol=${encodeURIComponent(symbol)}&limit=${Math.min(500, Math.max(1, limit))}`,
      {
        root: "array",
        ...(signal ? { signal } : {}),
        onHeaders: (headers) => {
          const raw = headers.get("X-News-Status");
          if (raw === "FRESH" || raw === "STALE" || raw === "UNAVAILABLE")
            status = raw;
          const refreshed = headers.get("X-News-Last-Refreshed-At");
          if (
            refreshed &&
            refreshed.length <= 40 &&
            !Number.isNaN(Date.parse(refreshed))
          )
            lastRefreshedAt = refreshed;
        },
      },
    );
    return { articles, status, lastRefreshedAt };
  },
  portfolio: () => request<Portfolio>(coreOrigin, "/api/v1/portfolio"),
  positions: () =>
    request<Position[]>(coreOrigin, "/api/v1/positions", { root: "array" }),
  orders: () =>
    request<Order[]>(coreOrigin, "/api/v1/orders?limit=100", { root: "array" }),
  executions: () =>
    request<Execution[]>(coreOrigin, "/api/v1/executions?limit=100", {
      root: "array",
    }),
  watchlist: () => request<Watchlist>(coreOrigin, "/api/v1/watchlist"),
  search: (query: string) =>
    request<Instrument[]>(
      coreOrigin,
      `/api/v1/instruments/search?q=${encodeURIComponent(query)}&limit=12`,
      { root: "array" },
    ),
  bars: (symbol: string, interval: string, range: string) =>
    request<Bars>(
      gatewayOrigin,
      `/api/v1/market/bars?symbol=${encodeURIComponent(symbol)}&interval=${interval}&range=${range}`,
    ),
  addWatch: (symbol: string) =>
    request<Watchlist>(coreOrigin, "/api/v1/watchlist/items", {
      method: "POST",
      body: { symbol },
    }),
  removeWatch: (symbol: string) =>
    request<void>(
      coreOrigin,
      `/api/v1/watchlist/items/${encodeURIComponent(symbol)}`,
      {
        method: "DELETE",
      },
    ),
  submit: (body: OrderRequest, key: string) =>
    request<Order>(coreOrigin, "/api/v1/orders", {
      method: "POST",
      body,
      idempotencyKey: key,
    }),
  confirm: (id: string, confirm: boolean) =>
    request<Order>(
      coreOrigin,
      `/api/v1/orders/${encodeURIComponent(id)}/confirmation`,
      {
        method: "POST",
        body: { confirm },
      },
    ),
  cancel: (id: string) =>
    request<Order>(coreOrigin, `/api/v1/orders/${encodeURIComponent(id)}`, {
      method: "DELETE",
    }),
};

export function errorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.category === "STALE_MARKET_DATA")
      return "Market data is stale. Wait for a fresh quote.";
    if (error.category === "BROKER_UNAVAILABLE")
      return "Broker unavailable. Check the order status before retrying.";
    if (error.category === "RATE_LIMITED")
      return "Request limit reached. Try again shortly.";
    return error.message;
  }
  return "Something went wrong. Please retry.";
}
