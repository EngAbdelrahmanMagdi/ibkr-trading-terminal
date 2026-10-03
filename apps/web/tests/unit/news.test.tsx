import { render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { NewsPanel, articleLink } from "@/features/NewsPanel";
import { api, type NewsResult } from "@/lib/api";

jest.mock("@/lib/api", () => ({
  api: { news: jest.fn() },
  errorMessage: () => "News unavailable",
}));
const news = jest.mocked(api.news);
function mount(symbol = "NVDA") {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={client}>
      <NewsPanel symbol={symbol} />
    </QueryClientProvider>,
  );
}
beforeEach(() => jest.clearAllMocks());

test.each([
  ["synthetic-news.v1", "Synthetic insight"],
  ["private-synthetic-news.v1", "AI insight"],
])(
  "identifies insight only by the exact reserved model %s",
  async (model, label) => {
    news.mockResolvedValueOnce({
      status: "FRESH",
      lastRefreshedAt: null,
      articles: [
        {
          id: "d9c877ab-4a67-4457-a3ca-61c2b912bf85",
          symbols: ["NVDA"],
          headline: "Raw headline",
          source: "Publisher",
          url: "https://example.com/news",
          publishedAt: "2026-10-03T11:00:00Z",
          rawSummary: "Raw snippet",
          enrichment: {
            model,
            modelVersion: "1",
            promptVersion: "news-insight.v1",
            enrichedAt: "2026-10-03T11:01:00Z",
            insight: {
              summary: "<script>interpretation</script>",
              sentiment: "NEUTRAL",
              sentimentScore: 0,
              relevanceScore: 0.5,
              confidence: 0.4,
              catalysts: [],
              evidence: ["d9c877ab-4a67-4457-a3ca-61c2b912bf85"],
              flags: ["LOW_CONFIDENCE"],
            },
          },
        },
      ],
    });
    mount();
    expect(await screen.findByText(label)).toBeVisible();
    expect(screen.getByText("<script>interpretation</script>")).toBeVisible();
    expect(screen.getByText("Raw snippet")).toBeVisible();
    expect(screen.getByText("low confidence")).toBeVisible();
    expect(document.querySelector("script")).toBeNull();
  },
);

test("successful empty data differs from provider failure", async () => {
  news.mockResolvedValueOnce({
    articles: [],
    status: "FRESH",
    lastRefreshedAt: "2026-10-03T12:00:00Z",
  });
  const view = mount();
  expect(await screen.findByText("No recent news for NVDA.")).toBeVisible();
  expect(screen.getByText("Up to date")).toBeVisible();
  view.unmount();
  news.mockRejectedValueOnce(new Error("Provider failure"));
  mount();
  expect(await screen.findByText("Provider unavailable")).toBeVisible();
  expect(
    screen.queryByText("No recent news for NVDA."),
  ).not.toBeInTheDocument();
});
test("keeps stale provider text plain and marks synthetic news without a fake article link", async () => {
  news.mockResolvedValueOnce({
    articles: [
      {
        id: "d9c877ab-4a67-4457-a3ca-61c2b912bf85",
        symbols: ["NVDA"],
        headline: "<script>untrusted headline</script>",
        source: "Synthetic demo",
        url: "https://news.trading-terminal.invalid/a",
        publishedAt: "2026-10-03T11:00:00Z",
        rawSummary: "Provider snippet",
        enrichment: null,
      },
    ],
    status: "STALE",
    lastRefreshedAt: "2026-10-03T11:00:00Z",
  });
  mount();
  expect(await screen.findByText("News delayed")).toBeVisible();
  expect(screen.getByText("Synthetic demo")).toBeVisible();
  expect(screen.getByText("<script>untrusted headline</script>")).toBeVisible();
  expect(document.querySelector("script")).toBeNull();
  expect(screen.queryByRole("link")).not.toBeInTheDocument();
});
test("symbol changes do not display a delayed response for the previous selection", async () => {
  let finish: (result: NewsResult) => void = () => {};
  news.mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const view = mount("NVDA");
  expect(screen.getByText("Loading NVDA news…")).toBeVisible();
  view.unmount();
  news.mockResolvedValueOnce({
    articles: [],
    status: "FRESH",
    lastRefreshedAt: null,
  });
  mount("AMD");
  finish({ articles: [], status: "FRESH", lastRefreshedAt: null });
  await waitFor(() =>
    expect(screen.getByText("No recent news for AMD.")).toBeVisible(),
  );
  expect(
    screen.queryByText("No recent news for NVDA."),
  ).not.toBeInTheDocument();
});
test("rejects unsafe article links", () => {
  expect(articleLink("javascript:alert(1)")).toBeNull();
  expect(articleLink("http://127.0.0.1/private")).toBeNull();
  expect(articleLink("https://user:pass@example.com/a")).toBeNull();
  expect(articleLink("https://example.com/a")).toBe("https://example.com/a");
});
test("a failed refresh after a previous empty success does not claim there is no current news", async () => {
  news.mockResolvedValueOnce({
    articles: [],
    status: "UNAVAILABLE",
    lastRefreshedAt: "2026-10-03T11:00:00Z",
  });
  mount();
  expect(await screen.findByText("Provider unavailable")).toBeVisible();
  expect(
    screen.getByText("No saved news for NVDA. Refresh is unavailable."),
  ).toBeVisible();
  expect(
    screen.queryByText("No recent news for NVDA."),
  ).not.toBeInTheDocument();
});
