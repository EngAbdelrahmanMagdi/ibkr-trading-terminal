import { expect, test } from "@playwright/test";
import { writeFile } from "node:fs/promises";
import type { Portfolio, Position } from "@/lib/api";

const newYorkDay = () =>
  new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date());

test("record a fresh simulated round trip", async ({ page }, testInfo) => {
  const core = process.env.DEMO_CORE_ORIGIN;
  expect(core, "A disposable Core origin is required").toBeTruthy();
  const day = newYorkDay();
  const initial = await page.request.get(`${core}/api/v1/executions`);
  expect(initial.ok()).toBe(true);
  expect(await initial.json()).toEqual([]);
  await page.goto("/");
  await expect(page).toHaveTitle("MarketPulse Terminal");
  await expect(
    page.getByRole("status").filter({ hasText: "LIVE" }),
  ).toBeVisible();
  await expect(
    page.getByLabel("Price chart").locator("canvas").first(),
  ).toBeVisible();
  // These pauses are solely recording presentation; every state transition is asserted.
  await page.waitForTimeout(8_000);
  await page
    .getByRole("listbox", { name: "Watchlist symbols" })
    .getByRole("button", { name: /AAPL/ })
    .click();
  await expect(page.getByRole("heading", { name: "AAPL" })).toBeVisible();
  await expect(page.getByText("AWAITING QUOTE")).toHaveCount(0);
  await page.waitForTimeout(8_000);
  await page.getByRole("button", { name: "Place buy order" }).click();
  await expect
    .poll(async () => {
      const response = await page.request.get(`${core}/api/v1/executions`);
      expect(response.ok()).toBe(true);
      return (await response.json()).length;
    })
    .toBe(1);
  await page.getByRole("button", { name: /^Notifications/ }).click();
  const history = page.getByRole("region", { name: "Notification history" });
  await expect(history).toContainText(/Bought 1 AAPL @/);
  const bellBounds = await page
    .getByRole("button", { name: /^Notifications/ })
    .boundingBox();
  const historyBounds = await history.boundingBox();
  expect(bellBounds).not.toBeNull();
  expect(historyBounds).not.toBeNull();
  expect(
    Math.abs(
      historyBounds!.x +
        historyBounds!.width -
        bellBounds!.x -
        bellBounds!.width,
    ),
  ).toBeLessThan(2);
  await page.waitForTimeout(8_000);
  await page.getByRole("button", { name: "Close notifications" }).click();
  await page.getByRole("button", { name: "SELL", exact: true }).click();
  await page.getByRole("button", { name: "Place sell order" }).click();
  await expect
    .poll(async () => {
      const response = await page.request.get(`${core}/api/v1/executions`);
      expect(response.ok()).toBe(true);
      return (await response.json()).length;
    })
    .toBe(2);
  await expect
    .poll(async () => {
      const response = await page.request.get(`${core}/api/v1/positions`);
      expect(response.ok()).toBe(true);
      const positions = (await response.json()) as Position[];
      return positions.filter((position) => position.quantity !== "0").length;
    })
    .toBe(0);
  const portfolioResponse = await page.request.get(`${core}/api/v1/portfolio`);
  expect(portfolioResponse.ok()).toBe(true);
  const portfolio = (await portfolioResponse.json()) as Portfolio;
  expect(portfolio.accountMode).toBe("MOCK");
  expect(portfolio.dayPnl.available).toBe(true);
  expect(portfolio.dayPnl.value).not.toBeNull();
  const dayPnl = page
    .getByText("DAY P&L", { exact: true })
    .locator("..")
    .locator("strong");
  await expect(dayPnl).toHaveText(
    `${portfolio.dayPnl.value} ${portfolio.dayPnl.currency}`,
  );
  await testInfo.attach("core-portfolio-evidence", {
    body: JSON.stringify({ newYorkDay: day, portfolio }, null, 2),
    contentType: "application/json",
  });
  await page.getByRole("tab", { name: /Executions/ }).click();
  await expect(
    page.getByRole("tabpanel").getByRole("cell", { name: "AAPL", exact: true }),
  ).toHaveCount(2);
  await page.waitForTimeout(8_000);
  await page.getByRole("tab", { name: "News", exact: true }).click();
  const news = page.getByRole("region", { name: "AAPL news" });
  await expect(news.getByText("Synthetic demo")).toHaveCount(2);
  await expect
    .poll(
      async () => {
        await news.getByRole("button", { name: "Refresh news" }).click();
        return news.getByText("Synthetic insight", { exact: true }).count();
      },
      { timeout: 45_000, intervals: [1_000] },
    )
    .toBe(2);
  await page.waitForTimeout(8_000);
  await page.getByRole("button", { name: /^Notifications/ }).click();
  await expect(history).toContainText(/Sold 1 AAPL @/);
  await page.waitForTimeout(8_000);
  await page.screenshot({ path: `${process.env.DEMO_OUTPUT}/terminal.png` });
  expect(
    newYorkDay(),
    "Recording crossed the New York day: rerun with fresh state",
  ).toBe(day);
  await writeFile(
    `${process.env.DEMO_OUTPUT}/demo-evidence.json`,
    JSON.stringify({
      newYorkDay: day,
      portfolio,
      executions: 2,
      closedPosition: true,
      syntheticInsights: 2,
    }),
  );
});
