import { expect, test, type Page } from "@playwright/test";

async function expectHistoryAnchored(page: Page) {
  const trigger = await page
    .getByRole("button", { name: /^Notifications/ })
    .boundingBox();
  const history = await page
    .getByRole("region", { name: "Notification history" })
    .boundingBox();
  expect(trigger).not.toBeNull();
  expect(history).not.toBeNull();
  expect(
    Math.abs(history!.x + history!.width - trigger!.x - trigger!.width),
  ).toBeLessThan(2);
  expect(history!.y - trigger!.y - trigger!.height).toBeGreaterThanOrEqual(0);
  expect(history!.y - trigger!.y - trigger!.height).toBeLessThanOrEqual(12);
  expect(history!.x).toBeGreaterThanOrEqual(0);
}

test("MOCK terminal: live quote, chart, fill, position, execution and cancellation", async ({
  page,
}) => {
  await page.goto("/");
  await expect(page).toHaveTitle("MarketPulse Terminal");
  await expect(page.locator("header")).toContainText("MarketPulse");
  await expect(page.locator("header")).not.toContainText(/APERTURE/i);
  for (const path of ["/icon.svg", "/favicon.ico"])
    expect((await page.request.get(path)).ok()).toBe(true);
  await expect(page.getByRole("heading", { name: "Watchlist" })).toBeVisible();
  await page
    .getByRole("listbox", { name: "Watchlist symbols" })
    .getByRole("button", { name: /AAPL/ })
    .click();
  await expect(page.getByRole("heading", { name: "AAPL" })).toBeVisible();
  await expect(
    page.getByRole("status").filter({ hasText: "LIVE" }),
  ).toBeVisible();
  await expect(
    page.getByLabel("Price chart").locator("canvas").first(),
  ).toBeVisible();
  await expect(page.getByText("AWAITING QUOTE")).toHaveCount(0);
  await expect(page.getByRole("tabpanel")).not.toContainText(
    "Loading positions",
  );

  const aaplPosition = page
    .getByRole("tabpanel")
    .getByRole("row")
    .filter({ hasText: "AAPL" });
  const positionBefore = (await aaplPosition.count())
    ? Number(await aaplPosition.locator("td").nth(1).innerText())
    : 0;
  const executionsTab = page.getByRole("tab", { name: /Executions/ });
  const executionsBefore = Number(
    (await executionsTab.innerText()).match(/\d+/)?.[0] ?? "0",
  );

  const buyingPower = page
    .getByText("BUYING POWER", { exact: true })
    .locator("..")
    .locator("strong");
  await expect(buyingPower).not.toHaveText("—");
  const buyingPowerBefore = await buyingPower.innerText();

  await page.getByRole("button", { name: "Place buy order" }).click();
  await page.getByRole("button", { name: /^Notifications/ }).click();
  await expect(
    page.getByRole("region", { name: "Notification history" }),
  ).toContainText(/Bought 1 AAPL @/);
  await expectHistoryAnchored(page);
  await page.getByRole("button", { name: "Close notifications" }).click();
  await expect(aaplPosition).toBeVisible();
  await expect
    .poll(() => aaplPosition.locator("td").nth(1).innerText().then(Number))
    .toBeGreaterThan(positionBefore);
  await expect.poll(() => buyingPower.innerText()).not.toBe(buyingPowerBefore);
  await expect
    .poll(async () =>
      Number((await executionsTab.innerText()).match(/\d+/)?.[0]),
    )
    .toBeGreaterThan(executionsBefore);

  await executionsTab.click();
  await expect(
    page.getByRole("tabpanel").getByRole("cell", { name: "AAPL" }).first(),
  ).toBeVisible();

  await page.getByRole("button", { name: "Limit", exact: true }).click();
  await page.getByLabel("LIMIT PRICE").fill("1.00");
  await page.getByRole("button", { name: "Place buy order" }).click();
  await page.getByRole("tab", { name: /Open orders/ }).click();
  const openRow = page
    .getByRole("tabpanel")
    .getByRole("row")
    .filter({ hasText: "AAPL" })
    .filter({ hasText: "1.00" })
    .first();
  await expect(openRow).toContainText("SUBMITTED");
  await openRow.getByRole("button", { name: "Cancel" }).click();
  await page.getByRole("button", { name: "All orders" }).click();
  await expect(
    page
      .getByRole("tabpanel")
      .getByRole("row")
      .filter({ hasText: "AAPL" })
      .filter({ hasText: "1.00" })
      .first(),
  ).toContainText("CANCELLED");
  await page.getByRole("button", { name: /^Notifications/ }).click();
  await expect(
    page.getByRole("region", { name: "Notification history" }),
  ).toContainText("Order cancelled");
  await expect(
    page.getByRole("region", { name: "Notification history" }),
  ).toContainText("Cancellation requested");
  await expectHistoryAnchored(page);
  await page.getByRole("button", { name: "Close notifications" }).click();
  await page.getByRole("tab", { name: "News", exact: true }).click();
  const news = page.getByRole("region", { name: "AAPL news" });
  await expect(
    news.getByText("AAPL: synthetic company outlook update"),
  ).toBeVisible();
  await expect(news.getByRole("listitem")).toHaveCount(2);
  await expect(news.getByText("Synthetic demo")).toHaveCount(2);
  await expect
    .poll(
      async () => {
        await news.getByRole("button", { name: "Refresh news" }).click();
        return news.getByText("Synthetic insight", { exact: true }).count();
      },
      { timeout: 30000 },
    )
    .toBe(2);
  await page
    .getByRole("listbox", { name: "Watchlist symbols" })
    .getByRole("button", { name: /NVDA/ })
    .click();
  await expect(
    page
      .getByRole("region", { name: "NVDA news" })
      .getByText("NVDA: synthetic company outlook update"),
  ).toBeVisible();
  await expect(
    page.getByText("AAPL: synthetic company outlook update"),
  ).toHaveCount(0);
});

test("narrow workspace keeps chart and trading controls reachable", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await expect(page).toHaveTitle("MarketPulse Terminal");
  await expect(page.getByLabel("Price chart")).toBeVisible();
  await page.getByRole("tab", { name: "News", exact: true }).click();
  await expect(
    page
      .getByRole("region", { name: "NVDA news" })
      .getByText("NVDA: synthetic company outlook update"),
  ).toBeVisible();
  const notifications = page.getByRole("button", { name: /^Notifications/ });
  await notifications.focus();
  await notifications.press("Enter");
  const history = page.getByRole("region", { name: "Notification history" });
  await expect(history).toBeFocused();
  await expectHistoryAnchored(page);
  await history.press("Escape");
  await expect(notifications).toBeFocused();
  await page.getByRole("button", { name: "Trade NVDA" }).click();
  await expect(
    page.getByRole("heading", { name: "Order ticket" }),
  ).toBeVisible();
});
