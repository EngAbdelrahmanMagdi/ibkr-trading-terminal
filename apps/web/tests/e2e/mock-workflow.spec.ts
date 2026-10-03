import { expect, test } from "@playwright/test";

test("MOCK terminal: live quote, chart, fill, position, execution and cancellation", async ({
  page,
}) => {
  await page.goto("/");
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
  await expect(page.getByText(/Order filled\./i)).toBeVisible();
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
  await expect(page.getByLabel("Price chart")).toBeVisible();
  await page.getByRole("tab", { name: "News", exact: true }).click();
  await expect(
    page
      .getByRole("region", { name: "NVDA news" })
      .getByText("NVDA: synthetic company outlook update"),
  ).toBeVisible();
  await page.getByRole("button", { name: "Trade NVDA" }).click();
  await expect(
    page.getByRole("heading", { name: "Order ticket" }),
  ).toBeVisible();
});
