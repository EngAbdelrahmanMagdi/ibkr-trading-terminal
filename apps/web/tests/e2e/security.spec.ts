import { expect, test } from "@playwright/test";

test("production CSP uses distinct nonces and blocks untrusted inline scripts", async ({
  page,
  request,
}) => {
  const first = await request.get("/");
  const second = await request.get("/");
  const firstPolicy = first.headers()["content-security-policy"];
  const secondPolicy = second.headers()["content-security-policy"];
  if (!firstPolicy || !secondPolicy) throw new Error("Missing enforced CSP");
  expect(firstPolicy).toContain("'strict-dynamic'");
  expect(
    firstPolicy
      .split(";")
      .find((part) => part.trim().startsWith("script-src ")),
  ).not.toContain("unsafe-inline");
  expect(firstPolicy).not.toContain("unsafe-eval");
  expect(firstPolicy.match(/'nonce-([^']+)'/)?.[1]).not.toEqual(
    secondPolicy.match(/'nonce-([^']+)'/)?.[1],
  );
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Watchlist" })).toBeVisible();
  const violations: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") violations.push(message.text());
  });
  await page.route("**/security-probe", (route) =>
    route.fulfill({
      status: 200,
      headers: {
        "content-type": "text/html",
        "content-security-policy": firstPolicy,
      },
      body: "<!doctype html><html><body><script>document.documentElement.dataset.untrustedScript = 'executed'</script></body></html>",
    }),
  );
  await page.goto("/security-probe");
  await expect(page.locator("html")).not.toHaveAttribute(
    "data-untrusted-script",
  );
  expect(
    violations.some((message) => message.includes("Content Security Policy")),
  ).toBe(true);
});
