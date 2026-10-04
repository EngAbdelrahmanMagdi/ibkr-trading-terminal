import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/demo",
  workers: 1,
  retries: 0,
  timeout: 180_000,
  expect: { timeout: 30_000 },
  outputDir: process.env.DEMO_OUTPUT ?? "test-results/demo",
  reporter: "line",
  use: {
    baseURL: process.env.PLAYWRIGHT_BASE_URL ?? "http://127.0.0.1:46000",
    viewport: { width: 1440, height: 900 },
    video: { mode: "on", size: { width: 1440, height: 900 } },
    trace: "off",
  },
});
