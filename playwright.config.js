// @ts-check
const { defineConfig } = require("@playwright/test");

module.exports = defineConfig({
  testDir: ".",
  timeout: 45_000,
  retries: process.env.CI ? 1 : 0,
  use: {
    baseURL: process.env.PEPI_UI_BASE_URL || "http://127.0.0.1:8000",
    headless: true,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "off"
  },
  reporter: [
    ["line"],
    ["html", { outputFolder: "playwright-report", open: "never" }]
  ],
  outputDir: "test-results"
});
