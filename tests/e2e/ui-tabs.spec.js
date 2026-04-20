const path = require("path");
const fs = require("fs");
const { test, expect } = require("@playwright/test");

const defaultFixturePath = path.resolve(__dirname, "../fixtures/ui_tab_sample.log");

function resolveLogPath() {
  const envDir = process.env.PEPI_E2E_LOG_DIR;
  if (!envDir) return defaultFixturePath;

  if (!fs.existsSync(envDir)) return defaultFixturePath;
  const entries = fs
    .readdirSync(envDir)
    .filter((name) => name.endsWith(".log") || name.includes(".log."))
    .map((name) => path.join(envDir, name))
    .sort();

  if (entries.length === 0) return defaultFixturePath;
  return entries[0];
}

const fixturePath = resolveLogPath();

const tabs = [
  { key: "basic", paneSelector: "#basic #basicInfo" },
  { key: "extractor", paneSelector: "#extractor .extractor-filters" },
  { key: "connections", paneSelector: "#connections #connectionStats" },
  { key: "clients", paneSelector: "#clients #clientsContent" },
  { key: "queries", paneSelector: "#queries #queryStats" },
  { key: "timeseries", paneSelector: "#timeseries #slowQueriesPlot" },
  { key: "replica-set", paneSelector: "#replica-set #replicaSetContent" }
];

test("all requested tabs are visible and render primary UI containers", async ({ page }) => {
  await page.goto("/");

  await page.setInputFiles("#fileInput", fixturePath);
  await expect(page.locator("#filesSection")).toBeVisible();

  const firstAnalyzeButton = page.locator(".file-item .file-actions button[title='Analyze']").first();
  await firstAnalyzeButton.click();

  await expect(page.locator("#basic.tab-pane.active")).toBeVisible();
  await expect(page.locator("#basic #basicInfo")).toBeVisible();

  for (const tab of tabs) {
    await page.click(`.tab-btn[data-tab='${tab.key}']`);
    await expect(page.locator(`#${tab.key}.tab-pane.active`)).toBeVisible();
    await expect(page.locator(tab.paneSelector)).toBeVisible();
  }
});

test("removed FTDC tab is no longer present", async ({ page }) => {
  await page.goto("/");
  await expect(page.locator('.tab-btn[data-tab="ftdc-viewer"]')).toHaveCount(0);
  await expect(page.locator('#ftdc-viewer')).toHaveCount(0);
  await expect(page.locator('#fsBrowserModal')).toHaveCount(0);
});
