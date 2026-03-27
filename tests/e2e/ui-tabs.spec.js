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
  { key: "connections", paneSelector: "#connections .analysis-header h2" },
  { key: "clients", paneSelector: "#clients .analysis-header h2" },
  { key: "queries", paneSelector: "#queries .analysis-header h2" },
  { key: "timeseries", paneSelector: "#timeseries .analysis-header h2" },
  { key: "replica-set", paneSelector: "#replica-set .analysis-header h2" }
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
    if (tab.key === "queries") {
      await page.getByRole("button", { name: /Analyze Queries/i }).click();
      await expect(page.locator("#queriesTable .queries-primary-table")).toBeVisible({
        timeout: 30_000
      });
      await expect(
        page.locator('#queriesTable button.queries-expand-btn[aria-label*="metrics"]')
      ).toBeVisible();
      await expect(page.locator("#queriesTable th[data-sort-key='sum_ms']")).toHaveText(/Total/);
    }
  }
});
