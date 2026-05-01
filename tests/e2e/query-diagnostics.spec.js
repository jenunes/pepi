const { test, expect } = require("@playwright/test");

const mockQueriesResponse = {
  status: "success",
  data: {
    queries: [
      {
        namespace: "test.users",
        operation: "find",
        pattern: '{"status":"A"}',
        count: 150,
        min_ms: 10,
        max_ms: 800,
        mean_ms: 240,
        percentile_95_ms: 650,
        sum_ms: 36000,
        allow_disk_use: false,
        indexes: ["COLLSCAN"],
        health_score: 25,
        health_severity: "CRITICAL",
        scan_ratio: 5000,
        key_efficiency: 0,
        findings_count: 3,
        in_memory_sort_pct: 0,
        disk_usage_pct: 0,
        yield_rate: 12,
        avg_response_size: 450,
      },
      {
        namespace: "test.orders",
        operation: "find",
        pattern: '{"customer_id":42}',
        count: 30,
        min_ms: 2,
        max_ms: 15,
        mean_ms: 5,
        percentile_95_ms: 12,
        sum_ms: 150,
        allow_disk_use: false,
        indexes: ["IXSCAN { customer_id: 1 }"],
        health_score: 92,
        health_severity: "HEALTHY",
        scan_ratio: 1,
        key_efficiency: 1,
        findings_count: 0,
        in_memory_sort_pct: 0,
        disk_usage_pct: 0,
        yield_rate: 0,
        avg_response_size: 200,
      },
    ],
    total_patterns: 2,
    summary: {
      overall_health_score: 58,
      health_distribution: { healthy: 1, warning: 0, critical: 1 },
      top_by_total_time: [
        { namespace: "test.users", operation: "find", pattern: '{"status":"A"}', value: 36000, health_score: 25 },
      ],
      top_by_avg_latency: [],
      top_by_scan_ratio: [],
      top_by_execution_count: [],
      collection_scan_patterns: 1,
      in_memory_sort_patterns: 0,
      disk_spill_patterns: 0,
    },
    findings: [
      { severity: "critical", category: "execution_plan", title: "COLLSCAN", detail: "Pattern used COLLSCAN", recommendation: "Add index" },
    ],
  },
};

const mockDiagnostics = {
  status: "success",
  data: {
    health: {
      plan_type_score: 0,
      scan_ratio_score: 0,
      key_efficiency_score: 50,
      sort_score: 100,
      latency_score: 50,
      disk_score: 100,
      total: 25,
      severity: "CRITICAL",
    },
    findings: [
      { severity: "critical", category: "execution_plan", title: "COLLSCAN", detail: "Used COLLSCAN", recommendation: "Add index" },
      { severity: "warning", category: "performance", title: "High P95", detail: "P95 is 650ms", recommendation: "Review" },
    ],
    exec_stats: {
      keysExamined: [0],
      docsExamined: [15000],
      nreturned: [3],
      numYields: [12],
      reslen: [450],
      in_memory_sort_pct: 0,
      disk_usage_pct: 0,
    },
  },
};

test.describe("Query Diagnostics", () => {
  test.beforeEach(async ({ page }) => {
    await page.route("**/api/analyze/*/queries*", (route) => {
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(mockQueriesResponse) });
    });
    await page.route("**/api/analyze/*/query-diagnostics*", (route) => {
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(mockDiagnostics) });
    });
    await page.route("**/api/files", (route) => {
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ files: [{ file_id: "test-1", filename: "test.log", size: 1000, lines: 100, is_preloaded: false, sample_percentage: 100, preflight_tier: "ok", can_proceed: true }] }) });
    });
  });

  test("should display AWR summary panel with health gauge and distribution cards", async ({ page }) => {
    await page.goto("/");
    await page.evaluate(() => { window.currentFileId = "test-1"; });
    await page.evaluate(() => { window.analyzeQueries && window.analyzeQueries(); });
    await page.waitForTimeout(500);

    await expect(page.locator('[data-testid="awr-summary"]')).toBeVisible();
    await expect(page.locator('[data-testid="query-health-gauge"]')).toBeVisible();
    await expect(page.locator('[data-testid="health-distribution"]')).toBeVisible();
  });

  test("should show health badges and scan ratio on each query row", async ({ page }) => {
    await page.goto("/");
    await page.evaluate(() => { window.currentFileId = "test-1"; });
    await page.evaluate(() => { window.analyzeQueries && window.analyzeQueries(); });
    await page.waitForTimeout(500);

    const healthBadges = page.locator('[data-testid="query-health-badge"]');
    await expect(healthBadges).toHaveCount(2);
    const scanRatios = page.locator('[data-testid="query-scan-ratio"]');
    await expect(scanRatios).toHaveCount(2);
  });

  test("should open diagnostics drill-down panel when clicking findings badge", async ({ page }) => {
    await page.goto("/");
    await page.evaluate(() => { window.currentFileId = "test-1"; });
    await page.evaluate(() => { window.analyzeQueries && window.analyzeQueries(); });
    await page.waitForTimeout(500);

    const findingsBadge = page.locator('[data-testid="query-findings-count"]').first();
    await findingsBadge.click();
    await page.waitForTimeout(500);

    await expect(page.locator('[data-testid="diagnostics-panel"]')).toBeVisible();
    await expect(page.locator('[data-testid="diagnostics-tab-overview"]')).toBeVisible();
  });

  test("should switch between diagnostics tabs", async ({ page }) => {
    await page.goto("/");
    await page.evaluate(() => { window.currentFileId = "test-1"; });
    await page.evaluate(() => { window.analyzeQueries && window.analyzeQueries(); });
    await page.waitForTimeout(500);

    await page.locator('[data-testid="query-findings-count"]').first().click();
    await page.waitForTimeout(500);

    await page.locator('[data-testid="diagnostics-tab-findings"]').click();
    await expect(page.locator("#diag-findings.diag-pane.active")).toBeVisible();

    await page.locator('[data-testid="diagnostics-tab-exec-stats"]').click();
    await expect(page.locator("#diag-exec.diag-pane.active")).toBeVisible();
  });

  test("should have export report button", async ({ page }) => {
    await page.goto("/");
    await page.evaluate(() => { window.currentFileId = "test-1"; });
    await page.evaluate(() => { window.analyzeQueries && window.analyzeQueries(); });
    await page.waitForTimeout(500);

    await expect(page.locator('[data-testid="export-report-btn"]')).toBeVisible();
  });
});
