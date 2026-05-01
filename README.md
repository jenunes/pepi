# Pepi

**Current version: 2.2.5**

Pepi is a **local-first** MongoDB log analyzer. It reads JSON-line MongoDB log files, extracts connection activity, command/query patterns, replica set signals, client/driver metadata, and time-series style signals. It targets engineers triaging production and staging deployments **without** shipping logs to a third-party service.

**Who uses it:** DBAs, SREs, and application engineers reviewing `mongod` diagnostic logs.

**What problem it solves:** Large logs are hard to search by hand. Pepi aggregates patterns, surfaces slow queries and plan summaries where present, and offers a small FastAPI + browser UI for the same analyses the CLI performs.

---

## Key features

- **CLI:** Summary (dates, line count, OS/DB versions, startup options), replica set config/state, connections (counts, optional duration stats, sort, IP compare), clients/drivers, queries (pattern stats, filters, histogram, full-pattern export), log trim by time range, optional line sampling, pickle cache with TTL, version and upgrade helpers.
- **Web UI:** Upload (or preload via CLI), preflight by file size, optional **ingest** job into a local SQLite DB, analysis tabs (basic, raw extractor, connections, clients, queries, time series, replica set), trim, download, delete.
- **API:** FastAPI routes under `/api/...` (see [API overview](#api-overview)); OpenAPI at `/docs` when the server is running.
- **Index recommendations:** Rule-based analysis in-tree (`pepi/index_advisor.py`); API responses set `has_llm: false` in current code.

---

## Installation

### Editable install (recommended for development)

```bash
git clone https://github.com/jenunes/pepi.git
cd pepi
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e .
```

With dev tools (pytest, ruff, httpx):

```bash
pip install -e ".[dev]"
```

**Python:** `>=3.8` (see `pyproject.toml`).

### Install script (home directory layout)

If you use the project’s `install.sh`, it installs under `~/.pepi` and wires `pepi` into your PATH (see script for details).

```bash
curl -sSL https://raw.githubusercontent.com/jenunes/pepi/main/install.sh | bash
```

### Playwright (optional UI e2e)

From repo root, with Node/npm:

```bash
npm install
npx playwright install
```

Config: `playwright.config.js` — `baseURL` defaults to `http://127.0.0.1:8000`, overridable with `PEPI_UI_BASE_URL`. Tests live under `tests/e2e/*.spec.js`.

---

## Upgrade and uninstall

### `pepi --upgrade` (GitHub install layout only)

`pepi/upgrade.py` runs `git pull` and `pip install -r requirements.txt` inside **`~/.pepi`**. If that directory does not exist, upgrade prints instructions to reinstall via the install script.

**Editable / venv installs:** upgrade with `git pull` and `pip install -e .` (or your packaging workflow); do not rely on `pepi --upgrade` unless you use the `~/.pepi` layout.

### Version check (CLI, background)

On normal CLI runs (not `--web-ui`), a daemon thread calls GitHub’s API for repository tags (`check_version_async` → `check_for_updates`). This is a **version check**, not log telemetry.

### Uninstall (`~/.pepi` layout)

```bash
rm -rf ~/.pepi
rm -f ~/.local/bin/pepi   # if symlinked there
```

Remove `~/.pepi_cache` if you want to drop CLI cache and default ingest DB (see [Cache behavior](#cache-behavior--troubleshooting)).

---

## CLI quickstart

```bash
# Summary + reconstructed mongod startup command line (from log)
pepi --fetch /var/log/mongodb/mongod.log

# Connection counts (warns on stderr if >50k lines and sample is 100%)
pepi --fetch /var/log/mongodb/mongod.log --connections

# Query pattern table (truncated patterns in terminal; use report file for full text)
pepi --fetch /var/log/mongodb/mongod.log --queries --sort-by count

# Launch web UI (browser: first free port among 8000–8002; see server log / tmp port file)
pepi --web-ui
pepi --fetch /var/log/mongodb/mongod.log --web-ui --sample 50
```

---

## CLI reference

General form:

```text
pepi --fetch PATH [options]
pepi --web-ui [--fetch PATH] [--sample N]
```

**Required for most commands:** `--fetch` / `-f` must point to an **existing** file (Click validates the path), **except** when you only run `--web-ui` (no file) or `--version` / `--upgrade`.

If neither `--fetch` nor `--web-ui` is given, Pepi prints: `Pepi didn't find anything to fetch` and exits.

### Default (no analysis flag)

Reads the log once for basic metadata (unless cached), prints **Node Command Line Startup** (reconstructed `mongod` arguments when found) and **MongoDB Log Summary** (file, dates, line count, host from startup options if present, OS/kernel/DB version, replica set name and node count when derivable).

### `--rs-conf`

Prints the **latest** replica set configuration document found in the log (JSON). Uses parser cache when available.

### `--rs-state`

Prints current node status lines and per-host **state transitions** from the log.

### `--connections`

Per-IP opened/closed counts. Optional:

- **`--stats`:** duration min/max/avg per IP and overall (from paired accept/end events).
- **`--sort-by opened|closed`:** descending sort by count (other `sort-by` values accepted by Click but **do not** change connection sort order in code).
- **`--compare HOST ...`:** two or three hosts; fewer than two is an error; more than three uses the first three with a warning.

Uses `parse_connections(..., sample_percentage=--sample)`; see [Sampling](#sampling-behavior--trade-offs).

### `--clients`

Lists driver name/version, app name (if any), connection count, IPs, and users per driver grouping.

### `--queries`

Query pattern statistics (namespace, operation, truncated pattern, counts, timing stats, index/plan summary strings from logs). Optional:

- **`--sort-by count|min|max|95%|sum|mean`**
- **`--namespace NS`** and **`--operation OP`**
- **`--report-full-patterns FILE`:** writes full table to **file** and exits (does not print the table to stdout).
- **`--report-histogram`:** duration histogram across **filtered** query stats.

### `--trim` with `--from` / `--until`

Requires **at least one** of `--from` or `--until`. Datetimes use flexible parsing (`DD/MM/YYYY` and optional time parts; see `--trim --help`).

Reads matching lines, reports counts, then interactively asks whether to save and the output filename (default `*_trimmed*`).

### `--web-ui`

Starts `python -m pepi.web_api` as a subprocess. Optional env: `PEPI_PRELOAD_FILE` (absolute path from `--fetch`), `PEPI_SAMPLE_PERCENTAGE` (from `--sample`). Prints `http://localhost:<port>` (port discovered via tmp file or psutil).

### `--sample N`

Integer **0–100**, default **100**. Passed into connection/query parsers. Behavior is defined in `pepi/sampling.py` and `pepi/parser.py` (see [Sampling](#sampling-behavior--trade-offs)).

### `--clear-cache`

Deletes **all** `*.pkl` under `~/.pepi_cache` and exits. Due to CLI ordering, you must still pass a valid **`--fetch`** path even though cache clearing is global:

```bash
pepi --fetch /path/to/any.log --clear-cache
```

### `--version`

Prints `pepi version`, version string, and repo URL; exits.

### `--upgrade`

Runs the `~/.pepi`-only upgrade path (see [Upgrade and uninstall](#upgrade-and-uninstall)).

### Contextual help

```bash
pepi --connections --help
pepi --queries --help
pepi --trim --help
pepi --web-ui --help
```

---

## Web UI quickstart

```bash
pepi --web-ui
# Open the printed URL (typically http://localhost:8000–8002)
```

- **Upload:** drag/drop or browse; accepted extensions are enforced server-side (`.log`, `.txt`, `.json`, and names containing `.log.` for rotation).
- **Sampling field:** 0–100; sent as `sample` query parameter on relevant analyze calls.
- **Preflight:** large files show tiered warnings; ingest may require confirmation or block unless overridden (API `force` / env `PEPI_ALLOW_OVERSIZE`).
- **FAQ (in UI):** `/static/faq.html` (footer link on the main page).

---

## Web UI tabs guide

| Tab | Purpose |
|-----|---------|
| **Basic Info** | File metadata, sampling metadata text, MongoDB fields from log, **Trim log** (calls `POST /api/trim/{file_id}`), ingest status when used. |
| **Raw Extractor** | Filtered log lines via `POST /api/analyze/{file_id}/extract` with `source=raw` or `ingest` depending on whether ingest completed. |
| **Connections** | Charts/tables from `POST /api/analyze/{file_id}/connections` (`source`, `sample`, `include_details`). |
| **Clients** | Driver/client breakdown from `POST /api/analyze/{file_id}/clients`. |
| **Queries** | Pattern list and drill-down; uses queries + diagnostics + examples + index recommendation endpoints. |
| **Time Series** | Slow queries, connections, errors; raw arrays may be omitted when line count ≥ 200k (`include_raw=false`); plot points capped client-side (see [Performance](#performance--large-log-guidance)). |
| **Replica Set** | Config + state from `POST /api/analyze/{file_id}/replica-set`. |

Tabs stay hidden until a file is selected.

---

## Query diagnostics and index recommendations

### How patterns are built

`parse_queries` (raw file scan) groups COMMAND entries (`msg` in `command`, `Slow query`) by `(namespace, operation, pattern)` where `pattern` comes from `extract_query_pattern` in `pepi/parser.py`. Stats (`calculate_query_stats`) feed AWR-style bundles via `build_queries_analysis_data` / `build_query_diagnostics_data` (`pepi/queries_awr.py`).

### Web workflow

1. Run **Analyze queries** (`POST /api/analyze/{file_id}/queries`) with optional namespace/operation filters and `sample`.
2. Select a pattern; the UI loads **query diagnostics** (`POST /api/analyze/{file_id}/query-diagnostics` with body `namespace`, `operation`, `pattern`) and **query examples** (`POST .../query-examples`) by scanning the log for matching COMMAND lines (up to five examples with `raw_log_line`).
3. **Index recommendations** (`POST /api/analyze/{file_id}/index-recommendations`):
   - With an example’s **`raw_log_line`** in the JSON body: `analyze_single_query` runs; response `recommendation_source` = **`selected_example`**.
   - Without `raw_log_line` but with namespace/operation/pattern/stats: filters aggregated stats; `recommendation_source` = **`selected_pattern_fallback`** (empty list if no match).
   - Without a body (or broad bulk): **`bulk_all_patterns`** (capped by `top_n`, default 10).

Rule-based engine: `pepi/index_advisor.py`. API always returns **`has_llm: false`** today.

---

## Performance and large-log guidance

- **Parser sampling:** At `sample=100` and `total_lines > 50000`, connections/queries use automatic line stride (5/10/20 by size band). Lower `--sample` to force coarser stride (`100/N` → every `N`th line).
- **CLI warning:** For `--connections` / `--queries` with `sample=100` and `>50000` lines, a stderr warning suggests e.g. `--sample 50`.
- **Web preflight:** Uses **file size** tiers (`PEPI_FILE_WARN_GB`, `PEPI_FILE_CONFIRM_GB`, `PEPI_FILE_BLOCK_GB`). Prefer trimming to the incident window (`/api/trim/{file_id}` or CLI `--trim`).
- **Web time series:** For `totalLines >= 200000`, the UI sets large-dataset mode: `include_raw=false` on timeseries request, plot caps **2000** points per series (otherwise **6000**), extract page size **5000** (otherwise **10000**).

---

## Sampling behavior and trade-offs

- **Mechanism:** deterministic **line stride** (`line_count % sample_rate != 0` skipped), not random sampling.
- **`sample=100`:** if `total_lines > 50000`, effective `sample_rate` is 5 (&lt;200k lines), 10 (&lt;500k), or 20 (≥500k). Metadata is exposed as `sampling_metadata` in API responses.
- **`sample<100`:** `sample_rate = int(100/sample)` when sample&gt;0; no extra auto stride.
- **`sample=0`:** skip all lines in parsers (not usually useful except edge/testing).

Implication: rare events can be missed; counts are **approximate** when sampled.

---

## Cache behavior and troubleshooting

- **Location:** `~/.pepi_cache/` (`CACHE_DIR` in `pepi/cache.py`).
- **TTL:** 7 days since last **successful read** (mtime refreshed on load).
- **Corrupt pickle:** removed on read error.
- **Keys:** file SHA256 + analysis type + sampling variant (for connections/queries).
- **Clear:** CLI `--clear-cache` (all `*.pkl`) or delete files manually.
- **Ingest DB:** default `~/.pepi_cache/pepi_ingest.db` (override `PEPI_INGEST_DB_PATH`). Deleting a file via API runs `delete_file_ingest_data` for that `file_id`.

---

## API overview

Base URL: your server origin (e.g. `http://127.0.0.1:8000`). CORS allows localhost on ports **8000–8002** only.

| Method | Path | Purpose / notes |
|--------|------|-------------------|
| `POST` | `/api/upload` | Multipart file upload; streamed to temp dir; returns `file_id`. |
| `GET` | `/api/files` | Lists in-memory uploads + preflight tier fields. |
| `GET` | `/api/files/{file_id}/preflight` | Size-based preflight payload. |
| `DELETE` | `/api/files/{file_id}` | Removes temp file (not preloaded original), clears ingest rows for id. |
| `POST` | `/api/ingest/{file_id}/start` | Query `force` bool. Starts background ingest unless job already running. |
| `GET` | `/api/ingest/{file_id}/status` | Latest job row or synthetic `not_started`. |
| `POST` | `/api/ingest/{file_id}/cancel` | Sets cancel event on runtime worker. |
| `POST` | `/api/analyze/{file_id}/basic` | Query `sample` (optional, default 100). |
| `POST` | `/api/analyze/{file_id}/connections` | Query: `sample`, `include_details`, `source` (`raw` \| `ingest`). |
| `POST` | `/api/analyze/{file_id}/queries` | Query: `namespace`, `operation`, `sample`. |
| `POST` | `/api/analyze/{file_id}/query-diagnostics` | JSON body `QueryDiagnosticsRequest`; query `sample`. |
| `POST` | `/api/analyze/{file_id}/query-examples` | JSON body `QueryExamplesRequest`. |
| `POST` | `/api/analyze/{file_id}/timeseries` | Query: `namespace`, `include_raw`, `source` (`raw` \| `ingest`). |
| `POST` | `/api/analyze/{file_id}/index-recommendations` | Optional JSON `SingleQueryRequest`; query `top_n`, `single_query`. |
| `POST` | `/api/analyze/{file_id}/replica-set` | No body. |
| `POST` | `/api/analyze/{file_id}/clients` | No body. |
| `POST` | `/api/analyze/{file_id}/extract` | JSON `LogFilterRequest`; query `offset`, `source`. |
| `GET` | `/api/analyze/{file_id}/filter-options` | Scans file once for filter vocabularies (namespace list capped at 20). |
| `POST` | `/api/trim/{file_id}` | JSON `TrimRequest` (`from_date`, `until_date`). |
| `GET` | `/api/download/{file_id}` | Attachment of stored path. |
| `GET` | `/api/system/tmp-health` | Free space vs configured thresholds for upload tmp dir. |

---

## Data processing paths

- **Raw:** Each request reads the uploaded JSON log from disk (`source=raw` or default). Heavy passes re-parse as needed; mitigated by UI sampling and extract limits.
- **Ingest:** `POST .../ingest/.../start` runs `run_ingest_job`: full-file scan, inserts into SQLite (`log_events`, `connection_events`, `timeseries_agg`, …). `source=ingest` on connections/timeseries/extract reads that DB. Ingest is **rebuilt** from scratch when a new job starts (`delete_file_ingest_data` first). UI switches `currentAnalysisSource` to `ingest` when latest job status is **`completed`**.

---

## Privacy and security model

- **Processing:** analysis runs on the machine hosting the Pepi process (CLI or local uvicorn). Logs are not sent to a Pepi-operated cloud by this codebase.
- **Network:** Optional GitHub tag fetch on CLI; `pip` / `git` during upgrade; browser may load CDN assets referenced by `index.html` (Chart.js, Plotly, Flatpickr, Font Awesome).
- **Disk:** uploads under configurable temp dir; trimmed copies under system temp; cache and ingest DB under `~/.pepi_cache` by default.
- **Retention:** upload files removed on API shutdown for non-preloaded entries; tmp cleanup glob `pepi_upload_*.log` respects `PEPI_TMP_CLEANUP_MAX_AGE_SECONDS` (default 86400s). Preloaded files are not deleted on shutdown.

---

## FAQ

See the in-app **[FAQ](/static/faq.html)** (served as `http://<host>:<port>/static/faq.html`).

---

## Contributing and development

```bash
pip install -e ".[dev]"
ruff check pepi tests
pytest -q
# optional UI e2e (server must be running unless you automate it):
npm install && npx playwright test tests/e2e/ui-tabs.spec.js
```

Package layout: Python package under `pepi/`, tests under `tests/`.

---

## Documentation changelog (2.2.5)

**Corrected:** Removed duplicate README merge; fixed Python floor (3.8+); fixed time-series plot cap claims (2000/6000 vs old “10000”); CLI `--clear-cache` requires existing `--fetch` path; `pepi --upgrade` only for `~/.pepi`; analyze routes are **POST**; FAQ URL is `/static/faq.html`; sampling described as line-stride with auto tiers at `sample=100`.

**Clarified:** `sort-by` for connections; ingest vs raw; `recommendation_source`; tmp/env knobs; port range 8000–8002 and max three servers.

**VERIFY:** Optional LLM deps in `requirements.txt` comments are not reflected as `has_llm: true` in API responses; confirm product direction before documenting local LLM.
