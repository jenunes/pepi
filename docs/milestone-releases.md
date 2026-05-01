# Milestone GitHub release notes (archival)

These bodies follow [VERSIONING.md](../VERSIONING.md) § **GitHub Release body (Summary + Details)**. They match the **curated** GitHub Releases (not every git tag). Sources: annotated tag messages, `git log` between tags on the maintainer clone, and code at each tag where noted.

---

# Pepi v1.0.0

## Summary

Major **v1.0.0** release: index recommendations with a **two-tier** model (fast rule-based analysis plus optional deeper explanations), Time Series UX improvements, and a published FAQ. Optional **local LLM** path via `llama-cpp-python` and user-supplied **GGUF** models (models not bundled in the repo).

## Highlights

- Index advisor workflow with rule-based tier plus optional local LLM tier (no external API calls in that design).
- Time Series: synchronized zoom/pan across panels; Index column behavior refined for query views.
- FAQ page covering privacy, AI/index usage, and technical setup; minimalist footer with FAQ link.
- Aggregate query pattern extraction improvements and system-collection filtering (`admin`, `config`, `local`, `system.*`).
- Installation guidance for optional LLM setup (user-downloaded GGUF).

## Details

### Web UI

- Time Series interactions; FAQ; modals for installation guidance.

### Docs

- FAQ content for privacy and index/AI topics.

### Packaging / install

- Installation and upgrade flow as documented for the v1.0.0 tag.

## Upgrade

- Editable install: `git pull` and `pip install -e .` from your clone; follow tag-era README for optional `llama-cpp-python` / model download steps.

## Breaking changes

None documented for this tag; verify against your MongoDB JSON diagnostic log format.

## Compatibility

- Toolchain and Python floor as of the **v1.0.0** tag; MongoDB **4.4+** JSON log format per release messaging.

## Links

- Full diff: https://github.com/jenunes/pepi/compare/v0.0.2.3...v1.0.0

---

# Pepi v1.0.5

## Summary

Last v1.0.x maintenance release before v2.0.0: Raw Extractor and Query Examples UX, Connections tab enhancements, install/upgrade helpers, and launcher fixes.

## Highlights

- Raw Log Extractor tab with dynamic filter options.
- Connections tab: interactive charts and data quality indicators.
- Raw Extractor: “Apply format” flow; Raw JSON vs Pretty JSON (dropped “one per line” option).
- Query Examples: full-screen expand modal, format toggle, expand-by-default, download support.
- Added installation and upgrade system; `pepi.sh` fixes for relative paths and launcher behavior.
- Version metadata bumped through v1.0.4 → v1.0.5.

## Details

### Web UI

- Raw Extractor, Query Examples modals/formatting, Connections visualizations.

### Packaging / install

- Install/upgrade scripts and `pepi.sh` path handling.

## Upgrade

- Editable install: `git pull` and `pip install -e .` from your clone.

## Breaking changes

None.

## Compatibility

- Same log format expectations as v1.0.x series.

## Links

- Full diff: https://github.com/jenunes/pepi/compare/v1.0.0...v1.0.5

---

# Pepi v2.0.0

## Summary

v2.0.0 introduces native **FTDC Viewer** integration in the product line represented by this tag (see v2.2.x notes: FTDC was later removed in a subsequent release).

## Highlights

- Integrated native FTDC Viewer support (as shipped at this tag).

## Details

### Web UI

- FTDC Viewer integration (v2.0.0 scope).

## Upgrade

- Editable install: `git pull` and `pip install -e .`; review any FTDC-related assets or docs bundled with that release.

## Breaking changes

- Major version bump to 2.x; review release notes for v1.0.6 intermediate tag on your branch if present in history.

## Compatibility

- Toolchain as of tag `v2.0.0`; follow repository README for that revision if pinning.

## Links

- Full diff: https://github.com/jenunes/pepi/compare/v1.0.5...v2.0.0

---

# Pepi v2.2.0

## Summary

2.2 line baseline on this repository graph: **large-file** UX guardrails and clearer guidance when analyzing big MongoDB logs in the Web UI.

## Highlights

- Lower default preflight warning threshold to **0.5 GB** (aligns with server-side preflight tiers).
- **Dismissible** “large dataset” banner in the Web UI with trim guidance wording aligned to API/docs.
- Tests updated for warning-tier / banner behavior.

## Details

### Web UI

- Large-file warnings and dismissible dataset banner.

### HTTP API

- Preflight messaging thresholds consistent with `PEPI_FILE_WARN_GB` semantics (see code at this tag).

### Docs

- Trim guidance wording aligned with UI copy.

## Upgrade

- Editable install: `git pull` and `pip install -e .` from your clone.

## Breaking changes

None.

## Compatibility

- Python **≥3.8** (project metadata at this tag); MongoDB JSON log lines unchanged.

## Links

- Full diff: https://github.com/jenunes/pepi/compare/v2.0.0...v2.2.0

---

# Pepi v2.2.4

## Summary

Stability release for **2.2.x**: removes the FTDC feature from this line, bumps project version to **2.2.4**, and locks **Connections** and **Time Series** to consistent **raw-path** behavior with **sample-aware** cache keys to avoid cross-tab inconsistencies.

## Highlights

- Removed FTDC stack; version bumped to 2.2.2 then stabilized for **2.2.4** release tag.
- Analysis consistency across tabs: deterministic raw-path behavior for Connections and Time Series.
- Fixed **sample-sensitive** parser cache key collisions (connections/queries) so UI and API agree for a given sample percentage.

## Details

### Web UI

- Connections and Time Series behavior aligned with raw analysis path.

### HTTP API

- Sampling metadata / cache variant behavior aligned with parser sampling (see `pepi/parser.py` / cache at this tag).

### Performance / large logs

- Continued emphasis on large-log workflows alongside v2.2.0 preflight/banner work.

## Upgrade

- Editable install: `git pull` and `pip install -e .` from your clone.
- If you relied on FTDC in a v2.0.0-era build, plan migration off that feature path for this line.

## Breaking changes

- **FTDC** feature removed relative to earlier v2.0.0-era direction.

## Compatibility

- Python **≥3.8**; standard MongoDB JSON diagnostic logs.

## Links

- Full diff: https://github.com/jenunes/pepi/compare/v2.2.0...v2.2.4

---

# Pepi v2.2.5

## Summary

**Documentation** release at tag `v2.2.5`: README and web FAQ aligned with actual CLI, Web API, sampling, cache, ingest vs raw, and privacy behavior; version strings set to **2.2.5**. (`VERSIONING.md` and GitHub release-note policy landed on **`dev` in commits after this tag**—see branch history if you need those files at the same time as the tag.)

## Highlights

- README replaced with a single, code-grounded guide (CLI flags, API methods, sampling, cache, `main`/`dev` notes).
- Web FAQ expanded (privacy, performance, raw vs ingest, diagnostics, troubleshooting, TMP, tips).
- Bumped `pepi/version.py`, package metadata, and web footer to **2.2.5**.

## Details

### Docs

- README, `faq.html` (at this tag).

### CLI

- No intentional behavior changes in this tag (verify with `pepi/cli.py` at this revision).

### Web UI

- Footer / version display only as part of version bump (no feature churn in this single-doc commit).

## Upgrade

- Editable install: `git pull` and `pip install -e .` from your clone.
- Re-read README for corrected CLI examples (e.g. `--clear-cache` with `--fetch`, connection `sort-by` scope).

## Breaking changes

None.

## Compatibility

- Python **≥3.8** unchanged; log format unchanged.

## Links

- Full diff: https://github.com/jenunes/pepi/compare/v2.2.4...v2.2.5
