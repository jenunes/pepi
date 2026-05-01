"""Rule-based query diagnostics findings from log-derived statistics."""

from __future__ import annotations

from typing import Any

from pepi.types import QueryFinding


def _indexes_list(stats: dict[str, Any]) -> list[str]:
    idx = stats.get("indexes", [])
    if isinstance(idx, set):
        return list(idx)
    return list(idx) if idx else []


def _check_plan_findings(stats: dict[str, Any]) -> list[QueryFinding]:
    findings: list[QueryFinding] = []
    indexes = _indexes_list(stats)
    count = int(stats.get("count", 0))

    if "COLLSCAN" in indexes and count > 5:
        findings.append(
            QueryFinding(
                severity="critical",
                category="execution_plan",
                title="Collection scan (COLLSCAN)",
                detail=f"Pattern executed {count} times using a collection scan.",
                recommendation="Add an index matching filter and sort fields; verify with explain('executionStats').",
            )
        )
    elif "COLLSCAN" in indexes:
        findings.append(
            QueryFinding(
                severity="warning",
                category="execution_plan",
                title="Collection scan (COLLSCAN)",
                detail="Query used a collection scan.",
                recommendation="Review filters and add a supporting index if this pattern grows.",
            )
        )

    in_mem_pct = float(stats.get("in_memory_sort_pct", 0.0))
    if in_mem_pct > 0:
        findings.append(
            QueryFinding(
                severity="warning" if in_mem_pct < 50 else "critical",
                category="execution_plan",
                title="In-memory sort stage",
                detail=f"hasSortStage reported on {in_mem_pct:.0f}% of executions.",
                recommendation="Add an index that covers the sort keys (equality fields first, then sort per ESR).",
            )
        )

    scan_ratio = float(stats.get("scan_ratio", 0.0))
    nret = stats.get("nreturned") or []
    if nret and any(int(x) > 0 for x in nret) and scan_ratio > 50:
        findings.append(
            QueryFinding(
                severity="warning",
                category="execution_plan",
                title="High documents examined per returned document",
                detail=f"Mean docsExamined/nreturned ratio is {scan_ratio:.1f}.",
                recommendation="Tighten filters, add selective indexes, or reduce working set.",
            )
        )

    key_eff = float(stats.get("key_efficiency", 0.0))
    docs_ex = stats.get("docsExamined") or []
    if docs_ex and any(int(x) > 0 for x in docs_ex) and key_eff > 5:
        findings.append(
            QueryFinding(
                severity="warning",
                category="execution_plan",
                title="High keys examined relative to documents",
                detail=f"Mean keysExamined/docsExamined is {key_eff:.1f}.",
                recommendation="Review index prefix order and filter alignment with the index.",
            )
        )

    disk_pct = float(stats.get("disk_usage_pct", 0.0))
    if disk_pct > 0:
        findings.append(
            QueryFinding(
                severity="critical" if disk_pct > 25 else "warning",
                category="execution_plan",
                title="Disk spill (usedDisk)",
                detail=f"usedDisk reported on {disk_pct:.0f}% of executions.",
                recommendation="Reduce sort/group memory footprint, add indexes, or increase allowDiskUse only after sizing.",
            )
        )

    return findings


def _check_performance_findings(stats: dict[str, Any]) -> list[QueryFinding]:
    findings: list[QueryFinding] = []
    p95 = float(stats.get("percentile_95", 0.0))
    mean_ms = float(stats.get("mean", 0.0))
    count = int(stats.get("count", 0))

    if p95 > 1000:
        findings.append(
            QueryFinding(
                severity="critical",
                category="performance",
                title="High P95 latency",
                detail=f"95th percentile duration is {p95:.0f} ms.",
                recommendation="Profile with execution stats; check indexes, cardinality, and result size.",
            )
        )
    elif p95 > 100:
        findings.append(
            QueryFinding(
                severity="warning",
                category="performance",
                title="Elevated P95 latency",
                detail=f"95th percentile duration is {p95:.0f} ms.",
                recommendation="Review query shape and indexes for this pattern.",
            )
        )

    if mean_ms > 200 and count > 20:
        findings.append(
            QueryFinding(
                severity="warning",
                category="performance",
                title="Sustained latency with high volume",
                detail=f"Mean {mean_ms:.0f} ms over {count} executions.",
                recommendation="Prioritize indexing or query rewrite; total time impact is significant.",
            )
        )

    yield_rate = float(stats.get("yield_rate", 0.0))
    if yield_rate > 50:
        findings.append(
            QueryFinding(
                severity="info",
                category="performance",
                title="Frequent yields",
                detail=f"Average numYields per execution is {yield_rate:.1f}.",
                recommendation="May indicate lock contention or long-running ops; correlate with concurrent workload.",
            )
        )

    avg_res = float(stats.get("avg_response_size", 0.0))
    if avg_res > 1_000_000:
        findings.append(
            QueryFinding(
                severity="warning",
                category="performance",
                title="Large response payloads",
                detail=f"Average response size ~{avg_res / 1_000_000:.1f} MB.",
                recommendation="Use projection, pagination, or smaller working sets.",
            )
        )

    return findings


def _check_schema_findings(stats: dict[str, Any]) -> list[QueryFinding]:
    findings: list[QueryFinding] = []
    if stats.get("allowDiskUse"):
        findings.append(
            QueryFinding(
                severity="info",
                category="schema",
                title="allowDiskUse enabled",
                detail="Aggregation may use disk for large sorts/group stages.",
                recommendation="Prefer indexes and bounded pipelines; size RAM vs spill risk.",
            )
        )

    pattern = stats.get("pattern") or ""
    if "$regex" in pattern and "?" not in pattern[:20]:
        findings.append(
            QueryFinding(
                severity="warning",
                category="schema",
                title="Regex filter detected",
                detail="Regex predicates are often non-selective for indexes.",
                recommendation="Anchor regex where possible or use text index / dedicated search.",
            )
        )

    if '"$in"' in pattern or "'$in'" in pattern:
        findings.append(
            QueryFinding(
                severity="info",
                category="schema",
                title="$in operator present",
                detail="Large $in arrays behave like range predicates for index planning.",
                recommendation="Keep $in lists small or split queries; see MongoDB ESR guidance.",
            )
        )

    if pattern in ("{}", "null", "[]"):
        findings.append(
            QueryFinding(
                severity="warning",
                category="schema",
                title="Broad or empty filter pattern",
                detail="Normalized pattern suggests little filter selectivity.",
                recommendation="Confirm filters in raw examples; avoid unbounded scans on large collections.",
            )
        )

    return findings


def generate_findings(stats: dict[str, Any]) -> list[QueryFinding]:
    out: list[QueryFinding] = []
    out.extend(_check_plan_findings(stats))
    out.extend(_check_performance_findings(stats))
    out.extend(_check_schema_findings(stats))
    return out


def aggregate_top_findings(per_pattern: list[list[QueryFinding]], max_items: int = 20) -> list[QueryFinding]:
    """Flatten and cap cross-pattern findings for summary (dedupe by title)."""
    seen: set[str] = set()
    merged: list[QueryFinding] = []
    for group in per_pattern:
        for f in group:
            key = f.title
            if key in seen:
                continue
            seen.add(key)
            merged.append(f)
            if len(merged) >= max_items:
                return merged
    return merged
