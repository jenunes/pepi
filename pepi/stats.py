"""Statistics calculations for MongoDB log analysis."""

from __future__ import annotations

import json
from typing import Any, Optional


def _shape_facets_from_pattern(operation: str, pattern: str) -> dict[str, Any]:
    """Derive display facets from normalized pattern string."""
    empty = {
        "sort_shape": "",
        "projection_shape": "",
        "has_limit": False,
        "has_skip": False,
        "aggregate_shape_summary": "",
    }
    if operation == "find":
        try:
            d = json.loads(pattern)
        except (json.JSONDecodeError, TypeError):
            return empty
        if not isinstance(d, dict):
            return empty
        sort_d = d.get("sort") or {}
        proj = d.get("projection") or {}
        return {
            "sort_shape": json.dumps(sort_d, sort_keys=True) if sort_d else "",
            "projection_shape": json.dumps(proj, sort_keys=True) if proj else "",
            "has_limit": bool(d.get("has_limit")),
            "has_skip": bool(d.get("has_skip")),
            "aggregate_shape_summary": "",
        }
    if operation == "aggregate":
        summary = pattern if len(pattern) <= 200 else pattern[:197] + "..."
        return {
            "sort_shape": "",
            "projection_shape": "",
            "has_limit": False,
            "has_skip": False,
            "aggregate_shape_summary": summary,
        }
    return empty


def _list_stats(raw: list[Any]) -> tuple[int, Optional[float]]:
    nums: list[int] = []
    for x in raw:
        if x is None:
            continue
        try:
            nums.append(int(x))
        except (TypeError, ValueError):
            continue
    if not nums:
        return 0, None
    return sum(nums), sum(nums) / len(nums)


def calculate_query_stats(queries_data: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Calculate query statistics including percentiles, grouped by pattern."""
    stats: dict[tuple[Any, ...], dict[str, Any]] = {}

    for group_key, query_info in queries_data.items():
        if not query_info["durations"]:
            continue

        durations = list(query_info["durations"])
        durations.sort()

        count = len(durations)
        min_duration = min(durations)
        max_duration = max(durations)
        sum_duration = sum(durations)
        mean_duration = sum_duration / count

        percentile_95 = durations[int(0.95 * count) - 1] if count > 0 else 0

        _, operation, pattern = group_key
        facets = _shape_facets_from_pattern(operation, pattern)

        sum_docs, avg_docs = _list_stats(query_info.get("docs_examined", []))
        sum_keys, avg_keys = _list_stats(query_info.get("keys_examined", []))
        sum_nr, avg_nr = _list_stats(query_info.get("n_returned", []))
        sum_pm, avg_pm = _list_stats(query_info.get("planning_micros", []))

        scan_eff: Optional[float] = None
        if avg_docs is not None and avg_docs > 0 and avg_keys is not None:
            scan_eff = avg_keys / avg_docs

        fetch_eff: Optional[float] = None
        if avg_nr is not None and avg_nr > 0 and avg_docs is not None:
            fetch_eff = avg_docs / avg_nr

        stats[group_key] = {
            "count": count,
            "min": min_duration,
            "max": max_duration,
            "sum": sum_duration,
            "mean": mean_duration,
            "percentile_95": percentile_95,
            "allowDiskUse": query_info["allowDiskUse"],
            "pattern": query_info["pattern"],
            "durations": query_info["durations"],
            "indexes": query_info["indexes"],
            "repr_command": query_info.get("repr_command"),
            "sort_shape": facets["sort_shape"],
            "projection_shape": facets["projection_shape"],
            "has_limit": facets["has_limit"],
            "has_skip": facets["has_skip"],
            "aggregate_shape_summary": facets["aggregate_shape_summary"],
            "sum_docs_examined": sum_docs,
            "sum_keys_examined": sum_keys,
            "sum_n_returned": sum_nr,
            "sum_planning_micros": sum_pm,
            "avg_docs_examined": avg_docs,
            "avg_keys_examined": avg_keys,
            "avg_n_returned": avg_nr,
            "avg_planning_micros": avg_pm,
            "scan_efficiency": scan_eff,
            "fetch_efficiency": fetch_eff,
            "exec_event_count": max(
                len(query_info.get("docs_examined", [])),
                len(query_info.get("keys_examined", [])),
                len(query_info.get("n_returned", [])),
                len(query_info.get("planning_micros", [])),
            ),
        }

    return stats


def calculate_connection_stats(
    connections_data: dict[str, dict[str, Any]],
) -> tuple[Optional[dict[str, Any]], dict[str, dict[str, Any]]]:
    """Calculate connection duration statistics."""
    all_durations = []
    for ip, conn_info in connections_data.items():
        all_durations.extend(conn_info["durations"])

    if not all_durations:
        return None, {}

    # Overall statistics
    overall_stats = {
        "avg": sum(all_durations) / len(all_durations),
        "min": min(all_durations),
        "max": max(all_durations),
    }

    # Per-IP statistics
    ip_stats = {}
    for ip, conn_info in connections_data.items():
        if conn_info["durations"]:
            ip_stats[ip] = {
                "avg": sum(conn_info["durations"]) / len(conn_info["durations"]),
                "min": min(conn_info["durations"]),
                "max": max(conn_info["durations"]),
            }

    return overall_stats, ip_stats
