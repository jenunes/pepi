"""Statistics calculations for MongoDB log analysis."""

from __future__ import annotations

import json
from typing import Any, Optional


def _mean_ratio(numerators: list[int], denominators: list[int]) -> float:
    """Mean of numerators[i]/denominators[i] for entries where denominator > 0."""
    if not numerators or not denominators:
        return 0.0
    n = min(len(numerators), len(denominators))
    if n == 0:
        return 0.0
    ratios: list[float] = []
    for i in range(n):
        d = int(denominators[i])
        if d <= 0:
            continue
        ratios.append(float(numerators[i]) / float(d))
    if not ratios:
        return 0.0
    return sum(ratios) / len(ratios)


def _bool_list_pct(values: list[bool]) -> float:
    if not values:
        return 0.0
    return 100.0 * sum(1 for v in values if v) / len(values)


def _query_metric_lists(query_info: dict[str, Any]) -> dict[str, list[Any]]:
    """Normalize camelCase (release tests) and snake_case (parser) metric keys."""
    return {
        "keys_examined": list(
            query_info.get("keysExamined")
            or query_info.get("keys_examined")
            or []
        ),
        "docs_examined": list(
            query_info.get("docsExamined")
            or query_info.get("docs_examined")
            or []
        ),
        "n_returned": list(
            query_info.get("nreturned")
            or query_info.get("n_returned")
            or []
        ),
        "has_sort_stage": list(
            query_info.get("hasSortStage")
            or query_info.get("has_sort_stage")
            or []
        ),
        "used_disk": list(query_info.get("usedDisk") or query_info.get("used_disk") or []),
        "num_yields": list(query_info.get("numYields") or query_info.get("num_yields") or []),
        "reslen": list(query_info.get("reslen") or []),
    }


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
        metrics = _query_metric_lists(query_info)

        sum_docs, avg_docs = _list_stats(metrics["docs_examined"])
        sum_keys, avg_keys = _list_stats(metrics["keys_examined"])
        sum_nr, avg_nr = _list_stats(metrics["n_returned"])
        sum_pm, avg_pm = _list_stats(query_info.get("planning_micros", []))

        scan_ratio = _mean_ratio(metrics["docs_examined"], metrics["n_returned"])
        key_efficiency = _mean_ratio(metrics["keys_examined"], metrics["docs_examined"])
        in_memory_sort_pct = _bool_list_pct(metrics["has_sort_stage"])
        disk_usage_pct = _bool_list_pct(metrics["used_disk"])
        yield_rate = (
            sum(metrics["num_yields"]) / len(metrics["num_yields"])
            if metrics["num_yields"]
            else 0.0
        )
        avg_response_size = (
            sum(metrics["reslen"]) / len(metrics["reslen"]) if metrics["reslen"] else 0.0
        )

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
            "scan_ratio": scan_ratio,
            "key_efficiency": key_efficiency,
            "in_memory_sort_pct": in_memory_sort_pct,
            "disk_usage_pct": disk_usage_pct,
            "yield_rate": yield_rate,
            "avg_response_size": avg_response_size,
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
