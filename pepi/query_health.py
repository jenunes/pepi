"""Weighted query health scores from aggregated log statistics."""

from __future__ import annotations

import logging
from typing import Any

from pepi.types import QueryHealthBreakdown

logger = logging.getLogger(__name__)

WEIGHT_PLAN = 0.25
WEIGHT_SCAN = 0.25
WEIGHT_KEY = 0.15
WEIGHT_SORT = 0.10
WEIGHT_LATENCY = 0.15
WEIGHT_DISK = 0.10


def get_health_severity(score: int) -> str:
    if score >= 80:
        return "HEALTHY"
    if score >= 50:
        return "WARNING"
    return "CRITICAL"


def _score_plan_type(indexes: list[str]) -> int:
    if not indexes:
        return 60
    if any(x == "COLLSCAN" for x in indexes):
        return 0
    if any(x == "IDHACK" or (isinstance(x, str) and "IDHACK" in x) for x in indexes):
        return 100
    if any(isinstance(x, str) and "IXSCAN" in x for x in indexes):
        return 80
    return 60


def _score_scan_ratio(scan_ratio: float, has_ratio_data: bool) -> int:
    if not has_ratio_data:
        return 50
    if scan_ratio <= 1.0:
        return 100
    if scan_ratio <= 10.0:
        return int(100 - (scan_ratio - 1.0) * (50.0 / 9.0))
    if scan_ratio <= 100.0:
        return int(50 - (scan_ratio - 10.0) * (30.0 / 90.0))
    if scan_ratio <= 1000.0:
        return int(20 - (scan_ratio - 100.0) * (20.0 / 900.0))
    return 0


def _score_key_efficiency(key_efficiency: float, has_key_data: bool) -> int:
    if not has_key_data:
        return 50
    if key_efficiency <= 1.0:
        return 100
    if key_efficiency <= 5.0:
        return int(100 - (key_efficiency - 1.0) * (50.0 / 4.0))
    if key_efficiency <= 50.0:
        return int(50 - (key_efficiency - 5.0) * (30.0 / 45.0))
    return 0


def _score_sort_pct(in_memory_sort_pct: float) -> int:
    if in_memory_sort_pct <= 0:
        return 100
    if in_memory_sort_pct >= 100:
        return 0
    return int(100 - in_memory_sort_pct)


def _score_latency_p95(p95_ms: float) -> int:
    if p95_ms < 10:
        return 100
    if p95_ms < 100:
        return 80
    if p95_ms < 1000:
        return 50
    return 20


def _score_disk_pct(disk_usage_pct: float) -> int:
    if disk_usage_pct <= 0:
        return 100
    if disk_usage_pct >= 100:
        return 0
    return int(100 - disk_usage_pct * 0.7)


def _apply_selectivity_multiplier(stats: dict[str, Any], base_total: int) -> int:
    pattern = stats.get("pattern") or ""
    operation = stats.get("operation") or "find"
    if not pattern:
        return base_total
    try:
        from pepi.index_advisor import (
            _analyze_selectivity,
            _extract_query_fields,
            _get_current_index_info,
        )

        fields = _extract_query_fields(pattern, operation)
        if not fields:
            return base_total
        query_field_types = {f: t for f, t in fields}
        current = _get_current_index_info(stats)
        structure = current.get("structure") or []
        analysis = _analyze_selectivity(query_field_types, stats, structure)
        mult = max(0.5, min(1.0, analysis.get("selectivity_score", 100) / 100.0))
        return int(max(0, min(100, round(base_total * mult))))
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("selectivity adjustment skipped: %s", exc)
        return base_total


def calculate_health_score(stats: dict[str, Any]) -> QueryHealthBreakdown:
    indexes = stats.get("indexes", [])
    if isinstance(indexes, set):
        indexes = list(indexes)

    nreturned = stats.get("nreturned") or []
    docs_ex = stats.get("docsExamined") or []
    has_ratio_data = bool(nreturned) and any(int(x) > 0 for x in nreturned)
    scan_ratio = float(stats.get("scan_ratio", 0.0))

    keys_ex = stats.get("keysExamined") or []
    has_key_data = bool(docs_ex) and any(int(x) > 0 for x in docs_ex)
    key_efficiency = float(stats.get("key_efficiency", 0.0))

    in_mem_pct = float(stats.get("in_memory_sort_pct", 0.0))
    disk_pct = float(stats.get("disk_usage_pct", 0.0))
    p95 = float(stats.get("percentile_95", 0.0))

    plan_s = _score_plan_type(indexes)
    scan_s = _score_scan_ratio(scan_ratio, has_ratio_data)
    key_s = _score_key_efficiency(key_efficiency, has_key_data)
    sort_s = _score_sort_pct(in_mem_pct)
    lat_s = _score_latency_p95(p95)
    disk_s = _score_disk_pct(disk_pct)

    weighted = (
        WEIGHT_PLAN * plan_s
        + WEIGHT_SCAN * scan_s
        + WEIGHT_KEY * key_s
        + WEIGHT_SORT * sort_s
        + WEIGHT_LATENCY * lat_s
        + WEIGHT_DISK * disk_s
    )
    total = int(max(0, min(100, round(weighted))))
    total = _apply_selectivity_multiplier(stats, total)
    severity = get_health_severity(total)

    return QueryHealthBreakdown(
        plan_type_score=plan_s,
        scan_ratio_score=scan_s,
        key_efficiency_score=key_s,
        sort_score=sort_s,
        latency_score=lat_s,
        disk_score=disk_s,
        total=total,
        severity=severity,
    )
