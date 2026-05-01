from __future__ import annotations

from pepi.query_health import calculate_health_score, get_health_severity


def test_health_score_collscan_is_critical() -> None:
    stats = {
        "indexes": ["COLLSCAN"],
        "scan_ratio": 5000.0,
        "key_efficiency": 0.0,
        "in_memory_sort_pct": 0.0,
        "disk_usage_pct": 0.0,
        "percentile_95": 350.0,
        "nreturned": [3],
        "docsExamined": [15000],
        "keysExamined": [0],
    }
    result = calculate_health_score(stats)
    assert result.total < 50
    assert result.severity == "CRITICAL"
    assert result.plan_type_score == 0


def test_health_score_ixscan_healthy() -> None:
    stats = {
        "indexes": ["IXSCAN { customer_id: 1 }"],
        "scan_ratio": 1.0,
        "key_efficiency": 1.0,
        "in_memory_sort_pct": 0.0,
        "disk_usage_pct": 0.0,
        "percentile_95": 8.0,
        "nreturned": [5],
        "docsExamined": [5],
        "keysExamined": [5],
    }
    result = calculate_health_score(stats)
    assert result.total >= 80
    assert result.severity == "HEALTHY"


def test_health_score_handles_missing_fields() -> None:
    stats = {
        "indexes": ["COLLSCAN"],
        "scan_ratio": 0.0,
        "key_efficiency": 0.0,
        "in_memory_sort_pct": 0.0,
        "disk_usage_pct": 0.0,
        "percentile_95": 50.0,
        "nreturned": [],
        "docsExamined": [],
        "keysExamined": [],
    }
    result = calculate_health_score(stats)
    assert 0 <= result.total <= 100
    assert result.scan_ratio_score == 50
    assert result.key_efficiency_score == 50


def test_health_severity_thresholds() -> None:
    assert get_health_severity(100) == "HEALTHY"
    assert get_health_severity(80) == "HEALTHY"
    assert get_health_severity(79) == "WARNING"
    assert get_health_severity(50) == "WARNING"
    assert get_health_severity(49) == "CRITICAL"
    assert get_health_severity(0) == "CRITICAL"
