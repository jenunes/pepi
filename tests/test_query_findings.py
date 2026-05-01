from __future__ import annotations

from pepi.query_findings import generate_findings


def test_findings_detects_collscan() -> None:
    stats = {
        "indexes": ["COLLSCAN"],
        "count": 100,
        "scan_ratio": 0.0,
        "key_efficiency": 0.0,
        "in_memory_sort_pct": 0.0,
        "disk_usage_pct": 0.0,
        "percentile_95": 50.0,
        "mean": 50.0,
        "yield_rate": 0.0,
        "avg_response_size": 0.0,
        "pattern": '{"status": "A"}',
        "nreturned": [],
        "docsExamined": [],
    }
    findings = generate_findings(stats)
    titles = [f.title for f in findings]
    assert any("COLLSCAN" in t for t in titles)
    collscan_f = next(f for f in findings if "COLLSCAN" in f.title)
    assert collscan_f.severity == "critical"


def test_findings_detects_high_scan_ratio() -> None:
    stats = {
        "indexes": ["IXSCAN"],
        "count": 10,
        "scan_ratio": 200.0,
        "key_efficiency": 1.0,
        "in_memory_sort_pct": 0.0,
        "disk_usage_pct": 0.0,
        "percentile_95": 50.0,
        "mean": 40.0,
        "yield_rate": 0.0,
        "avg_response_size": 0.0,
        "pattern": '{"x": 1}',
        "nreturned": [5],
        "docsExamined": [1000],
    }
    findings = generate_findings(stats)
    titles = [f.title for f in findings]
    assert any("documents examined" in t.lower() for t in titles)


def test_findings_detects_in_memory_sort() -> None:
    stats = {
        "indexes": ["IXSCAN"],
        "count": 10,
        "scan_ratio": 1.0,
        "key_efficiency": 1.0,
        "in_memory_sort_pct": 80.0,
        "disk_usage_pct": 0.0,
        "percentile_95": 30.0,
        "mean": 20.0,
        "yield_rate": 0.0,
        "avg_response_size": 0.0,
        "pattern": '{"y": 1}',
        "nreturned": [10],
        "docsExamined": [10],
    }
    findings = generate_findings(stats)
    titles = [f.title for f in findings]
    assert any("sort" in t.lower() for t in titles)


def test_findings_empty_for_healthy_query() -> None:
    stats = {
        "indexes": ["IXSCAN { _id: 1 }"],
        "count": 5,
        "scan_ratio": 1.0,
        "key_efficiency": 1.0,
        "in_memory_sort_pct": 0.0,
        "disk_usage_pct": 0.0,
        "percentile_95": 5.0,
        "mean": 3.0,
        "yield_rate": 0.0,
        "avg_response_size": 500.0,
        "allowDiskUse": False,
        "pattern": '{"_id": 1}',
        "nreturned": [],
        "docsExamined": [],
    }
    findings = generate_findings(stats)
    assert len(findings) == 0
