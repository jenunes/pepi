from __future__ import annotations

import json
from pathlib import Path

import pytest

from pepi.index_advisor import (
    _build_esr_breakdown,
    _generate_explain_command,
    analyze_queries,
)
from pepi.parser import extract_query_pattern, parse_queries
from pepi.stats import calculate_query_stats


def test_extract_query_pattern_find_shape() -> None:
    cmd = {
        "find": "orders",
        "filter": {"status": "ok"},
        "sort": {"createdAt": -1},
        "limit": 10,
        "projection": {"_id": 0, "x": 1},
    }
    pat = extract_query_pattern("find", cmd)
    d = json.loads(pat)
    assert d["has_limit"] is True
    assert d["has_skip"] is False
    assert d["sort"] == {"createdAt": "?"}
    assert d["filter"] == {"status": "?"}


def test_extract_query_pattern_aggregate_pipeline_shape() -> None:
    cmd = {
        "aggregate": "orders",
        "pipeline": [
            {"$match": {"a": 1}},
            {"$sort": {"b": -1}},
            {"$lookup": {"from": "other", "localField": "x", "foreignField": "y", "as": "z"}},
        ],
    }
    pat = extract_query_pattern("aggregate", cmd)
    stages = json.loads(pat)
    assert len(stages) == 3
    assert "$match" in stages[0]
    assert "$sort" in stages[1]
    assert "$lookup" in stages[2]


def test_parse_queries_repr_command_exec_stats(tmp_path: Path) -> None:
    line = json.dumps(
        {
            "c": "COMMAND",
            "msg": "Slow query",
            "attr": {
                "ns": "test.orders",
                "durationMillis": 50,
                "planSummary": "COLLSCAN",
                "docsExamined": 1000,
                "keysExamined": 100,
                "nreturned": 5,
                "planningTimeMicros": 200,
                "command": {
                    "find": "orders",
                    "filter": {"a": 1},
                    "sort": {"b": -1},
                },
            },
        }
    )
    log = tmp_path / "q.log"
    log.write_text(line + "\n", encoding="utf-8")
    raw = parse_queries(str(log))
    stats = calculate_query_stats(raw)
    key = next(iter(stats))
    s = stats[key]
    assert s["repr_command"] is not None
    assert s["repr_command"].get("find") == "orders"
    assert s["avg_docs_examined"] == 1000
    assert s["avg_n_returned"] == 5
    assert s["fetch_efficiency"] == pytest.approx(200)
    assert s["scan_efficiency"] == pytest.approx(0.1)
    assert s["sort_shape"]


def test_calculate_query_stats_shape_facets_find(tmp_path: Path) -> None:
    cmd = {"find": "c", "filter": {"x": 1}, "sort": {"y": 1}}
    pat = extract_query_pattern("find", cmd)
    raw = {
        ("db.col", "find", pat): {
            "count": 1,
            "durations": [10],
            "allowDiskUse": False,
            "operations": ["find"],
            "pattern": pat,
            "indexes": ["COLLSCAN"],
            "repr_command": None,
            "docs_examined": [],
            "keys_examined": [],
            "n_returned": [],
            "planning_micros": [],
        }
    }
    stats = calculate_query_stats(raw)
    s = stats[("db.col", "find", pat)]
    assert '"y"' in s["sort_shape"] or "y" in s["sort_shape"]


def test_needs_index_high_fetch_ratio() -> None:
    from pepi.index_advisor import _needs_index

    assert _needs_index(
        {
            "indexes": {"IXSCAN { x: 1 }"},
            "mean": 1,
            "count": 10,
            "avg_docs_examined": 5000,
            "avg_n_returned": 10,
        }
    )


def test_analyze_queries_repr_includes_sort_and_explain() -> None:
    pat = json.dumps(
        {
            "filter": {"x": "?"},
            "sort": {},
            "projection": {},
            "has_limit": False,
            "has_skip": False,
        },
        sort_keys=True,
    )
    query_stats = {
        ("app.orders", "find", pat): {
            "count": 20,
            "mean": 120,
            "min": 100,
            "max": 150,
            "sum": 2400,
            "percentile_95": 140,
            "indexes": {"COLLSCAN"},
            "pattern": pat,
            "repr_command": {
                "find": "orders",
                "filter": {"x": 1},
                "sort": {"createdAt": -1},
            },
            "durations": [120] * 20,
            "allowDiskUse": False,
        }
    }
    recs = analyze_queries(query_stats)
    assert recs
    r0 = recs[0]
    assert "createdAt" in r0["recommendation"]["index_spec"]
    assert r0["recommendation"].get("explain_command")
    assert r0.get("esr_breakdown")
    assert any(e["field"] == "createdAt" for e in r0["esr_breakdown"])


def test_build_esr_breakdown_positions() -> None:
    fields = [("a", "equality"), ("b", "sort")]
    spec = {"a": 1, "b": 1}
    rows = _build_esr_breakdown(fields, spec)
    assert rows[0]["position_in_index"] == 1
    assert rows[1]["position_in_index"] == 2


def test_generate_explain_command_find() -> None:
    cmd = json.dumps(
        {
            "find": "orders",
            "filter": {"a": 1},
            "sort": {"b": -1},
            "limit": 5,
        }
    )
    ex = _generate_explain_command("test.orders", "find", cmd)
    assert 'explain("executionStats")' in ex
    assert "sort" in ex


def test_api_queries_includes_new_fields(client, tmp_path: Path) -> None:
    line = json.dumps(
        {
            "c": "COMMAND",
            "msg": "Slow query",
            "attr": {
                "ns": "test.items",
                "durationMillis": 80,
                "planSummary": "COLLSCAN",
                "docsExamined": 200,
                "nreturned": 2,
                "command": {
                    "find": "items",
                    "filter": {"sku": "x"},
                    "sort": {"price": 1},
                },
            },
        }
    )
    log = tmp_path / "oneq.log"
    log.write_text(line + "\n", encoding="utf-8")
    with log.open("rb") as f:
        upload_response = client.post(
            "/api/upload",
            files={"file": ("oneq.log", f, "text/plain")},
        )
    assert upload_response.status_code == 200
    file_id = upload_response.json()["file_id"]
    queries = client.post(f"/api/analyze/{file_id}/queries")
    assert queries.status_code == 200
    rows = queries.json()["data"]["queries"]
    assert rows
    row = rows[0]
    assert "sum_ms" in row
    assert "sort_shape" in row
    assert row.get("fetch_efficiency") == pytest.approx(100)
    client.delete(f"/api/files/{file_id}")
