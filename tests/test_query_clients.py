from __future__ import annotations

import json
from pathlib import Path

from pepi.connection_log import extract_slow_query_app_name, extract_slow_query_client_ip
from pepi.parser import aggregate_query_clients, extract_query_pattern, parse_queries


def _slow_hello_line(ip: str, app_name: str = "app1") -> str:
    command = {"hello": 1, "helloOk": True, "$db": "sampledb"}
    return json.dumps(
        {
            "t": {"$date": "2026-07-14T12:00:10.230+00:00"},
            "c": "COMMAND",
            "msg": "Slow query",
            "attr": {
                "ns": "sampledb.$cmd",
                "appName": app_name,
                "command": command,
                "durationMillis": 100,
                "remote": f"{ip}:34374",
            },
        }
    )


def test_extract_slow_query_client_fields() -> None:
    entry = json.loads(_slow_hello_line("10.0.0.1"))
    assert extract_slow_query_client_ip(entry) == "10.0.0.1"
    assert extract_slow_query_app_name(entry) == "app1"


def test_aggregate_query_clients_ranks_ips(tmp_path: Path) -> None:
    file_path = tmp_path / "hello-storm.log"
    lines = [
        _slow_hello_line("10.0.0.1"),
        _slow_hello_line("10.0.0.1"),
        _slow_hello_line("10.0.0.1"),
        _slow_hello_line("10.0.0.2"),
    ]
    file_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    command = {"hello": 1, "helloOk": True, "$db": "sampledb"}
    pattern = extract_query_pattern("hello", command)

    result = aggregate_query_clients(
        str(file_path),
        "sampledb.$cmd",
        "hello",
        pattern,
        sample_percentage=100,
    )

    assert result["total_matched"] == 4
    assert result["has_remote_pct"] == 100.0
    assert len(result["clients"]) >= 2
    assert result["clients"][0]["ip"] == "10.0.0.1"
    assert result["clients"][0]["count"] == 3
    assert result["clients"][0]["pct"] == 75.0
    assert result["clients"][1]["ip"] == "10.0.0.2"


def test_parse_queries_stores_client_ip_counts(tmp_path: Path) -> None:
    file_path = tmp_path / "queries-clients.log"
    file_path.write_text(_slow_hello_line("10.0.0.8") + "\n", encoding="utf-8")

    command = {"hello": 1, "helloOk": True, "$db": "sampledb"}
    pattern = extract_query_pattern("hello", command)
    queries = parse_queries(str(file_path), sample_percentage=100)
    key = ("sampledb.$cmd", "hello", pattern)

    assert key in queries
    assert queries[key]["client_ip_counts"]["10.0.0.8"] == 1
    assert queries[key]["client_with_remote"] == 1


def test_query_diagnostics_api_returns_client_breakdown(client, tmp_path: Path) -> None:
    file_path = tmp_path / "hello-diag.log"
    lines = [
        _slow_hello_line("10.0.0.1"),
        _slow_hello_line("10.0.0.1"),
        _slow_hello_line("10.0.0.2"),
    ]
    file_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with file_path.open("rb") as handle:
        upload = client.post(
            "/api/upload",
            files={"file": ("hello-diag.log", handle, "text/plain")},
        )
    assert upload.status_code == 200
    file_id = upload.json()["file_id"]

    queries_resp = client.post(f"/api/analyze/{file_id}/queries?sample=100")
    queries = queries_resp.json()["data"]["queries"]
    hello_row = next(q for q in queries if q["operation"] == "hello")

    diag_resp = client.post(
        f"/api/analyze/{file_id}/query-diagnostics?sample=100",
        json={
            "namespace": hello_row["namespace"],
            "operation": hello_row["operation"],
            "pattern": hello_row["pattern"],
        },
    )
    assert diag_resp.status_code == 200
    diag = diag_resp.json()["data"]
    assert len(diag["client_breakdown"]) >= 2
    assert diag["client_breakdown"][0]["ip"] == "10.0.0.1"
    assert diag["client_breakdown"][0]["count"] == 2
    assert diag["client_breakdown_meta"]["has_remote_pct"] == 100.0

    client.delete(f"/api/files/{file_id}")
