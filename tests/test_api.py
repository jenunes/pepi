from __future__ import annotations


def test_upload_accepts_rotated_filename(client, sample_log_file) -> None:
    with sample_log_file.open("rb") as f:
        response = client.post(
            "/api/upload",
            files={"file": ("mongod.log.2026-03-06T21-30-43", f, "text/plain")},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["file_id"]
    assert body["lines"] >= 0


def test_basic_analysis_endpoint(client, sample_log_file) -> None:
    with sample_log_file.open("rb") as f:
        upload_response = client.post(
            "/api/upload",
            files={"file": ("mongod.log.2026-03-06T21-30-43", f, "text/plain")},
        )
    file_id = upload_response.json()["file_id"]

    response = client.post(f"/api/analyze/{file_id}/basic")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "success"
    assert payload["data"]["start_date"] is not None
    assert payload["data"]["end_date"] is not None

    cleanup = client.delete(f"/api/files/{file_id}")
    assert cleanup.status_code == 200


def _upload(client, sample_log_file) -> str:
    with sample_log_file.open("rb") as f:
        resp = client.post(
            "/api/upload",
            files={"file": ("mongod.log.2026-03-06T21-30-43", f, "text/plain")},
        )
    assert resp.status_code == 200
    return resp.json()["file_id"]


def test_queries_endpoint_returns_summary(client, sample_log_file) -> None:
    file_id = _upload(client, sample_log_file)
    resp = client.post(f"/api/analyze/{file_id}/queries")
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert "summary" in data
    assert "overall_health_score" in data["summary"]
    assert "health_distribution" in data["summary"]
    assert data["total_patterns"] >= 1


def test_queries_endpoint_returns_health_scores(client, sample_log_file) -> None:
    file_id = _upload(client, sample_log_file)
    resp = client.post(f"/api/analyze/{file_id}/queries")
    data = resp.json()["data"]
    for q in data["queries"]:
        assert "health_score" in q
        assert 0 <= q["health_score"] <= 100
        assert q["health_severity"] in ("HEALTHY", "WARNING", "CRITICAL")


def test_query_diagnostics_endpoint(client, sample_log_file) -> None:
    file_id = _upload(client, sample_log_file)
    queries_resp = client.post(f"/api/analyze/{file_id}/queries")
    queries = queries_resp.json()["data"]["queries"]
    assert len(queries) >= 1
    q = queries[0]
    diag_resp = client.post(
        f"/api/analyze/{file_id}/query-diagnostics",
        json={"namespace": q["namespace"], "operation": q["operation"], "pattern": q["pattern"]},
    )
    assert diag_resp.status_code == 200
    diag = diag_resp.json()["data"]
    assert "health" in diag
    assert "findings" in diag
    assert "exec_stats" in diag
    assert "client_breakdown" in diag
    assert "client_breakdown_meta" in diag
    assert 0 <= diag["health"]["total"] <= 100


def test_queries_endpoint_includes_top_client_fields(client, tmp_path) -> None:
    file_path = tmp_path / "hello-top-client.log"
    import json

    cmd = {"hello": 1, "helloOk": True, "$db": "sampledb"}
    lines = [
        json.dumps(
            {
                "c": "COMMAND",
                "msg": "Slow query",
                "attr": {
                    "ns": "sampledb.$cmd",
                    "appName": "app1",
                    "command": cmd,
                    "durationMillis": 100,
                    "remote": "10.0.0.1:34374",
                },
            }
        ),
        json.dumps(
            {
                "c": "COMMAND",
                "msg": "Slow query",
                "attr": {
                    "ns": "sampledb.$cmd",
                    "appName": "app1",
                    "command": cmd,
                    "durationMillis": 120,
                    "remote": "10.0.0.2:34375",
                },
            }
        ),
    ]
    file_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with file_path.open("rb") as handle:
        upload = client.post(
            "/api/upload",
            files={"file": ("hello-top-client.log", handle, "text/plain")},
        )
    file_id = upload.json()["file_id"]
    resp = client.post(f"/api/analyze/{file_id}/queries?sample=100")
    hello = next(q for q in resp.json()["data"]["queries"] if q["operation"] == "hello")
    assert hello["top_client_ip"] == "10.0.0.1"
    assert hello["top_client_count"] == 1
    assert hello["top_client_pct"] == 50.0
    client.delete(f"/api/files/{file_id}")
