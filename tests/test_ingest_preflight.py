from __future__ import annotations

import time


def test_preflight_endpoint_available(client, sample_log_file) -> None:
    with sample_log_file.open("rb") as f:
        upload_response = client.post(
            "/api/upload",
            files={"file": ("mongod.log.2026-03-06T21-30-43", f, "text/plain")},
        )
    assert upload_response.status_code == 200
    file_id = upload_response.json()["file_id"]

    preflight = client.get(f"/api/files/{file_id}/preflight")
    assert preflight.status_code == 200
    payload = preflight.json()
    assert payload["status"] == "success"
    assert payload["data"]["tier"] in {"ok", "warning", "confirm", "block"}
    assert "trim the file" in payload["data"]["message"].lower()


def test_ingest_start_and_ingest_source_paths(client, sample_log_file) -> None:
    with sample_log_file.open("rb") as f:
        upload_response = client.post(
            "/api/upload",
            files={"file": ("mongod.log.2026-03-06T21-30-43", f, "text/plain")},
        )
    file_id = upload_response.json()["file_id"]

    start = client.post(f"/api/ingest/{file_id}/start")
    assert start.status_code == 200

    for _ in range(30):
        status = client.get(f"/api/ingest/{file_id}/status")
        assert status.status_code == 200
        state = status.json()["data"]["status"]
        if state in {"completed", "failed", "cancelled"}:
            break
        time.sleep(0.05)

    conn_result = client.post(f"/api/analyze/{file_id}/connections?source=ingest")
    assert conn_result.status_code == 200
    assert conn_result.json()["status"] == "success"

    ts_result = client.post(f"/api/analyze/{file_id}/timeseries?source=ingest")
    assert ts_result.status_code == 200
    assert ts_result.json()["status"] == "success"

    extract_result = client.post(
        f"/api/analyze/{file_id}/extract?source=ingest",
        json={"text_search": "Connection", "limit": 10},
    )
    assert extract_result.status_code == 200
    assert "lines" in extract_result.json()
