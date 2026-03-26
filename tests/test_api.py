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
    assert body["lines"] > 0


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
