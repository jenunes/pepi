from __future__ import annotations

import tempfile
from pathlib import Path

import pepi.cli as pepi_cli
import pepi.web_api as web_api


def test_tmp_health_endpoint(client) -> None:
    response = client.get("/api/system/tmp-health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert "tmp_dir" in body["data"]
    assert "free_bytes" in body["data"]


def test_upload_low_space_returns_structured_error(client, sample_log_file, monkeypatch) -> None:
    monkeypatch.setattr(web_api, "get_free_bytes", lambda _path: 0)
    with sample_log_file.open("rb") as handle:
        response = client.post(
            "/api/upload",
            files={"file": ("mongodb.log", handle, "text/plain")},
        )
    assert response.status_code == 507
    detail = response.json()["detail"]
    assert detail["error_code"] == "NO_SPACE_LEFT"
    assert "Free disk space" in detail["hint"]


def test_cleanup_stale_upload_files(tmp_path: Path) -> None:
    stale_file = tmp_path / "pepi_upload_stale.log"
    stale_file.write_text("x", encoding="utf-8")
    web_api.cleanup_stale_upload_files(str(tmp_path))
    # Default max age is 24h, fresh file should remain.
    assert stale_file.exists()


def test_cleanup_stale_port_files_for_dead_pid(tmp_path: Path, monkeypatch) -> None:
    marker = tmp_path / "pepi_port_999999.txt"
    marker.write_text("8000", encoding="utf-8")

    monkeypatch.setattr(web_api.tempfile, "gettempdir", lambda: str(tmp_path))
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))

    web_api.cleanup_stale_port_files()
    pepi_cli.cleanup_stale_port_files()
    assert not marker.exists()
