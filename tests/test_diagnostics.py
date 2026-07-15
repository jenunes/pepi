from __future__ import annotations

import json
from pathlib import Path

from pepi.parser import (
    parse_auth_failures,
    parse_collscan_trends,
    parse_errors_detail,
    parse_lock_contention,
    parse_repl_health,
)


def _line(payload: dict) -> str:
    return json.dumps(payload) + "\n"


def _build_diagnostics_log(tmp_path: Path) -> Path:
    file_path = tmp_path / "diagnostics.log"
    t0 = "2026-03-06T21:30:00.000Z"
    lines = [
        _line(
            {
                "t": {"$date": t0},
                "c": "NETWORK",
                "msg": "Connection accepted",
                "attr": {
                    "remote": "10.0.0.1:50231",
                    "connectionId": 10,
                    "connectionCount": 1,
                },
            }
        ),
        _line(
            {
                "t": {"$date": "2026-03-06T21:30:01.000Z"},
                "c": "COMMAND",
                "msg": "Slow query",
                "attr": {
                    "ns": "db.users",
                    "durationMillis": 240,
                    "planSummary": "COLLSCAN",
                    "command": {"find": "users", "filter": {"status": "A"}},
                },
            }
        ),
        _line(
            {
                "t": {"$date": "2026-03-06T21:30:02.000Z"},
                "c": "COMMAND",
                "msg": "Slow query",
                "attr": {
                    "ns": "db.users",
                    "durationMillis": 22,
                    "planSummary": "IXSCAN { status: 1 }",
                    "command": {"find": "users", "filter": {"status": "B"}},
                },
            }
        ),
        _line(
            {
                "t": {"$date": "2026-03-06T21:30:03.000Z"},
                "c": "REPL",
                "msg": "Replica set state transition",
                "attr": {"oldState": "SECONDARY", "newState": "PRIMARY"},
            }
        ),
        _line(
            {
                "t": {"$date": "2026-03-06T21:30:04.000Z"},
                "c": "REPL",
                "msg": "starting election",
                "attr": {
                    "reason": "priority takeover",
                    "durationMillis": 120,
                    "outcome": "won",
                },
            }
        ),
        _line(
            {
                "t": {"$date": "2026-03-06T21:30:05.000Z"},
                "c": "REPL",
                "msg": "Heartbeat timeout detected",
                "attr": {"member": "node2"},
            }
        ),
        _line(
            {
                "t": {"$date": "2026-03-06T21:30:06.000Z"},
                "c": "REPL",
                "msg": "Rollback detected",
                "attr": {"reason": "divergent history"},
            }
        ),
        _line(
            {
                "t": {"$date": "2026-03-06T21:30:07.000Z"},
                "c": "STORAGE",
                "msg": "flowControl is engaged",
                "attr": {"sustainerRate": 3},
            }
        ),
        _line(
            {
                "t": {"$date": "2026-03-06T21:30:08.000Z"},
                "c": "STORAGE",
                "msg": "Waiting for ticket",
                "attr": {"totalTickets": 128},
            }
        ),
        _line(
            {
                "t": {"$date": "2026-03-06T21:30:09.000Z"},
                "c": "STORAGE",
                "msg": "Checkpoint completed",
                "attr": {"durationMillis": 42},
            }
        ),
        _line(
            {
                "t": {"$date": "2026-03-06T21:30:10.000Z"},
                "c": "ACCESS",
                "s": "E",
                "msg": "Authentication failed",
                "attr": {"user": "app", "remote": "10.0.0.8:59120"},
            }
        ),
        _line(
            {
                "t": {"$date": "2026-03-06T21:30:11.000Z"},
                "c": "ACCESS",
                "s": "W",
                "msg": "Not authorized on db to execute command",
                "attr": {"principalName": "reporter", "client": "10.0.0.9:51000"},
            }
        ),
        _line(
            {
                "t": {"$date": "2026-03-06T21:30:12.000Z"},
                "c": "NETWORK",
                "msg": "client metadata",
                "attr": {
                    "remote": "10.0.0.8:59120",
                    "client": "conn10",
                    "doc": {
                        "driver": {"name": "PyMongo", "version": "4.8"},
                        "application": {"name": "svc"},
                        "os": {"name": "Linux", "version": "6.1"},
                    },
                },
            }
        ),
        _line(
            {
                "t": {"$date": "2026-03-06T21:30:13.000Z"},
                "c": "NETWORK",
                "msg": "Connection ended",
                "attr": {"remote": "10.0.0.1:50231", "connectionId": 10},
            }
        ),
        _line(
            {
                "t": {"$date": "2026-03-06T21:30:14.000Z"},
                "c": "COMMAND",
                "s": "E",
                "msg": "error during command execution",
                "attr": {"ns": "db.users"},
            }
        ),
    ]
    file_path.write_text("".join(lines), encoding="utf-8")
    return file_path


def test_parse_errors_detail(tmp_path: Path) -> None:
    log_file = _build_diagnostics_log(tmp_path)
    data = parse_errors_detail(str(log_file))

    assert data["total_errors"] >= 1
    assert data["errors_timeline"]
    assert "COMMAND" in data["errors_by_component"]


def test_parse_collscan_trends(tmp_path: Path) -> None:
    log_file = _build_diagnostics_log(tmp_path)
    data = parse_collscan_trends(str(log_file))

    assert data["total_collscans"] >= 1
    assert data["total_ixscans"] >= 1
    assert data["collscan_top_namespaces"]


def test_parse_repl_health(tmp_path: Path) -> None:
    log_file = _build_diagnostics_log(tmp_path)
    data = parse_repl_health(str(log_file))

    assert data["has_elections"] is True
    assert data["has_rollbacks"] is True
    assert data["stability_score"] < 100


def test_parse_lock_contention(tmp_path: Path) -> None:
    log_file = _build_diagnostics_log(tmp_path)
    data = parse_lock_contention(str(log_file))

    assert data["has_contention"] is True
    assert data["contention_total_by_type"].get("flowcontrol", 0) >= 1
    assert data["checkpoint_durations"]


def test_parse_auth_failures(tmp_path: Path) -> None:
    log_file = _build_diagnostics_log(tmp_path)
    data = parse_auth_failures(str(log_file))

    assert data["has_auth_failures"] is True
    assert data["auth_total_failures"] >= 2
    assert data["auth_by_type"].get("authn", 0) >= 1


def test_api_enriched_existing_tabs(client, tmp_path: Path) -> None:
    log_file = _build_diagnostics_log(tmp_path)

    with log_file.open("rb") as f:
        upload_response = client.post(
            "/api/upload",
            files={"file": ("mongod.log.2026-03-06T21-30-43", f, "text/plain")},
        )
    assert upload_response.status_code == 200
    file_id = upload_response.json()["file_id"]

    timeseries = client.post(f"/api/analyze/{file_id}/timeseries")
    assert timeseries.status_code == 200
    t_data = timeseries.json()["data"]
    assert "errors_timeline" in t_data
    assert "top_errors" in t_data

    queries = client.post(f"/api/analyze/{file_id}/queries")
    assert queries.status_code == 200
    q_data = queries.json()["data"]
    assert "collscan_timeline" in q_data
    assert "scan_ratio_timeline" in q_data

    replica_set = client.post(f"/api/analyze/{file_id}/replica-set")
    assert replica_set.status_code == 200
    rs_data = replica_set.json()["data"]
    assert "repl_events" in rs_data
    assert "stability_score" in rs_data

    connections = client.post(f"/api/analyze/{file_id}/connections")
    assert connections.status_code == 200
    c_data = connections.json()["data"]
    assert "contention_timeline" in c_data
    assert "has_contention" in c_data

    clients = client.post(f"/api/analyze/{file_id}/clients")
    assert clients.status_code == 200
    cl_data = clients.json()["data"]
    assert "auth_timeline" in cl_data
    assert "has_auth_failures" in cl_data

    cleanup = client.delete(f"/api/files/{file_id}")
    assert cleanup.status_code == 200
