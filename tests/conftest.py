from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from pepi.web_api import app


def _log_line(payload: dict) -> str:
    return json.dumps(payload) + "\n"


@pytest.fixture
def sample_log_file(tmp_path: Path) -> Path:
    file_path = tmp_path / "mongod.log.2026-03-06T21-30-43"
    lines = [
        _log_line(
            {
                "t": {"$date": "2026-03-06T21:30:00.000Z"},
                "c": "CONTROL",
                "msg": "Operating System",
                "attr": {"os": {"name": "Linux", "version": "6.1.0"}},
            }
        ),
        _log_line(
            {
                "t": {"$date": "2026-03-06T21:30:01.000Z"},
                "c": "CONTROL",
                "msg": "Build Info",
                "attr": {"buildInfo": {"version": "7.0.0"}},
            }
        ),
        _log_line(
            {
                "t": {"$date": "2026-03-06T21:30:02.000Z"},
                "c": "CONTROL",
                "msg": "Options set by command line",
                "attr": {"options": {"replSet": "rs0"}},
            }
        ),
        _log_line(
            {
                "t": {"$date": "2026-03-06T21:30:03.000Z"},
                "c": "NETWORK",
                "msg": "Connection accepted",
                "attr": {"remote": "127.0.0.1:50000", "connectionId": 1},
            }
        ),
        _log_line(
            {
                "t": {"$date": "2026-03-06T21:30:04.000Z"},
                "c": "COMMAND",
                "msg": "Slow query",
                "attr": {
                    "ns": "test.users",
                    "durationMillis": 240,
                    "planSummary": "COLLSCAN",
                    "command": {"find": "users", "filter": {"status": "A"}},
                    "keysExamined": 0,
                    "docsExamined": 15000,
                    "nreturned": 3,
                    "hasSortStage": False,
                    "usedDisk": False,
                    "numYields": 12,
                    "reslen": 450,
                },
            }
        ),
        _log_line(
            {
                "t": {"$date": "2026-03-06T21:30:04.500Z"},
                "c": "COMMAND",
                "msg": "Slow query",
                "attr": {
                    "ns": "test.orders",
                    "durationMillis": 15,
                    "planSummary": "IXSCAN { customer_id: 1 }",
                    "command": {"find": "orders", "filter": {"customer_id": 42}},
                    "keysExamined": 5,
                    "docsExamined": 5,
                    "nreturned": 5,
                    "hasSortStage": False,
                    "usedDisk": False,
                    "numYields": 0,
                    "reslen": 200,
                },
            }
        ),
        _log_line(
            {
                "t": {"$date": "2026-03-06T21:30:05.000Z"},
                "c": "NETWORK",
                "msg": "Connection ended",
                "attr": {"remote": "127.0.0.1:50000", "connectionId": 1},
            }
        ),
        _log_line(
            {
                "t": {"$date": "2026-03-06T21:31:00.000Z"},
                "c": "REPL",
                "msg": "New replica set config in use",
                "attr": {
                    "config": {
                        "_id": "rs0",
                        "members": [{"_id": 0, "host": "localhost:27017"}],
                    }
                },
            }
        ),
    ]
    file_path.write_text("".join(lines), encoding="utf-8")
    return file_path


@pytest.fixture
def client() -> TestClient:
    with TestClient(app) as test_client:
        yield test_client
