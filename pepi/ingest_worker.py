from __future__ import annotations

import hashlib
import json
import threading
import time
from datetime import datetime
from typing import Any

from .ingest_store import delete_file_ingest_data, upsert_job


def _bucket_minute(ts_value: str | None) -> str | None:
    if not ts_value:
        return None
    try:
        normalized = ts_value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        return dt.replace(second=0, microsecond=0).isoformat()
    except ValueError:
        return None


def _extract_ts(entry: dict[str, Any]) -> str | None:
    return entry.get("t", {}).get("$date")


def _extract_operation(entry: dict[str, Any]) -> str | None:
    command = entry.get("attr", {}).get("command", {})
    if isinstance(command, dict):
        for key in command.keys():
            if key not in {"$db", "lsid", "$clusterTime", "$readPreference"}:
                return str(key)
    return None


def _extract_namespace(entry: dict[str, Any]) -> str | None:
    attr = entry.get("attr", {})
    return attr.get("ns") or attr.get("namespace")


def _extract_message(entry: dict[str, Any]) -> str:
    return entry.get("msg") or ""


def _event_type(entry: dict[str, Any], message: str, duration_ms: int) -> str:
    if duration_ms > 100:
        return "slow_query"
    if entry.get("s") in {"E", "F"}:
        return "error"
    lowered = message.lower()
    if "connection accepted" in lowered:
        return "connection_open"
    if "end connection" in lowered:
        return "connection_close"
    return "generic"


def run_ingest_job(
    *,
    conn,
    file_id: str,
    file_path: str,
    job_id: str,
    cancel_event: threading.Event,
) -> None:
    start_ts = time.time()
    upsert_job(
        conn,
        {
            "job_id": job_id,
            "file_id": file_id,
            "status": "running",
            "bytes_processed": 0,
            "lines_processed": 0,
            "started_at": start_ts,
            "finished_at": None,
            "error_message": None,
        },
    )
    delete_file_ingest_data(conn, file_id)
    lines_processed = 0
    bytes_processed = 0
    conn_deltas: dict[str, int] = {}
    query_buckets: dict[tuple[str, str, str], list[float]] = {}

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as handle:
            for raw_line in handle:
                if cancel_event.is_set():
                    upsert_job(
                        conn,
                        {
                            "job_id": job_id,
                            "file_id": file_id,
                            "status": "cancelled",
                            "bytes_processed": bytes_processed,
                            "lines_processed": lines_processed,
                            "started_at": start_ts,
                            "finished_at": time.time(),
                            "error_message": None,
                        },
                    )
                    return

                lines_processed += 1
                bytes_processed += len(raw_line.encode("utf-8", errors="ignore"))
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    continue

                ts = _extract_ts(entry)
                message = _extract_message(entry)
                duration_ms = int(entry.get("attr", {}).get("durationMillis", 0) or 0)
                namespace = _extract_namespace(entry)
                operation = _extract_operation(entry)
                evt = _event_type(entry, message, duration_ms)
                component = entry.get("c")
                severity = entry.get("s")

                conn.execute(
                    """
                    INSERT INTO log_events (
                        file_id, line_no, ts, component, severity, namespace, operation, event_type, duration_ms, message, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        file_id,
                        lines_processed,
                        ts,
                        component,
                        severity,
                        namespace,
                        operation,
                        evt,
                        duration_ms,
                        message,
                        line,
                    ),
                )

                bucket_ts = _bucket_minute(ts)
                if evt == "slow_query" and bucket_ts:
                    conn.execute(
                        """
                        INSERT INTO timeseries_agg (file_id, bucket_ts, metric, namespace, value)
                        VALUES (?, ?, 'slow_queries', ?, 1)
                        """,
                        (file_id, bucket_ts, namespace),
                    )
                if evt == "error" and bucket_ts:
                    conn.execute(
                        """
                        INSERT INTO timeseries_agg (file_id, bucket_ts, metric, namespace, value)
                        VALUES (?, ?, 'errors', ?, 1)
                        """,
                        (file_id, bucket_ts, namespace),
                    )
                if evt in {"connection_open", "connection_close"} and bucket_ts:
                    delta = 1 if evt == "connection_open" else -1
                    conn_deltas[bucket_ts] = conn_deltas.get(bucket_ts, 0) + delta
                    ip = entry.get("attr", {}).get("remote")
                    conn.execute(
                        """
                        INSERT INTO connection_events (file_id, ts, ip, event, connection_id, duration_s)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            file_id,
                            ts,
                            ip,
                            "open" if evt == "connection_open" else "close",
                            entry.get("attr", {}).get("connectionId"),
                            None,
                        ),
                    )
                if evt == "slow_query" and namespace and operation:
                    command = entry.get("attr", {}).get("command", {})
                    pattern_json = json.dumps(command, sort_keys=True, default=str)
                    key = (namespace, operation, pattern_json)
                    query_buckets.setdefault(key, []).append(float(duration_ms))

                if lines_processed % 1000 == 0:
                    upsert_job(
                        conn,
                        {
                            "job_id": job_id,
                            "file_id": file_id,
                            "status": "running",
                            "bytes_processed": bytes_processed,
                            "lines_processed": lines_processed,
                            "started_at": start_ts,
                            "finished_at": None,
                            "error_message": None,
                        },
                    )
                    conn.commit()

        for bucket_ts, value in conn_deltas.items():
            conn.execute(
                """
                INSERT INTO timeseries_agg (file_id, bucket_ts, metric, namespace, value)
                VALUES (?, ?, 'connections_delta', NULL, ?)
                """,
                (file_id, bucket_ts, value),
            )

        for (namespace, operation, pattern_json), durations in query_buckets.items():
            sorted_durations = sorted(durations)
            p95_idx = max(0, min(len(sorted_durations) - 1, int(0.95 * (len(sorted_durations) - 1))))
            pattern_hash = hashlib.sha256(pattern_json.encode("utf-8")).hexdigest()
            conn.execute(
                """
                INSERT INTO query_patterns (
                    file_id, namespace, operation, pattern_hash, pattern_json, count, min_ms, max_ms, sum_ms, p95_ms, indexes_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    file_id,
                    namespace,
                    operation,
                    pattern_hash,
                    pattern_json,
                    len(durations),
                    min(durations),
                    max(durations),
                    sum(durations),
                    sorted_durations[p95_idx],
                    "[]",
                ),
            )

        conn.commit()
        upsert_job(
            conn,
            {
                "job_id": job_id,
                "file_id": file_id,
                "status": "completed",
                "bytes_processed": bytes_processed,
                "lines_processed": lines_processed,
                "started_at": start_ts,
                "finished_at": time.time(),
                "error_message": None,
            },
        )
    except Exception as exc:
        upsert_job(
            conn,
            {
                "job_id": job_id,
                "file_id": file_id,
                "status": "failed",
                "bytes_processed": bytes_processed,
                "lines_processed": lines_processed,
                "started_at": start_ts,
                "finished_at": time.time(),
                "error_message": str(exc),
            },
        )
        raise
