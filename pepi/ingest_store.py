from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any


def get_ingest_db_path() -> str:
    custom_path = os.environ.get("PEPI_INGEST_DB_PATH")
    if custom_path:
        return os.path.abspath(os.path.expanduser(custom_path))
    return str(Path.home() / ".pepi_cache" / "pepi_ingest.db")


def get_connection(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def bootstrap_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS ingest_jobs (
            job_id TEXT PRIMARY KEY,
            file_id TEXT NOT NULL,
            status TEXT NOT NULL,
            bytes_processed INTEGER DEFAULT 0,
            lines_processed INTEGER DEFAULT 0,
            started_at REAL NOT NULL,
            finished_at REAL,
            error_message TEXT
        );

        CREATE TABLE IF NOT EXISTS log_events (
            file_id TEXT NOT NULL,
            line_no INTEGER NOT NULL,
            ts TEXT,
            component TEXT,
            severity TEXT,
            namespace TEXT,
            operation TEXT,
            event_type TEXT,
            duration_ms INTEGER,
            message TEXT,
            raw_json TEXT
        );

        CREATE TABLE IF NOT EXISTS connection_events (
            file_id TEXT NOT NULL,
            ts TEXT,
            ip TEXT,
            event TEXT,
            connection_id INTEGER,
            duration_s REAL
        );

        CREATE TABLE IF NOT EXISTS query_patterns (
            file_id TEXT NOT NULL,
            namespace TEXT NOT NULL,
            operation TEXT NOT NULL,
            pattern_hash TEXT NOT NULL,
            pattern_json TEXT NOT NULL,
            count INTEGER NOT NULL,
            min_ms REAL NOT NULL,
            max_ms REAL NOT NULL,
            sum_ms REAL NOT NULL,
            p95_ms REAL NOT NULL,
            indexes_json TEXT NOT NULL,
            PRIMARY KEY (file_id, namespace, operation, pattern_hash)
        );

        CREATE TABLE IF NOT EXISTS timeseries_agg (
            file_id TEXT NOT NULL,
            bucket_ts TEXT NOT NULL,
            metric TEXT NOT NULL,
            namespace TEXT,
            value REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_log_events_file_ts ON log_events(file_id, ts);
        CREATE INDEX IF NOT EXISTS idx_log_events_file_ns_op ON log_events(file_id, namespace, operation);
        CREATE INDEX IF NOT EXISTS idx_log_events_file_evt_cmp_sev ON log_events(file_id, event_type, component, severity);
        CREATE INDEX IF NOT EXISTS idx_conn_events_file_ts_ip ON connection_events(file_id, ts, ip);
        CREATE INDEX IF NOT EXISTS idx_timeseries_file_metric_ts ON timeseries_agg(file_id, metric, bucket_ts);
        """
    )
    conn.commit()


def upsert_job(conn: sqlite3.Connection, job_data: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO ingest_jobs (job_id, file_id, status, bytes_processed, lines_processed, started_at, finished_at, error_message)
        VALUES (:job_id, :file_id, :status, :bytes_processed, :lines_processed, :started_at, :finished_at, :error_message)
        ON CONFLICT(job_id) DO UPDATE SET
            status=excluded.status,
            bytes_processed=excluded.bytes_processed,
            lines_processed=excluded.lines_processed,
            finished_at=excluded.finished_at,
            error_message=excluded.error_message
        """,
        job_data,
    )
    conn.commit()


def get_latest_job_for_file(conn: sqlite3.Connection, file_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT * FROM ingest_jobs
        WHERE file_id = ?
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (file_id,),
    ).fetchone()
    if row is None:
        return None
    return dict(row)


def delete_file_ingest_data(conn: sqlite3.Connection, file_id: str) -> None:
    conn.execute("DELETE FROM log_events WHERE file_id = ?", (file_id,))
    conn.execute("DELETE FROM connection_events WHERE file_id = ?", (file_id,))
    conn.execute("DELETE FROM query_patterns WHERE file_id = ?", (file_id,))
    conn.execute("DELETE FROM timeseries_agg WHERE file_id = ?", (file_id,))
    conn.commit()


def query_connections_summary(conn: sqlite3.Connection, file_id: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT ip, event, COUNT(*) AS cnt
        FROM connection_events
        WHERE file_id = ? AND ip IS NOT NULL
        GROUP BY ip, event
        """,
        (file_id,),
    ).fetchall()
    connections: dict[str, dict[str, Any]] = {}
    total_opened = 0
    total_closed = 0
    for row in rows:
        ip = row["ip"] or "unknown"
        if ip not in connections:
            connections[ip] = {"opened": 0, "closed": 0, "durations": []}
        if row["event"] == "open":
            connections[ip]["opened"] = int(row["cnt"])
            total_opened += int(row["cnt"])
        if row["event"] == "close":
            connections[ip]["closed"] = int(row["cnt"])
            total_closed += int(row["cnt"])

    ts_rows = conn.execute(
        """
        SELECT bucket_ts, value
        FROM timeseries_agg
        WHERE file_id = ? AND metric = 'connections_delta'
        ORDER BY bucket_ts
        """,
        (file_id,),
    ).fetchall()
    current = 0
    connections_timeseries = []
    for row in ts_rows:
        current += int(row["value"])
        connections_timeseries.append({"timestamp": row["bucket_ts"], "connection_count": max(current, 0)})

    return {
        "connections": connections,
        "total_opened": total_opened,
        "total_closed": total_closed,
        "connections_timeseries": connections_timeseries,
        "connections_by_ip_timeseries": {},
        "connection_events": [],
        "overall_stats": None,
        "ip_stats": {},
        "data_quality": {
            "validation_results": {},
            "warnings": [],
            "recommendations": [],
            "quality_score": 1.0,
            "is_consistent": True,
        },
    }


def query_timeseries(conn: sqlite3.Connection, file_id: str, include_raw: bool) -> dict[str, Any]:
    agg_queries = conn.execute(
        """
        SELECT namespace, COUNT(*) AS cnt, AVG(duration_ms) AS mean_duration_ms
        FROM log_events
        WHERE file_id = ? AND event_type = 'slow_query' AND namespace IS NOT NULL
        GROUP BY namespace
        ORDER BY cnt DESC
        """,
        (file_id,),
    ).fetchall()
    agg_errors = conn.execute(
        """
        SELECT message, COUNT(*) AS cnt
        FROM log_events
        WHERE file_id = ? AND event_type = 'error' AND message IS NOT NULL
        GROUP BY message
        ORDER BY cnt DESC
        """,
        (file_id,),
    ).fetchall()
    unique_namespaces = [row["namespace"] for row in agg_queries if row["namespace"]]
    slow_queries: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    connections: list[dict[str, Any]] = []
    if include_raw:
        sq_rows = conn.execute(
            """
            SELECT ts, namespace, operation, duration_ms, message
            FROM log_events
            WHERE file_id = ? AND event_type = 'slow_query'
            ORDER BY ts
            LIMIT 10000
            """,
            (file_id,),
        ).fetchall()
        err_rows = conn.execute(
            """
            SELECT ts, message, severity
            FROM log_events
            WHERE file_id = ? AND event_type = 'error'
            ORDER BY ts
            LIMIT 10000
            """,
            (file_id,),
        ).fetchall()
        conn_rows = conn.execute(
            """
            SELECT bucket_ts, value
            FROM timeseries_agg
            WHERE file_id = ? AND metric = 'connections_delta'
            ORDER BY bucket_ts
            LIMIT 10000
            """,
            (file_id,),
        ).fetchall()
        current = 0
        for row in conn_rows:
            current += int(row["value"])
            connections.append({"timestamp": row["bucket_ts"], "connection_count": max(current, 0)})
        slow_queries = [
            {
                "timestamp": row["ts"],
                "namespace": row["namespace"] or "unknown",
                "operation": row["operation"] or "unknown",
                "duration_ms": row["duration_ms"] or 0,
                "plan_summary": "N/A",
                "command": {},
            }
            for row in sq_rows
        ]
        errors = [
            {"timestamp": row["ts"], "message": row["message"] or "Unknown", "severity": row["severity"] or ""}
            for row in err_rows
        ]
    return {
        "slow_queries": slow_queries,
        "connections": connections,
        "errors": errors,
        "aggregated_queries": [
            {
                "namespace": row["namespace"],
                "count": int(row["cnt"]),
                "mean_duration_ms": round(float(row["mean_duration_ms"] or 0), 1),
            }
            for row in agg_queries
        ],
        "aggregated_errors": [{"message": row["message"], "count": int(row["cnt"])} for row in agg_errors],
        "unique_namespaces": sorted(unique_namespaces),
        "total_slow_queries": len(slow_queries) if include_raw else sum(int(row["cnt"]) for row in agg_queries),
        "sampled": False,
    }


def query_extract(
    conn: sqlite3.Connection,
    file_id: str,
    *,
    offset: int,
    limit: int,
    text_search: str | None,
    case_sensitive: bool,
    components: list[str],
    severities: list[str],
    operations: list[str],
    namespace: str | None,
    date_from: str | None,
    date_to: str | None,
) -> dict[str, Any]:
    where = ["file_id = ?"]
    params: list[Any] = [file_id]

    if text_search:
        if case_sensitive:
            where.append("message LIKE ?")
            params.append(f"%{text_search}%")
        else:
            where.append("LOWER(message) LIKE LOWER(?)")
            params.append(f"%{text_search}%")
    if components:
        where.append(f"component IN ({','.join('?' for _ in components)})")
        params.extend(components)
    if severities:
        where.append(f"severity IN ({','.join('?' for _ in severities)})")
        params.extend(severities)
    if operations:
        where.append(f"operation IN ({','.join('?' for _ in operations)})")
        params.extend(operations)
    if namespace:
        where.append("namespace = ?")
        params.append(namespace)
    if date_from:
        where.append("ts >= ?")
        params.append(date_from)
    if date_to:
        where.append("ts <= ?")
        params.append(date_to)

    where_sql = " AND ".join(where)
    page_limit = max(1, min(limit, 5000))
    total_scanned = conn.execute("SELECT COUNT(*) FROM log_events WHERE file_id = ?", (file_id,)).fetchone()[0]
    total_matched = conn.execute(
        f"SELECT COUNT(*) FROM log_events WHERE {where_sql}",
        params,
    ).fetchone()[0]
    rows = conn.execute(
        f"""
        SELECT raw_json
        FROM log_events
        WHERE {where_sql}
        ORDER BY line_no
        LIMIT ? OFFSET ?
        """,
        [*params, page_limit, offset],
    ).fetchall()
    lines = [row["raw_json"] for row in rows if row["raw_json"]]
    return {
        "total_scanned": int(total_scanned),
        "total_matched": int(total_matched),
        "lines": lines,
        "truncated": (offset + len(lines)) < int(total_matched),
    }
