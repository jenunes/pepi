from __future__ import annotations

import os
import re
import sqlite3
from collections import defaultdict
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

        CREATE INDEX IF NOT EXISTS idx_log_events_file_ts
            ON log_events(file_id, ts);
        CREATE INDEX IF NOT EXISTS idx_log_events_file_ns_op
            ON log_events(file_id, namespace, operation);
        CREATE INDEX IF NOT EXISTS idx_log_events_file_evt_cmp_sev
            ON log_events(file_id, event_type, component, severity);
        CREATE INDEX IF NOT EXISTS idx_conn_events_file_ts_ip
            ON connection_events(file_id, ts, ip);
        CREATE INDEX IF NOT EXISTS idx_timeseries_file_metric_ts
            ON timeseries_agg(file_id, metric, bucket_ts);
        """
    )
    conn.commit()


def upsert_job(conn: sqlite3.Connection, job_data: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO ingest_jobs (
            job_id, file_id, status, bytes_processed, lines_processed,
            started_at, finished_at, error_message
        )
        VALUES (
            :job_id, :file_id, :status, :bytes_processed, :lines_processed,
            :started_at, :finished_at, :error_message
        )
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
        connections_timeseries.append(
            {"timestamp": row["bucket_ts"], "connection_count": max(current, 0)}
        )

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
            {
                "timestamp": row["ts"],
                "message": row["message"] or "Unknown",
                "severity": row["severity"] or "",
            }
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
        "aggregated_errors": [
            {"message": row["message"], "count": int(row["cnt"])} for row in agg_errors
        ],
        "unique_namespaces": sorted(unique_namespaces),
        "total_slow_queries": len(slow_queries)
        if include_raw
        else sum(int(row["cnt"]) for row in agg_queries),
        "sampled": False,
    }


def _event_types_sql(event_types: list[str], slow_threshold: int) -> tuple[str, list[Any]]:
    if not event_types:
        return "", []
    parts: list[str] = []
    params: list[Any] = []
    for et in event_types:
        if et == "COLLSCAN":
            parts.append("INSTR(raw_json, 'COLLSCAN') > 0")
        elif et == "IXSCAN":
            parts.append("INSTR(raw_json, 'IXSCAN') > 0")
        elif et == "slow_query":
            parts.append("COALESCE(duration_ms, 0) > ?")
            params.append(slow_threshold)
        elif et == "error":
            parts.append("(severity IN ('E', 'F') OR event_type = 'error')")
    if not parts:
        return "", []
    return "(" + " OR ".join(parts) + ")", params


def _build_extract_where(
    *,
    file_id: str,
    text_search: str | None,
    case_sensitive: bool,
    include_text_clause: bool,
    event_types: list[str],
    slow_threshold: int,
    min_duration_ms: int | None,
    log_id: int | None,
    context: str | None,
    components: list[str],
    severities: list[str],
    operations: list[str],
    namespace: str | None,
    date_from: str | None,
    date_to: str | None,
) -> tuple[str, list[Any]]:
    where = ["file_id = ?"]
    params: list[Any] = [file_id]

    if include_text_clause and text_search:
        if case_sensitive:
            where.append("(INSTR(raw_json, ?) > 0 OR INSTR(COALESCE(message, ''), ?) > 0)")
            params.extend([text_search, text_search])
        else:
            needle = text_search.lower()
            where.append(
                "(INSTR(LOWER(raw_json), ?) > 0 OR INSTR(LOWER(COALESCE(message, '')), ?) > 0)"
            )
            params.extend([needle, needle])

    et_sql, et_params = _event_types_sql(event_types, slow_threshold)
    if et_sql:
        where.append(et_sql)
        params.extend(et_params)

    if min_duration_ms is not None:
        where.append("COALESCE(duration_ms, 0) >= ?")
        params.append(min_duration_ms)

    if log_id is not None:
        where.append("CAST(json_extract(raw_json, '$.id') AS INTEGER) = ?")
        params.append(log_id)

    if context:
        where.append(
            "(INSTR(COALESCE(json_extract(raw_json, '$.ctx'), ''), ?) > 0 "
            "OR INSTR(COALESCE(message, ''), ?) > 0)"
        )
        params.extend([context, context])

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
        where.append("INSTR(COALESCE(namespace, ''), ?) > 0")
        params.append(namespace)
    if date_from:
        where.append("ts >= ?")
        params.append(date_from)
    if date_to:
        where.append("ts <= ?")
        params.append(date_to)

    return " AND ".join(where), params


def _fetch_match_summary(
    conn: sqlite3.Connection, where_sql: str, params: list[Any]
) -> dict[str, Any]:
    by_severity: dict[str, int] = {}
    rows = conn.execute(
        f"SELECT severity, COUNT(*) AS c FROM log_events WHERE {where_sql} GROUP BY severity",
        params,
    ).fetchall()
    for row in rows:
        key = row["severity"] if row["severity"] is not None else "—"
        by_severity[str(key)] = int(row["c"])
    span = conn.execute(
        f"SELECT MIN(ts) AS tmin, MAX(ts) AS tmax FROM log_events WHERE {where_sql}",
        params,
    ).fetchone()
    return {
        "by_severity": by_severity,
        "time_span_start": span["tmin"] if span else None,
        "time_span_end": span["tmax"] if span else None,
    }


def _query_extract_regex_stream(
    conn: sqlite3.Connection,
    *,
    where_sql: str,
    params: list[Any],
    compiled: re.Pattern[str],
    offset: int,
    page_limit: int,
) -> dict[str, Any]:
    by_severity: defaultdict[str, int] = defaultdict(int)
    time_min: str | None = None
    time_max: str | None = None
    matched_count = 0
    lines: list[str] = []
    match_line_numbers: list[int] = []

    cur = conn.execute(
        f"""
        SELECT line_no, raw_json, severity, ts
        FROM log_events
        WHERE {where_sql}
        ORDER BY line_no
        """,
        params,
    )
    for row in cur:
        raw = row["raw_json"]
        if not raw or not compiled.search(raw):
            continue
        matched_count += 1
        sev = row["severity"] if row["severity"] is not None else "—"
        by_severity[str(sev)] += 1
        ts = row["ts"]
        if ts:
            if time_min is None or ts < time_min:
                time_min = ts
            if time_max is None or ts > time_max:
                time_max = ts
        if matched_count <= offset:
            continue
        if len(lines) < page_limit:
            lines.append(raw.strip())
            match_line_numbers.append(int(row["line_no"]))

    return {
        "total_matched": matched_count,
        "lines": lines,
        "match_line_numbers": match_line_numbers,
        "truncated": offset + len(lines) < matched_count,
        "match_summary": {
            "by_severity": dict(by_severity),
            "time_span_start": time_min,
            "time_span_end": time_max,
        },
    }


def query_extract(
    conn: sqlite3.Connection,
    file_id: str,
    *,
    offset: int,
    limit: int,
    text_search: str | None,
    case_sensitive: bool,
    use_regex: bool,
    event_types: list[str],
    components: list[str],
    severities: list[str],
    operations: list[str],
    namespace: str | None,
    log_id: int | None,
    context: str | None,
    date_from: str | None,
    date_to: str | None,
    min_duration_ms: int | None,
    slow_query_threshold_ms: int | None,
) -> dict[str, Any]:
    page_limit = max(1, min(limit, 5000))
    off = max(0, offset)
    slow_thresh = slow_query_threshold_ms if slow_query_threshold_ms is not None else 100

    total_scanned = int(
        conn.execute("SELECT COUNT(*) FROM log_events WHERE file_id = ?", (file_id,)).fetchone()[0]
    )

    if use_regex and text_search:
        try:
            flags = 0 if case_sensitive else re.IGNORECASE
            compiled = re.compile(text_search, flags)
        except re.error as exc:
            raise ValueError(f"Invalid regex: {exc}") from exc
        where_sql, params = _build_extract_where(
            file_id=file_id,
            text_search=text_search,
            case_sensitive=case_sensitive,
            include_text_clause=False,
            event_types=event_types,
            slow_threshold=slow_thresh,
            min_duration_ms=min_duration_ms,
            log_id=log_id,
            context=context,
            components=components,
            severities=severities,
            operations=operations,
            namespace=namespace,
            date_from=date_from,
            date_to=date_to,
        )
        streamed = _query_extract_regex_stream(
            conn,
            where_sql=where_sql,
            params=params,
            compiled=compiled,
            offset=off,
            page_limit=page_limit,
        )
        return {
            "total_scanned": total_scanned,
            "total_matched": streamed["total_matched"],
            "lines": streamed["lines"],
            "match_line_numbers": streamed["match_line_numbers"],
            "truncated": streamed["truncated"],
            "match_summary": streamed["match_summary"],
        }

    where_sql, params = _build_extract_where(
        file_id=file_id,
        text_search=text_search,
        case_sensitive=case_sensitive,
        include_text_clause=True,
        event_types=event_types,
        slow_threshold=slow_thresh,
        min_duration_ms=min_duration_ms,
        log_id=log_id,
        context=context,
        components=components,
        severities=severities,
        operations=operations,
        namespace=namespace,
        date_from=date_from,
        date_to=date_to,
    )

    total_matched = int(
        conn.execute(
            f"SELECT COUNT(*) FROM log_events WHERE {where_sql}",
            params,
        ).fetchone()[0]
    )
    summary = _fetch_match_summary(conn, where_sql, params)
    rows = conn.execute(
        f"""
        SELECT raw_json, line_no
        FROM log_events
        WHERE {where_sql}
        ORDER BY line_no
        LIMIT ? OFFSET ?
        """,
        [*params, page_limit, off],
    ).fetchall()
    lines = [row["raw_json"].strip() for row in rows if row["raw_json"]]
    match_line_numbers = [int(row["line_no"]) for row in rows if row["raw_json"]]

    return {
        "total_scanned": total_scanned,
        "total_matched": total_matched,
        "lines": lines,
        "match_line_numbers": match_line_numbers,
        "truncated": off + len(lines) < total_matched,
        "match_summary": summary,
    }
