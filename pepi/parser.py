"""MongoDB log file parsing functions for Pepi."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from statistics import median
from typing import Any, Optional

import click
from tqdm import tqdm

from pepi.cache import get_cache_key, load_from_cache, save_to_cache
from pepi.sampling import get_sample_rate, get_sample_rate_from_percentage, get_sampling_metadata
from pepi.utils import count_lines


def parse_connections(
    logfile: str, sample_percentage: Optional[int] = None
) -> tuple[dict, int, int]:
    """Parse connection information from MongoDB log file."""
    cache_key = get_cache_key(logfile, "connections")
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached connection data...")
        return (
            cached_result["connections"],
            cached_result["total_opened"],
            cached_result["total_closed"],
        )

    def default_connection_data():
        return {"opened": 0, "closed": 0, "durations": []}

    connections = defaultdict(default_connection_data)
    total_opened = 0
    total_closed = 0
    connection_starts = {}

    total_lines = count_lines(logfile)

    if sample_percentage is not None:
        sample_rate = get_sample_rate_from_percentage(sample_percentage, total_lines)
        is_sampled = sample_rate > 1
        if is_sampled:
            click.echo(
                f"Sampling {sample_percentage}% of file ({total_lines:,} lines). "
                f"Processing every {sample_rate} lines..."
            )
    else:
        sample_rate = get_sample_rate(total_lines)
        is_sampled = sample_rate > 1
        if is_sampled:
            click.echo(
                f"Large file detected ({total_lines:,} lines). "
                f"Sampling every {sample_rate} lines for performance..."
            )

    with open(logfile, "r") as f:
        line_count = 0
        for line in tqdm(f, total=total_lines, desc="Parsing connections", unit="lines"):
            line_count += 1

            if is_sampled and line_count % sample_rate != 0:
                continue
            try:
                entry = json.loads(line)

                if (
                    entry.get("msg") == "Connection accepted"
                    and entry.get("c") == "NETWORK"
                    and entry.get("attr")
                ):
                    attr = entry["attr"]
                    if "remote" in attr:
                        ip = attr["remote"].split(":")[0]
                        connections[ip]["opened"] += 1
                        total_opened += 1

                        if "connectionId" in attr:
                            conn_id = attr["connectionId"]
                            start_time = entry.get("t", {}).get("$date")
                            if start_time:
                                connection_starts[conn_id] = {"start_time": start_time, "ip": ip}

                elif (
                    entry.get("msg") == "Connection ended"
                    and entry.get("c") == "NETWORK"
                    and entry.get("attr")
                ):
                    attr = entry["attr"]
                    if "remote" in attr:
                        ip = attr["remote"].split(":")[0]
                        connections[ip]["closed"] += 1
                        total_closed += 1

                        if "connectionId" in attr:
                            conn_id = attr["connectionId"]
                            if conn_id in connection_starts:
                                start_data = connection_starts[conn_id]
                                if start_data["ip"] == ip:
                                    start_time = start_data["start_time"]
                                    end_time = entry.get("t", {}).get("$date")

                                    if start_time and end_time:
                                        try:
                                            start_dt = datetime.fromisoformat(
                                                start_time.replace("Z", "+00:00")
                                            )
                                            end_dt = datetime.fromisoformat(
                                                end_time.replace("Z", "+00:00")
                                            )
                                            duration = (end_dt - start_dt).total_seconds()
                                            connections[ip]["durations"].append(duration)
                                        except (ValueError, TypeError, OverflowError):
                                            pass

                                del connection_starts[conn_id]

            except Exception:
                pass

    cache_data = {
        "connections": dict(connections),
        "total_opened": total_opened,
        "total_closed": total_closed,
        "sampling_metadata": get_sampling_metadata(total_lines, sample_percentage),
    }
    save_to_cache(cache_key, cache_data)

    return connections, total_opened, total_closed


def parse_replica_set_config(logfile: str) -> list[dict[str, Any]]:
    """Parse replica set configuration from MongoDB log file."""
    cache_key = get_cache_key(logfile, "rs_config")
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached replica set config data...")
        return cached_result["configs"]

    configs = []

    total_lines = count_lines(logfile)

    with open(logfile, "r") as f:
        for line in tqdm(f, total=total_lines, desc="Parsing replica set config", unit="lines"):
            try:
                entry = json.loads(line)
                if (
                    entry.get("msg") == "New replica set config in use"
                    and entry.get("c") == "REPL"
                    and entry.get("attr", {}).get("config")
                ):
                    config = entry["attr"]["config"]
                    configs.append({"timestamp": entry.get("t", {}).get("$date"), "config": config})
            except Exception:
                pass

    cache_data = {"configs": configs}
    save_to_cache(cache_key, cache_data)

    return configs


def parse_replica_set_state(logfile: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Parse replica set state transitions and current node status from MongoDB log file."""
    cache_key = get_cache_key(logfile, "rs_state")
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached replica set state data...")
        return cached_result["states"], cached_result["node_status"]

    states = []
    node_status = {}
    replica_set_config = None
    current_host = None
    state_transitions = {}

    total_lines = count_lines(logfile)

    with open(logfile, "r") as f:
        for line in tqdm(f, total=total_lines, desc="Parsing replica set state", unit="lines"):
            try:
                entry = json.loads(line)

                if (
                    entry.get("msg") == "Found self in config"
                    and entry.get("c") == "REPL"
                    and entry.get("attr")
                ):
                    current_host = entry.get("attr", {}).get("hostAndPort")

                if (
                    entry.get("msg") == "Replica set state transition"
                    and entry.get("c") == "REPL"
                    and entry.get("attr")
                ):
                    attr = entry["attr"]
                    new_state = attr.get("newState")
                    timestamp = entry.get("t", {}).get("$date")

                    states.append(
                        {
                            "host": current_host,
                            "timestamp": timestamp,
                            "new_state": new_state,
                            "old_state": attr.get("oldState"),
                        }
                    )

                    if current_host:
                        state_transitions[current_host] = {
                            "state": new_state,
                            "timestamp": timestamp,
                        }

                if (
                    entry.get("msg") == "New replica set config in use"
                    and entry.get("c") == "REPL"
                    and entry.get("attr", {}).get("config")
                ):
                    replica_set_config = entry["attr"]["config"]
                    if "members" in replica_set_config:
                        for member in replica_set_config["members"]:
                            host_port = member.get("host")
                            if host_port:
                                latest_state = state_transitions.get(
                                    host_port,
                                    {
                                        "state": "STARTUP",
                                        "timestamp": entry.get("t", {}).get("$date"),
                                    },
                                )
                                node_status[host_port] = {
                                    "state": latest_state["state"],
                                    "timestamp": latest_state["timestamp"],
                                    "member_id": member.get("_id"),
                                }

            except Exception:
                pass

    cache_data = {"states": states, "node_status": node_status}
    save_to_cache(cache_key, cache_data)

    return states, node_status


def parse_clients(logfile: str) -> dict[str, dict[str, Any]]:
    """Parse client/driver information from MongoDB log file."""
    cache_key = get_cache_key(logfile, "clients")
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached client data...")
        return cached_result["clients"]

    clients = {}
    connection_drivers = {}

    total_lines = count_lines(logfile)

    with open(logfile, "r") as f:
        for line in tqdm(f, total=total_lines, desc="Parsing clients", unit="lines"):
            try:
                entry = json.loads(line)

                if (
                    entry.get("c") == "NETWORK"
                    and entry.get("msg") == "client metadata"
                    and entry.get("attr")
                ):
                    attr = entry["attr"]
                    remote = attr.get("remote", "unknown")
                    client_id = attr.get("client", "unknown")

                    doc = attr.get("doc", {})
                    driver_info = doc.get("driver", {})
                    app_info = doc.get("application", {})
                    os_info = doc.get("os", {})

                    driver_name = driver_info.get("name", "Unknown")
                    driver_version = driver_info.get("version", "")
                    app_name = app_info.get("name", "")

                    if app_name:
                        driver_key = f"{app_name} v{driver_version}" if driver_version else app_name
                    else:
                        driver_key = (
                            f"{driver_name} v{driver_version}" if driver_version else driver_name
                        )

                    if driver_key not in clients:
                        clients[driver_key] = {
                            "connections": set(),
                            "ips": set(),
                            "users": set(),
                            "driver_name": driver_name,
                            "driver_version": driver_version,
                            "app_name": app_name,
                            "os_name": os_info.get("name", ""),
                            "os_version": os_info.get("version", ""),
                        }

                    clients[driver_key]["connections"].add(client_id)
                    clients[driver_key]["ips"].add(remote.split(":")[0])

                    connection_drivers[client_id] = driver_key

                elif (
                    entry.get("c") == "NETWORK"
                    and entry.get("msg") == "Connection accepted"
                    and entry.get("attr")
                ):
                    attr = entry["attr"]
                    remote = attr.get("remote", "unknown")
                    client_id = attr.get("connectionId", "unknown")

                    if client_id in connection_drivers:
                        driver_key = connection_drivers[client_id]
                        if driver_key in clients:
                            clients[driver_key]["connections"].add(client_id)
                            clients[driver_key]["ips"].add(remote.split(":")[0])

            except Exception:
                pass

    for driver_key, client_info in clients.items():
        client_info["connections"] = list(client_info["connections"])
        client_info["ips"] = list(client_info["ips"])
        client_info["users"] = list(client_info["users"])

    cache_data = {"clients": clients}
    save_to_cache(cache_key, cache_data)

    return clients


def _normalize_query_shape(obj: Any) -> Any:
    """Replace leaf values with '?' for stable grouping keys."""
    if isinstance(obj, dict):
        return {k: _normalize_query_shape(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_normalize_query_shape(v) for v in obj]
    return "?"


def _aggregate_stage_shape(stage: dict[str, Any]) -> dict[str, Any]:
    """Build a normalized summary of one aggregation pipeline stage."""
    if not stage:
        return {}
    op = list(stage.keys())[0]
    body = stage[op]
    if op == "$match" and isinstance(body, dict):
        return {op: _normalize_query_shape(body)}
    if op == "$sort" and isinstance(body, dict):
        return {op: _normalize_query_shape(body)}
    if op == "$lookup" and isinstance(body, dict):
        sub: dict[str, Any] = {}
        for k in ("from", "localField", "foreignField", "as"):
            if k in body:
                sub[k] = "?" if k == "from" else _normalize_query_shape(body[k])
        return {op: sub}
    if op == "$group" and isinstance(body, dict):
        gid = body.get("_id")
        return {op: {"_id": _normalize_query_shape(gid) if gid is not None else "?"}}
    return {op: "?"}


def extract_query_pattern(operation: str, command: dict[str, Any]) -> str:
    """Extract normalized query pattern for grouping (filter, sort, pipeline shape)."""
    if operation == "find":
        filt = command.get("filter", {})
        sort_raw = command.get("sort") or {}
        sort_d: dict[str, Any] = sort_raw if isinstance(sort_raw, dict) else {}
        proj = command.get("projection")
        if proj is None:
            proj = command.get("fields")
        proj_norm: Any
        if isinstance(proj, dict):
            proj_norm = _normalize_query_shape(proj)
        elif proj is None:
            proj_norm = {}
        else:
            proj_norm = "?"
        shape = {
            "filter": _normalize_query_shape(filt),
            "sort": _normalize_query_shape(sort_d),
            "projection": proj_norm,
            "has_limit": command.get("limit") is not None,
            "has_skip": command.get("skip") is not None,
        }
        return json.dumps(shape, sort_keys=True)
    if operation == "update":
        updates = command.get("updates", [])
        return json.dumps(_normalize_query_shape(updates), sort_keys=True)
    if operation == "delete":
        deletes = command.get("deletes", [])
        return json.dumps(_normalize_query_shape(deletes), sort_keys=True)
    if operation == "insert":
        docs = command.get("documents", [])
        if docs and isinstance(docs, list):
            keys = sorted(set(k for doc in docs for k in doc.keys()))
            return "insert_keys:" + ",".join(keys)
        return "insert_keys:unknown"
    if operation == "aggregate":
        pipeline = command.get("pipeline", [])
        if pipeline and isinstance(pipeline, list):
            stages_out = []
            for stage in pipeline:
                if isinstance(stage, dict) and stage:
                    stages_out.append(_aggregate_stage_shape(stage))
            return json.dumps(stages_out)
        return "[unknown]"
    return json.dumps(sorted(command.keys()))


def parse_timeseries_data(logfile: str) -> tuple[list[dict], list[dict], list[dict]]:
    """Parse time-series data for slow queries, connections, and errors."""
    cache_key = get_cache_key(logfile, "timeseries")
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached time-series data...")
        return cached_result["slow_queries"], cached_result["connections"], cached_result["errors"]

    slow_queries = []
    connections = []
    errors = []

    total_lines = count_lines(logfile)

    with open(logfile, "r") as f:
        for line in tqdm(f, total=total_lines, desc="Parsing time-series data", unit="lines"):
            try:
                entry = json.loads(line)
                timestamp = entry.get("t", {}).get("$date")

                if not timestamp:
                    continue

                if entry.get("c") == "COMMAND" and entry.get("msg") in ("command", "Slow query"):
                    attr = entry.get("attr", {})
                    namespace = attr.get("ns", "")
                    duration_ms = attr.get("durationMillis", 0)
                    command = attr.get("command", {})
                    plan_summary = attr.get("planSummary", "N/A")

                    if namespace and duration_ms > 0:
                        slow_queries.append(
                            {
                                "timestamp": timestamp,
                                "duration_ms": duration_ms,
                                "namespace": namespace,
                                "command": command,
                                "plan_summary": plan_summary,
                            }
                        )

                elif entry.get("c") == "NETWORK" and entry.get("msg") == "Connection accepted":
                    attr = entry.get("attr", {})
                    connection_count = attr.get("connectionCount", 0)

                    connections.append(
                        {"timestamp": timestamp, "connection_count": connection_count}
                    )

                severity = entry.get("s", "")
                msg = entry.get("msg", "")
                if severity in ("E", "W") or "error" in msg.lower() or "warning" in msg.lower():
                    errors.append(
                        {
                            "timestamp": timestamp,
                            "severity": severity,
                            "message": msg,
                            "component": entry.get("c", "Unknown"),
                        }
                    )

            except Exception:
                pass

    cache_data = {"slow_queries": slow_queries, "connections": connections, "errors": errors}
    save_to_cache(cache_key, cache_data)

    return slow_queries, connections, errors


def parse_connections_timeseries_by_ip(logfile: str) -> dict[str, list[dict]]:
    """Parse connection time series data grouped by IP address with improved edge case handling."""
    cache_key = get_cache_key(logfile, "connections_timeseries_by_ip")
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached IP-specific connection data...")
        return cached_result["connections_by_ip"]

    ip_events = defaultdict(list)
    connection_starts = {}
    connection_ends = {}
    total_connection_counts = []

    total_lines = count_lines(logfile)

    with open(logfile, "r") as f:
        for line in tqdm(
            f, total=total_lines, desc="Parsing IP-specific connections", unit="lines"
        ):
            try:
                entry = json.loads(line)
                timestamp = entry.get("t", {}).get("$date")

                if not timestamp:
                    continue

                if (
                    entry.get("msg") == "Connection accepted"
                    and entry.get("c") == "NETWORK"
                    and entry.get("attr")
                ):
                    attr = entry["attr"]
                    if "remote" in attr:
                        ip = attr["remote"].split(":")[0]
                        conn_id = attr.get("connectionId")
                        connection_count = attr.get("connectionCount", 0)

                        total_connection_counts.append(
                            {"timestamp": timestamp, "connection_count": connection_count}
                        )

                        ip_events[ip].append(
                            {"timestamp": timestamp, "event": "open", "connection_id": conn_id}
                        )

                        if conn_id:
                            connection_starts[conn_id] = {"start_time": timestamp, "ip": ip}

                elif (
                    entry.get("msg") == "Connection ended"
                    and entry.get("c") == "NETWORK"
                    and entry.get("attr")
                ):
                    attr = entry["attr"]
                    conn_id = attr.get("connectionId")

                    if conn_id in connection_starts:
                        ip = connection_starts[conn_id]["ip"]
                        ip_events[ip].append(
                            {"timestamp": timestamp, "event": "close", "connection_id": conn_id}
                        )
                        connection_ends[conn_id] = timestamp
                        del connection_starts[conn_id]

            except Exception:
                pass

    connections_by_ip = {}
    data_quality_metrics = {}

    for ip, events in ip_events.items():
        events.sort(key=lambda x: x["timestamp"])

        time_series = []
        active_connections = 0
        open_events = 0
        close_events = 0
        unmatched_opens = 0

        for event in events:
            if event["event"] == "open":
                active_connections += 1
                open_events += 1
            elif event["event"] == "close":
                active_connections = max(0, active_connections - 1)
                close_events += 1

            time_series.append(
                {"timestamp": event["timestamp"], "connection_count": active_connections}
            )

        unmatched_opens = open_events - close_events

        data_quality_metrics[ip] = {
            "open_events": open_events,
            "close_events": close_events,
            "unmatched_opens": unmatched_opens,
            "final_count": active_connections,
            "data_completeness": close_events / open_events if open_events > 0 else 1.0,
        }

        connections_by_ip[ip] = time_series

    total_opens = sum(metrics["open_events"] for metrics in data_quality_metrics.values())
    total_closes = sum(metrics["close_events"] for metrics in data_quality_metrics.values())
    overall_completeness = total_closes / total_opens if total_opens > 0 else 1.0

    validation_data = {
        "total_connection_counts": total_connection_counts,
        "overall_completeness": overall_completeness,
        "total_opens": total_opens,
        "total_closes": total_closes,
        "unmatched_connections": total_opens - total_closes,
    }

    cache_data = {
        "connections_by_ip": connections_by_ip,
        "data_quality_metrics": data_quality_metrics,
        "validation_data": validation_data,
    }
    save_to_cache(cache_key, cache_data)

    return connections_by_ip


def validate_connection_data_consistency(
    connections_by_ip: dict[str, list[dict]],
    total_connections_timeseries: list[dict],
) -> dict[str, Any]:
    """Validate that per-IP connection data is consistent with total connection data."""
    validation_results = {
        "is_consistent": True,
        "discrepancies": [],
        "data_quality_score": 1.0,
        "warnings": [],
        "recommendations": [],
    }

    if not connections_by_ip or not total_connections_timeseries:
        validation_results["warnings"].append("No connection data available for validation")
        return validation_results

    total_counts_by_time = {}
    for conn in total_connections_timeseries:
        timestamp = conn["timestamp"]
        total_counts_by_time[timestamp] = conn["connection_count"]

    max_discrepancy = 0
    total_checks = 0
    discrepancies = 0

    for ip, ip_data in connections_by_ip.items():
        for point in ip_data:
            timestamp = point["timestamp"]
            if timestamp in total_counts_by_time:
                total_checks += 1
                ip_count = point["connection_count"]
                total_count = total_counts_by_time[timestamp]

                if ip_count > total_count:
                    discrepancies += 1
                    max_discrepancy = max(max_discrepancy, ip_count - total_count)
                    validation_results["discrepancies"].append(
                        {
                            "timestamp": timestamp,
                            "ip": ip,
                            "ip_count": ip_count,
                            "total_count": total_count,
                            "difference": ip_count - total_count,
                        }
                    )

    if total_checks > 0:
        validation_results["data_quality_score"] = 1.0 - (discrepancies / total_checks)
        validation_results["is_consistent"] = discrepancies == 0

    if validation_results["data_quality_score"] < 0.8:
        validation_results["warnings"].append(
            f"Data quality is {validation_results['data_quality_score']:.1%} - "
            "some connection events may be missing"
        )
        validation_results["recommendations"].append(
            "Consider using a longer log period to capture complete connection lifecycles"
        )

    if max_discrepancy > 0:
        validation_results["warnings"].append(
            f"Maximum discrepancy found: {max_discrepancy} connections"
        )
        validation_results["recommendations"].append(
            "Per-IP tracking may be missing some connection close events"
        )

    return validation_results


def parse_connection_events(logfile: str) -> list[dict[str, Any]]:
    """Parse individual connection open/close events from MongoDB log file."""
    cache_key = get_cache_key(logfile, "connection_events")
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached connection events data...")
        return cached_result["connection_events"]

    connection_events = []

    total_lines = count_lines(logfile)

    with open(logfile, "r") as f:
        for line in tqdm(f, total=total_lines, desc="Parsing connection events", unit="lines"):
            try:
                entry = json.loads(line)
                timestamp = entry.get("t", {}).get("$date")

                if not timestamp:
                    continue

                if (
                    entry.get("msg") == "Connection accepted"
                    and entry.get("c") == "NETWORK"
                    and entry.get("attr")
                ):
                    attr = entry["attr"]
                    if "remote" in attr:
                        ip = attr["remote"].split(":")[0]
                        conn_id = attr.get("connectionId")
                        connection_count = attr.get("connectionCount", 0)

                        connection_events.append(
                            {
                                "timestamp": timestamp,
                                "event_type": "opened",
                                "ip": ip,
                                "connection_id": conn_id,
                                "total_connections": connection_count,
                                "log_message": line.strip(),
                            }
                        )

                elif (
                    entry.get("msg") == "Connection ended"
                    and entry.get("c") == "NETWORK"
                    and entry.get("attr")
                ):
                    attr = entry["attr"]
                    conn_id = attr.get("connectionId")

                    connection_events.append(
                        {
                            "timestamp": timestamp,
                            "event_type": "closed",
                            "ip": "unknown",
                            "connection_id": conn_id,
                            "total_connections": 0,
                            "log_message": line.strip(),
                        }
                    )

            except Exception:
                pass

    connection_events.sort(key=lambda x: x["timestamp"])

    cache_data = {"connection_events": connection_events}
    save_to_cache(cache_key, cache_data)

    return connection_events


def _deep_copy_json_safe(obj: Any) -> Optional[Any]:
    """Deep copy via JSON when possible (drops non-JSON-serializable values)."""
    try:
        return json.loads(json.dumps(obj))
    except (TypeError, ValueError):
        return None


def parse_queries(logfile: str, sample_percentage: Optional[int] = None) -> dict:
    """Parse query patterns and statistics from MongoDB log file, grouped by pattern."""
    cache_key = get_cache_key(logfile, "queries_v2")
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached query data...")
        queries = cached_result["queries"]
        for key, value in queries.items():
            if isinstance(value.get("operations"), list):
                value["operations"] = set(value["operations"])
            if isinstance(value.get("indexes"), list):
                value["indexes"] = set(value["indexes"])
            if "indexes" not in value:
                value["indexes"] = set()
            value.setdefault("repr_command", None)
            value.setdefault("docs_examined", [])
            value.setdefault("keys_examined", [])
            value.setdefault("n_returned", [])
            value.setdefault("planning_micros", [])
        return queries

    def default_query_data():
        return {
            "count": 0,
            "durations": [],
            "allowDiskUse": False,
            "operations": set(),
            "pattern": None,
            "indexes": set(),
            "repr_command": None,
            "docs_examined": [],
            "keys_examined": [],
            "n_returned": [],
            "planning_micros": [],
        }

    queries = defaultdict(default_query_data)

    total_lines = count_lines(logfile)

    if sample_percentage is not None:
        sample_rate = get_sample_rate_from_percentage(sample_percentage, total_lines)
        is_sampled = sample_rate > 1
        if is_sampled:
            click.echo(
                f"Sampling {sample_percentage}% of file ({total_lines:,} lines). "
                f"Processing every {sample_rate} lines..."
            )
    else:
        sample_rate = get_sample_rate(total_lines)
        is_sampled = sample_rate > 1
        if is_sampled:
            click.echo(
                f"Large file detected ({total_lines:,} lines). "
                f"Sampling every {sample_rate} lines for performance..."
            )

    with open(logfile, "r") as f:
        line_count = 0
        for line in tqdm(f, total=total_lines, desc="Parsing queries", unit="lines"):
            line_count += 1

            if is_sampled and line_count % sample_rate != 0:
                continue
            try:
                entry = json.loads(line)
                if (
                    entry.get("c") == "COMMAND"
                    and entry.get("msg") in ("command", "Slow query")
                    and entry.get("attr")
                ):
                    attr = entry["attr"]
                    namespace = attr.get("ns", "")
                    if not namespace:
                        continue
                    command = attr.get("command", {})
                    if not command:
                        continue
                    operation = list(command.keys())[0] if command else "unknown"
                    pattern = extract_query_pattern(operation, command)
                    group_key = (namespace, operation, pattern)
                    duration_ms = attr.get("durationMillis", 0)
                    allow_disk_use = command.get("allowDiskUse", False)
                    plan_summary = attr.get("planSummary", "")
                    index_used = "COLLSCAN" if plan_summary == "COLLSCAN" else plan_summary
                    if not index_used:
                        index_used = "N/A"
                    queries[group_key]["count"] += 1
                    queries[group_key]["durations"].append(duration_ms)
                    queries[group_key]["operations"].add(operation)
                    queries[group_key]["allowDiskUse"] = (
                        queries[group_key]["allowDiskUse"] or allow_disk_use
                    )
                    queries[group_key]["pattern"] = pattern
                    queries[group_key]["indexes"].add(index_used)
                    if queries[group_key]["repr_command"] is None:
                        queries[group_key]["repr_command"] = _deep_copy_json_safe(command)
                    de = attr.get("docsExamined")
                    if de is not None:
                        try:
                            queries[group_key]["docs_examined"].append(int(de))
                        except (TypeError, ValueError):
                            pass
                    ke = attr.get("keysExamined")
                    if ke is not None:
                        try:
                            queries[group_key]["keys_examined"].append(int(ke))
                        except (TypeError, ValueError):
                            pass
                    nr = attr.get("nreturned")
                    if nr is None:
                        nr = attr.get("nReturned")
                    if nr is not None:
                        try:
                            queries[group_key]["n_returned"].append(int(nr))
                        except (TypeError, ValueError):
                            pass
                    pm = attr.get("planningTimeMicros")
                    if pm is not None:
                        try:
                            queries[group_key]["planning_micros"].append(int(pm))
                        except (TypeError, ValueError):
                            pass
            except Exception:
                pass

    queries_dict = {}
    for key, value in queries.items():
        queries_dict[key] = {
            "count": value["count"],
            "durations": value["durations"],
            "allowDiskUse": value["allowDiskUse"],
            "operations": list(value["operations"]),
            "pattern": value["pattern"],
            "indexes": list(value["indexes"]),
            "repr_command": value.get("repr_command"),
            "docs_examined": value.get("docs_examined", []),
            "keys_examined": value.get("keys_examined", []),
            "n_returned": value.get("n_returned", []),
            "planning_micros": value.get("planning_micros", []),
        }
    cache_data = {
        "queries": queries_dict,
        "sampling_metadata": get_sampling_metadata(total_lines, sample_percentage),
    }
    save_to_cache(cache_key, cache_data)

    return queries


def _to_minute_bucket(timestamp: str) -> str:
    if not timestamp:
        return ""
    return timestamp[:16] + ":00Z"


def parse_errors_detail(logfile: str) -> dict[str, Any]:
    """Parse detailed error information for time-series diagnostics."""
    cache_key = get_cache_key(logfile, "errors_detail")
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached detailed error data...")
        return cached_result["data"]

    total_lines = count_lines(logfile)
    severity_by_bucket: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    top_errors: dict[tuple[str, str, str], dict[str, Any]] = {}
    errors_by_component: dict[str, int] = defaultdict(int)
    bucket_totals: dict[str, int] = defaultdict(int)
    total_errors = 0
    total_warnings = 0
    total_fatal = 0

    with open(logfile, "r") as f:
        for line in tqdm(f, total=total_lines, desc="Parsing detailed errors", unit="lines"):
            try:
                entry = json.loads(line)
            except Exception:
                continue

            timestamp = entry.get("t", {}).get("$date")
            if not timestamp:
                continue

            message = str(entry.get("msg", ""))
            severity = str(entry.get("s", ""))
            component = str(entry.get("c", "Unknown"))
            message_lc = message.lower()
            has_error_keyword = "error" in message_lc or "warning" in message_lc
            if severity not in {"E", "W", "F"} and not has_error_keyword:
                continue

            bucket_ts = _to_minute_bucket(timestamp)
            severity_key = severity if severity in {"E", "W", "F"} else "W"
            severity_by_bucket[bucket_ts][severity_key] += 1
            bucket_totals[bucket_ts] += 1
            errors_by_component[component] += 1

            if severity_key == "E":
                total_errors += 1
            elif severity_key == "W":
                total_warnings += 1
            elif severity_key == "F":
                total_fatal += 1

            error_key = (message, component, severity_key)
            if error_key not in top_errors:
                top_errors[error_key] = {
                    "message": message,
                    "component": component,
                    "severity": severity_key,
                    "count": 0,
                    "first_seen": timestamp,
                    "last_seen": timestamp,
                }
            top_errors[error_key]["count"] += 1
            if timestamp < top_errors[error_key]["first_seen"]:
                top_errors[error_key]["first_seen"] = timestamp
            if timestamp > top_errors[error_key]["last_seen"]:
                top_errors[error_key]["last_seen"] = timestamp

    if not bucket_totals:
        empty_data = {
            "errors_timeline": [],
            "top_errors": [],
            "errors_by_component": {},
            "error_spikes": [],
            "total_errors": 0,
            "total_warnings": 0,
            "total_fatal": 0,
        }
        save_to_cache(cache_key, {"data": empty_data})
        return empty_data

    timeline = []
    for bucket_ts in sorted(severity_by_bucket.keys()):
        for sev in ["F", "E", "W"]:
            count = severity_by_bucket[bucket_ts].get(sev, 0)
            if count > 0:
                timeline.append({"bucket_ts": bucket_ts, "severity": sev, "count": count})

    baseline = float(median(bucket_totals.values())) if bucket_totals else 0.0
    threshold = baseline * 3.0 if baseline > 0 else float("inf")
    error_spikes = [
        {"bucket_ts": bucket_ts, "count": total, "baseline": baseline}
        for bucket_ts, total in sorted(bucket_totals.items())
        if total > threshold
    ]

    top_error_rows = sorted(top_errors.values(), key=lambda item: item["count"], reverse=True)[:50]
    data = {
        "errors_timeline": timeline,
        "top_errors": top_error_rows,
        "errors_by_component": dict(
            sorted(errors_by_component.items(), key=lambda item: item[1], reverse=True)
        ),
        "error_spikes": error_spikes,
        "total_errors": total_errors,
        "total_warnings": total_warnings,
        "total_fatal": total_fatal,
    }
    save_to_cache(cache_key, {"data": data})
    return data


def parse_collscan_trends(logfile: str) -> dict[str, Any]:
    """Parse detailed COLLSCAN trends for query diagnostics."""
    cache_key = get_cache_key(logfile, "collscan_trends")
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached COLLSCAN trend data...")
        return cached_result["data"]

    total_lines = count_lines(logfile)
    bucket_counts: dict[str, dict[str, int]] = defaultdict(lambda: {"collscan": 0, "ixscan": 0})
    namespace_totals: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "count": 0,
            "total_duration_ms": 0,
            "patterns": Counter(),
        }
    )
    total_collscans = 0
    total_ixscans = 0
    total_collscan_duration_ms = 0

    with open(logfile, "r") as f:
        for line in tqdm(f, total=total_lines, desc="Parsing COLLSCAN trends", unit="lines"):
            try:
                entry = json.loads(line)
            except Exception:
                continue

            if entry.get("c") != "COMMAND" or entry.get("msg") not in ("command", "Slow query"):
                continue

            attr = entry.get("attr", {})
            timestamp = entry.get("t", {}).get("$date")
            namespace = attr.get("ns", "")
            command = attr.get("command", {})
            plan_summary = str(attr.get("planSummary", ""))
            if not timestamp or not namespace or not command:
                continue

            bucket_ts = _to_minute_bucket(timestamp)
            operation = list(command.keys())[0] if command else "unknown"
            duration_ms = int(attr.get("durationMillis", 0) or 0)

            if plan_summary == "COLLSCAN":
                total_collscans += 1
                total_collscan_duration_ms += duration_ms
                bucket_counts[bucket_ts]["collscan"] += 1
                pattern = extract_query_pattern(operation, command)
                namespace_totals[namespace]["count"] += 1
                namespace_totals[namespace]["total_duration_ms"] += duration_ms
                namespace_totals[namespace]["patterns"][pattern] += 1
            elif plan_summary.startswith("IXSCAN"):
                total_ixscans += 1
                bucket_counts[bucket_ts]["ixscan"] += 1

    collscan_timeline = []
    ratio_timeline = []
    for bucket_ts in sorted(bucket_counts.keys()):
        collscan_count = bucket_counts[bucket_ts]["collscan"]
        ixscan_count = bucket_counts[bucket_ts]["ixscan"]
        denominator = collscan_count + ixscan_count
        ratio = (collscan_count / denominator) if denominator > 0 else 0.0
        collscan_timeline.append({"bucket_ts": bucket_ts, "count": collscan_count})
        ratio_timeline.append(
            {
                "bucket_ts": bucket_ts,
                "collscan_count": collscan_count,
                "ixscan_count": ixscan_count,
                "ratio": round(ratio, 4),
            }
        )

    top_namespaces = []
    for namespace, stats in namespace_totals.items():
        top_pattern = ""
        if stats["patterns"]:
            top_pattern = stats["patterns"].most_common(1)[0][0]
        top_namespaces.append(
            {
                "namespace": namespace,
                "count": stats["count"],
                "total_duration_ms": stats["total_duration_ms"],
                "top_pattern": top_pattern,
            }
        )
    top_namespaces.sort(key=lambda item: item["count"], reverse=True)

    data = {
        "collscan_timeline": collscan_timeline,
        "scan_ratio_timeline": ratio_timeline,
        "collscan_top_namespaces": top_namespaces[:20],
        "total_collscans": total_collscans,
        "total_ixscans": total_ixscans,
        "total_collscan_duration_ms": total_collscan_duration_ms,
    }
    save_to_cache(cache_key, {"data": data})
    return data


def parse_repl_health(logfile: str) -> dict[str, Any]:
    """Parse replication health events including elections and rollbacks."""
    cache_key = get_cache_key(logfile, "repl_health")
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached replication health data...")
        return cached_result["data"]

    total_lines = count_lines(logfile)
    repl_events = []
    elections = []
    rollbacks = []
    heartbeat_failures = []
    no_primary_periods = []
    primary_lost_at = None

    with open(logfile, "r") as f:
        for line in tqdm(f, total=total_lines, desc="Parsing replication health", unit="lines"):
            try:
                entry = json.loads(line)
            except Exception:
                continue

            if entry.get("c") != "REPL":
                continue

            timestamp = entry.get("t", {}).get("$date")
            if not timestamp:
                continue
            message = str(entry.get("msg", ""))
            msg_lc = message.lower()
            attr = entry.get("attr", {}) if isinstance(entry.get("attr", {}), dict) else {}

            event_type = ""
            if "rollback" in msg_lc:
                event_type = "rollback"
            elif "stepped down" in msg_lc:
                event_type = "stepdown"
            elif "election" in msg_lc or "vote" in msg_lc:
                event_type = "election"
            elif "initial sync" in msg_lc:
                event_type = "sync"
            elif "heartbeat" in msg_lc and ("fail" in msg_lc or "timeout" in msg_lc):
                event_type = "heartbeat_failure"
            elif "catchup" in msg_lc:
                event_type = "catchup"

            if message == "Replica set state transition":
                old_state = str(attr.get("oldState", ""))
                new_state = str(attr.get("newState", ""))
                if old_state == "PRIMARY" and new_state != "PRIMARY":
                    primary_lost_at = timestamp
                if new_state == "PRIMARY" and primary_lost_at:
                    try:
                        start_dt = datetime.fromisoformat(primary_lost_at.replace("Z", "+00:00"))
                        end_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                        seconds = (end_dt - start_dt).total_seconds()
                    except Exception:
                        seconds = 0.0
                    no_primary_periods.append(
                        {
                            "start": primary_lost_at,
                            "end": timestamp,
                            "duration_seconds": max(seconds, 0.0),
                        }
                    )
                    primary_lost_at = None

            if not event_type:
                continue

            event = {
                "timestamp": timestamp,
                "event_type": event_type,
                "message": message,
                "details": attr,
            }
            repl_events.append(event)

            if event_type == "election":
                elections.append(
                    {
                        "timestamp": timestamp,
                        "reason": str(attr.get("reason", "")),
                        "duration_ms": attr.get("durationMillis"),
                        "outcome": str(attr.get("outcome", "")),
                    }
                )
            elif event_type == "rollback":
                rollbacks.append(event)
            elif event_type == "heartbeat_failure":
                heartbeat_failures.append(event)

    stability_score = (
        100 - (10 * len(elections)) - (20 * len(rollbacks)) - (5 * len(heartbeat_failures))
    )
    data = {
        "repl_events": repl_events,
        "elections": elections,
        "rollbacks": rollbacks,
        "heartbeat_failures": heartbeat_failures,
        "stability_score": max(stability_score, 0),
        "no_primary_periods": no_primary_periods,
        "has_elections": len(elections) > 0,
        "has_rollbacks": len(rollbacks) > 0,
    }
    save_to_cache(cache_key, {"data": data})
    return data


def parse_lock_contention(logfile: str) -> dict[str, Any]:
    """Parse lock contention and flow control diagnostics."""
    cache_key = get_cache_key(logfile, "lock_contention")
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached lock contention data...")
        return cached_result["data"]

    total_lines = count_lines(logfile)
    events = []
    by_type = defaultdict(int)
    timeline = defaultdict(lambda: defaultdict(int))
    checkpoint_durations = []
    flow_control_periods = []
    flow_start = None

    with open(logfile, "r") as f:
        for line in tqdm(f, total=total_lines, desc="Parsing lock contention", unit="lines"):
            try:
                entry = json.loads(line)
            except Exception:
                continue

            timestamp = entry.get("t", {}).get("$date")
            if not timestamp:
                continue

            component = str(entry.get("c", ""))
            message = str(entry.get("msg", ""))
            msg_lc = message.lower()
            attr = entry.get("attr", {}) if isinstance(entry.get("attr", {}), dict) else {}

            if component != "STORAGE" and not any(
                key in msg_lc for key in ["flowcontrol", "ticket", "checkpoint", "wiredtiger"]
            ):
                continue

            event_type = ""
            if "flowcontrol" in msg_lc:
                event_type = "flowcontrol"
            elif "ticket" in msg_lc:
                event_type = "ticket"
            elif "checkpoint" in msg_lc:
                event_type = "checkpoint"
            elif "transaction too large" in msg_lc or ("cache" in msg_lc and "pressure" in msg_lc):
                event_type = "transaction"
            else:
                continue

            duration_ms = attr.get("durationMillis")
            if duration_ms is None and event_type == "checkpoint":
                duration_ms = attr.get("duration_ms")

            event = {
                "timestamp": timestamp,
                "event_type": event_type,
                "details": attr,
                "duration_ms": duration_ms,
            }
            events.append(event)
            by_type[event_type] += 1

            bucket_ts = _to_minute_bucket(timestamp)
            timeline[bucket_ts][event_type] += 1

            if event_type == "checkpoint" and isinstance(duration_ms, (int, float)):
                checkpoint_durations.append(
                    {"timestamp": timestamp, "duration_ms": int(duration_ms)}
                )

            if event_type == "flowcontrol":
                if flow_start is None:
                    flow_start = timestamp
                elif flow_start is not None:
                    flow_control_periods.append({"start": flow_start, "end": timestamp})
                    flow_start = None

    if flow_start is not None:
        flow_control_periods.append({"start": flow_start, "end": None})

    timeline_rows = []
    for bucket_ts in sorted(timeline.keys()):
        for event_type, count in sorted(timeline[bucket_ts].items()):
            timeline_rows.append({"bucket_ts": bucket_ts, "event_type": event_type, "count": count})

    data = {
        "contention_events": events,
        "contention_timeline": timeline_rows,
        "checkpoint_durations": checkpoint_durations,
        "flow_control_periods": flow_control_periods,
        "contention_total_by_type": dict(by_type),
        "has_contention": len(events) > 0,
    }
    save_to_cache(cache_key, {"data": data})
    return data


def parse_auth_failures(logfile: str) -> dict[str, Any]:
    """Parse authentication and authorization failure diagnostics."""
    cache_key = get_cache_key(logfile, "auth_failures")
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached auth failure data...")
        return cached_result["data"]

    total_lines = count_lines(logfile)
    by_user = defaultdict(int)
    by_ip = defaultdict(int)
    by_type = defaultdict(int)
    by_bucket = defaultdict(int)
    grouped_failures = {}

    with open(logfile, "r") as f:
        for line in tqdm(f, total=total_lines, desc="Parsing auth failures", unit="lines"):
            try:
                entry = json.loads(line)
            except Exception:
                continue

            if entry.get("c") != "ACCESS":
                continue

            timestamp = entry.get("t", {}).get("$date")
            if not timestamp:
                continue

            message = str(entry.get("msg", ""))
            msg_lc = message.lower()
            severity = str(entry.get("s", ""))
            attr = entry.get("attr", {}) if isinstance(entry.get("attr", {}), dict) else {}

            failure_type = ""
            if "authentication failed" in msg_lc or "scram" in msg_lc:
                failure_type = "authn"
            elif "not authorized" in msg_lc or "unauthorized" in msg_lc:
                failure_type = "authz"
            elif severity in {"E", "W"}:
                failure_type = "authn"

            if not failure_type:
                continue

            user = str(attr.get("user") or attr.get("principalName") or "")
            ip_source = str(attr.get("remote") or attr.get("client") or "")
            client_ip = ip_source.split(":")[0] if ip_source else ""

            if user:
                by_user[user] += 1
            if client_ip:
                by_ip[client_ip] += 1
            by_type[failure_type] += 1
            bucket_ts = _to_minute_bucket(timestamp)
            by_bucket[bucket_ts] += 1

            group_key = (user, client_ip, message)
            if group_key not in grouped_failures:
                grouped_failures[group_key] = {
                    "user": user,
                    "ip": client_ip,
                    "reason": message,
                    "count": 0,
                    "first_seen": timestamp,
                    "last_seen": timestamp,
                }
            grouped_failures[group_key]["count"] += 1
            if timestamp < grouped_failures[group_key]["first_seen"]:
                grouped_failures[group_key]["first_seen"] = timestamp
            if timestamp > grouped_failures[group_key]["last_seen"]:
                grouped_failures[group_key]["last_seen"] = timestamp

    timeline = [
        {"bucket_ts": bucket_ts, "count": count} for bucket_ts, count in sorted(by_bucket.items())
    ]

    baseline = float(median(by_bucket.values())) if by_bucket else 0.0
    burst_threshold = baseline * 3.0 if baseline > 0 else float("inf")
    auth_burst_periods = [
        {"start": bucket_ts, "end": bucket_ts, "count": count, "baseline": baseline}
        for bucket_ts, count in sorted(by_bucket.items())
        if count > burst_threshold
    ]

    top_failures = sorted(grouped_failures.values(), key=lambda item: item["count"], reverse=True)[
        :30
    ]
    data = {
        "auth_timeline": timeline,
        "auth_by_user": dict(sorted(by_user.items(), key=lambda item: item[1], reverse=True)),
        "auth_by_ip": dict(sorted(by_ip.items(), key=lambda item: item[1], reverse=True)),
        "auth_by_type": dict(by_type),
        "auth_top_failures": top_failures,
        "auth_total_failures": sum(by_bucket.values()),
        "auth_burst_periods": auth_burst_periods,
        "has_auth_failures": sum(by_bucket.values()) > 0,
    }
    save_to_cache(cache_key, {"data": data})
    return data
