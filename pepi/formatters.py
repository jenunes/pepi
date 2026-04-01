"""Output formatting functions for CLI display."""

from __future__ import annotations

from typing import Any, Optional


def generate_histogram(durations: list[Any], max_bars: int = 50) -> str:
    """Generate histogram of execution time distribution."""
    if not durations:
        return "No data available for histogram."

    # Define time buckets (in milliseconds) with explicit range notation
    buckets = [
        (0, 1, "<1ms"),
        (1, 10, "[1,10)ms"),
        (10, 100, "[10,100)ms"),
        (100, 1000, "[100,1000)ms"),
        (1000, 10000, "[1,10)s"),
        (10000, float("inf"), "≥10s"),
    ]

    # Count durations in each bucket
    bucket_counts = {label: 0 for _, _, label in buckets}

    for duration in durations:
        for min_val, max_val, label in buckets:
            if min_val <= duration < max_val:
                bucket_counts[label] += 1
                break

    # Find the maximum count for scaling
    max_count = max(bucket_counts.values()) if bucket_counts.values() else 1

    # Generate histogram
    histogram_lines = ["# Execution time distribution"]

    for _, _, label in buckets:
        count = bucket_counts[label]
        if count > 0:
            # Scale the bar length
            bar_length = int((count / max_count) * max_bars) if max_count > 0 else 0
            bar = "#" * bar_length
            histogram_lines.append(f"{label:>6}  {bar} ({count})")

    return "\n".join(histogram_lines)


def reconstruct_command_line(options: Optional[dict[str, Any]]) -> Optional[str]:
    """Reconstruct the command line from MongoDB options."""
    if not options:
        return None

    cmd_parts = ["mongod"]

    # Config file
    if "config" in options:
        cmd_parts.append(f"--config {options['config']}")

    # Network options
    if "net" in options:
        net_opts = options["net"]
        if "port" in net_opts:
            cmd_parts.append(f"--port {net_opts['port']}")
        if "bindIp" in net_opts:
            cmd_parts.append(f"--bind_ip {net_opts['bindIp']}")

    # Process management
    if "processManagement" in options:
        pm_opts = options["processManagement"]
        if pm_opts.get("fork"):
            cmd_parts.append("--fork")

    # Replication
    if "replication" in options:
        repl_opts = options["replication"]
        if "replSetName" in repl_opts:
            cmd_parts.append(f"--replSet {repl_opts['replSetName']}")
        elif "replSet" in repl_opts:
            cmd_parts.append(f"--replSet {repl_opts['replSet']}")

    # Security
    if "security" in options:
        sec_opts = options["security"]
        if "keyFile" in sec_opts:
            cmd_parts.append(f"--keyFile {sec_opts['keyFile']}")
        if sec_opts.get("authorization") == "enabled":
            cmd_parts.append("--auth")

    # Storage
    if "storage" in options:
        storage_opts = options["storage"]
        if "dbPath" in storage_opts:
            cmd_parts.append(f"--dbpath {storage_opts['dbPath']}")

        if "wiredTiger" in storage_opts:
            wt_opts = storage_opts["wiredTiger"]
            if "engineConfig" in wt_opts:
                eng_opts = wt_opts["engineConfig"]
                if "cacheSizeGB" in eng_opts:
                    cmd_parts.append(f"--wiredTigerCacheSizeGB {eng_opts['cacheSizeGB']}")

    # System log
    if "systemLog" in options:
        syslog_opts = options["systemLog"]
        if "destination" in syslog_opts:
            cmd_parts.append(f"--logpath {syslog_opts.get('path', '/dev/null')}")

    return " ".join(cmd_parts)
