"""Statistics calculations for MongoDB log analysis."""

from __future__ import annotations

from typing import Any, Optional


def calculate_query_stats(queries_data: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Calculate query statistics including percentiles, grouped by pattern."""
    stats = {}

    for group_key, query_info in queries_data.items():
        if not query_info['durations']:
            continue

        durations = query_info['durations']
        durations.sort()

        # Calculate statistics
        count = len(durations)
        min_duration = min(durations)
        max_duration = max(durations)
        sum_duration = sum(durations)
        mean_duration = sum_duration / count

        # Calculate 95th percentile
        percentile_95 = durations[int(0.95 * count)-1] if count > 0 else 0

        stats[group_key] = {
            'count': count,
            'min': min_duration,
            'max': max_duration,
            'sum': sum_duration,
            'mean': mean_duration,
            'percentile_95': percentile_95,
            'allowDiskUse': query_info['allowDiskUse'],
            'pattern': query_info['pattern'],
            'durations': query_info['durations'],
            'indexes': query_info['indexes']
        }

    return stats


def calculate_connection_stats(
    connections_data: dict[str, dict[str, Any]],
) -> tuple[Optional[dict[str, Any]], dict[str, dict[str, Any]]]:
    """Calculate connection duration statistics."""
    all_durations = []
    for ip, conn_info in connections_data.items():
        all_durations.extend(conn_info['durations'])

    if not all_durations:
        return None, {}

    # Overall statistics
    overall_stats = {
        'avg': sum(all_durations) / len(all_durations),
        'min': min(all_durations),
        'max': max(all_durations)
    }

    # Per-IP statistics
    ip_stats = {}
    for ip, conn_info in connections_data.items():
        if conn_info['durations']:
            ip_stats[ip] = {
                'avg': sum(conn_info['durations']) / len(conn_info['durations']),
                'min': min(conn_info['durations']),
                'max': max(conn_info['durations'])
            }

    return overall_stats, ip_stats
