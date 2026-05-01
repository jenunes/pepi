"""Statistics calculations for MongoDB log analysis."""

from __future__ import annotations

from typing import Any, Optional


def _mean_ratio(numerators: list[int], denominators: list[int]) -> float:
    """Mean of numerators[i]/denominators[i] for entries where denominator > 0."""
    if not numerators or not denominators:
        return 0.0
    n = min(len(numerators), len(denominators))
    if n == 0:
        return 0.0
    ratios: list[float] = []
    for i in range(n):
        d = int(denominators[i])
        if d <= 0:
            continue
        ratios.append(float(numerators[i]) / float(d))
    if not ratios:
        return 0.0
    return sum(ratios) / len(ratios)


def _bool_list_pct(values: list[bool]) -> float:
    if not values:
        return 0.0
    return 100.0 * sum(1 for v in values if v) / len(values)


def calculate_query_stats(queries_data: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Calculate query statistics including percentiles, grouped by pattern."""
    stats: dict[str, dict[str, Any]] = {}

    for group_key, query_info in queries_data.items():
        if not query_info['durations']:
            continue

        durations = list(query_info['durations'])
        durations.sort()

        # Calculate statistics
        count = len(durations)
        min_duration = min(durations)
        max_duration = max(durations)
        sum_duration = sum(durations)
        mean_duration = sum_duration / count

        # Calculate 95th percentile
        percentile_95 = durations[int(0.95 * count) - 1] if count > 0 else 0

        keys_ex = list(query_info.get('keysExamined') or [])
        docs_ex = list(query_info.get('docsExamined') or [])
        nret = list(query_info.get('nreturned') or [])
        has_sort = list(query_info.get('hasSortStage') or [])
        used_disk = list(query_info.get('usedDisk') or [])
        num_yields = list(query_info.get('numYields') or [])
        reslen_list = list(query_info.get('reslen') or [])

        scan_ratio = _mean_ratio(docs_ex, nret)
        key_efficiency = _mean_ratio(keys_ex, docs_ex)
        in_memory_sort_pct = _bool_list_pct(has_sort)
        disk_usage_pct = _bool_list_pct(used_disk)
        yield_rate = sum(num_yields) / len(num_yields) if num_yields else 0.0
        avg_response_size = sum(reslen_list) / len(reslen_list) if reslen_list else 0.0

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
            'indexes': query_info['indexes'],
            'scan_ratio': scan_ratio,
            'key_efficiency': key_efficiency,
            'in_memory_sort_pct': in_memory_sort_pct,
            'disk_usage_pct': disk_usage_pct,
            'yield_rate': yield_rate,
            'avg_response_size': avg_response_size,
            'keysExamined': keys_ex,
            'docsExamined': docs_ex,
            'nreturned': nret,
            'hasSortStage': has_sort,
            'usedDisk': used_disk,
            'numYields': num_yields,
            'reslen': reslen_list,
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
        'max': max(all_durations),
    }

    # Per-IP statistics
    ip_stats = {}
    for ip, conn_info in connections_data.items():
        if conn_info['durations']:
            ip_stats[ip] = {
                'avg': sum(conn_info['durations']) / len(conn_info['durations']),
                'min': min(conn_info['durations']),
                'max': max(conn_info['durations']),
            }

    return overall_stats, ip_stats
