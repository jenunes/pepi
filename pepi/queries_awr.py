"""Assemble AWR-style query analysis payloads from parsed query statistics."""

from __future__ import annotations

from typing import Any, Optional

from pepi.query_findings import aggregate_top_findings, generate_findings
from pepi.query_health import calculate_health_score, get_health_severity
from pepi.types import (
    AWRSummary,
    AWRTopPattern,
    EnrichedQuery,
    HealthDistribution,
    QueriesAnalysisData,
    QueryClientBreakdown,
    QueryClientBreakdownMeta,
    QueryDiagnosticsData,
    QueryFinding,
    QueryHealthBreakdown,
)


def _stats_for_health(namespace: str, operation: str, pattern: str, stats: dict[str, Any]) -> dict[str, Any]:
    merged = dict(stats)
    merged["namespace"] = namespace
    merged["operation"] = operation
    merged["pattern"] = pattern
    if isinstance(merged.get("indexes"), set):
        merged["indexes"] = list(merged["indexes"])
    return merged


def _to_enriched_row(
    namespace: str,
    operation: str,
    pattern: str,
    stats: dict[str, Any],
) -> EnrichedQuery:
    health_stats = _stats_for_health(namespace, operation, pattern, stats)
    breakdown = calculate_health_score(health_stats)
    findings = generate_findings(health_stats)

    indexes = health_stats.get("indexes", [])
    if isinstance(indexes, set):
        indexes = list(indexes)

    return EnrichedQuery(
        namespace=namespace,
        operation=operation,
        pattern=pattern,
        count=int(stats["count"]),
        min_ms=float(stats["min"]),
        max_ms=float(stats["max"]),
        mean_ms=float(stats["mean"]),
        percentile_95_ms=float(stats["percentile_95"]),
        sum_ms=float(stats["sum"]),
        allow_disk_use=bool(stats.get("allowDiskUse", False)),
        indexes=indexes,
        health_score=breakdown.total,
        health_severity=breakdown.severity,
        scan_ratio=float(stats.get("scan_ratio", 0.0)),
        key_efficiency=float(stats.get("key_efficiency", 0.0)),
        findings_count=len(findings),
        in_memory_sort_pct=float(stats.get("in_memory_sort_pct", 0.0)),
        disk_usage_pct=float(stats.get("disk_usage_pct", 0.0)),
        yield_rate=float(stats.get("yield_rate", 0.0)),
        avg_response_size=float(stats.get("avg_response_size", 0.0)),
    )


def _top_patterns(rows: list[EnrichedQuery], key: str, n: int = 5) -> list[AWRTopPattern]:
    def sort_key(r: EnrichedQuery) -> float:
        if key == "total_time":
            return float(r.count) * r.mean_ms
        if key == "mean_ms":
            return r.mean_ms
        if key == "scan_ratio":
            return r.scan_ratio
        if key == "count":
            return float(r.count)
        return 0.0

    sorted_rows = sorted(rows, key=sort_key, reverse=True)[:n]
    out: list[AWRTopPattern] = []
    for r in sorted_rows:
        val = sort_key(r)
        out.append(
            AWRTopPattern(
                namespace=r.namespace,
                operation=r.operation,
                pattern=r.pattern[:200],
                value=round(val, 3),
                health_score=r.health_score,
            )
        )
    return out


def build_queries_analysis_data(query_stats: dict[tuple[str, str, str], dict[str, Any]]) -> QueriesAnalysisData:
    if not query_stats:
        return QueriesAnalysisData(
            queries=[],
            total_patterns=0,
            summary=AWRSummary(health_distribution=HealthDistribution()),
            findings=[],
        )

    enriched: list[EnrichedQuery] = []
    all_findings_groups: list[list[QueryFinding]] = []

    for (namespace, operation, pattern), stats in query_stats.items():
        row = _to_enriched_row(namespace, operation, pattern, stats)
        enriched.append(row)
        hs = _stats_for_health(namespace, operation, pattern, stats)
        all_findings_groups.append(generate_findings(hs))

    collscan = sum(1 for r in enriched if "COLLSCAN" in r.indexes)
    in_mem = sum(1 for r in enriched if r.in_memory_sort_pct > 0)
    disk_spill = sum(1 for r in enriched if r.disk_usage_pct > 0)

    h_ok = sum(1 for r in enriched if r.health_severity == "HEALTHY")
    h_warn = sum(1 for r in enriched if r.health_severity == "WARNING")
    h_crit = sum(1 for r in enriched if r.health_severity == "CRITICAL")
    overall = int(round(sum(r.health_score for r in enriched) / len(enriched))) if enriched else 0

    summary = AWRSummary(
        top_by_total_time=_top_patterns(enriched, "total_time"),
        top_by_avg_latency=_top_patterns(enriched, "mean_ms"),
        top_by_scan_ratio=_top_patterns(enriched, "scan_ratio"),
        top_by_execution_count=_top_patterns(enriched, "count"),
        collection_scan_patterns=collscan,
        in_memory_sort_patterns=in_mem,
        disk_spill_patterns=disk_spill,
        overall_health_score=overall,
        health_distribution=HealthDistribution(healthy=h_ok, warning=h_warn, critical=h_crit),
    )

    top_findings = aggregate_top_findings(all_findings_groups, max_items=25)

    return QueriesAnalysisData(
        queries=enriched,
        total_patterns=len(enriched),
        summary=summary,
        findings=top_findings,
    )


def build_query_diagnostics_data(
    namespace: str,
    operation: str,
    pattern: str,
    stats: dict[str, Any],
    client_breakdown_payload: Optional[dict[str, Any]] = None,
) -> QueryDiagnosticsData:
    hs = _stats_for_health(namespace, operation, pattern, stats)
    health = calculate_health_score(hs)
    findings = list(generate_findings(hs))
    exec_stats = {
        "keysExamined": hs.get("keysExamined") or [],
        "docsExamined": hs.get("docsExamined") or [],
        "nreturned": hs.get("nreturned") or [],
        "hasSortStage": hs.get("hasSortStage") or [],
        "usedDisk": hs.get("usedDisk") or [],
        "numYields": hs.get("numYields") or [],
        "reslen": hs.get("reslen") or [],
        "scan_ratio": float(hs.get("scan_ratio", 0.0)),
        "key_efficiency": float(hs.get("key_efficiency", 0.0)),
        "in_memory_sort_pct": float(hs.get("in_memory_sort_pct", 0.0)),
        "disk_usage_pct": float(hs.get("disk_usage_pct", 0.0)),
    }

    client_breakdown: list[QueryClientBreakdown] = []
    client_breakdown_meta = QueryClientBreakdownMeta()
    if client_breakdown_payload:
        client_breakdown = [
            QueryClientBreakdown.model_validate(row)
            for row in client_breakdown_payload.get("clients", [])
        ]
        client_breakdown_meta = QueryClientBreakdownMeta(
            total_matched=int(client_breakdown_payload.get("total_matched", 0)),
            unknown_count=int(client_breakdown_payload.get("unknown_count", 0)),
            has_remote_pct=float(client_breakdown_payload.get("has_remote_pct", 0.0)),
            sampling_metadata=client_breakdown_payload.get("sampling_metadata") or {},
        )
        findings.extend(
            _client_breakdown_findings(
                operation=operation,
                stats=stats,
                client_breakdown=client_breakdown,
            )
        )

    return QueryDiagnosticsData(
        health=QueryHealthBreakdown.model_validate(health.model_dump()),
        findings=findings,
        exec_stats=exec_stats,
        client_breakdown=client_breakdown,
        client_breakdown_meta=client_breakdown_meta,
    )


def _client_breakdown_findings(
    *,
    operation: str,
    stats: dict[str, Any],
    client_breakdown: list[QueryClientBreakdown],
) -> list[QueryFinding]:
    extra: list[QueryFinding] = []
    if not client_breakdown:
        return extra

    top = client_breakdown[0]
    if top.pct >= 70.0 and top.ip != "unknown":
        extra.append(
            QueryFinding(
                severity="warning",
                category="client",
                title="Dominated by single client IP",
                detail=(
                    f"Client {top.ip} accounts for {top.pct:.1f}% of sampled events "
                    f"({top.count:,} of pattern count {int(stats.get('count', 0)):,})."
                ),
                recommendation=(
                    "Inspect this host's application pool, connection churn, and driver "
                    "settings (maxPoolSize, heartbeatFrequencyMS)."
                ),
            )
        )

    if operation == "hello" and int(stats.get("count", 0)) >= 100:
        extra.append(
            QueryFinding(
                severity="warning",
                category="client",
                title="Possible hello storm",
                detail=(
                    f"High volume of hello commands ({int(stats.get('count', 0)):,} sampled events). "
                    "Often caused by excessive topology polling or misconfigured clients."
                ),
                recommendation=(
                    "Review client heartbeat/topology settings and load balancer health checks. "
                    "Use the Clients tab to identify the busiest source IPs."
                ),
            )
        )
    return extra
