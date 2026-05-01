"""
Pydantic models and type definitions for Pepi.

Provides typed data structures for all core data flowing through
the parsing pipeline, API endpoints, and CLI output.
"""

from __future__ import annotations

from typing import Any, Optional, Set, Union

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Core domain models (used by parser / stats / cache layers)
# ---------------------------------------------------------------------------

class SamplingMetadata(BaseModel):
    total_lines: int
    is_sampled: bool
    sample_rate: Union[int, float]
    sampled_lines: int
    estimated_original_size: int
    is_user_forced: bool
    user_percentage: Optional[int] = None


class ConnectionData(BaseModel):
    opened: int = 0
    closed: int = 0
    durations: list[float] = Field(default_factory=list)


class ConnectionStats(BaseModel):
    avg: float
    min: float
    max: float


class ConnectionEvent(BaseModel):
    timestamp: str
    event_type: str
    ip: str
    connection_id: Optional[int] = None
    total_connections: int = 0
    log_message: str = ""


class DataQuality(BaseModel):
    validation_results: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    recommendations: list[str] = Field(default_factory=list)
    quality_score: float = 1.0
    is_consistent: bool = True


class SlowQuery(BaseModel):
    timestamp: str
    namespace: str
    operation: str
    duration_ms: int
    plan_summary: str = "N/A"
    command: dict[str, Any] = Field(default_factory=dict)


class ErrorEntry(BaseModel):
    timestamp: str
    message: str
    severity: str = ""


class QueryStats(BaseModel):
    count: int
    min: float
    max: float
    sum: float
    mean: float
    percentile_95: float
    allow_disk_use: bool = Field(default=False, alias="allowDiskUse")
    pattern: str = ""
    durations: list[float] = Field(default_factory=list)
    indexes: Union[list[str], Set[str]] = Field(default_factory=set)
    scan_ratio: float = 0.0
    key_efficiency: float = 0.0
    in_memory_sort_pct: float = 0.0
    disk_usage_pct: float = 0.0
    yield_rate: float = 0.0
    avg_response_size: float = 0.0

    model_config = {"populate_by_name": True}


class ReplicaSetConfig(BaseModel):
    timestamp: str
    config: dict[str, Any]


class ClientInfo(BaseModel):
    driver: str
    connections: int = 0
    ip_addresses: list[str] = Field(default_factory=list)
    users: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# API request models
# ---------------------------------------------------------------------------

class TrimRequest(BaseModel):
    from_date: Optional[str] = None
    until_date: Optional[str] = None


class QueryExamplesRequest(BaseModel):
    namespace: str
    operation: str
    pattern: str


class LogFilterRequest(BaseModel):
    text_search: Optional[str] = None
    case_sensitive: bool = False
    event_types: list[str] = Field(default_factory=list)
    components: list[str] = Field(default_factory=list)
    severities: list[str] = Field(default_factory=list)
    operations: list[str] = Field(default_factory=list)
    namespace: Optional[str] = None
    log_id: Optional[int] = None
    context: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    limit: int = 10000


class SingleQueryRequest(BaseModel):
    namespace: str
    operation: str
    pattern: str
    raw_log_line: Optional[str] = None
    stats: dict[str, Any]


class QueryDiagnosticsRequest(BaseModel):
    namespace: str
    operation: str
    pattern: str


class QueryFinding(BaseModel):
    severity: str
    category: str
    title: str
    detail: str
    recommendation: str


class QueryHealthBreakdown(BaseModel):
    plan_type_score: int
    scan_ratio_score: int
    key_efficiency_score: int
    sort_score: int
    latency_score: int
    disk_score: int
    total: int
    severity: str


class HealthDistribution(BaseModel):
    healthy: int = 0
    warning: int = 0
    critical: int = 0


class AWRTopPattern(BaseModel):
    namespace: str
    operation: str
    pattern: str
    value: float
    health_score: int


class AWRSummary(BaseModel):
    top_by_total_time: list[AWRTopPattern] = Field(default_factory=list)
    top_by_avg_latency: list[AWRTopPattern] = Field(default_factory=list)
    top_by_scan_ratio: list[AWRTopPattern] = Field(default_factory=list)
    top_by_execution_count: list[AWRTopPattern] = Field(default_factory=list)
    collection_scan_patterns: int = 0
    in_memory_sort_patterns: int = 0
    disk_spill_patterns: int = 0
    overall_health_score: int = 0
    health_distribution: HealthDistribution = Field(default_factory=HealthDistribution)


class EnrichedQuery(BaseModel):
    namespace: str
    operation: str
    pattern: str
    count: int
    min_ms: float
    max_ms: float
    mean_ms: float
    percentile_95_ms: float
    sum_ms: float
    allow_disk_use: bool = False
    indexes: list[str] = Field(default_factory=list)
    health_score: int = 0
    health_severity: str = "HEALTHY"
    scan_ratio: float = 0.0
    key_efficiency: float = 0.0
    findings_count: int = 0
    in_memory_sort_pct: float = 0.0
    disk_usage_pct: float = 0.0
    yield_rate: float = 0.0
    avg_response_size: float = 0.0

    model_config = {"populate_by_name": True}


class QueriesAnalysisData(BaseModel):
    queries: list[EnrichedQuery] = Field(default_factory=list)
    total_patterns: int = 0
    summary: AWRSummary = Field(default_factory=AWRSummary)
    findings: list[QueryFinding] = Field(default_factory=list)


class QueryDiagnosticsData(BaseModel):
    health: QueryHealthBreakdown
    findings: list[QueryFinding] = Field(default_factory=list)
    exec_stats: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# API response models
# ---------------------------------------------------------------------------

class AnalysisResult(BaseModel):
    status: str
    data: dict[str, Any]
    message: Optional[str] = None


class UploadResponse(BaseModel):
    file_id: str
    filename: str
    size: int
    lines: int
    message: str = "File uploaded successfully"


class FileInfo(BaseModel):
    file_id: str
    filename: str
    size: int
    lines: int
    is_preloaded: bool = False
    sample_percentage: int = 100
    preflight_tier: Optional[str] = None
    can_proceed: Optional[bool] = None


class FileListResponse(BaseModel):
    files: list[FileInfo]


class ExtractResponse(BaseModel):
    status: str = "success"
    total_scanned: int
    total_matched: int
    lines: list[str]
    truncated: bool = False


class EventTypesAvailable(BaseModel):
    COLLSCAN: bool = False
    IXSCAN: bool = False
    slow_query: bool = False
    error: bool = False


class FilterOptionsData(BaseModel):
    event_types: EventTypesAvailable
    components: list[str] = Field(default_factory=list)
    severities: list[str] = Field(default_factory=list)
    operations: list[str] = Field(default_factory=list)
    namespaces: list[str] = Field(default_factory=list)


class FilterOptionsResponse(BaseModel):
    status: str = "success"
    data: FilterOptionsData


class StatusMessage(BaseModel):
    status: str = "success"
    message: str = ""


class PreflightThresholds(BaseModel):
    warning_gb: float
    confirm_gb: float
    block_gb: float


class PreflightData(BaseModel):
    file_id: str
    size_bytes: int
    size_gb: float
    tier: str
    can_proceed: bool
    requires_confirmation: bool
    message: str
    recommendation: str = "trim_by_time_window"
    thresholds: PreflightThresholds


class PreflightResponse(BaseModel):
    status: str = "success"
    data: PreflightData


class IngestStatusData(BaseModel):
    job_id: str
    file_id: str
    status: str
    bytes_processed: int = 0
    lines_processed: int = 0
    started_at: float
    finished_at: Optional[float] = None
    error_message: Optional[str] = None


class IngestStatusResponse(BaseModel):
    status: str = "success"
    data: IngestStatusData


class TmpHealthData(BaseModel):
    tmp_dir: str
    free_bytes: int
    min_required_bytes: int
    headroom_factor: float
    has_space: bool


class TmpHealthResponse(BaseModel):
    status: str = "success"
    data: TmpHealthData
    message: Optional[str] = None


# ---------------------------------------------------------------------------
# Index advisor models
# ---------------------------------------------------------------------------

class IndexRecommendationStats(BaseModel):
    count: int = 0
    mean_ms: float = 0.0
    p95_ms: float = 0.0


class MigrationCommand(BaseModel):
    action: str
    command: str
    description: str = ""


class MigrationStrategy(BaseModel):
    type: str
    commands: list[MigrationCommand] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    estimated_impact: str = "low"


class CoverageAnalysis(BaseModel):
    coverage_score: int = 0
    esr_violations: list[str] = Field(default_factory=list)
    missing_fields: list[str] = Field(default_factory=list)
    suboptimal_order: list[str] = Field(default_factory=list)
    recommendation_type: str = "CREATE_NEW"
    improvement_details: list[str] = Field(default_factory=list)


class IndexRecommendationDetail(BaseModel):
    index_spec: Any = None
    command: str = ""
    reason: str = ""
    migration_strategy: Optional[MigrationStrategy] = None
    estimated_improvement: Optional[str] = None


class IndexRecommendation(BaseModel):
    namespace: str
    operation: str
    pattern: str
    current_index: str = "COLLSCAN"
    current_index_structure: list[Any] = Field(default_factory=list)
    stats: IndexRecommendationStats = Field(default_factory=IndexRecommendationStats)
    coverage_analysis: Optional[CoverageAnalysis] = None
    recommendation: IndexRecommendationDetail = Field(default_factory=IndexRecommendationDetail)
    priority: float = 0.0
    priority_level: str = "LOW"


# ---------------------------------------------------------------------------
# Upload store entry (internal state)
# ---------------------------------------------------------------------------

class UploadedFileInfo(BaseModel):
    path: str
    original_name: str
    size: int
    lines: int
    is_preloaded: bool = False
    sample_percentage: int = 100
