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
    repr_command: Optional[dict[str, Any]] = None
    sort_shape: str = ""
    projection_shape: str = ""
    has_limit: bool = False
    has_skip: bool = False
    aggregate_shape_summary: str = ""
    sum_docs_examined: int = 0
    sum_keys_examined: int = 0
    sum_n_returned: int = 0
    sum_planning_micros: int = 0
    avg_docs_examined: Optional[float] = None
    avg_keys_examined: Optional[float] = None
    avg_n_returned: Optional[float] = None
    avg_planning_micros: Optional[float] = None
    scan_efficiency: Optional[float] = None
    fetch_efficiency: Optional[float] = None
    exec_event_count: int = 0

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
    use_regex: bool = False
    event_types: list[str] = Field(default_factory=list)
    components: list[str] = Field(default_factory=list)
    severities: list[str] = Field(default_factory=list)
    operations: list[str] = Field(default_factory=list)
    namespace: Optional[str] = None
    log_id: Optional[int] = None
    context: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    min_duration_ms: Optional[int] = None
    slow_query_threshold_ms: Optional[int] = None
    limit: int = 10000


class SingleQueryRequest(BaseModel):
    namespace: str
    operation: str
    pattern: str
    raw_log_line: Optional[str] = None
    stats: dict[str, Any]


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


class MatchSummary(BaseModel):
    by_severity: dict[str, int] = Field(default_factory=dict)
    time_span_start: Optional[str] = None
    time_span_end: Optional[str] = None


class ExtractResponse(BaseModel):
    status: str = "success"
    total_scanned: int
    total_matched: int
    lines: list[str]
    match_line_numbers: list[int] = Field(default_factory=list)
    match_summary: Optional[MatchSummary] = None
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
    log_ts_min: Optional[str] = None
    log_ts_max: Optional[str] = None


class FilterOptionsResponse(BaseModel):
    status: str = "success"
    data: FilterOptionsData


class LogContextRequest(BaseModel):
    line_no: int = Field(..., ge=1)
    before: int = Field(default=5, ge=0, le=200)
    after: int = Field(default=5, ge=0, le=200)


class LogContextLine(BaseModel):
    line_no: int
    content: str
    is_target: bool = False


class LogContextResponse(BaseModel):
    status: str = "success"
    lines: list[LogContextLine]


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


class ErrorTimelineBucket(BaseModel):
    bucket_ts: str
    severity: str
    count: int


class TopErrorEntry(BaseModel):
    message: str
    component: str
    severity: str
    count: int
    first_seen: str
    last_seen: str


class ErrorSpike(BaseModel):
    bucket_ts: str
    count: int
    baseline: float


class ErrorsDetailResult(BaseModel):
    errors_timeline: list[ErrorTimelineBucket] = Field(default_factory=list)
    top_errors: list[TopErrorEntry] = Field(default_factory=list)
    errors_by_component: dict[str, int] = Field(default_factory=dict)
    error_spikes: list[ErrorSpike] = Field(default_factory=list)
    total_errors: int = 0
    total_warnings: int = 0
    total_fatal: int = 0


class ScanRatioBucket(BaseModel):
    bucket_ts: str
    collscan_count: int
    ixscan_count: int
    ratio: float


class CollscanNamespace(BaseModel):
    namespace: str
    count: int
    total_duration_ms: int
    top_pattern: str


class CollscanTimelinePoint(BaseModel):
    bucket_ts: str
    count: int


class CollscanTrendsResult(BaseModel):
    collscan_timeline: list[CollscanTimelinePoint] = Field(default_factory=list)
    scan_ratio_timeline: list[ScanRatioBucket] = Field(default_factory=list)
    collscan_top_namespaces: list[CollscanNamespace] = Field(default_factory=list)
    total_collscans: int = 0
    total_ixscans: int = 0
    total_collscan_duration_ms: int = 0


class ReplEvent(BaseModel):
    timestamp: str
    event_type: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class ElectionEvent(BaseModel):
    timestamp: str
    reason: str = ""
    duration_ms: Optional[int] = None
    outcome: str = ""


class NoPrimaryPeriod(BaseModel):
    start: str
    end: str
    duration_seconds: float


class ReplHealthResult(BaseModel):
    repl_events: list[ReplEvent] = Field(default_factory=list)
    elections: list[ElectionEvent] = Field(default_factory=list)
    rollbacks: list[ReplEvent] = Field(default_factory=list)
    heartbeat_failures: list[ReplEvent] = Field(default_factory=list)
    stability_score: int = 100
    no_primary_periods: list[NoPrimaryPeriod] = Field(default_factory=list)
    has_elections: bool = False
    has_rollbacks: bool = False


class ContentionEvent(BaseModel):
    timestamp: str
    event_type: str
    details: dict[str, Any] = Field(default_factory=dict)
    duration_ms: Optional[int] = None


class CheckpointDuration(BaseModel):
    timestamp: str
    duration_ms: int


class FlowControlPeriod(BaseModel):
    start: str
    end: Optional[str] = None


class ContentionTimelinePoint(BaseModel):
    bucket_ts: str
    event_type: str
    count: int


class LockContentionResult(BaseModel):
    contention_events: list[ContentionEvent] = Field(default_factory=list)
    contention_timeline: list[ContentionTimelinePoint] = Field(default_factory=list)
    checkpoint_durations: list[CheckpointDuration] = Field(default_factory=list)
    flow_control_periods: list[FlowControlPeriod] = Field(default_factory=list)
    contention_total_by_type: dict[str, int] = Field(default_factory=dict)
    has_contention: bool = False


class AuthFailure(BaseModel):
    user: str = ""
    ip: str = ""
    reason: str
    count: int
    first_seen: str
    last_seen: str


class AuthBurstPeriod(BaseModel):
    start: str
    end: str
    count: int
    baseline: float


class AuthTimelinePoint(BaseModel):
    bucket_ts: str
    count: int


class AuthFailuresResult(BaseModel):
    auth_timeline: list[AuthTimelinePoint] = Field(default_factory=list)
    auth_by_user: dict[str, int] = Field(default_factory=dict)
    auth_by_ip: dict[str, int] = Field(default_factory=dict)
    auth_by_type: dict[str, int] = Field(default_factory=dict)
    auth_top_failures: list[AuthFailure] = Field(default_factory=list)
    auth_total_failures: int = 0
    auth_burst_periods: list[AuthBurstPeriod] = Field(default_factory=list)
    has_auth_failures: bool = False


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


class ESRFieldBreakdown(BaseModel):
    field: str
    classification: str
    evidence: str = ""
    position_in_index: Optional[int] = None


class IndexRecommendationDetail(BaseModel):
    index_spec: Any = None
    command: str = ""
    reason: str = ""
    migration_strategy: Optional[MigrationStrategy] = None
    estimated_improvement: Optional[str] = None
    explain_command: str = ""


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
    esr_breakdown: list[ESRFieldBreakdown] = Field(default_factory=list)


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
