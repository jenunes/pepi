from __future__ import annotations

import collections
import errno
import json
import logging
import os
import socket
import threading
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from . import (
    calculate_connection_stats,
    calculate_query_stats,
    count_lines,
    get_date_range,
    get_sampling_metadata,
    parse_clients,
    parse_connection_events,
    parse_connections,
    parse_connections_timeseries_by_ip,
    parse_queries,
    parse_replica_set_config,
    parse_replica_set_state,
    parse_timeseries_data,
    trim_log_file,
    validate_connection_data_consistency,
)
from .errors import get_validated_file_path, validate_sample_param
from .index_advisor import analyze_queries as ia_analyze_queries
from .index_advisor import analyze_single_query
from .ingest_store import (
    bootstrap_schema,
    delete_file_ingest_data,
    get_connection,
    get_ingest_db_path,
    get_latest_job_for_file,
    query_connections_summary,
    query_extract,
    query_timeseries,
)
from .ingest_worker import run_ingest_job
from .types import (
    AnalysisResult,
    ExtractResponse,
    FileInfo,
    FileListResponse,
    FilterOptionsResponse,
    FsBrowseResponse,
    FtdcStartRequest,
    FtdcStatusResponse,
    IngestStatusResponse,
    LogFilterRequest,
    PreflightData,
    PreflightResponse,
    PreflightThresholds,
    QueryExamplesRequest,
    SingleQueryRequest,
    StatusMessage,
    TrimRequest,
    TmpHealthResponse,
    UploadResponse,
)

logger = logging.getLogger(__name__)

script_dir = Path(__file__).parent
web_static_dir = script_dir / "web_static"

_LOCALHOST_ORIGINS = [
    "http://localhost:8000", "http://127.0.0.1:8000",
    "http://localhost:8001", "http://127.0.0.1:8001",
    "http://localhost:8002", "http://127.0.0.1:8002",
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events for the FastAPI app."""
    os.makedirs(web_static_dir, exist_ok=True)

    app.state.upload_store = {}
    app.state.analysis_cache = {}
    app.state.ingest_db_path = get_ingest_db_path()
    app.state.ingest_conn = get_connection(app.state.ingest_db_path)
    bootstrap_schema(app.state.ingest_conn)
    app.state.ingest_runtime = {}
    app.state.upload_tmp_dir = resolve_upload_tmp_dir()

    cleanup_stale_upload_files(app.state.upload_tmp_dir)

    preload_file(app.state.upload_store)

    yield

    for info in app.state.upload_store.values():
        if not info.get('is_preloaded') and os.path.exists(info['path']):
            try:
                os.unlink(info['path'])
            except OSError:
                pass
    cleanup_stale_upload_files(app.state.upload_tmp_dir)
    app.state.ingest_conn.close()


app = FastAPI(title="Pepi MongoDB Log Analyzer", version="2.2.0", lifespan=lifespan)

app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_LOCALHOST_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    # Keep middleware in place but avoid noisy per-request terminal logs by default.
    if os.environ.get("PEPI_HTTP_LOG", "false").lower() != "true":
        return await call_next(request)
    start_ts = time.time()
    response = await call_next(request)
    elapsed_ms = (time.time() - start_ts) * 1000
    logger.debug(
        "%s %s -> %s (%.1fms)",
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
    )
    return response


# ---------------------------------------------------------------------------
# Dependency injection helpers
# ---------------------------------------------------------------------------

def get_upload_store(request: Request) -> dict:
    return request.app.state.upload_store


def get_analysis_cache(request: Request) -> dict:
    return request.app.state.analysis_cache


def get_ingest_conn(request: Request):
    return request.app.state.ingest_conn


def get_ingest_runtime(request: Request) -> dict:
    return request.app.state.ingest_runtime


def get_upload_tmp_dir(request: Request) -> str:
    return request.app.state.upload_tmp_dir


def _is_accepted_log_filename(filename: str) -> bool:
    """Accept .log, .txt, .json, and MongoDB rotated logs (e.g. mongod.log.2026-03-06T21-30-43)."""
    if filename.endswith(('.log', '.txt', '.json')):
        return True
    if '.log.' in filename:
        return True
    return False


def _get_size_thresholds() -> PreflightThresholds:
    return PreflightThresholds(
        warning_gb=float(os.environ.get("PEPI_FILE_WARN_GB", "1")),
        confirm_gb=float(os.environ.get("PEPI_FILE_CONFIRM_GB", "2")),
        block_gb=float(os.environ.get("PEPI_FILE_BLOCK_GB", "4")),
    )


def _build_preflight_data(file_id: str, size_bytes: int) -> PreflightData:
    size_gb = size_bytes / (1024**3)
    thresholds = _get_size_thresholds()
    allow_oversize = os.environ.get("PEPI_ALLOW_OVERSIZE", "false").lower() == "true"
    tier = "ok"
    can_proceed = True
    requires_confirmation = False
    if size_gb >= thresholds.block_gb:
        tier = "block"
        can_proceed = allow_oversize
    elif size_gb >= thresholds.confirm_gb:
        tier = "confirm"
        requires_confirmation = True
    elif size_gb >= thresholds.warning_gb:
        tier = "warning"
    message = "Large file detected. To avoid slow analysis, trim the file to the specific period you need."
    return PreflightData(
        file_id=file_id,
        size_bytes=size_bytes,
        size_gb=round(size_gb, 3),
        tier=tier,
        can_proceed=can_proceed,
        requires_confirmation=requires_confirmation,
        message=message,
        thresholds=thresholds,
    )


def _get_or_compute_line_count(upload_store: dict, file_id: str) -> int:
    current = int(upload_store[file_id].get("lines", 0) or 0)
    if current > 0:
        return current
    file_path = upload_store[file_id]["path"]
    computed = count_lines(file_path)
    upload_store[file_id]["lines"] = computed
    return computed


def resolve_upload_tmp_dir() -> str:
    configured = os.environ.get("PEPI_UPLOAD_TMPDIR")
    if configured:
        target = os.path.abspath(os.path.expanduser(configured))
    else:
        env_tmp = os.environ.get("TMPDIR")
        target = os.path.abspath(os.path.expanduser(env_tmp)) if env_tmp else tempfile.gettempdir()
    Path(target).mkdir(parents=True, exist_ok=True)
    if not os.access(target, os.W_OK):
        raise RuntimeError(f"Upload temp dir is not writable: {target}")
    return target


def get_free_bytes(path: str) -> int:
    stat = os.statvfs(path)
    return stat.f_bavail * stat.f_frsize


def get_tmp_requirements() -> tuple[int, float]:
    min_free_mb = int(os.environ.get("PEPI_UPLOAD_MIN_FREE_MB", "0"))
    headroom_factor = float(os.environ.get("PEPI_UPLOAD_HEADROOM_FACTOR", "1.5"))
    return min_free_mb * 1024 * 1024, headroom_factor


def assert_min_free_space(tmp_dir: str, required_bytes: int, headroom_factor: float) -> None:
    free_bytes = get_free_bytes(tmp_dir)
    required_with_headroom = int(required_bytes * max(1.0, headroom_factor))
    if free_bytes < required_with_headroom:
        raise OSError(errno.ENOSPC, "No space left on device")


def estimate_required_upload_bytes(request: Request, min_required_bytes: int, headroom_factor: float) -> int:
    content_length = request.headers.get("content-length")
    if not content_length:
        return min_required_bytes
    try:
        length = int(content_length)
    except ValueError:
        return min_required_bytes
    return max(min_required_bytes, int(length * max(1.0, headroom_factor)))


def cleanup_stale_upload_files(tmp_dir: str) -> None:
    max_age_seconds = int(os.environ.get("PEPI_TMP_CLEANUP_MAX_AGE_SECONDS", "86400"))
    now = time.time()
    for path in Path(tmp_dir).glob("pepi_upload_*.log"):
        try:
            if now - path.stat().st_mtime > max_age_seconds:
                path.unlink(missing_ok=True)
        except OSError:
            continue


app.mount("/static", StaticFiles(directory=str(web_static_dir)), name="static")


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the main dashboard page."""
    try:
        index_file = web_static_dir / "index.html"
        with open(index_file, "r") as f:
            return f.read()
    except FileNotFoundError:
        return HTMLResponse("<h1>Web interface not found. Please create web_static/index.html</h1>")


@app.post("/api/upload", response_model=UploadResponse)
async def upload_log_file(
    request: Request,
    file: UploadFile = File(...),
    upload_store: dict = Depends(get_upload_store),
    upload_tmp_dir: str = Depends(get_upload_tmp_dir),
):
    """Upload and store a MongoDB log file."""
    if not _is_accepted_log_filename(file.filename):
        raise HTTPException(
            status_code=400,
            detail="Unsupported file type. Accepted: .log, .txt, .json, and MongoDB rotated logs",
        )

    temp_file = tempfile.NamedTemporaryFile(
        delete=False,
        suffix=".log",
        prefix="pepi_upload_",
        dir=upload_tmp_dir,
    )
    try:
        file_size = 0
        min_required_bytes, headroom_factor = get_tmp_requirements()
        required_bytes = estimate_required_upload_bytes(request, min_required_bytes, headroom_factor)
        assert_min_free_space(upload_tmp_dir, required_bytes, 1.0)
        while chunk := await file.read(1024 * 1024):
            temp_file.write(chunk)
            file_size += len(chunk)
        temp_file.close()

        file_id = os.path.basename(temp_file.name)
        upload_store[file_id] = {
            'path': temp_file.name,
            'original_name': file.filename,
            'size': file_size,
            # Deferred line counting for large uploads.
            'lines': 0,
        }

        return UploadResponse(
            file_id=file_id,
            filename=file.filename,
            size=file_size,
            lines=0,
        )
    except OSError as e:
        logger.exception("Upload failed for filename=%s", file.filename)
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
        if e.errno == errno.ENOSPC:
            raise HTTPException(
                status_code=507,
                detail={
                    "error_code": "NO_SPACE_LEFT",
                    "detail": "No space left on device for upload temporary files.",
                    "hint": "Free disk space, set PEPI_UPLOAD_TMPDIR to a larger partition, or trim the file to a smaller time range.",
                },
            )
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")
    except Exception as e:
        logger.exception("Upload failed for filename=%s", file.filename)
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@app.get("/api/system/tmp-health", response_model=TmpHealthResponse)
def get_tmp_health(
    upload_tmp_dir: str = Depends(get_upload_tmp_dir),
):
    min_required_bytes, headroom_factor = get_tmp_requirements()
    free_bytes = get_free_bytes(upload_tmp_dir)
    has_space = free_bytes >= min_required_bytes
    message = None
    if not has_space:
        message = (
            "Low temporary storage space. Large file uploads may fail. "
            "Consider PEPI_UPLOAD_TMPDIR or trimming logs by time range."
        )
    return TmpHealthResponse(
        data={
            "tmp_dir": upload_tmp_dir,
            "free_bytes": free_bytes,
            "min_required_bytes": min_required_bytes,
            "headroom_factor": headroom_factor,
            "has_space": has_space,
        },
        message=message,
    )

@app.get("/api/files", response_model=FileListResponse)
def list_uploaded_files(upload_store: dict = Depends(get_upload_store)):
    """List all uploaded files."""
    files = []
    stale_ids = []
    for file_id, info in upload_store.items():
        if os.path.exists(info['path']):
            preflight = _build_preflight_data(file_id, info["size"])
            files.append(FileInfo(
                file_id=file_id,
                filename=info['original_name'],
                size=info['size'],
                lines=info['lines'],
                is_preloaded=info.get('is_preloaded', False),
                sample_percentage=info.get('sample_percentage', 100),
                preflight_tier=preflight.tier,
                can_proceed=preflight.can_proceed,
            ))
        else:
            stale_ids.append(file_id)
    for fid in stale_ids:
        del upload_store[fid]
    return FileListResponse(files=files)


@app.get("/api/files/{file_id}/preflight", response_model=PreflightResponse)
def get_file_preflight(
    file_id: str,
    upload_store: dict = Depends(get_upload_store),
):
    get_validated_file_path(file_id, upload_store)
    info = upload_store[file_id]
    data = _build_preflight_data(file_id, info["size"])
    return PreflightResponse(data=data)


@app.post("/api/ingest/{file_id}/start", response_model=IngestStatusResponse)
def start_ingest(
    file_id: str,
    force: bool = False,
    upload_store: dict = Depends(get_upload_store),
    ingest_conn=Depends(get_ingest_conn),
    ingest_runtime: dict = Depends(get_ingest_runtime),
):
    file_path = get_validated_file_path(file_id, upload_store)
    preflight = _build_preflight_data(file_id, upload_store[file_id]["size"])
    if not preflight.can_proceed and not force:
        raise HTTPException(status_code=400, detail=preflight.message)
    latest = get_latest_job_for_file(ingest_conn, file_id)
    if latest and latest["status"] == "running":
        return IngestStatusResponse(status="success", data=latest)

    job_id = f"{file_id}:{int(time.time())}"
    cancel_event = threading.Event()
    worker = threading.Thread(
        target=run_ingest_job,
        kwargs={
            "conn": ingest_conn,
            "file_id": file_id,
            "file_path": file_path,
            "job_id": job_id,
            "cancel_event": cancel_event,
        },
        daemon=True,
    )
    ingest_runtime[file_id] = {"job_id": job_id, "cancel_event": cancel_event, "thread": worker}
    worker.start()
    data = {
        "job_id": job_id,
        "file_id": file_id,
        "status": "running",
        "bytes_processed": 0,
        "lines_processed": 0,
        "started_at": time.time(),
        "finished_at": None,
        "error_message": None,
    }
    return IngestStatusResponse(status="success", data=data)


@app.get("/api/ingest/{file_id}/status", response_model=IngestStatusResponse)
def ingest_status(
    file_id: str,
    upload_store: dict = Depends(get_upload_store),
    ingest_conn=Depends(get_ingest_conn),
):
    get_validated_file_path(file_id, upload_store)
    latest = get_latest_job_for_file(ingest_conn, file_id)
    if not latest:
        now = time.time()
        latest = {
            "job_id": f"{file_id}:none",
            "file_id": file_id,
            "status": "not_started",
            "bytes_processed": 0,
            "lines_processed": 0,
            "started_at": now,
            "finished_at": None,
            "error_message": None,
        }
    return IngestStatusResponse(status="success", data=latest)


@app.post("/api/ingest/{file_id}/cancel", response_model=IngestStatusResponse)
def cancel_ingest(
    file_id: str,
    upload_store: dict = Depends(get_upload_store),
    ingest_conn=Depends(get_ingest_conn),
    ingest_runtime: dict = Depends(get_ingest_runtime),
):
    get_validated_file_path(file_id, upload_store)
    runtime = ingest_runtime.get(file_id)
    if runtime:
        runtime["cancel_event"].set()
    latest = get_latest_job_for_file(ingest_conn, file_id)
    if not latest:
        now = time.time()
        latest = {
            "job_id": f"{file_id}:none",
            "file_id": file_id,
            "status": "cancelled",
            "bytes_processed": 0,
            "lines_processed": 0,
            "started_at": now,
            "finished_at": now,
            "error_message": None,
        }
    return IngestStatusResponse(status="success", data=latest)

@app.post("/api/analyze/{file_id}/basic", response_model=AnalysisResult)
def analyze_basic_info(
    file_id: str,
    sample: Optional[int] = 100,
    upload_store: dict = Depends(get_upload_store),
):
    """Get basic log file information."""
    file_path = get_validated_file_path(file_id, upload_store)

    try:
        start_date = end_date = None
        os_version = kernel_version = db_version = cmd_options = None

        # Single pass: read first 1000 lines (covers start_date + version info)
        first_lines: list[str] = []
        with open(file_path, 'r') as f:
            for i, line in enumerate(f):
                if i >= 1000:
                    break
                first_lines.append(line)

        for line in first_lines[:100]:
            try:
                entry = json.loads(line.strip())
                if entry.get('t', {}).get('$date'):
                    start_date = entry['t']['$date']
                    break
            except (json.JSONDecodeError, ValueError, KeyError):
                continue

        for line in first_lines:
            try:
                entry = json.loads(line)
                if entry.get('msg') == 'Operating System':
                    os_info = entry.get('attr', {}).get('os', {})
                    os_version = os_info.get('name')
                    kernel_version = os_info.get('version')
                elif entry.get('msg') == 'Build Info':
                    db_version = entry.get('attr', {}).get('buildInfo', {}).get('version')
                elif entry.get('msg') == 'Options set by command line':
                    cmd_options = entry.get('attr', {}).get('options')
            except (json.JSONDecodeError, ValueError, KeyError):
                continue

        # Stream last 100 lines without loading entire file
        last_lines: collections.deque[str] = collections.deque(maxlen=100)
        with open(file_path, 'r') as f:
            for line in f:
                last_lines.append(line)

        for line in reversed(last_lines):
            try:
                entry = json.loads(line.strip())
                if entry.get('t', {}).get('$date'):
                    end_date = entry['t']['$date']
                    break
            except (json.JSONDecodeError, ValueError, KeyError):
                continue

        total_lines = _get_or_compute_line_count(upload_store, file_id)
        sampling_metadata = get_sampling_metadata(total_lines, sample)

        return AnalysisResult(
            status="success",
            data={
                "filename": upload_store[file_id]['original_name'],
                "size": upload_store[file_id]['size'],
                "lines": total_lines,
                "start_date": start_date,
                "end_date": end_date,
                "os_version": os_version,
                "kernel_version": kernel_version,
                "db_version": db_version,
                "startup_options": cmd_options,
                "sampling_metadata": sampling_metadata,
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

@app.post("/api/analyze/{file_id}/connections", response_model=AnalysisResult)
def analyze_connections(
    file_id: str,
    sample: Optional[int] = 100,
    include_details: bool = False,
    source: str = "raw",
    upload_store: dict = Depends(get_upload_store),
    ingest_conn=Depends(get_ingest_conn),
):
    """Analyze connection information with time series data."""
    file_path = get_validated_file_path(file_id, upload_store)
    validate_sample_param(sample)
    if source not in {"raw", "ingest"}:
        raise HTTPException(status_code=400, detail="Invalid source. Allowed: raw, ingest")

    try:
        if source == "ingest":
            ingest_data = query_connections_summary(ingest_conn, file_id)
            total_lines = _get_or_compute_line_count(upload_store, file_id)
            sampling_metadata = get_sampling_metadata(total_lines, sample)
            ingest_data["sampling_metadata"] = sampling_metadata
            if not include_details:
                ingest_data["connection_events"] = []
            return AnalysisResult(status="success", data=ingest_data)

        connections_data, total_opened, total_closed = parse_connections(file_path, sample_percentage=sample)
        overall_stats, ip_stats = calculate_connection_stats(connections_data)

        slow_queries, connections_timeseries, errors = parse_timeseries_data(file_path)

        try:
            connections_by_ip_timeseries = parse_connections_timeseries_by_ip(file_path)
        except Exception as e:
            logger.warning("Failed to parse IP-specific connections: %s", e)
            connections_by_ip_timeseries = {}

        validation_results = validate_connection_data_consistency(connections_by_ip_timeseries, connections_timeseries)

        try:
            connection_events = parse_connection_events(file_path)
        except Exception as e:
            logger.warning("Failed to parse connection events: %s", e)
            connection_events = []

        connections_dict = {}
        for ip, data in connections_data.items():
            connections_dict[ip] = {
                'opened': data['opened'],
                'closed': data['closed'],
                'durations': data['durations'] if include_details else [],
            }

        total_lines = _get_or_compute_line_count(upload_store, file_id)
        sampling_metadata = get_sampling_metadata(total_lines, sample)

        return AnalysisResult(
            status="success",
            data={
                "connections": connections_dict,
                "total_opened": total_opened,
                "total_closed": total_closed,
                "overall_stats": overall_stats,
                "ip_stats": ip_stats,
                "connections_timeseries": connections_timeseries,
                "connections_by_ip_timeseries": connections_by_ip_timeseries,
                "connection_events": connection_events if include_details else [],
                "sampling_metadata": sampling_metadata,
                "data_quality": {
                    "validation_results": validation_results,
                    "warnings": validation_results.get('warnings', []),
                    "recommendations": validation_results.get('recommendations', []),
                    "quality_score": validation_results.get('data_quality_score', 1.0),
                    "is_consistent": validation_results.get('is_consistent', True),
                },
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connection analysis failed: {str(e)}")

@app.post("/api/analyze/{file_id}/queries", response_model=AnalysisResult)
def analyze_queries_route(
    file_id: str,
    namespace: Optional[str] = None,
    operation: Optional[str] = None,
    sample: Optional[int] = 100,
    upload_store: dict = Depends(get_upload_store),
):
    """Analyze query patterns and performance."""
    file_path = get_validated_file_path(file_id, upload_store)
    validate_sample_param(sample)

    try:
        queries_data = parse_queries(file_path, sample_percentage=sample)
        query_stats = calculate_query_stats(queries_data)

        if namespace:
            query_stats = {k: v for k, v in query_stats.items() if k[0] == namespace}
        if operation:
            query_stats = {k: v for k, v in query_stats.items() if k[1] == operation}

        results = []
        for (ns, op, pattern), stats in query_stats.items():
            results.append({
                "namespace": ns,
                "operation": op,
                "pattern": pattern,
                "count": stats['count'],
                "min_ms": stats['min'],
                "max_ms": stats['max'],
                "mean_ms": stats['mean'],
                "percentile_95_ms": stats['percentile_95'],
                "sum_ms": stats['sum'],
                "allow_disk_use": stats['allowDiskUse'],
                "indexes": list(stats['indexes']) if isinstance(stats['indexes'], set) else stats['indexes'],
            })

        return AnalysisResult(
            status="success",
            data={
                "queries": results,
                "total_patterns": len(results),
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query analysis failed: {str(e)}")

@app.post("/api/analyze/{file_id}/query-examples", response_model=AnalysisResult)
def get_query_examples(
    file_id: str,
    request: QueryExamplesRequest,
    upload_store: dict = Depends(get_upload_store),
):
    """Get actual query examples for a specific pattern."""
    file_path = get_validated_file_path(file_id, upload_store)
    namespace = request.namespace
    operation = request.operation
    pattern = request.pattern
    
    logger.info(
        "Looking for query examples ns=%s op=%s pattern=%s",
        namespace,
        operation,
        pattern[:100],
    )
    
    try:
        examples = []
        match_attempts = 0
        with open(file_path, 'r') as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    if (entry.get('c') == 'COMMAND' and 
                        entry.get('msg') in ('command', 'Slow query') and
                        entry.get('attr')):
                        attr = entry['attr']
                        
                        # Check if this matches our pattern
                        ns = attr.get('ns', '')
                        command = attr.get('command', {})
                        if not command or ns != namespace:
                            continue
                            
                        op = list(command.keys())[0] if command else 'unknown'
                        if op != operation:
                            continue
                            
                        from . import extract_query_pattern
                        query_pattern = extract_query_pattern(op, command)
                        match_attempts += 1
                        if match_attempts <= 3:  # Log first few attempts
                            logger.debug("Comparing %s == %s", query_pattern[:100], pattern[:100])
                        
                        # Try exact match first
                        if query_pattern == pattern:
                            pass  # Match found
                        # For aggregate queries, also try legacy format compatibility
                        elif op == 'aggregate':
                            # Convert between formats: ["$match","$project"] <=> [$match,$project]
                            try:
                                # Parse new format (JSON array)
                                stages_new = json.loads(query_pattern) if query_pattern.startswith('[') else []
                                # Parse old format (bracket-comma-separated)
                                stages_old = pattern.strip('[]').split(',') if pattern.startswith('[') and not pattern.startswith('["') else []
                                
                                # Compare stage lists
                                if stages_new and stages_old and stages_new == stages_old:
                                    pass  # Match found (different format but same stages)
                                else:
                                    continue  # No match
                            except (json.JSONDecodeError, ValueError, TypeError):
                                continue  # No match
                        else:
                            continue  # No match
                            
                        # This is a match - add the example
                        examples.append({
                            'timestamp': entry.get('t', {}).get('$date', ''),
                            'duration_ms': attr.get('durationMillis', 0),
                            'command': command,
                            'plan_summary': attr.get('planSummary', 'N/A'),
                            'raw_log_line': line.strip()  # Store the original log line
                        })
                        
                        # Limit to 5 examples
                        if len(examples) >= 5:
                            break
                            
                except Exception:
                    continue
        
        logger.info("Found %d examples (checked %d patterns)", len(examples), match_attempts)
        
        return AnalysisResult(
            status="success",
            data={
                "examples": examples,
                "namespace": namespace,
                "operation": operation,
                "pattern": pattern
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get query examples: {str(e)}")

@app.post("/api/analyze/{file_id}/replica-set", response_model=AnalysisResult)
def analyze_replica_set(
    file_id: str,
    upload_store: dict = Depends(get_upload_store),
):
    """Analyze replica set configuration and state."""
    file_path = get_validated_file_path(file_id, upload_store)
    
    try:
        configs = parse_replica_set_config(file_path)
        states, node_status = parse_replica_set_state(file_path)
        
        return AnalysisResult(
            status="success",
            data={
                "configs": configs,
                "states": states,
                "node_status": node_status
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Replica set analysis failed: {str(e)}")

@app.post("/api/analyze/{file_id}/clients", response_model=AnalysisResult)
def analyze_clients(
    file_id: str,
    upload_store: dict = Depends(get_upload_store),
):
    """Analyze client/driver information."""
    file_path = get_validated_file_path(file_id, upload_store)
    
    try:
        clients_data = parse_clients(file_path)
        
        return AnalysisResult(
            status="success",
            data={
                "clients": clients_data
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Client analysis failed: {str(e)}")

@app.post("/api/analyze/{file_id}/timeseries", response_model=AnalysisResult)
def analyze_timeseries(
    file_id: str,
    namespace: Optional[str] = None,
    include_raw: bool = True,
    source: str = "raw",
    upload_store: dict = Depends(get_upload_store),
    ingest_conn=Depends(get_ingest_conn),
):
    """Analyze time-series data for slow queries, connections, and errors."""
    file_path = get_validated_file_path(file_id, upload_store)
    if source not in {"raw", "ingest"}:
        raise HTTPException(status_code=400, detail="Invalid source. Allowed: raw, ingest")
    
    try:
        if source == "ingest":
            data = query_timeseries(ingest_conn, file_id, include_raw=include_raw)
            if namespace:
                data["slow_queries"] = [q for q in data["slow_queries"] if q["namespace"] == namespace]
                data["aggregated_queries"] = [
                    q for q in data["aggregated_queries"] if q["namespace"] == namespace
                ]
                data["unique_namespaces"] = sorted({q["namespace"] for q in data["aggregated_queries"]})
            return AnalysisResult(status="success", data=data)

        slow_queries, connections, errors = parse_timeseries_data(file_path)
        
        # Filter slow queries by namespace if specified
        if namespace:
            slow_queries = [q for q in slow_queries if q['namespace'] == namespace]
        
        # Sample data if too large (> 10,000 points) to prevent browser overload
        max_points = 10000
        if len(slow_queries) > max_points:
            import random
            slow_queries = random.sample(slow_queries, max_points)
        
        # Get unique namespaces for filtering
        unique_namespaces = sorted(set(q['namespace'] for q in slow_queries))
        
        # Aggregate slow queries by namespace
        from collections import defaultdict
        namespace_stats = defaultdict(lambda: {'count': 0, 'total_duration': 0})
        for q in slow_queries:
            ns = q['namespace']
            namespace_stats[ns]['count'] += 1
            namespace_stats[ns]['total_duration'] += q['duration_ms']
        
        # Calculate average duration and format for response
        aggregated_queries = [
            {
                'namespace': ns,
                'count': stats['count'],
                'mean_duration_ms': round(stats['total_duration'] / stats['count'], 1)
            }
            for ns, stats in namespace_stats.items()
        ]
        aggregated_queries.sort(key=lambda x: x['count'], reverse=True)
        
        # Aggregate errors by message
        error_stats = defaultdict(int)
        for e in errors:
            error_stats[e['message']] += 1
        
        aggregated_errors = [
            {
                'message': msg,
                'count': count
            }
            for msg, count in error_stats.items()
        ]
        aggregated_errors.sort(key=lambda x: x['count'], reverse=True)
        
        # Get MongoDB info from slow queries
        total_slow_queries = len(slow_queries)
        sampled = len(slow_queries) == max_points
        
        return AnalysisResult(
            status="success",
            data={
                "slow_queries": slow_queries if include_raw else [],
                "connections": connections if include_raw else [],
                "errors": errors if include_raw else [],
                "aggregated_queries": aggregated_queries,
                "aggregated_errors": aggregated_errors,
                "unique_namespaces": unique_namespaces,
                "total_slow_queries": total_slow_queries,
                "sampled": sampled
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Time-series analysis failed: {str(e)}")

@app.post("/api/analyze/{file_id}/index-recommendations", response_model=AnalysisResult)
def get_index_recommendations(
    file_id: str,
    request: Optional[SingleQueryRequest] = None,
    top_n: int = 10,
    single_query: bool = False,
    upload_store: dict = Depends(get_upload_store),
):
    """Get index recommendations for queries."""
    file_path = get_validated_file_path(file_id, upload_store)
    
    try:
        # If we have a single query with raw log line, use it directly
        if request and request.raw_log_line:
            logger.info("Using raw log line for index recommendation analysis")
            # Parse the actual command from the log line
            log_entry = json.loads(request.raw_log_line)
            command = log_entry.get('attr', {}).get('command', {})
            plan_summary = log_entry.get('attr', {}).get('planSummary', 'COLLSCAN')
            
            # Pass the ENTIRE command object as JSON to the AI
            # This includes filter, sort, projection, limit, skip, pipeline, etc.
            full_command_json = json.dumps(command)
            
            logger.debug("Passing full command JSON prefix: %s", full_command_json[:200])
            logger.debug("Plan summary: %s", plan_summary)
            
            # Enhance stats with planSummary for coverage analysis
            enhanced_stats = request.stats.copy()
            enhanced_stats['plan_summary'] = plan_summary
            
            # Analyze single query
            recommendation = analyze_single_query(
                namespace=request.namespace,
                operation=request.operation,
                pattern=full_command_json,
                stats=enhanced_stats
            )
            
            return AnalysisResult(
                status="success",
                data={
                    "recommendations": [recommendation] if recommendation else [],
                    "total_analyzed": 1,
                    "has_llm": False
                }
            )
        
        # Otherwise, parse all queries from log file (bulk analysis)
        queries_data = parse_queries(file_path)
        query_stats = calculate_query_stats(queries_data)
        
        # Enhance query stats with planSummary information for coverage analysis
        for (namespace, operation, pattern), stats in query_stats.items():
            # Get the most common planSummary from the indexes set
            indexes = stats.get('indexes', set())
            if indexes:
                # Find the most common planSummary (not COLLSCAN if possible)
                plan_summaries = list(indexes)
                # Prefer non-COLLSCAN planSummaries
                non_collscan = [ps for ps in plan_summaries if ps != 'COLLSCAN']
                if non_collscan:
                    stats['plan_summary'] = non_collscan[0]  # Use first non-COLLSCAN
                else:
                    stats['plan_summary'] = 'COLLSCAN'
            else:
                stats['plan_summary'] = 'COLLSCAN'
        
        # Generate recommendations (LLM only if single_query=True)
        recommendations = ia_analyze_queries(query_stats)
        
        # Limit to top N
        recommendations = recommendations[:top_n]
        
        return AnalysisResult(
            status="success",
            data={
                "recommendations": recommendations,
                "total_analyzed": len(query_stats),
                "has_llm": False
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Index recommendation failed: {str(e)}")

@app.post("/api/trim/{file_id}", response_model=AnalysisResult)
def trim_log(
    file_id: str,
    trim_request: TrimRequest,
    upload_store: dict = Depends(get_upload_store),
):
    """Trim log file by date/time range."""
    file_path = get_validated_file_path(file_id, upload_store)
    
    try:
        start_dt, end_dt = get_date_range(trim_request.from_date, trim_request.until_date)
        filtered_lines, total_lines, skipped_lines = trim_log_file(file_path, start_dt, end_dt)
        
        if not filtered_lines:
            return AnalysisResult(
                status="error",
                data={},
                message="No lines found in the specified date range"
            )
        
        # Create a new temporary file with trimmed content
        original_name = upload_store[file_id]['original_name']
        base_name = Path(original_name).stem
        extension = Path(original_name).suffix
        
        # Create a more permanent temporary file
        temp_dir = tempfile.gettempdir()
        temp_filename = f"{base_name}_trimmed{extension}"
        temp_file_path = os.path.join(temp_dir, temp_filename)
        
        with open(temp_file_path, 'w', encoding='utf-8') as f:
            for line in filtered_lines:
                f.write(line)
        
        # Ensure file is properly written and closed
        if not os.path.exists(temp_file_path):
            raise Exception("Failed to create trimmed file")
        
        file_size = os.path.getsize(temp_file_path)
        if file_size == 0:
            raise Exception("Trimmed file is empty")
        
        # Generate new file ID with timestamp to ensure uniqueness
        new_file_id = f"trimmed_{int(time.time())}_{file_id}"
        
        # Store trimmed file
        upload_store[new_file_id] = {
            'path': temp_file_path,
            'original_name': f"{base_name}_trimmed{extension}",
            'size': os.path.getsize(temp_file_path),
            'lines': len(filtered_lines)
        }
        
        return AnalysisResult(
            status="success",
            data={
                "new_file_id": new_file_id,
                "filename": f"{base_name}_trimmed{extension}",
                "total_lines": total_lines,
                "included_lines": len(filtered_lines),
                "skipped_lines": skipped_lines,
                "start_date": start_dt.isoformat() if start_dt else None,
                "end_date": end_dt.isoformat() if end_dt else None
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Trimming failed: {str(e)}")

@app.get("/api/download/{file_id}")
async def download_file(
    file_id: str,
    upload_store: dict = Depends(get_upload_store),
):
    """Download a processed log file."""
    file_path = get_validated_file_path(file_id, upload_store)
    original_name = upload_store[file_id]['original_name']
    
    try:
        # Check file size
        file_size = os.path.getsize(file_path)
        if file_size == 0:
            raise HTTPException(status_code=400, detail="File is empty")
        
        return FileResponse(
            path=file_path,
            filename=original_name,
            media_type='application/octet-stream',
            headers={
                "Content-Disposition": f"attachment; filename=\"{original_name}\"",
                "Content-Length": str(file_size)
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")

@app.delete("/api/files/{file_id}", response_model=StatusMessage)
async def delete_file(
    file_id: str,
    upload_store: dict = Depends(get_upload_store),
    ingest_conn=Depends(get_ingest_conn),
):
    """Delete an uploaded file. For preloaded files, only removes from store (does not delete the original file)."""
    get_validated_file_path(file_id, upload_store)

    info = upload_store[file_id]
    file_path = info['path']
    if not info.get('is_preloaded', False):
        if os.path.exists(file_path):
            os.unlink(file_path)
    delete_file_ingest_data(ingest_conn, file_id)
    
    del upload_store[file_id]
    
    return StatusMessage(message="File deleted successfully")


def preload_file(upload_store: dict) -> str | None:
    """Pre-load a file if specified via environment variable. Uses the original path (no copy)."""
    preload_path = os.environ.get('PEPI_PRELOAD_FILE')
    if not preload_path:
        return None
    preload_path = os.path.abspath(os.path.expanduser(preload_path))
    if not os.path.exists(preload_path):
        return None
    try:
        original_name = os.path.basename(preload_path)
        file_size = os.path.getsize(preload_path)
        sample_percentage = int(os.environ.get('PEPI_SAMPLE_PERCENTAGE', '100'))
        file_id = f"{original_name}_{os.getpid()}"
        if file_id in upload_store:
            file_id = f"{original_name}_{os.getpid()}_{id(preload_path)}"
        upload_store[file_id] = {
            'path': preload_path,
            'original_name': original_name,
            'size': file_size,
            'lines': 0,
            'is_preloaded': True,
            'sample_percentage': sample_percentage,
        }
        logger.info("Pre-loaded file %s (ID: %s)", original_name, file_id)
        return file_id
    except Exception as e:
        logger.error("Failed to pre-load file %s: %s", preload_path, str(e))
    return None

@app.post("/api/analyze/{file_id}/extract", response_model=ExtractResponse)
def extract_logs(
    file_id: str,
    filters: LogFilterRequest,
    offset: int = 0,
    source: str = "raw",
    upload_store: dict = Depends(get_upload_store),
    ingest_conn=Depends(get_ingest_conn),
):
    """Extract raw log entries based on filters."""
    file_path = get_validated_file_path(file_id, upload_store)
    if source not in {"raw", "ingest"}:
        raise HTTPException(status_code=400, detail="Invalid source. Allowed: raw, ingest")
    if source == "ingest":
        data = query_extract(
            ingest_conn,
            file_id,
            offset=offset,
            limit=filters.limit,
            text_search=filters.text_search,
            case_sensitive=filters.case_sensitive,
            components=filters.components,
            severities=filters.severities,
            operations=filters.operations,
            namespace=filters.namespace,
            date_from=filters.date_from,
            date_to=filters.date_to,
        )
        return ExtractResponse(
            total_scanned=data["total_scanned"],
            total_matched=data["total_matched"],
            lines=data["lines"],
            truncated=data["truncated"],
        )
    matched_lines = []
    matched_count = 0
    total_lines = 0
    page_limit = max(1, min(filters.limit, 5000))

    with open(file_path, 'r') as f:
        for line in f:
            total_lines += 1
            
            # Quick text search first (fastest)
            if filters.text_search:
                if filters.case_sensitive:
                    if filters.text_search not in line:
                        continue
                else:
                    if filters.text_search.lower() not in line.lower():
                        continue
            
            # Parse JSON for detailed filtering
            try:
                entry = json.loads(line.strip())
            except (json.JSONDecodeError, ValueError):
                continue
            
            # Apply filters
            if not apply_filters(entry, line, filters):
                continue
            
            matched_count += 1
            if matched_count <= offset:
                continue

            matched_lines.append(line.strip())

            if len(matched_lines) >= page_limit:
                break
    
    return ExtractResponse(
        total_scanned=total_lines,
        total_matched=matched_count,
        lines=matched_lines,
        truncated=len(matched_lines) >= page_limit,
    )


def apply_filters(entry, line, filters):
    """Check if entry matches all filters."""
    
    # Event type filters
    if filters.event_types:
        matched = False
        if 'COLLSCAN' in filters.event_types and 'COLLSCAN' in line:
            matched = True
        if 'IXSCAN' in filters.event_types and 'IXSCAN' in line:
            matched = True
        if 'slow_query' in filters.event_types:
            if entry.get('attr', {}).get('durationMillis', 0) > 100:
                matched = True
        if 'error' in filters.event_types and entry.get('s') in ['E', 'F']:
            matched = True
        
        if not matched:
            return False
    
    # Component filter
    if filters.components:
        if entry.get('c') not in filters.components:
            return False
    
    # Severity filter
    if filters.severities:
        if entry.get('s') not in filters.severities:
            return False
    
    # Operation filter
    if filters.operations:
        cmd = entry.get('attr', {}).get('command', {})
        op = next(iter(cmd.keys())) if cmd else None
        if op not in filters.operations:
            return False
    
    # Namespace filter
    if filters.namespace:
        ns = entry.get('attr', {}).get('ns', '')
        if filters.namespace not in ns:
            return False
    
    # Log ID filter
    if filters.log_id:
        if entry.get('id') != filters.log_id:
            return False
    
    # Context filter
    if filters.context:
        if filters.context not in entry.get('ctx', ''):
            return False
    
    # Time range filter
    if filters.date_from or filters.date_to:
        timestamp = entry.get('t', {}).get('$date')
        if not timestamp:
            return False
        
        if filters.date_from and timestamp < filters.date_from:
            return False
        if filters.date_to and timestamp > filters.date_to:
            return False
    
    return True

@app.get("/api/analyze/{file_id}/filter-options", response_model=FilterOptionsResponse)
def get_filter_options(
    file_id: str,
    upload_store: dict = Depends(get_upload_store),
):
    """Get available filter options based on log content."""
    file_path = get_validated_file_path(file_id, upload_store)
    
    # Scan log to find available options
    available_components = set()
    available_severities = set()
    available_operations = set()
    available_namespaces = set()
    has_collscan = False
    has_ixscan = False
    has_slow_queries = False
    has_errors = False
    
    with open(file_path, 'r') as f:
        for line in f:
            try:
                entry = json.loads(line.strip())
                
                # Collect components
                if entry.get('c'):
                    available_components.add(entry['c'])
                
                # Collect severities
                if entry.get('s'):
                    available_severities.add(entry['s'])
                
                # Check for COLLSCAN/IXSCAN
                if 'COLLSCAN' in line:
                    has_collscan = True
                if 'IXSCAN' in line:
                    has_ixscan = True
                
                # Check for slow queries
                if entry.get('attr', {}).get('durationMillis', 0) > 100:
                    has_slow_queries = True
                
                # Check for errors
                if entry.get('s') in ['E', 'F']:
                    has_errors = True
                
                # Collect operations
                cmd = entry.get('attr', {}).get('command', {})
                if cmd:
                    op = next(iter(cmd.keys()))
                    available_operations.add(op)
                
                # Collect namespaces
                ns = entry.get('attr', {}).get('ns')
                if ns:
                    available_namespaces.add(ns)
                    
            except (json.JSONDecodeError, ValueError, KeyError):
                continue
    
    return {
        "status": "success",
        "data": {
            "event_types": {
                "COLLSCAN": has_collscan,
                "IXSCAN": has_ixscan,
                "slow_query": has_slow_queries,
                "error": has_errors
            },
            "components": sorted(list(available_components)),
            "severities": sorted(list(available_severities)),
            "operations": sorted(list(available_operations)),
            "namespaces": sorted(list(available_namespaces))[:20]  # Limit to top 20
        }
    }

@app.get("/api/fs/browse", response_model=FsBrowseResponse)
async def browse_fs(path: str = None):
    """Browse the server file system to select a directory."""
    if not path:
        path = os.path.expanduser("~")
        
    try:
        path = os.path.abspath(path)
        if not os.path.exists(path) or not os.path.isdir(path):
            return {"status": "error", "message": "Directory does not exist."}
            
        directories = []
        
        # Add parent directory ".." if not at root
        parent = os.path.dirname(path)
        if parent and parent != path:
            directories.append({"name": "..", "path": parent})
            
        with os.scandir(path) as it:
            for entry in it:
                if entry.is_dir() and not entry.name.startswith('.'):
                    directories.append({"name": entry.name, "path": entry.path})
                    
        # Sort directories (ignoring "..")
        sorted_dirs = [d for d in directories if d["name"] == ".."] + sorted([d for d in directories if d["name"] != ".."], key=lambda x: x["name"].lower())
        
        return {
            "status": "success", 
            "data": {
                "current_path": path,
                "directories": sorted_dirs
            }
        }
    except PermissionError:
        return {"status": "error", "message": "Permission denied."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# --- FTDC Endpoints ---

@app.get("/api/ftdc/list", response_model=AnalysisResult)
async def list_ftdc_dirs():
    """Returns some discovered FTDC directories."""
    dirs = []
    base_paths = [os.path.expanduser("~/repositories"), os.path.expanduser("~/")]
    for base in base_paths:
        if os.path.exists(base):
            try:
                for entry in os.scandir(base):
                    if entry.is_dir():
                        diag_path = os.path.join(entry.path, "data", "diagnostic.data")
                        if os.path.exists(diag_path):
                            dirs.append(diag_path)
                        diag_path2 = os.path.join(entry.path, "diagnostic.data")
                        if os.path.exists(diag_path2):
                            dirs.append(diag_path2)
            except PermissionError:
                pass
    return {"status": "success", "data": {"directories": dirs}}

@app.post("/api/ftdc/start", response_model=StatusMessage)
async def start_ftdc(request: FtdcStartRequest):
    try:
        from pepi.ftdc import launch_viewer
        import threading
        t = threading.Thread(target=launch_viewer, args=(request.path, False))
        t.start()
        return {"status": "success", "message": "FTDC Viewer starting"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ftdc/status", response_model=FtdcStatusResponse)
async def status_ftdc():
    import urllib.request
    try:
        # First check if Grafana is actually responding
        urllib.request.urlopen("http://localhost:3001/api/health", timeout=1)
        
        # If Grafana is up, let's see if the exporter has finished and output the exact URL yet
        from pepi.ftdc import get_ftdc_dashboard_url
        url = get_ftdc_dashboard_url()
        
        # If it returns the default URL, the exporter isn't done ingesting yet.
        # We want the frontend to keep checking until we get the exact timestamped URL.
        if "from=" not in url:
             return {"status": "success", "data": {"running": False}}
             
        # Exporter is done! Return the precise URL.
        return {"status": "success", "data": {"running": True, "url": url}}
    except (OSError, ImportError, ValueError):
        return {"status": "success", "data": {"running": False}}

@app.post("/api/ftdc/stop", response_model=StatusMessage)
async def stop_ftdc():
    try:
        import subprocess
        from pepi.ftdc import get_docker_compose_cmd
        compose_file = os.path.join(os.path.dirname(__file__), "ftdc", "docker-compose.yml")
        cmd = get_docker_compose_cmd() + ["-f", compose_file, "down"]
        
        env = os.environ.copy()
        env.setdefault("INPUT_DIR", "/tmp")
        env.setdefault("PARALLEL", "10")
        env.setdefault("BATCH_SIZE", "200")
        env.setdefault("INFLUX_DB_DATA_DIRECTORY", "/tmp")
        env.setdefault("INFLUX_ADMIN_PASSWORD", "admin")
        env.setdefault("INFLUX_API_TOKEN", "token")
        env.setdefault("GRAFANA_ADMIN_PASSWORD", "admin")
        env.setdefault("INFLUX_ORG", "org")
        env.setdefault("INFLUX_BUCKET", "bucket")

        subprocess.run(cmd, env=env, check=True)
        return {"status": "success", "message": "FTDC Viewer stopped"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def find_available_port(start_port=8000):
    """Find an available port for pepi web-ui, allowing max 3 instances."""
    import subprocess
    import psutil
    
    # Define the allowed ports for pepi (max 3 instances)
    allowed_ports = [8000, 8001, 8002]
    
    try:
        # Find existing pepi processes and their ports
        pepi_processes = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if proc.info['cmdline'] and any('pepi.web_api' in cmd for cmd in proc.info['cmdline']):
                    # Get the port this process is using
                    for conn in proc.net_connections():
                        if conn.status == 'LISTEN' and conn.laddr.ip in ['0.0.0.0', '127.0.0.1']:
                            pepi_processes.append({
                                'pid': proc.info['pid'],
                                'port': conn.laddr.port,
                                'cmdline': ' '.join(proc.info['cmdline'])
                            })
                            break
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        # Show existing pepi processes
        if pepi_processes:
            logger.info("Found %d existing pepi web-ui process(es)", len(pepi_processes))
            for proc in pepi_processes:
                logger.info("Existing pepi process PID %s on port %s", proc['pid'], proc['port'])
        
        # Check if we've reached the limit
        if len(pepi_processes) >= 3:
            logger.error("Maximum of 3 pepi web-ui instances already running.")
            logger.error("Please stop one of the existing instances before starting a new one.")
            raise RuntimeError("Maximum pepi instances reached (3)")
        
        # Find the first available port from our allowed list
        used_ports = [proc['port'] for proc in pepi_processes if proc['port'] in allowed_ports]
        
        for port in allowed_ports:
            if port not in used_ports:
                # Double-check the port is actually available by trying to bind
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        s.bind(('0.0.0.0', port))
                        logger.info("Selected port %s for this instance", port)
                        return port
                except OSError as e:
                    # Only warn if it's actually in use (not TIME_WAIT)
                    if "Address already in use" in str(e):
                        logger.warning("Port %s is in use, trying next", port)
                    else:
                        logger.warning("Port %s unavailable (%s), trying next", port, e)
                    continue
        
        raise RuntimeError("No available ports in range 8000-8002")
        
    except ImportError:
        # Fallback if psutil is not available
        logger.warning("psutil not available, using basic port detection")
        for port in allowed_ports:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    s.bind(('0.0.0.0', port))
                    return port
            except OSError:
                continue
        raise RuntimeError("No available ports in range 8000-8002")


def cleanup_stale_port_files() -> None:
    """Remove stale /tmp/pepi_port_<pid>.txt files for dead processes."""
    temp_dir = Path(tempfile.gettempdir())
    for path in temp_dir.glob("pepi_port_*.txt"):
        try:
            pid_text = path.stem.replace("pepi_port_", "")
            pid = int(pid_text)
        except ValueError:
            continue
        try:
            os.kill(pid, 0)
            # Process exists: keep marker.
            continue
        except OSError:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass


if __name__ == "__main__":
    import uvicorn
    
    logger.info("Starting Pepi Web Interface")
    
    # Find an available port
    try:
        cleanup_stale_port_files()
        port = find_available_port(8000)
        
        # Write the port to a temporary file so the main process can read it
        import tempfile
        port_file = tempfile.gettempdir() + f"/pepi_port_{os.getpid()}.txt"
        with open(port_file, 'w') as f:
            f.write(str(port))
        
        try:
            uvicorn.run(app, host="0.0.0.0", port=port, access_log=False, log_level="error")
        finally:
            try:
                os.unlink(port_file)
            except OSError:
                pass
    except RuntimeError as e:
        logger.error("Failed to start server: %s", e)