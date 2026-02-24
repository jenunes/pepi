from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import tempfile
import os
import json
import asyncio
import time
from pathlib import Path
import socket
from contextlib import asynccontextmanager

# Import our existing pepi functions
from . import (
    parse_connections, parse_replica_set_config, parse_replica_set_state,
    parse_clients, parse_queries, calculate_query_stats, calculate_connection_stats,
    count_lines, get_date_range, trim_log_file, parse_timeseries_data,
    parse_connections_timeseries_by_ip, validate_connection_data_consistency,
    parse_connection_events
)
from .index_advisor import IndexAdvisor

# Get the directory where this script is located
script_dir = Path(__file__).parent
web_static_dir = script_dir / "web_static"

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events for the FastAPI app."""
    # Startup
    # Create web_static directory if it doesn't exist
    os.makedirs(web_static_dir, exist_ok=True)
    
    # Pre-load file if specified
    preload_file()
    
    yield
    
    # Shutdown (nothing to do for now)

app = FastAPI(title="Pepi MongoDB Log Analyzer", version="2.0.0", lifespan=lifespan)

# Enable GZip compression for better performance
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Enable CORS for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store for uploaded files and analysis results
upload_store = {}
analysis_cache = {}

# Pydantic models for API responses
class LogInfo(BaseModel):
    filename: str
    size: int
    lines: int
    start_date: Optional[str]
    end_date: Optional[str]

class AnalysisResult(BaseModel):
    status: str
    data: Dict[str, Any]
    message: Optional[str] = None

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
    event_types: List[str] = []  # ['COLLSCAN', 'IXSCAN', 'slow_query', 'planSummary']
    components: List[str] = []    # MongoDB components: NETWORK, COMMAND, QUERY, etc.
    severities: List[str] = []    # ['I', 'W', 'E', 'F', 'D'] or specific D1-D5
    operations: List[str] = []    # ['find', 'aggregate', 'insert', 'update', 'delete']
    namespace: Optional[str] = None  # Filter by ns in attr
    log_id: Optional[int] = None     # Filter by specific log ID
    context: Optional[str] = None    # Filter by ctx (thread/connection)
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    limit: int = 10000  # Max lines to return

class FtdcStartRequest(BaseModel):
    path: str

# Serve static files

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

@app.post("/api/upload")
async def upload_log_file(file: UploadFile = File(...)):
    """Upload and store a MongoDB log file."""
    if not file.filename.endswith(('.log', '.txt', '.json')):
        raise HTTPException(status_code=400, detail="Only .log, .txt, and .json files are supported")
    
    # Create a temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.log')
    try:
        # Copy uploaded file content
        content = await file.read()
        temp_file.write(content)
        temp_file.close()
        
        # Store file info
        file_id = os.path.basename(temp_file.name)
        upload_store[file_id] = {
            'path': temp_file.name,
            'original_name': file.filename,
            'size': len(content),
            'lines': count_lines(temp_file.name)
        }
        
        return {
            "file_id": file_id,
            "filename": file.filename,
            "size": len(content),
            "lines": upload_store[file_id]['lines'],
            "message": "File uploaded successfully"
        }
    except Exception as e:
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.get("/api/files")
async def list_uploaded_files():
    """List all uploaded files."""
    files = []
    for file_id, info in upload_store.items():
        if os.path.exists(info['path']):
            files.append({
                "file_id": file_id,
                "filename": info['original_name'],
                "size": info['size'],
                "lines": info['lines'],
                "is_preloaded": info.get('is_preloaded', False),
                "sample_percentage": info.get('sample_percentage', 100)
            })
        else:
            # Clean up missing files
            del upload_store[file_id]
    return {"files": files}

@app.post("/api/analyze/{file_id}/basic")
async def analyze_basic_info(file_id: str, sample: Optional[int] = 100):
    """Get basic log file information."""
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = upload_store[file_id]['path']
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File no longer exists")
    
    try:
        # Parse basic information from log file
        start_date = end_date = None
        os_version = kernel_version = db_version = cmd_options = None
        
        # More robust date range extraction
        with open(file_path, 'r') as f:
            lines = f.readlines()
            
        # Get start date from first valid entry
        for line in lines[:100]:  # Check first 100 lines for start date
            try:
                entry = json.loads(line.strip())
                if entry.get('t', {}).get('$date'):
                    start_date = entry.get('t', {}).get('$date')
                    break
            except:
                continue
        
        # Get end date from last valid entry
        for line in reversed(lines[-100:]):  # Check last 100 lines for end date
            try:
                entry = json.loads(line.strip())
                if entry.get('t', {}).get('$date'):
                    end_date = entry.get('t', {}).get('$date')
                    break
            except:
                continue
        
        # Scan for version info and startup options (limit to first 1000 lines for performance)
        for line in lines[:1000]:
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
            except:
                continue
        
        # Get sampling metadata
        from . import get_sampling_metadata, count_lines
        total_lines = count_lines(file_path)
        sampling_metadata = get_sampling_metadata(total_lines, sample)
        
        return AnalysisResult(
            status="success",
            data={
                "filename": upload_store[file_id]['original_name'],
                "size": upload_store[file_id]['size'],
                "lines": upload_store[file_id]['lines'],
                "start_date": start_date,
                "end_date": end_date,
                "os_version": os_version,
                "kernel_version": kernel_version,
                "db_version": db_version,
                "startup_options": cmd_options,
                "sampling_metadata": sampling_metadata
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

@app.post("/api/analyze/{file_id}/connections")
async def analyze_connections(file_id: str, sample: Optional[int] = 100):
    """Analyze connection information with time series data."""
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = upload_store[file_id]['path']
    
    # Validate sample parameter
    if sample is not None and (sample < 0 or sample > 100):
        raise HTTPException(status_code=400, detail="Sample percentage must be between 0 and 100")
    
    try:
        # Get both aggregate data and time series data
        connections_data, total_opened, total_closed = parse_connections(file_path, sample_percentage=sample)
        overall_stats, ip_stats = calculate_connection_stats(connections_data)
        
        # Get time series data for connections
        slow_queries, connections_timeseries, errors = parse_timeseries_data(file_path)
        
        # Get IP-specific connection time series data
        try:
            connections_by_ip_timeseries = parse_connections_timeseries_by_ip(file_path)
        except Exception as e:
            print(f"Warning: Failed to parse IP-specific connections: {e}")
            connections_by_ip_timeseries = {}
        
        # Validate data consistency and get quality metrics
        validation_results = validate_connection_data_consistency(connections_by_ip_timeseries, connections_timeseries)
        
        # Get individual connection events
        try:
            connection_events = parse_connection_events(file_path)
        except Exception as e:
            print(f"Warning: Failed to parse connection events: {e}")
            connection_events = []
        
        # Convert defaultdict to regular dict for JSON serialization
        connections_dict = {}
        for ip, data in connections_data.items():
            connections_dict[ip] = {
                'opened': data['opened'],
                'closed': data['closed'],
                'durations': data['durations']
            }
        
        # Get sampling metadata from the parse_connections function
        from . import get_sampling_metadata, count_lines
        total_lines = count_lines(file_path)
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
                "connection_events": connection_events,
                "sampling_metadata": sampling_metadata,
                "data_quality": {
                    "validation_results": validation_results,
                    "warnings": validation_results.get('warnings', []),
                    "recommendations": validation_results.get('recommendations', []),
                    "quality_score": validation_results.get('data_quality_score', 1.0),
                    "is_consistent": validation_results.get('is_consistent', True)
                }
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connection analysis failed: {str(e)}")

@app.post("/api/analyze/{file_id}/queries")
async def analyze_queries(file_id: str, namespace: Optional[str] = None, operation: Optional[str] = None, sample: Optional[int] = 100):
    """Analyze query patterns and performance."""
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = upload_store[file_id]['path']
    
    # Validate sample parameter
    if sample is not None and (sample < 0 or sample > 100):
        raise HTTPException(status_code=400, detail="Sample percentage must be between 0 and 100")
    
    try:
        queries_data = parse_queries(file_path, sample_percentage=sample)
        query_stats = calculate_query_stats(queries_data)
        
        # Apply filters if specified
        if namespace:
            query_stats = {k: v for k, v in query_stats.items() if k[0] == namespace}
        if operation:
            query_stats = {k: v for k, v in query_stats.items() if k[1] == operation}
        
        # Convert to JSON-serializable format
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
                "indexes": list(stats['indexes']) if isinstance(stats['indexes'], set) else stats['indexes']
            })
        
        return AnalysisResult(
            status="success",
            data={
                "queries": results,
                "total_patterns": len(results)
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query analysis failed: {str(e)}")

@app.post("/api/analyze/{file_id}/query-examples")
async def get_query_examples(file_id: str, request: QueryExamplesRequest):
    """Get actual query examples for a specific pattern."""
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = upload_store[file_id]['path']
    namespace = request.namespace
    operation = request.operation
    pattern = request.pattern
    
    print(f"🔍 Looking for examples: ns={namespace}, op={operation}, pattern={pattern[:100]}")
    
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
                            print(f"  Comparing: {query_pattern[:100]} == {pattern[:100]}")
                        
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
                            except:
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
        
        print(f"✅ Found {len(examples)} examples (checked {match_attempts} patterns)")
        
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

@app.post("/api/analyze/{file_id}/replica-set")
async def analyze_replica_set(file_id: str):
    """Analyze replica set configuration and state."""
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = upload_store[file_id]['path']
    
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

@app.post("/api/analyze/{file_id}/clients")
async def analyze_clients(file_id: str):
    """Analyze client/driver information."""
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = upload_store[file_id]['path']
    
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

@app.post("/api/analyze/{file_id}/timeseries")
async def analyze_timeseries(file_id: str, namespace: Optional[str] = None):
    """Analyze time-series data for slow queries, connections, and errors."""
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = upload_store[file_id]['path']
    
    try:
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
                "slow_queries": slow_queries,
                "connections": connections,
                "errors": errors,
                "aggregated_queries": aggregated_queries,
                "aggregated_errors": aggregated_errors,
                "unique_namespaces": unique_namespaces,
                "total_slow_queries": total_slow_queries,
                "sampled": sampled
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Time-series analysis failed: {str(e)}")

class SingleQueryRequest(BaseModel):
    namespace: str
    operation: str
    pattern: str
    raw_log_line: Optional[str] = None
    stats: Dict

@app.post("/api/analyze/{file_id}/index-recommendations")
async def get_index_recommendations(file_id: str, request: Optional[SingleQueryRequest] = None, top_n: int = 10, single_query: bool = False):
    """Get AI-powered index recommendations for queries.
    
    Args:
        file_id: Uploaded file ID
        request: Single query data (if analyzing one specific query)
        top_n: Number of recommendations to return (for bulk analysis)
        single_query: If True, use LLM enhancement (for single query from UI button)
    """
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = upload_store[file_id]['path']
    
    try:
        # Initialize index advisor
        advisor = IndexAdvisor()
        
        # If we have a single query with raw log line, use it directly
        if request and request.raw_log_line:
            print(f"🎯 Using raw log line for analysis")
            # Parse the actual command from the log line
            log_entry = json.loads(request.raw_log_line)
            command = log_entry.get('attr', {}).get('command', {})
            plan_summary = log_entry.get('attr', {}).get('planSummary', 'COLLSCAN')
            
            # Pass the ENTIRE command object as JSON to the AI
            # This includes filter, sort, projection, limit, skip, pipeline, etc.
            full_command_json = json.dumps(command)
            
            print(f"📊 Passing full command to AI (first 200 chars): {full_command_json[:200]}")
            print(f"📊 Plan summary: {plan_summary}")
            
            # Enhance stats with planSummary for coverage analysis
            enhanced_stats = request.stats.copy()
            enhanced_stats['plan_summary'] = plan_summary
            
            # Analyze single query
            recommendation = advisor.analyze_single_query(
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
        recommendations = advisor.analyze_queries(query_stats)
        
        # Limit to top N
        recommendations = recommendations[:top_n]
        
        return AnalysisResult(
            status="success",
            data={
                "recommendations": recommendations,
                "total_analyzed": len(query_stats),
                "has_llm": advisor.llm is not None
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Index recommendation failed: {str(e)}")

@app.post("/api/trim/{file_id}")
async def trim_log(file_id: str, trim_request: TrimRequest):
    """Trim log file by date/time range."""
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = upload_store[file_id]['path']
    
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
async def download_file(file_id: str):
    """Download a processed log file."""
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = upload_store[file_id]['path']
    original_name = upload_store[file_id]['original_name']
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File no longer exists")
    
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

@app.delete("/api/files/{file_id}")
async def delete_file(file_id: str):
    """Delete an uploaded file. For preloaded files, only removes from store (does not delete the original file)."""
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    info = upload_store[file_id]
    file_path = info['path']
    if not info.get('is_preloaded', False):
        if os.path.exists(file_path):
            os.unlink(file_path)
    
    del upload_store[file_id]
    
    return {"message": "File deleted successfully"}

def preload_file():
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
        # Stable file_id so the same preload is identifiable; pid avoids collisions
        file_id = f"{original_name}_{os.getpid()}"
        if file_id in upload_store:
            file_id = f"{original_name}_{os.getpid()}_{id(preload_path)}"
        upload_store[file_id] = {
            'path': preload_path,
            'original_name': original_name,
            'size': file_size,
            'lines': count_lines(preload_path),
            'is_preloaded': True,
            'sample_percentage': sample_percentage,
        }
        print(f"📁 Pre-loaded file: {original_name} (ID: {file_id})")
        return file_id
    except Exception as e:
        print(f"❌ Failed to pre-load file {preload_path}: {str(e)}")
    return None

@app.post("/api/analyze/{file_id}/extract")
async def extract_logs(file_id: str, filters: LogFilterRequest):
    """Extract raw log entries based on filters."""
    
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = upload_store[file_id]['path']
    matched_lines = []
    total_lines = 0
    
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
            except:
                continue
            
            # Apply filters
            if not apply_filters(entry, line, filters):
                continue
            
            matched_lines.append(line.strip())
            
            # Limit results
            if len(matched_lines) >= filters.limit:
                break
    
    return {
        "status": "success",
        "total_scanned": total_lines,
        "total_matched": len(matched_lines),
        "lines": matched_lines,
        "truncated": len(matched_lines) >= filters.limit
    }

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

@app.get("/api/analyze/{file_id}/filter-options")
async def get_filter_options(file_id: str):
    """Get available filter options based on log content."""
    
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = upload_store[file_id]['path']
    
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
                    
            except:
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

# --- File System Browsing ---
@app.get("/api/fs/browse")
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

@app.get("/api/ftdc/list")
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

@app.post("/api/ftdc/start")
async def start_ftdc(request: FtdcStartRequest):
    try:
        from pepi.ftdc import launch_viewer
        import threading
        t = threading.Thread(target=launch_viewer, args=(request.path, False))
        t.start()
        return {"status": "success", "message": "FTDC Viewer starting"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ftdc/status")
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
    except:
        return {"status": "success", "data": {"running": False}}

@app.post("/api/ftdc/stop")
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
                    for conn in proc.connections():
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
            print(f"📋 Found {len(pepi_processes)} existing pepi web-ui process(es):")
            for proc in pepi_processes:
                print(f"   • PID {proc['pid']} on port {proc['port']}")
        
        # Check if we've reached the limit
        if len(pepi_processes) >= 3:
            print("❌ Maximum of 3 pepi web-ui instances already running.")
            print("   Please stop one of the existing instances before starting a new one.")
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
                        print(f"🎯 Selected port {port} for this instance")
                        return port
                except OSError as e:
                    # Only warn if it's actually in use (not TIME_WAIT)
                    if "Address already in use" in str(e):
                        print(f"⚠️  Port {port} is in use, trying next...")
                    else:
                        print(f"⚠️  Port {port} unavailable ({e}), trying next...")
                    continue
        
        raise RuntimeError("No available ports in range 8000-8002")
        
    except ImportError:
        # Fallback if psutil is not available
        print("⚠️  psutil not available, using basic port detection...")
        for port in allowed_ports:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    s.bind(('0.0.0.0', port))
                    return port
            except OSError:
                continue
        raise RuntimeError("No available ports in range 8000-8002")

if __name__ == "__main__":
    import uvicorn
    
    print("🚀 Starting Pepi Web Interface...")
    
    # Find an available port
    try:
        port = find_available_port(8000)
        
        # Write the port to a temporary file so the main process can read it
        import tempfile
        port_file = tempfile.gettempdir() + f"/pepi_port_{os.getpid()}.txt"
        with open(port_file, 'w') as f:
            f.write(str(port))
        
        uvicorn.run(app, host="0.0.0.0", port=port)
    except RuntimeError as e:
        print(f"❌ Failed to start server: {e}") 