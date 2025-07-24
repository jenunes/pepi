from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import tempfile
import os
import json
import asyncio
from pathlib import Path
import shutil

# Import our existing pepi functions
from pepi import (
    parse_connections, parse_replica_set_config, parse_replica_set_state,
    parse_clients, parse_queries, calculate_query_stats, calculate_connection_stats,
    count_lines, get_date_range, trim_log_file
)

app = FastAPI(title="Pepi MongoDB Log Analyzer", version="1.0.0")

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

# Serve static files
app.mount("/static", StaticFiles(directory="web_static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the main dashboard page."""
    try:
        with open("web_static/index.html", "r") as f:
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
                "lines": info['lines']
            })
        else:
            # Clean up missing files
            del upload_store[file_id]
    return {"files": files}

@app.post("/api/analyze/{file_id}/basic")
async def analyze_basic_info(file_id: str):
    """Get basic log file information."""
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = upload_store[file_id]['path']
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File no longer exists")
    
    try:
        # Parse basic information from first and last lines
        start_date = end_date = None
        os_version = kernel_version = db_version = None
        
        with open(file_path, 'r') as f:
            lines = f.readlines()
            
        # Get start date from first line
        if lines:
            try:
                first_entry = json.loads(lines[0])
                start_date = first_entry.get('t', {}).get('$date')
            except:
                pass
        
        # Get end date from last line
        if lines:
            try:
                last_entry = json.loads(lines[-1])
                end_date = last_entry.get('t', {}).get('$date')
            except:
                pass
        
        # Scan for version info (limit to first 1000 lines for performance)
        for line in lines[:1000]:
            try:
                entry = json.loads(line)
                if entry.get('msg') == 'Operating System':
                    os_info = entry.get('attr', {}).get('os', {})
                    os_version = os_info.get('name')
                    kernel_version = os_info.get('version')
                elif entry.get('msg') == 'Build Info':
                    db_version = entry.get('attr', {}).get('buildInfo', {}).get('version')
            except:
                continue
        
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
                "db_version": db_version
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

@app.post("/api/analyze/{file_id}/connections")
async def analyze_connections(file_id: str):
    """Analyze connection information."""
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = upload_store[file_id]['path']
    
    try:
        connections_data, total_opened, total_closed = parse_connections(file_path)
        overall_stats, ip_stats = calculate_connection_stats(connections_data)
        
        # Convert defaultdict to regular dict for JSON serialization
        connections_dict = {}
        for ip, data in connections_data.items():
            connections_dict[ip] = {
                'opened': data['opened'],
                'closed': data['closed'],
                'durations': data['durations']
            }
        
        return AnalysisResult(
            status="success",
            data={
                "connections": connections_dict,
                "total_opened": total_opened,
                "total_closed": total_closed,
                "overall_stats": overall_stats,
                "ip_stats": ip_stats
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connection analysis failed: {str(e)}")

@app.post("/api/analyze/{file_id}/queries")
async def analyze_queries(file_id: str, namespace: Optional[str] = None, operation: Optional[str] = None):
    """Analyze query patterns and performance."""
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = upload_store[file_id]['path']
    
    try:
        queries_data = parse_queries(file_path)
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
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='_trimmed.log', mode='w')
        for line in filtered_lines:
            temp_file.write(line)
        temp_file.close()
        
        # Generate new file ID
        new_file_id = os.path.basename(temp_file.name)
        original_name = upload_store[file_id]['original_name']
        base_name = Path(original_name).stem
        extension = Path(original_name).suffix
        
        # Store trimmed file
        upload_store[new_file_id] = {
            'path': temp_file.name,
            'original_name': f"{base_name}_trimmed{extension}",
            'size': os.path.getsize(temp_file.name),
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
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File no longer exists")
    
    return FileResponse(
        path=file_path,
        filename=upload_store[file_id]['original_name'],
        media_type='application/octet-stream'
    )

@app.delete("/api/files/{file_id}")
async def delete_file(file_id: str):
    """Delete an uploaded file."""
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_path = upload_store[file_id]['path']
    if os.path.exists(file_path):
        os.unlink(file_path)
    
    del upload_store[file_id]
    
    return {"message": "File deleted successfully"}

def preload_file():
    """Pre-load a file if specified via environment variable."""
    preload_path = os.environ.get('PEPI_PRELOAD_FILE')
    if preload_path and os.path.exists(preload_path):
        try:
            # Copy the file to a temporary location
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.log')
            shutil.copy2(preload_path, temp_file.name)
            temp_file.close()
            
            # Store file info
            file_id = os.path.basename(temp_file.name)
            original_name = os.path.basename(preload_path)
            file_size = os.path.getsize(temp_file.name)
            
            upload_store[file_id] = {
                'path': temp_file.name,
                'original_name': original_name,
                'size': file_size,
                'lines': count_lines(temp_file.name),
                'is_preloaded': True  # Mark as pre-loaded
            }
            
            print(f"📁 Pre-loaded file: {original_name} (ID: {file_id})")
            return file_id
        except Exception as e:
            print(f"❌ Failed to pre-load file {preload_path}: {str(e)}")
    return None

@app.on_event("startup")
async def startup_event():
    """Run startup tasks."""
    # Create web_static directory if it doesn't exist
    os.makedirs("web_static", exist_ok=True)
    
    # Pre-load file if specified
    preload_file()

if __name__ == "__main__":
    import uvicorn
    
    print("🚀 Starting Pepi Web Interface...")
    print("📊 Dashboard will be available at: http://localhost:8000")
    print("📋 API docs available at: http://localhost:8000/docs")
    
    uvicorn.run(app, host="0.0.0.0", port=8000) 