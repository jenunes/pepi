import subprocess
import os
import webbrowser
import time
import shutil
import re
import sys
import logging

logger = logging.getLogger(__name__)

def get_docker_compose_cmd():
    """Returns the available docker compose command as a list."""
    if shutil.which("docker-compose"):
        return ["docker-compose"]
    return ["docker", "compose"]

def parse_grafana_url_from_line(line: str) -> str:
    """Parses the Grafana URL from a log line and caps extreme >30 day ranges."""
    match = re.search(r'(http://[a-zA-Z0-9.-]+:3001/d/[^\s<>\'"]+)', line)
    if not match:
        return None
        
    grafana_url = match.group(1)
    try:
        import urllib.parse
        from datetime import datetime, timedelta
        
        parsed = urllib.parse.urlsplit(grafana_url)
        query_params = urllib.parse.parse_qs(parsed.query)
        if 'from' in query_params and 'to' in query_params:
            from_str = query_params['from'][0]
            to_str = query_params['to'][0]
            
            # Parse dates (e.g., 2024-05-25T19:23:09.607Z)
            from_dt = datetime.strptime(from_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            to_dt = datetime.strptime(to_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            
            # If the range is extremely wide (> 30 days), it's likely a corrupted data point
            if (to_dt - from_dt).days > 30:
                # Cap to 12 hours from the start time
                new_to = from_dt + timedelta(hours=12)
                query_params['to'] = [new_to.strftime("%Y-%m-%dT%H:%M:%S.000Z")]
                
                new_query = urllib.parse.urlencode(query_params, doseq=True)
                grafana_url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment))
    except Exception:
        pass
    return grafana_url

def get_ftdc_dashboard_url():
    """Reads docker compose logs to find the exact Grafana URL with from/to parameters."""
    compose_file = os.path.join(os.path.dirname(__file__), "docker-compose.yml")
    cmd = get_docker_compose_cmd() + ["-f", compose_file, "logs", "ftdc_exporter"]
    
    grafana_url = "http://localhost:3001/d/ddnw277huiv40ae/ftdc-dashboard"
    try:
        # Dummy env to satisfy warnings
        env = os.environ.copy()
        env.setdefault("INPUT_DIR", "/tmp")
        env.setdefault("INFLUX_API_TOKEN", "token")
        env.setdefault("INFLUX_ORG", "org")
        env.setdefault("INFLUX_BUCKET", "bucket")
        env.setdefault("PARALLEL", "10")
        env.setdefault("BATCH_SIZE", "200")
        env.setdefault("INFLUX_DB_DATA_DIRECTORY", "/tmp")
        env.setdefault("INFLUX_ADMIN_PASSWORD", "admin")
        env.setdefault("GRAFANA_ADMIN_PASSWORD", "admin")
        res = subprocess.run(cmd, env=env, capture_output=True, text=True)
        for line in reversed(res.stdout.splitlines()):
            if "ftdc-dashboard?from=" in line:
                parsed_url = parse_grafana_url_from_line(line)
                if parsed_url:
                    grafana_url = parsed_url
                    break
    except Exception:
        pass
    return grafana_url

def launch_viewer(data_path: str, open_browser: bool = True):
    logger.info("Starting FTDC Viewer for %s", data_path)
    
    compose_file = os.path.join(os.path.dirname(__file__), "docker-compose.yml")
    
    env = os.environ.copy()
    
    # Map the explicit input dir
    env["INPUT_DIR"] = os.path.abspath(data_path)
    
    # Core variables required by docker-compose.yml
    env.setdefault("PARALLEL", "10")
    env.setdefault("BATCH_SIZE", "200")
    
    # Stable data directory for InfluxDB so it persists between runs safely
    pepi_dir = os.path.expanduser("~/.pepi")
    influx_data_dir = os.path.join(pepi_dir, "influxdb_data")
    os.makedirs(influx_data_dir, exist_ok=True)
    env.setdefault("INFLUX_DB_DATA_DIRECTORY", influx_data_dir)
    
    # Fixed local credentials to avoid re-setup issues
    influx_pw = "pepi_influx_admin_123!"
    influx_token = "pepi_influx_token_auto_gen_123"
    grafana_pw = "pepi_grafana_admin_123!"
    
    env.setdefault("INFLUX_ADMIN_PASSWORD", influx_pw)
    env.setdefault("INFLUX_API_TOKEN", influx_token)
    env.setdefault("GRAFANA_ADMIN_PASSWORD", grafana_pw)
    env.setdefault("INFLUX_ORG", "org")
    env.setdefault("INFLUX_BUCKET", "bucket")
    
    cmd = get_docker_compose_cmd() + ["-f", compose_file, "up", "-d"]
    subprocess.run(cmd, env=env, check=True)
    
    logger.info("Influx URL = http://localhost:8086/")
    logger.info("Influx UI User = admin")
    logger.info("Influx UI Password = %s", env['INFLUX_ADMIN_PASSWORD'])
    logger.info("Influx API Token = %s", env['INFLUX_API_TOKEN'])
    logger.info("Grafana Dashboard URL = http://localhost:3001/d/ddnw277huiv40ae/ftdc-dashboard")
    logger.info("Grafana user = admin")
    logger.info("Grafana Password = %s", env['GRAFANA_ADMIN_PASSWORD'])
    logger.info("Tailing ftdc_exporter logs; Ctrl+C to stop trailing")
    
    log_cmd = get_docker_compose_cmd() + ["-f", compose_file, "logs", "-f", "ftdc_exporter"]
    grafana_url = "http://localhost:3001/d/ddnw277huiv40ae/ftdc-dashboard"
    url_found = False
    
    try:
        process = subprocess.Popen(log_cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        if process.stdout:
            for line in iter(process.stdout.readline, ''):
                if not line:
                    break
                sys.stdout.write(line)
                sys.stdout.flush()
                if "ftdc-dashboard?from=" in line and not url_found:
                    parsed_url = parse_grafana_url_from_line(line)
                    if parsed_url:
                        grafana_url = parsed_url
                        url_found = True
                        if open_browser:
                            logger.info("Data ingestion complete. Opening browser")
                            webbrowser.open(grafana_url)
        process.wait()
    except KeyboardInterrupt:
        logger.info("Stopped tailing logs")
        process.terminate()
        process.wait()
    except Exception as e:
        logger.error("Error reading logs: %s", e)
        
    if not url_found and open_browser:
        logger.info("Opening browser with default URL fallback")
        webbrowser.open(grafana_url)
