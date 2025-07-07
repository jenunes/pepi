import click
import json
import yaml
from collections import defaultdict
import statistics
import re
from tqdm import tqdm
import hashlib
import os
import pickle
from pathlib import Path
import time

# Cache management
CACHE_DIR = Path.home() / '.pepi_cache'
CACHE_DIR.mkdir(exist_ok=True)
CACHE_TTL_SECONDS = 7 * 24 * 3600  # 7 days

def is_cache_expired(cache_file):
    mtime = os.path.getmtime(cache_file)
    return (time.time() - mtime) > CACHE_TTL_SECONDS

def get_file_hash(filepath):
    """Calculate SHA256 hash of file for cache invalidation."""
    hash_sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()

def get_cache_key(filepath, analysis_type):
    """Generate cache key for specific analysis type."""
    file_hash = get_file_hash(filepath)
    return f"{file_hash}_{analysis_type}"

def get_cache_file(cache_key):
    """Get cache file path for given key."""
    return CACHE_DIR / f"{cache_key}.pkl"

def load_from_cache(cache_key):
    """Load cached results if available, valid, and not expired. Resets TTL on use."""
    cache_file = get_cache_file(cache_key)
    if cache_file.exists():
        if is_cache_expired(cache_file):
            cache_file.unlink(missing_ok=True)
            return None
        try:
            with open(cache_file, 'rb') as f:
                data = pickle.load(f)
            # Reset TTL: update mtime to now
            os.utime(cache_file, None)
            return data
        except Exception:
            # If cache is corrupted, remove it
            cache_file.unlink(missing_ok=True)
    return None

def save_to_cache(cache_key, data):
    """Save results to cache."""
    cache_file = get_cache_file(cache_key)
    try:
        with open(cache_file, 'wb') as f:
            pickle.dump(data, f)
    except Exception:
        # If cache write fails, continue without caching
        pass

class CustomCommand(click.Command):
    def format_help(self, ctx, formatter):
        """Custom help formatter with visual hierarchy."""
        # Write usage
        with formatter.section("Usage"):
            formatter.write_text("pepi.py --fetch <logfile> [OPTIONS]")
        
        # Write description
        with formatter.section("Description"):
            formatter.write_text("pepi: MongoDB log analysis tool for extracting insights from MongoDB log files.")
        
        # Write required options
        with formatter.section("Required Options"):
            formatter.write_text("--fetch, -f PATH    MongoDB log file to analyze")
        
        # Write main analysis modes
        with formatter.section("Main Analysis Modes"):
            formatter.write_text("--rs-conf           Print replica set configuration(s)")
            formatter.write_text("--rs-state          Print replica set node status and transitions")
            formatter.write_text("--clients           Print client/driver information")
            formatter.write_text("--queries           Print query pattern statistics and performance analysis")
        
        # Write connection analysis (with sub-options)
        with formatter.section("Connection Analysis"):
            formatter.write_text("--connections       Print connection information and statistics")
            formatter.write_text("")
            formatter.write_text("  Connection Sub-options (use with --connections):")
            formatter.write_text("    --stats          Include connection duration statistics")
            formatter.write_text("    --sort-by        Sort by: opened | closed")
            formatter.write_text("    --compare        Compare 2-3 specific hostnames/IPs")
        
        # Write query analysis (with sub-options)
        with formatter.section("Query Analysis"):
            formatter.write_text("--queries           Print query pattern statistics and performance analysis")
            formatter.write_text("")
            formatter.write_text("  Query Sub-options (use with --queries):")
            formatter.write_text("    --sort-by        Sort by: count | min | max | 95%-ile | sum | mean")
            formatter.write_text("    --report-full-patterns  Write complete patterns to file")
            formatter.write_text("")
            formatter.write_text("  Examples:")
            formatter.write_text("    pepi.py --fetch logfile --connections")
            formatter.write_text("    pepi.py --fetch logfile --connections --stats")
            formatter.write_text("    pepi.py --fetch logfile --connections --sort-by opened")
            formatter.write_text("    pepi.py --fetch logfile --connections --compare ip1 --compare ip2")
            formatter.write_text("    pepi.py --fetch logfile --connections --stats --sort-by opened --compare ip1 --compare ip2")
            formatter.write_text("    pepi.py --fetch logfile --queries")
            formatter.write_text("    pepi.py --fetch logfile --queries --sort-by count")
            formatter.write_text("    pepi.py --fetch logfile --queries --sort-by mean")
            formatter.write_text("    pepi.py --fetch logfile --queries --sort-by 95%-ile")
            formatter.write_text("    pepi.py --fetch logfile --queries --report-full-patterns report.txt")
        
        # Write default behavior
        with formatter.section("Default Behavior"):
            formatter.write_text("When no analysis mode is specified, shows MongoDB log summary and command line startup options.")

    def main(self, args=None, prog_name=None, complete_var=None, standalone_mode=True, **kwargs):
        try:
            return super().main(args, prog_name, complete_var, standalone_mode, **kwargs)
        except click.MissingParameter as e:
            if '--fetch' in str(e) or '-f' in str(e):
                click.echo("Pepi didn't find anything to fetch")
                return 1
            raise

def parse_connections(logfile):
    """Parse connection information from MongoDB log file."""
    # Check cache first
    cache_key = get_cache_key(logfile, 'connections')
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached connection data...")
        return cached_result['connections'], cached_result['total_opened'], cached_result['total_closed']
    
    def default_connection_data():
        return {'opened': 0, 'closed': 0, 'durations': []}
    
    connections = defaultdict(default_connection_data)
    total_opened = 0
    total_closed = 0
    connection_starts = {}  # Track connection start times by connection ID
    
    # Count lines for progress bar
    total_lines = count_lines(logfile)
    
    with open(logfile, 'r') as f:
        for line in tqdm(f, total=total_lines, desc="Parsing connections", unit="lines"):
            try:
                entry = json.loads(line)
                
                # Connection opened
                if (entry.get('msg') == 'Connection accepted' and 
                    entry.get('c') == 'NETWORK' and
                    entry.get('attr')):
                    attr = entry['attr']
                    if 'remote' in attr:
                        ip = attr['remote'].split(':')[0]  # Extract IP from host:port
                        connections[ip]['opened'] += 1
                        total_opened += 1
                        
                        # Track connection start time for duration calculation
                        if 'connectionId' in attr:
                            conn_id = attr['connectionId']
                            start_time = entry.get('t', {}).get('$date')
                            if start_time:
                                connection_starts[conn_id] = {
                                    'start_time': start_time,
                                    'ip': ip
                                }
                
                # Connection closed
                elif (entry.get('msg') == 'Connection ended' and 
                      entry.get('c') == 'NETWORK' and
                      entry.get('attr')):
                    attr = entry['attr']
                    if 'remote' in attr:
                        ip = attr['remote'].split(':')[0]  # Extract IP from host:port
                        connections[ip]['closed'] += 1
                        total_closed += 1
                        
                        # Calculate connection duration
                        if 'connectionId' in attr:
                            conn_id = attr['connectionId']
                            if conn_id in connection_starts:
                                start_data = connection_starts[conn_id]
                                if start_data['ip'] == ip:  # Ensure same IP
                                    start_time = start_data['start_time']
                                    end_time = entry.get('t', {}).get('$date')
                                    
                                    if start_time and end_time:
                                        # Parse timestamps and calculate duration
                                        try:
                                            from datetime import datetime
                                            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                                            end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                                            duration = (end_dt - start_dt).total_seconds()
                                            connections[ip]['durations'].append(duration)
                                        except:
                                            pass  # Skip if timestamp parsing fails
                                
                                # Clean up
                                del connection_starts[conn_id]
                        
            except Exception:
                pass
    
    # Save to cache - convert defaultdict to dict for pickling
    cache_data = {
        'connections': dict(connections),
        'total_opened': total_opened,
        'total_closed': total_closed
    }
    save_to_cache(cache_key, cache_data)
    
    return connections, total_opened, total_closed

def parse_replica_set_config(logfile):
    """Parse replica set configuration from MongoDB log file."""
    # Check cache first
    cache_key = get_cache_key(logfile, 'rs_config')
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached replica set config data...")
        return cached_result['configs']
    
    configs = []
    
    # Count lines for progress bar
    total_lines = count_lines(logfile)
    
    with open(logfile, 'r') as f:
        for line in tqdm(f, total=total_lines, desc="Parsing replica set config", unit="lines"):
            try:
                entry = json.loads(line)
                if (entry.get('msg') == 'New replica set config in use' and 
                    entry.get('c') == 'REPL' and
                    entry.get('attr', {}).get('config')):
                    config = entry['attr']['config']
                    configs.append({
                        'timestamp': entry.get('t', {}).get('$date'),
                        'config': config
                    })
            except Exception:
                pass
    
    # Save to cache
    cache_data = {'configs': configs}
    save_to_cache(cache_key, cache_data)
    
    return configs

def parse_replica_set_state(logfile):
    """Parse replica set state transitions and current node status from MongoDB log file."""
    # Check cache first
    cache_key = get_cache_key(logfile, 'rs_state')
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached replica set state data...")
        return cached_result['states'], cached_result['node_status']
    
    states = []
    node_status = {}
    replica_set_config = None
    current_host = None
    state_transitions = {}  # Track state transitions per host
    
    # Count lines for progress bar
    total_lines = count_lines(logfile)
    
    with open(logfile, 'r') as f:
        for line in tqdm(f, total=total_lines, desc="Parsing replica set state", unit="lines"):
            try:
                entry = json.loads(line)
                
                # Track current host from "Found self in config"
                if (entry.get('msg') == 'Found self in config' and 
                    entry.get('c') == 'REPL' and
                    entry.get('attr')):
                    current_host = entry.get('attr', {}).get('hostAndPort')
                
                # Track state transitions first
                if (entry.get('msg') == 'Replica set state transition' and 
                    entry.get('c') == 'REPL' and
                    entry.get('attr')):
                    attr = entry['attr']
                    new_state = attr.get('newState')
                    timestamp = entry.get('t', {}).get('$date')
                    
                    states.append({
                        'host': current_host,
                        'timestamp': timestamp,
                        'new_state': new_state,
                        'old_state': attr.get('oldState')
                    })
                    
                    # Track the latest state for the current host
                    if current_host:
                        state_transitions[current_host] = {
                            'state': new_state,
                            'timestamp': timestamp
                        }
                
                # Get replica set configuration
                if (entry.get('msg') == 'New replica set config in use' and 
                    entry.get('c') == 'REPL' and
                    entry.get('attr', {}).get('config')):
                    replica_set_config = entry['attr']['config']
                    # Initialize all nodes from config
                    if 'members' in replica_set_config:
                        for member in replica_set_config['members']:
                            host_port = member.get('host')
                            if host_port:
                                # Use the latest known state or STARTUP as default
                                latest_state = state_transitions.get(host_port, {'state': 'STARTUP', 'timestamp': entry.get('t', {}).get('$date')})
                                node_status[host_port] = {
                                    'state': latest_state['state'],
                                    'timestamp': latest_state['timestamp'],
                                    'member_id': member.get('_id')
                                }
                        
            except Exception:
                pass
    
    # Save to cache
    cache_data = {
        'states': states,
        'node_status': node_status
    }
    save_to_cache(cache_key, cache_data)
    
    return states, node_status

def parse_clients(logfile):
    """Parse client/driver information from MongoDB log file."""
    # Check cache first
    cache_key = get_cache_key(logfile, 'clients')
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached client data...")
        return cached_result['clients']
    
    clients = {}  # Group by driver info
    connection_drivers = {}  # Track which connections belong to which driver
    
    # Count lines for progress bar
    total_lines = count_lines(logfile)
    
    with open(logfile, 'r') as f:
        for line in tqdm(f, total=total_lines, desc="Parsing clients", unit="lines"):
            try:
                entry = json.loads(line)
                
                # Look for client metadata entries
                if (entry.get('c') == 'NETWORK' and 
                    entry.get('msg') == 'client metadata' and
                    entry.get('attr')):
                    attr = entry['attr']
                    remote = attr.get('remote', 'unknown')
                    client_id = attr.get('client', 'unknown')
                    
                    # Extract client document info
                    doc = attr.get('doc', {})
                    driver_info = doc.get('driver', {})
                    app_info = doc.get('application', {})
                    os_info = doc.get('os', {})
                    
                    # Build driver identifier
                    driver_name = driver_info.get('name', 'Unknown')
                    driver_version = driver_info.get('version', '')
                    app_name = app_info.get('name', '')
                    
                    # Create a unique driver identifier
                    if app_name:
                        driver_key = f"{app_name} v{driver_version}" if driver_version else app_name
                    else:
                        driver_key = f"{driver_name} v{driver_version}" if driver_version else driver_name
                    
                    # Initialize driver entry if not exists
                    if driver_key not in clients:
                        clients[driver_key] = {
                            'connections': set(),
                            'ips': set(),
                            'users': set(),
                            'driver_name': driver_name,
                            'driver_version': driver_version,
                            'app_name': app_name,
                            'os_name': os_info.get('name', ''),
                            'os_version': os_info.get('version', '')
                        }
                    
                    # Add connection info
                    clients[driver_key]['connections'].add(client_id)
                    clients[driver_key]['ips'].add(remote.split(':')[0])  # Extract IP from host:port
                    
                    # Track connection to driver mapping
                    connection_drivers[client_id] = driver_key
                
                # Also look for connection accepted entries (existing logic)
                elif (entry.get('c') == 'NETWORK' and 
                      entry.get('msg') == 'Connection accepted' and
                      entry.get('attr')):
                    attr = entry['attr']
                    remote = attr.get('remote', 'unknown')
                    client_id = attr.get('connectionId', 'unknown')
                    
                    # If we have driver info for this connection, associate it
                    if client_id in connection_drivers:
                        driver_key = connection_drivers[client_id]
                        if driver_key in clients:
                            clients[driver_key]['connections'].add(client_id)
                            clients[driver_key]['ips'].add(remote.split(':')[0])
                
            except Exception:
                pass
    
    # Convert sets to lists for JSON serialization
    for driver_key, client_info in clients.items():
        client_info['connections'] = list(client_info['connections'])
        client_info['ips'] = list(client_info['ips'])
        client_info['users'] = list(client_info['users'])
    
    # Save to cache
    cache_data = {'clients': clients}
    save_to_cache(cache_key, cache_data)
    
    return clients

def count_lines(logfile):
    """Count lines in file for progress bar."""
    try:
        with open(logfile, 'r') as f:
            return sum(1 for _ in f)
    except Exception:
        return 0

def extract_query_pattern(operation, command):
    """Extract a normalized query pattern string for grouping."""
    def normalize(obj):
        if isinstance(obj, dict):
            return {k: normalize(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [normalize(v) for v in obj]
        else:
            return '?'  # Replace all values with '?'

    if operation == 'find':
        # Use filter structure
        filt = command.get('filter', {})
        return json.dumps(normalize(filt), sort_keys=True)
    elif operation == 'update':
        # Use update structure
        updates = command.get('updates', [])
        return json.dumps(normalize(updates), sort_keys=True)
    elif operation == 'delete':
        deletes = command.get('deletes', [])
        return json.dumps(normalize(deletes), sort_keys=True)
    elif operation == 'insert':
        docs = command.get('documents', [])
        if docs and isinstance(docs, list):
            keys = sorted(set(k for doc in docs for k in doc.keys()))
            return 'insert_keys:' + ','.join(keys)
        return 'insert_keys:unknown'
    elif operation == 'aggregate':
        pipeline = command.get('pipeline', [])
        if pipeline and isinstance(pipeline, list):
            stages = [list(stage.keys())[0] for stage in pipeline if isinstance(stage, dict) and stage]
            return '[' + ','.join(stages) + ']'
        return '[unknown]'
    else:
        # For other commands, just show the keys
        return json.dumps(sorted(command.keys()))


def parse_queries(logfile):
    """Parse query patterns and statistics from MongoDB log file, grouped by pattern."""
    # Check cache first
    cache_key = get_cache_key(logfile, 'queries')
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached query data...")
        return cached_result['queries']
    
    def default_query_data():
        return {
            'count': 0,
            'durations': [],
            'allowDiskUse': False,
            'operations': set(),
            'pattern': None
        }
    
    queries = defaultdict(default_query_data)
    
    # Count lines for progress bar
    total_lines = count_lines(logfile)
    
    with open(logfile, 'r') as f:
        for line in tqdm(f, total=total_lines, desc="Parsing queries", unit="lines"):
            try:
                entry = json.loads(line)
                # Accept both 'command' and 'Slow query' as valid query log entries
                if (entry.get('c') == 'COMMAND' and 
                    entry.get('msg') in ('command', 'Slow query') and
                    entry.get('attr')):
                    attr = entry['attr']
                    # Extract namespace (database.collection)
                    namespace = attr.get('ns', '')
                    if not namespace:
                        continue
                    # Extract command details
                    command = attr.get('command', {})
                    if not command:
                        continue
                    # Get the operation type (first key in command)
                    operation = list(command.keys())[0] if command else 'unknown'
                    # Extract pattern
                    pattern = extract_query_pattern(operation, command)
                    # Group key: (namespace, operation, pattern)
                    group_key = (namespace, operation, pattern)
                    # Extract duration
                    duration_ms = attr.get('durationMillis', 0)
                    # Extract allowDiskUse flag
                    allow_disk_use = command.get('allowDiskUse', False)
                    # Update query statistics
                    queries[group_key]['count'] += 1
                    queries[group_key]['durations'].append(duration_ms)
                    queries[group_key]['operations'].add(operation)
                    queries[group_key]['allowDiskUse'] = queries[group_key]['allowDiskUse'] or allow_disk_use
                    queries[group_key]['pattern'] = pattern
            except Exception:
                pass
    # Save to cache - convert defaultdict to dict for pickling
    cache_data = {'queries': dict(queries)}
    save_to_cache(cache_key, cache_data)
    
    return queries

def calculate_query_stats(queries_data):
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
            'pattern': query_info['pattern']
        }
    
    return stats

def reconstruct_command_line(options):
    """Reconstruct the command line from MongoDB options."""
    if not options:
        return None
    
    cmd_parts = ['mongod']
    
    # Network options
    if 'net' in options:
        net_opts = options['net']
        if 'port' in net_opts:
            cmd_parts.append(f'--port {net_opts["port"]}')
    
    # Process management
    if 'processManagement' in options:
        pm_opts = options['processManagement']
        if pm_opts.get('fork'):
            cmd_parts.append('--fork')
    
    # Replication
    if 'replication' in options:
        repl_opts = options['replication']
        if 'replSet' in repl_opts:
            cmd_parts.append(f'--replSet {repl_opts["replSet"]}')
    
    # Security
    if 'security' in options:
        sec_opts = options['security']
        if 'keyFile' in sec_opts:
            cmd_parts.append(f'--keyFile {sec_opts["keyFile"]}')
    
    # Storage
    if 'storage' in options:
        storage_opts = options['storage']
        if 'dbPath' in storage_opts:
            cmd_parts.append(f'--dbpath {storage_opts["dbPath"]}')
        
        if 'wiredTiger' in storage_opts:
            wt_opts = storage_opts['wiredTiger']
            if 'engineConfig' in wt_opts:
                eng_opts = wt_opts['engineConfig']
                if 'cacheSizeGB' in eng_opts:
                    cmd_parts.append(f'--wiredTigerCacheSizeGB {eng_opts["cacheSizeGB"]}')
    
    # System log
    if 'systemLog' in options:
        syslog_opts = options['systemLog']
        if 'destination' in syslog_opts:
            cmd_parts.append(f'--logpath {syslog_opts.get("path", "/dev/null")}')
    
    return ' '.join(cmd_parts)

def calculate_connection_stats(connections_data):
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

@click.command(cls=CustomCommand)
@click.option('--fetch', '-f', 'logfile', type=click.Path(exists=True), 
              help='MongoDB log file to analyze.')
@click.option('--rs-conf', is_flag=True, 
              help='Print replica set configuration(s) from the log.')
@click.option('--rs-state', is_flag=True, 
              help='Print replica set node status and state transitions.')
@click.option('--connections', is_flag=True, 
              help='Print connection information and statistics.')
@click.option('--stats', is_flag=True, 
              help='Include connection duration statistics (use with --connections).')
@click.option('--sort-by', type=click.Choice(['opened', 'closed', 'count', 'min', 'max', '95%-ile', 'sum', 'mean']), 
              help='Sort by specified metric (use with --connections or --queries).')
@click.option('--compare', multiple=True, 
              help='Compare 2-3 specific hostnames/IPs (use with --connections).')
@click.option('--clients', is_flag=True, 
              help='Print client/driver information and authentication details.')
@click.option('--queries', is_flag=True, 
              help='Print query pattern statistics and performance analysis.')
@click.option('--report-full-patterns', type=click.Path(), 
              help='Write complete query patterns to file (requires output file path).')
@click.option('--clear-cache', is_flag=True, 
              help='Clear all cached data and re-parse files.')
def main(logfile, rs_conf, rs_state, connections, stats, clients, sort_by, compare, queries, report_full_patterns, clear_cache):
    """pepi: MongoDB log analysis tool."""
    
    # Check if logfile is provided
    if not logfile:
        click.echo("Pepi didn't find anything to fetch")
        return
    
    # Clear cache if requested
    if clear_cache:
        cache_files = list(CACHE_DIR.glob('*.pkl'))
        if cache_files:
            for cache_file in cache_files:
                cache_file.unlink()
            click.echo(f"Cleared {len(cache_files)} cached files.")
        else:
            click.echo("No cached files to clear.")
        return
    
    # Initialize variables for all sections
    start_date = None
    end_date = None
    num_lines = 0
    os_version = None
    kernel_version = None
    db_version = None
    cmd_options = None
    last_line = None

    # Check cache for basic log information
    cache_key = get_cache_key(logfile, 'basic_info')
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached basic log information...")
        start_date = cached_result['start_date']
        end_date = cached_result['end_date']
        num_lines = cached_result['num_lines']
        os_version = cached_result['os_version']
        kernel_version = cached_result['kernel_version']
        db_version = cached_result['db_version']
        cmd_options = cached_result['cmd_options']
    else:
                # Parse log file for basic information
        total_lines = count_lines(logfile)
        with open(logfile, 'r') as f:
            for line in tqdm(f, total=total_lines, desc="Reading log file", unit="lines"):
                num_lines += 1
                if not start_date:
                    try:
                        entry = json.loads(line)
                        start_date = entry.get('t', {}).get('$date')
                    except Exception:
                        pass
                # Check for OS and kernel version
                if not os_version or not kernel_version:
                    try:
                        entry = json.loads(line)
                        if entry.get('msg') == 'Operating System':
                            os_version = entry.get('attr', {}).get('os', {}).get('name')
                            kernel_version = entry.get('attr', {}).get('os', {}).get('version')
                    except Exception:
                        pass
                # Check for DB version
                if not db_version:
                    try:
                        entry = json.loads(line)
                        if entry.get('msg') == 'Build Info':
                            db_version = entry.get('attr', {}).get('buildInfo', {}).get('version')
                    except Exception:
                        pass
                # Check for command line options
                if not cmd_options:
                    try:
                        entry = json.loads(line)
                        if (
                            entry.get('msg') == 'Options set by command line' and
                            entry.get('ctx') == 'initandlisten'
                        ):
                            cmd_options = entry.get('attr', {}).get('options')
                    except Exception:
                        pass
                last_line = line

        # Get end date from last line
        if last_line:
            try:
                entry = json.loads(last_line)
                end_date = entry.get('t', {}).get('$date')
            except Exception:
                pass
        
        # Save to cache
        cache_data = {
            'start_date': start_date,
            'end_date': end_date,
            'num_lines': num_lines,
            'os_version': os_version,
            'kernel_version': kernel_version,
            'db_version': db_version,
            'cmd_options': cmd_options
        }
        save_to_cache(cache_key, cache_data)

    if rs_conf:
        click.echo("===== Replica Set Configuration =====")
        configs = parse_replica_set_config(logfile)
        if configs:
            # Show only the latest configuration
            latest_config = configs[-1]
            click.echo(f"Timestamp: {latest_config['timestamp']}")
            click.echo("-" * 50)
            json_str = json.dumps(latest_config['config'], indent=2, sort_keys=False)
            click.echo(json_str)
        else:
            click.echo("No replica set configuration found in the log.")
        return
    elif rs_state:
        click.echo("===== Replica Set State =====")
        states, node_status = parse_replica_set_state(logfile)
        
        # Show current node status
        if node_status:
            click.echo("\nCurrent Node Status:")
            click.echo("-" * 30)
            for host_port, status in node_status.items():
                click.echo(f"{host_port} - {status['state']} - {status['timestamp']}")
            click.echo()
        
        if states:
            click.echo("State Transitions:")
            click.echo("-" * 30)
            # Group states by host
            states_by_host = {}
            for state_data in states:
                host = state_data['host']
                if host not in states_by_host:
                    states_by_host[host] = []
                states_by_host[host].append(state_data)
            
            # Display grouped by host
            for host, host_states in states_by_host.items():
                click.echo(f"------- {host} -------")
                for state_data in host_states:
                    click.echo(f"{state_data['new_state']:<12} - {state_data['timestamp']}")
                click.echo("-" * 30)
        else:
            click.echo("No replica set state transitions found in the log.")
        return
    elif connections:
        # Get connection data
        connections_data, total_opened, total_closed = parse_connections(logfile)
        
        # Calculate statistics if requested
        overall_stats = None
        ip_stats = {}
        if stats:
            overall_stats, ip_stats = calculate_connection_stats(connections_data)
        
        # Display MongoDB Log Summary
        click.echo("===== MongoDB Log Summary =====")
        labels = [
            ("Log file", logfile),
            ("Start date", start_date if start_date else 'N/A'),
            ("End date", end_date if end_date else 'N/A'),
            ("Number of lines", num_lines),
        ]
        
        # Add host information if available
        host_info = None
        if cmd_options and 'net' in cmd_options and 'port' in cmd_options['net']:
            port = cmd_options['net']['port']
            # Try to get hostname from command line options or use localhost
            hostname = "localhost"  # Default
            host_info = f"{hostname}:{port}"
        if host_info:
            labels.append(("Host", host_info))
        
        labels.extend([
            ("OS version", os_version if os_version else 'N/A'),
            ("Kernel version", kernel_version if kernel_version else 'N/A'),
            ("MongoDB version", db_version if db_version else 'N/A'),
        ])
        # Add ReplicaSet Name if available
        repl_name = None
        if cmd_options and 'replication' in cmd_options and 'replSet' in cmd_options['replication']:
            repl_name = cmd_options['replication']['replSet']
        if repl_name:
            labels.append(("ReplicaSet Name", repl_name))
        # Add number of nodes if replica set config is available
        if repl_name:
            configs = parse_replica_set_config(logfile)
            if configs:
                # Get the latest config
                latest_config = configs[-1]['config']
                if 'members' in latest_config:
                    num_nodes = len(latest_config['members'])
                    labels.append(("Nodes", str(num_nodes)))
        
        max_label_len = max(len(label) for label, _ in labels)
        for label, value in labels:
            click.echo(f"{label.ljust(max_label_len)} : {value}")
        
        # Display connection information
        click.echo("\n===== Connection Details =====")
        click.echo(f"Total Connections Opened: {total_opened}")
        click.echo(f"Total Connections Closed: {total_closed}")
        
        # Display overall statistics if available
        if stats and overall_stats:
            click.echo(f"Overall Average Connection Duration: {overall_stats['avg']:.2f}s")
            click.echo(f"Overall Minimum Connection Duration: {overall_stats['min']:.2f}s")
            click.echo(f"Overall Maximum Connection Duration: {overall_stats['max']:.2f}s")
        
        # Sort connections if requested
        if sort_by:
            click.echo(f"--------Sorted by {sort_by}--------")
            if sort_by == 'opened':
                sorted_ips = sorted(connections_data.keys(), key=lambda ip: connections_data[ip]['opened'], reverse=True)
            elif sort_by == 'closed':
                sorted_ips = sorted(connections_data.keys(), key=lambda ip: connections_data[ip]['closed'], reverse=True)
        else:
            sorted_ips = list(connections_data.keys())
        
        # Filter for comparison if requested
        if compare:
            if len(compare) < 2:
                click.echo("Error: --compare requires at least 2 hostnames/IPs to compare.")
                return
            elif len(compare) > 3:
                click.echo("Error: --compare accepts maximum 3 hostnames/IPs. Only the first 3 will be used.")
                compare = compare[:3]
            
            click.echo(f"--------Comparing {', '.join(compare)}--------")
            # Filter to only show the specified hostnames/IPs
            filtered_ips = []
            for host in compare:
                if host in connections_data:
                    filtered_ips.append(host)
                else:
                    click.echo(f"Warning: {host} not found in connection data")
            
            if filtered_ips:
                # Keep the order of compare, not sorted_ips
                sorted_ips = [ip for ip in compare if ip in filtered_ips]
            else:
                click.echo("No matching hostnames/IPs found for comparison")
                return
        
        # Display per-IP details
        # Calculate max width for IP for alignment
        ip_list = list(sorted_ips)
        if ip_list:
            max_ip_len = max(len(ip) for ip in ip_list)
        else:
            max_ip_len = 15  # fallback
        fmt = f"{{:<{max_ip_len}}} | opened:{{:<5}} | closed:{{:<5}}"
        for ip in sorted_ips:
            conn_info = connections_data[ip]
            if stats and ip in ip_stats:
                stats_info = ip_stats[ip]
                click.echo(f"{ip.ljust(max_ip_len)} | opened:{str(conn_info['opened']).ljust(5)} | closed:{str(conn_info['closed']).ljust(5)} | dur-avg:{stats_info['avg']:.2f}s | dur-min:{stats_info['min']:.2f}s | dur-max:{stats_info['max']:.2f}s")
            else:
                click.echo(fmt.format(ip, conn_info['opened'], conn_info['closed']))
        return
    elif clients:
        # Get client data
        clients_data = parse_clients(logfile)
        
        # Display MongoDB Log Summary
        click.echo("===== MongoDB Log Summary =====")
        labels = [
            ("Log file", logfile),
            ("Start date", start_date if start_date else 'N/A'),
            ("End date", end_date if end_date else 'N/A'),
            ("Number of lines", num_lines),
        ]
        
        # Add host information if available
        host_info = None
        if cmd_options and 'net' in cmd_options and 'port' in cmd_options['net']:
            port = cmd_options['net']['port']
            # Try to get hostname from command line options or use localhost
            hostname = "localhost"  # Default
            host_info = f"{hostname}:{port}"
        if host_info:
            labels.append(("Host", host_info))
        
        labels.extend([
            ("OS version", os_version if os_version else 'N/A'),
            ("Kernel version", kernel_version if kernel_version else 'N/A'),
            ("MongoDB version", db_version if db_version else 'N/A'),
        ])
        # Add ReplicaSet Name if available
        repl_name = None
        if cmd_options and 'replication' in cmd_options and 'replSet' in cmd_options['replication']:
            repl_name = cmd_options['replication']['replSet']
        if repl_name:
            labels.append(("ReplicaSet Name", repl_name))
        # Add number of nodes if replica set config is available
        if repl_name:
            configs = parse_replica_set_config(logfile)
            if configs:
                # Get the latest config
                latest_config = configs[-1]['config']
                if 'members' in latest_config:
                    num_nodes = len(latest_config['members'])
                    labels.append(("Nodes", str(num_nodes)))
        
        max_label_len = max(len(label) for label, _ in labels)
        for label, value in labels:
            click.echo(f"{label.ljust(max_label_len)} : {value}")
        
        # Display client information
        click.echo("\n===== Client/Driver Information =====")
        if clients_data:
            for driver_key, client_info in clients_data.items():
                # Extract driver info for cleaner display
                driver_name = client_info['driver_name']
                driver_version = client_info['driver_version']
                app_info = client_info['app_name']
                
                # Format driver header
                if app_info:
                    click.echo(f"\n{driver_name} v{driver_version} ({app_info})")
                else:
                    click.echo(f"\n{driver_name} v{driver_version}")
                
                # Display connections count
                click.echo(f"├─ Connections: {len(client_info['connections'])}")
                
                # Display IP addresses
                ips = sorted(client_info['ips'])
                if ips:
                    click.echo(f"├─ IP Addresses: {', '.join(ips)}")
                else:
                    click.echo("├─ IP Addresses: None")
                
                # Display users
                users = sorted(client_info['users'])
                if users:
                    click.echo(f"└─ Users: {', '.join(users)}")
                else:
                    click.echo("└─ Users: None")
        else:
            click.echo("No client/driver information found in the log.")
        return
    elif queries:
        # Get query data
        queries_data = parse_queries(logfile)
        
        # Calculate statistics
        query_stats = calculate_query_stats(queries_data)
        
        # Handle full patterns output to file
        if report_full_patterns:
            with open(report_full_patterns, 'w') as f:
                f.write("===== Complete Query Pattern Statistics =====\n")
                f.write(f"Log file: {logfile}\n")
                f.write(f"Analysis date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                # Sort if requested
                if sort_by:
                    if sort_by == 'count':
                        sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['count'], reverse=True)
                    elif sort_by == 'min':
                        sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['min'], reverse=True)
                    elif sort_by == 'max':
                        sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['max'], reverse=True)
                    elif sort_by == '95%-ile':
                        sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['percentile_95'], reverse=True)
                    elif sort_by == 'sum':
                        sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['sum'], reverse=True)
                    elif sort_by == 'mean':
                        sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['mean'], reverse=True)
                    else:
                        sorted_queries = list(query_stats.items())
                else:
                    sorted_queries = list(query_stats.items())
                
                # Calculate column widths for file output
                if sorted_queries:
                    max_namespace_len = max(len(str(key[0])) for key in query_stats.keys())
                    max_operation_len = max(len(str(key[1])) for key in query_stats.keys())
                    max_pattern_len = max(len(pattern) for (_, _, pattern) in query_stats.keys())
                    max_count_len = max(len(str(stats['count'])) for stats in query_stats.values())
                    max_min_len = max(len(f"{stats['min']:.1f}") for stats in query_stats.values())
                    max_max_len = max(len(f"{stats['max']:.1f}") for stats in query_stats.values())
                    max_percentile_len = max(len(f"{stats['percentile_95']:.1f}") for stats in query_stats.values())
                    max_sum_len = max(len(f"{stats['sum']:.1f}") for stats in query_stats.values())
                    max_mean_len = max(len(f"{stats['mean']:.1f}") for stats in query_stats.values())
                    
                    # Ensure minimum widths for headers
                    max_namespace_len = max(max_namespace_len, len("Namespace"))
                    max_operation_len = max(max_operation_len, len("Operation"))
                    max_pattern_len = max(max_pattern_len, len("Pattern"))
                    max_count_len = max(max_count_len, len("Count"))
                    max_min_len = max(max_min_len, len("Min(ms)"))
                    max_max_len = max(max_max_len, len("Max(ms)"))
                    max_percentile_len = max(max_percentile_len, len("95%-ile(ms)"))
                    max_sum_len = max(max_sum_len, len("Sum(ms)"))
                    max_mean_len = max(max_mean_len, len("Mean(ms)"))
                else:
                    # Default widths if no data
                    max_namespace_len = len("Namespace")
                    max_operation_len = len("Operation")
                    max_pattern_len = len("Pattern")
                    max_count_len = len("Count")
                    max_min_len = len("Min(ms)")
                    max_max_len = len("Max(ms)")
                    max_percentile_len = len("95%-ile(ms)")
                    max_sum_len = len("Sum(ms)")
                    max_mean_len = len("Mean(ms)")
                
                # Write header with proper alignment
                header_fmt = f"{{:<{max_namespace_len}}} | {{:<{max_operation_len}}} | {{:<{max_pattern_len}}} | {{:>{max_count_len}}} | {{:>{max_min_len}}} | {{:>{max_max_len}}} | {{:>{max_percentile_len}}} | {{:>{max_sum_len}}} | {{:>{max_mean_len}}} | {{:<8}}"
                f.write(header_fmt.format("Namespace", "Operation", "Pattern", "Count", "Min(ms)", "Max(ms)", "95%-ile(ms)", "Sum(ms)", "Mean(ms)", "AllowDiskUse") + "\n")
                f.write("-" * (max_namespace_len + max_operation_len + max_pattern_len + max_count_len + max_min_len + max_max_len + max_percentile_len + max_sum_len + max_mean_len + 32))  # 32 for separators, spaces, and AllowDiskUse column
                f.write("\n")
                
                # Write each query with full pattern and proper alignment
                for (namespace, operation, pattern), stats_info in sorted_queries:
                    row_fmt = f"{{:<{max_namespace_len}}} | {{:<{max_operation_len}}} | {{:<{max_pattern_len}}} | {{:>{max_count_len}}} | {{:>{max_min_len}}} | {{:>{max_max_len}}} | {{:>{max_percentile_len}}} | {{:>{max_sum_len}}} | {{:>{max_mean_len}}} | {{:<8}}"
                    f.write(row_fmt.format(
                        namespace,
                        operation,
                        pattern,
                        stats_info['count'],
                        f"{stats_info['min']:.1f}",
                        f"{stats_info['max']:.1f}",
                        f"{stats_info['percentile_95']:.1f}",
                        f"{stats_info['sum']:.1f}",
                        f"{stats_info['mean']:.1f}",
                        'Yes' if stats_info['allowDiskUse'] else 'No'
                    ) + "\n")
            
            click.echo(f"Complete query patterns written to: {report_full_patterns}")
            return
        
        # Display MongoDB Log Summary
        click.echo("===== MongoDB Log Summary =====")
        labels = [
            ("Log file", logfile),
            ("Start date", start_date if start_date else 'N/A'),
            ("End date", end_date if end_date else 'N/A'),
            ("Number of lines", num_lines),
        ]
        
        # Add host information if available
        host_info = None
        if cmd_options and 'net' in cmd_options and 'port' in cmd_options['net']:
            port = cmd_options['net']['port']
            # Try to get hostname from command line options or use localhost
            hostname = "localhost"  # Default
            host_info = f"{hostname}:{port}"
        if host_info:
            labels.append(("Host", host_info))
        
        labels.extend([
            ("OS version", os_version if os_version else 'N/A'),
            ("Kernel version", kernel_version if kernel_version else 'N/A'),
            ("MongoDB version", db_version if db_version else 'N/A'),
        ])
        # Add ReplicaSet Name if available
        repl_name = None
        if cmd_options and 'replication' in cmd_options and 'replSet' in cmd_options['replication']:
            repl_name = cmd_options['replication']['replSet']
        if repl_name:
            labels.append(("ReplicaSet Name", repl_name))
        # Add number of nodes if replica set config is available
        if repl_name:
            configs = parse_replica_set_config(logfile)
            if configs:
                # Get the latest config
                latest_config = configs[-1]['config']
                if 'members' in latest_config:
                    num_nodes = len(latest_config['members'])
                    labels.append(("Nodes", str(num_nodes)))
        
        max_label_len = max(len(label) for label, _ in labels)
        for label, value in labels:
            click.echo(f"{label.ljust(max_label_len)} : {value}")
        
        # Display query pattern statistics
        click.echo("\n===== Query Pattern Statistics =====")
        
        # Sort if requested
        if sort_by:
            if sort_by == 'count':
                sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['count'], reverse=True)
            elif sort_by == 'min':
                sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['min'], reverse=True)
            elif sort_by == 'max':
                sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['max'], reverse=True)
            elif sort_by == '95%-ile':
                sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['percentile_95'], reverse=True)
            elif sort_by == 'sum':
                sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['sum'], reverse=True)
            elif sort_by == 'mean':
                sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['mean'], reverse=True)
            else:
                sorted_queries = list(query_stats.items())
        else:
            sorted_queries = list(query_stats.items())
        
        # Calculate column widths for proper alignment
        if sorted_queries:
            # Calculate max widths for each column
            max_namespace_len = max(len(str(key[0])) for key in query_stats.keys())
            max_operation_len = max(len(str(key[1])) for key in query_stats.keys())
            max_pattern_len = max(len(pattern[:150] + "..." if len(pattern) > 150 else pattern) for (_, _, pattern) in query_stats.keys())
            max_count_len = max(len(str(stats['count'])) for stats in query_stats.values())
            max_min_len = max(len(f"{stats['min']:.1f}") for stats in query_stats.values())
            max_max_len = max(len(f"{stats['max']:.1f}") for stats in query_stats.values())
            max_percentile_len = max(len(f"{stats['percentile_95']:.1f}") for stats in query_stats.values())
            max_sum_len = max(len(f"{stats['sum']:.1f}") for stats in query_stats.values())
            max_mean_len = max(len(f"{stats['mean']:.1f}") for stats in query_stats.values())
            
            # Ensure minimum widths for headers
            max_namespace_len = max(max_namespace_len, len("Namespace"))
            max_operation_len = max(max_operation_len, len("Operation"))
            max_pattern_len = max(max_pattern_len, len("Pattern"))
            max_count_len = max(max_count_len, len("Count"))
            max_min_len = max(max_min_len, len("Min(ms)"))
            max_max_len = max(max_max_len, len("Max(ms)"))
            max_percentile_len = max(max_percentile_len, len("95%-ile(ms)"))
            max_sum_len = max(max_sum_len, len("Sum(ms)"))
            max_mean_len = max(max_mean_len, len("Mean(ms)"))
        else:
            # Default widths if no data
            max_namespace_len = len("Namespace")
            max_operation_len = len("Operation")
            max_pattern_len = len("Pattern")
            max_count_len = len("Count")
            max_min_len = len("Min(ms)")
            max_max_len = len("Max(ms)")
            max_percentile_len = len("95%-ile(ms)")
            max_sum_len = len("Sum(ms)")
            max_mean_len = len("Mean(ms)")
        
        # Display header with proper alignment
        header_fmt = f"{{:<{max_namespace_len}}} | {{:<{max_operation_len}}} | {{:<{max_pattern_len}}} | {{:>{max_count_len}}} | {{:>{max_min_len}}} | {{:>{max_max_len}}} | {{:>{max_percentile_len}}} | {{:>{max_sum_len}}} | {{:>{max_mean_len}}}"
        click.echo(header_fmt.format("Namespace", "Operation", "Pattern", "Count", "Min(ms)", "Max(ms)", "95%-ile(ms)", "Sum(ms)", "Mean(ms)"))
        click.echo("-" * (max_namespace_len + max_operation_len + max_pattern_len + max_count_len + max_min_len + max_max_len + max_percentile_len + max_sum_len + max_mean_len + 24))  # 24 for separators and spaces
        
        # Display each query with truncated pattern and proper alignment
        for (namespace, operation, pattern), stats_info in sorted_queries:
            # Truncate pattern to 150 characters
            display_pattern = pattern[:150] + "..." if len(pattern) > 150 else pattern
            
            row_fmt = f"{{:<{max_namespace_len}}} | {{:<{max_operation_len}}} | {{:<{max_pattern_len}}} | {{:>{max_count_len}}} | {{:>{max_min_len}}} | {{:>{max_max_len}}} | {{:>{max_percentile_len}}} | {{:>{max_sum_len}}} | {{:>{max_mean_len}}}"
            click.echo(row_fmt.format(
                namespace,
                operation,
                display_pattern,
                stats_info['count'],
                f"{stats_info['min']:.1f}",
                f"{stats_info['max']:.1f}",
                f"{stats_info['percentile_95']:.1f}",
                f"{stats_info['sum']:.1f}",
                f"{stats_info['mean']:.1f}"
            ))
        
        if not query_stats:
            click.echo("No query patterns found in the log.")
        else:
            click.echo("\n💡 For the full pattern report, use --report-full-patterns <output-file>.")
        return

    # Default: summary mode
    # Print reconstructed command line
    click.echo("===== Node Command Line Startup =====")
    command_line = reconstruct_command_line(cmd_options)
    if command_line:
        click.echo(command_line, nl=False)
        click.echo()
    else:
        click.echo("No command line options found.")
    click.echo()

    # Prepare aligned output
    labels = [
        ("Log file", logfile),
        ("Start date", start_date if start_date else 'N/A'),
        ("End date", end_date if end_date else 'N/A'),
        ("Number of lines", num_lines),
    ]
    
    # Add host information if available
    host_info = None
    if cmd_options and 'net' in cmd_options and 'port' in cmd_options['net']:
        port = cmd_options['net']['port']
        # Try to get hostname from command line options or use localhost
        hostname = "localhost"  # Default
        host_info = f"{hostname}:{port}"
    if host_info:
        labels.append(("Host", host_info))
    
    labels.extend([
        ("OS version", os_version if os_version else 'N/A'),
        ("Kernel version", kernel_version if kernel_version else 'N/A'),
        ("MongoDB version", db_version if db_version else 'N/A'),
    ])
    # Add ReplicaSet Name if available
    repl_name = None
    if cmd_options and 'replication' in cmd_options and 'replSet' in cmd_options['replication']:
        repl_name = cmd_options['replication']['replSet']
    if repl_name:
        labels.append(("ReplicaSet Name", repl_name))
    # Add number of nodes if replica set config is available
    if repl_name:
        configs = parse_replica_set_config(logfile)
        if configs:
            # Get the latest config
            latest_config = configs[-1]['config']
            if 'members' in latest_config:
                num_nodes = len(latest_config['members'])
                labels.append(("Nodes", str(num_nodes)))
    
    max_label_len = max(len(label) for label, _ in labels)

    click.echo("===== MongoDB Log Summary =====")
    for label, value in labels:
        click.echo(f"{label.ljust(max_label_len)} : {value}")

if __name__ == '__main__':
    main()  # Let Click handle argument parsing
