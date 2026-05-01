"""MongoDB log file parsing functions for Pepi."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from typing import Any, Optional

import click
from tqdm import tqdm

from pepi.cache import build_cache_variant, get_cache_key, load_from_cache, save_to_cache
from pepi.sampling import get_sample_rate, get_sample_rate_from_percentage, get_sampling_metadata
from pepi.utils import count_lines


def _build_sampling_cache_variant(total_lines: int, sample_percentage: Optional[int]) -> str:
    """Build cache variant using effective sampling behavior."""
    if sample_percentage is not None:
        sample_rate = get_sample_rate_from_percentage(sample_percentage, total_lines)
    else:
        sample_rate = get_sample_rate(total_lines)
    return build_cache_variant(
        {
            "total_lines": total_lines,
            "sample_percentage": sample_percentage,
            "sample_rate": sample_rate,
        }
    )


def parse_connections(logfile: str, sample_percentage: Optional[int] = None) -> tuple[dict, int, int]:
    """Parse connection information from MongoDB log file."""
    total_lines = count_lines(logfile)
    cache_key = get_cache_key(
        logfile,
        'connections',
        variant=_build_sampling_cache_variant(total_lines, sample_percentage),
    )
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached connection data...")
        return cached_result['connections'], cached_result['total_opened'], cached_result['total_closed']
    
    def default_connection_data():
        return {'opened': 0, 'closed': 0, 'durations': []}
    
    connections = defaultdict(default_connection_data)
    total_opened = 0
    total_closed = 0
    connection_starts = {}
    
    if sample_percentage is not None:
        sample_rate = get_sample_rate_from_percentage(sample_percentage, total_lines)
        is_sampled = sample_rate > 1
        if is_sampled:
            click.echo(f"Sampling {sample_percentage}% of file ({total_lines:,} lines). Processing every {sample_rate} lines...")
    else:
        sample_rate = get_sample_rate(total_lines)
        is_sampled = sample_rate > 1
        if is_sampled:
            click.echo(f"Large file detected ({total_lines:,} lines). Sampling every {sample_rate} lines for performance...")
    
    with open(logfile, 'r') as f:
        line_count = 0
        for line in tqdm(f, total=total_lines, desc="Parsing connections", unit="lines"):
            line_count += 1
            
            if is_sampled and line_count % sample_rate != 0:
                continue
            try:
                entry = json.loads(line)
                
                if (entry.get('msg') == 'Connection accepted' and 
                    entry.get('c') == 'NETWORK' and
                    entry.get('attr')):
                    attr = entry['attr']
                    if 'remote' in attr:
                        ip = attr['remote'].split(':')[0]
                        connections[ip]['opened'] += 1
                        total_opened += 1
                        
                        if 'connectionId' in attr:
                            conn_id = attr['connectionId']
                            start_time = entry.get('t', {}).get('$date')
                            if start_time:
                                connection_starts[conn_id] = {
                                    'start_time': start_time,
                                    'ip': ip
                                }
                
                elif (entry.get('msg') == 'Connection ended' and 
                      entry.get('c') == 'NETWORK' and
                      entry.get('attr')):
                    attr = entry['attr']
                    if 'remote' in attr:
                        ip = attr['remote'].split(':')[0]
                        connections[ip]['closed'] += 1
                        total_closed += 1
                        
                        if 'connectionId' in attr:
                            conn_id = attr['connectionId']
                            if conn_id in connection_starts:
                                start_data = connection_starts[conn_id]
                                if start_data['ip'] == ip:
                                    start_time = start_data['start_time']
                                    end_time = entry.get('t', {}).get('$date')
                                    
                                    if start_time and end_time:
                                        try:
                                            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                                            end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                                            duration = (end_dt - start_dt).total_seconds()
                                            connections[ip]['durations'].append(duration)
                                        except (ValueError, TypeError, OverflowError):
                                            pass
                                
                                del connection_starts[conn_id]
                        
            except Exception:
                pass
    
    cache_data = {
        'connections': dict(connections),
        'total_opened': total_opened,
        'total_closed': total_closed,
        'sampling_metadata': get_sampling_metadata(total_lines, sample_percentage)
    }
    save_to_cache(cache_key, cache_data)
    
    return connections, total_opened, total_closed


def parse_replica_set_config(logfile: str) -> list[dict[str, Any]]:
    """Parse replica set configuration from MongoDB log file."""
    cache_key = get_cache_key(logfile, 'rs_config')
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached replica set config data...")
        return cached_result['configs']
    
    configs = []
    
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
    
    cache_data = {'configs': configs}
    save_to_cache(cache_key, cache_data)
    
    return configs


def parse_replica_set_state(logfile: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Parse replica set state transitions and current node status from MongoDB log file."""
    cache_key = get_cache_key(logfile, 'rs_state')
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached replica set state data...")
        return cached_result['states'], cached_result['node_status']
    
    states = []
    node_status = {}
    replica_set_config = None
    current_host = None
    state_transitions = {}
    
    total_lines = count_lines(logfile)
    
    with open(logfile, 'r') as f:
        for line in tqdm(f, total=total_lines, desc="Parsing replica set state", unit="lines"):
            try:
                entry = json.loads(line)
                
                if (entry.get('msg') == 'Found self in config' and 
                    entry.get('c') == 'REPL' and
                    entry.get('attr')):
                    current_host = entry.get('attr', {}).get('hostAndPort')
                
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
                    
                    if current_host:
                        state_transitions[current_host] = {
                            'state': new_state,
                            'timestamp': timestamp
                        }
                
                if (entry.get('msg') == 'New replica set config in use' and 
                    entry.get('c') == 'REPL' and
                    entry.get('attr', {}).get('config')):
                    replica_set_config = entry['attr']['config']
                    if 'members' in replica_set_config:
                        for member in replica_set_config['members']:
                            host_port = member.get('host')
                            if host_port:
                                latest_state = state_transitions.get(host_port, {'state': 'STARTUP', 'timestamp': entry.get('t', {}).get('$date')})
                                node_status[host_port] = {
                                    'state': latest_state['state'],
                                    'timestamp': latest_state['timestamp'],
                                    'member_id': member.get('_id')
                                }
                        
            except Exception:
                pass
    
    cache_data = {
        'states': states,
        'node_status': node_status
    }
    save_to_cache(cache_key, cache_data)
    
    return states, node_status


def parse_clients(logfile: str) -> dict[str, dict[str, Any]]:
    """Parse client/driver information from MongoDB log file."""
    cache_key = get_cache_key(logfile, 'clients')
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached client data...")
        return cached_result['clients']
    
    clients = {}
    connection_drivers = {}
    
    total_lines = count_lines(logfile)
    
    with open(logfile, 'r') as f:
        for line in tqdm(f, total=total_lines, desc="Parsing clients", unit="lines"):
            try:
                entry = json.loads(line)
                
                if (entry.get('c') == 'NETWORK' and 
                    entry.get('msg') == 'client metadata' and
                    entry.get('attr')):
                    attr = entry['attr']
                    remote = attr.get('remote', 'unknown')
                    client_id = attr.get('client', 'unknown')
                    
                    doc = attr.get('doc', {})
                    driver_info = doc.get('driver', {})
                    app_info = doc.get('application', {})
                    os_info = doc.get('os', {})
                    
                    driver_name = driver_info.get('name', 'Unknown')
                    driver_version = driver_info.get('version', '')
                    app_name = app_info.get('name', '')
                    
                    if app_name:
                        driver_key = f"{app_name} v{driver_version}" if driver_version else app_name
                    else:
                        driver_key = f"{driver_name} v{driver_version}" if driver_version else driver_name
                    
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
                    
                    clients[driver_key]['connections'].add(client_id)
                    clients[driver_key]['ips'].add(remote.split(':')[0])
                    
                    connection_drivers[client_id] = driver_key
                
                elif (entry.get('c') == 'NETWORK' and 
                      entry.get('msg') == 'Connection accepted' and
                      entry.get('attr')):
                    attr = entry['attr']
                    remote = attr.get('remote', 'unknown')
                    client_id = attr.get('connectionId', 'unknown')
                    
                    if client_id in connection_drivers:
                        driver_key = connection_drivers[client_id]
                        if driver_key in clients:
                            clients[driver_key]['connections'].add(client_id)
                            clients[driver_key]['ips'].add(remote.split(':')[0])
                
            except Exception:
                pass
    
    for driver_key, client_info in clients.items():
        client_info['connections'] = list(client_info['connections'])
        client_info['ips'] = list(client_info['ips'])
        client_info['users'] = list(client_info['users'])
    
    cache_data = {'clients': clients}
    save_to_cache(cache_key, cache_data)
    
    return clients


def extract_query_pattern(operation: str, command: dict[str, Any]) -> str:
    """Extract a normalized query pattern string for grouping."""
    def normalize(obj):
        if isinstance(obj, dict):
            return {k: normalize(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [normalize(v) for v in obj]
        else:
            return '?'

    if operation == 'find':
        filt = command.get('filter', {})
        return json.dumps(normalize(filt), sort_keys=True)
    elif operation == 'update':
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
            stages = []
            for stage in pipeline:
                if isinstance(stage, dict) and stage:
                    stage_keys = list(stage.keys())
                    if stage_keys:
                        stages.append(stage_keys[0])
            return json.dumps(stages)
        return '[unknown]'
    else:
        return json.dumps(sorted(command.keys()))


def parse_timeseries_data(logfile: str) -> tuple[list[dict], list[dict], list[dict]]:
    """Parse time-series data for slow queries, connections, and errors."""
    cache_key = get_cache_key(logfile, 'timeseries')
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached time-series data...")
        return cached_result['slow_queries'], cached_result['connections'], cached_result['errors']
    
    slow_queries = []
    connections = []
    errors = []
    
    total_lines = count_lines(logfile)
    
    with open(logfile, 'r') as f:
        for line in tqdm(f, total=total_lines, desc="Parsing time-series data", unit="lines"):
            try:
                entry = json.loads(line)
                timestamp = entry.get('t', {}).get('$date')
                
                if not timestamp:
                    continue
                
                if entry.get('c') == 'COMMAND' and entry.get('msg') in ('command', 'Slow query'):
                    attr = entry.get('attr', {})
                    namespace = attr.get('ns', '')
                    duration_ms = attr.get('durationMillis', 0)
                    command = attr.get('command', {})
                    plan_summary = attr.get('planSummary', 'N/A')
                    
                    if namespace and duration_ms > 0:
                        locks = attr.get('locks')
                        slow_queries.append({
                            'timestamp': timestamp,
                            'duration_ms': duration_ms,
                            'namespace': namespace,
                            'command': command,
                            'plan_summary': plan_summary,
                            'keysExamined': int(attr.get('keysExamined') or 0),
                            'docsExamined': int(attr.get('docsExamined') or 0),
                            'nreturned': int(attr.get('nreturned') or 0),
                            'hasSortStage': bool(attr.get('hasSortStage', False)),
                            'usedDisk': bool(attr.get('usedDisk', False)),
                            'numYields': int(attr.get('numYields') or 0),
                            'reslen': int(attr.get('reslen') or 0),
                            'locksPresent': bool(locks) if locks is not None else False,
                        })
                
                elif entry.get('c') == 'NETWORK' and entry.get('msg') == 'Connection accepted':
                    attr = entry.get('attr', {})
                    connection_count = attr.get('connectionCount', 0)
                    
                    connections.append({
                        'timestamp': timestamp,
                        'connection_count': connection_count
                    })
                
                severity = entry.get('s', '')
                msg = entry.get('msg', '')
                if severity in ('E', 'W') or 'error' in msg.lower() or 'warning' in msg.lower():
                    errors.append({
                        'timestamp': timestamp,
                        'severity': severity,
                        'message': msg,
                        'component': entry.get('c', 'Unknown')
                    })
                    
            except Exception:
                pass
    
    cache_data = {
        'slow_queries': slow_queries,
        'connections': connections,
        'errors': errors
    }
    save_to_cache(cache_key, cache_data)
    
    return slow_queries, connections, errors


def parse_connections_timeseries_by_ip(logfile: str) -> dict[str, list[dict]]:
    """Parse connection time series data grouped by IP address with improved edge case handling."""
    cache_key = get_cache_key(logfile, 'connections_timeseries_by_ip')
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached IP-specific connection data...")
        return cached_result['connections_by_ip']
    
    ip_events = defaultdict(list)
    connection_starts = {}
    connection_ends = {}
    total_connection_counts = []
    
    total_lines = count_lines(logfile)
    
    with open(logfile, 'r') as f:
        for line in tqdm(f, total=total_lines, desc="Parsing IP-specific connections", unit="lines"):
            try:
                entry = json.loads(line)
                timestamp = entry.get('t', {}).get('$date')
                
                if not timestamp:
                    continue
                
                if (entry.get('msg') == 'Connection accepted' and 
                    entry.get('c') == 'NETWORK' and
                    entry.get('attr')):
                    attr = entry['attr']
                    if 'remote' in attr:
                        ip = attr['remote'].split(':')[0]
                        conn_id = attr.get('connectionId')
                        connection_count = attr.get('connectionCount', 0)
                        
                        total_connection_counts.append({
                            'timestamp': timestamp,
                            'connection_count': connection_count
                        })
                        
                        ip_events[ip].append({
                            'timestamp': timestamp,
                            'event': 'open',
                            'connection_id': conn_id
                        })
                        
                        if conn_id:
                            connection_starts[conn_id] = {
                                'start_time': timestamp,
                                'ip': ip
                            }
                
                elif (entry.get('msg') == 'Connection ended' and 
                      entry.get('c') == 'NETWORK' and
                      entry.get('attr')):
                    attr = entry['attr']
                    conn_id = attr.get('connectionId')
                    
                    if conn_id in connection_starts:
                        ip = connection_starts[conn_id]['ip']
                        ip_events[ip].append({
                            'timestamp': timestamp,
                            'event': 'close',
                            'connection_id': conn_id
                        })
                        connection_ends[conn_id] = timestamp
                        del connection_starts[conn_id]
                        
            except Exception:
                pass
    
    connections_by_ip = {}
    data_quality_metrics = {}
    
    for ip, events in ip_events.items():
        events.sort(key=lambda x: x['timestamp'])
        
        time_series = []
        active_connections = 0
        open_events = 0
        close_events = 0
        unmatched_opens = 0
        
        for event in events:
            if event['event'] == 'open':
                active_connections += 1
                open_events += 1
            elif event['event'] == 'close':
                active_connections = max(0, active_connections - 1)
                close_events += 1
            
            time_series.append({
                'timestamp': event['timestamp'],
                'connection_count': active_connections
            })
        
        unmatched_opens = open_events - close_events
        
        data_quality_metrics[ip] = {
            'open_events': open_events,
            'close_events': close_events,
            'unmatched_opens': unmatched_opens,
            'final_count': active_connections,
            'data_completeness': close_events / open_events if open_events > 0 else 1.0
        }
        
        connections_by_ip[ip] = time_series
    
    total_opens = sum(metrics['open_events'] for metrics in data_quality_metrics.values())
    total_closes = sum(metrics['close_events'] for metrics in data_quality_metrics.values())
    overall_completeness = total_closes / total_opens if total_opens > 0 else 1.0
    
    validation_data = {
        'total_connection_counts': total_connection_counts,
        'overall_completeness': overall_completeness,
        'total_opens': total_opens,
        'total_closes': total_closes,
        'unmatched_connections': total_opens - total_closes
    }
    
    cache_data = {
        'connections_by_ip': connections_by_ip,
        'data_quality_metrics': data_quality_metrics,
        'validation_data': validation_data
    }
    save_to_cache(cache_key, cache_data)
    
    return connections_by_ip


def validate_connection_data_consistency(
    connections_by_ip: dict[str, list[dict]],
    total_connections_timeseries: list[dict],
) -> dict[str, Any]:
    """Validate that per-IP connection data is consistent with total connection data."""
    validation_results = {
        'is_consistent': True,
        'discrepancies': [],
        'data_quality_score': 1.0,
        'warnings': [],
        'recommendations': []
    }
    
    if not connections_by_ip or not total_connections_timeseries:
        validation_results['warnings'].append("No connection data available for validation")
        return validation_results
    
    total_counts_by_time = {}
    for conn in total_connections_timeseries:
        timestamp = conn['timestamp']
        total_counts_by_time[timestamp] = conn['connection_count']
    
    max_discrepancy = 0
    total_checks = 0
    discrepancies = 0
    
    for ip, ip_data in connections_by_ip.items():
        for point in ip_data:
            timestamp = point['timestamp']
            if timestamp in total_counts_by_time:
                total_checks += 1
                ip_count = point['connection_count']
                total_count = total_counts_by_time[timestamp]
                
                if ip_count > total_count:
                    discrepancies += 1
                    max_discrepancy = max(max_discrepancy, ip_count - total_count)
                    validation_results['discrepancies'].append({
                        'timestamp': timestamp,
                        'ip': ip,
                        'ip_count': ip_count,
                        'total_count': total_count,
                        'difference': ip_count - total_count
                    })
    
    if total_checks > 0:
        validation_results['data_quality_score'] = 1.0 - (discrepancies / total_checks)
        validation_results['is_consistent'] = discrepancies == 0
    
    if validation_results['data_quality_score'] < 0.8:
        validation_results['warnings'].append(f"Data quality is {validation_results['data_quality_score']:.1%} - some connection events may be missing")
        validation_results['recommendations'].append("Consider using a longer log period to capture complete connection lifecycles")
    
    if max_discrepancy > 0:
        validation_results['warnings'].append(f"Maximum discrepancy found: {max_discrepancy} connections")
        validation_results['recommendations'].append("Per-IP tracking may be missing some connection close events")
    
    return validation_results


def parse_connection_events(logfile: str) -> list[dict[str, Any]]:
    """Parse individual connection open/close events from MongoDB log file."""
    cache_key = get_cache_key(logfile, 'connection_events')
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached connection events data...")
        return cached_result['connection_events']
    
    connection_events = []
    
    total_lines = count_lines(logfile)
    
    with open(logfile, 'r') as f:
        for line in tqdm(f, total=total_lines, desc="Parsing connection events", unit="lines"):
            try:
                entry = json.loads(line)
                timestamp = entry.get('t', {}).get('$date')
                
                if not timestamp:
                    continue
                
                if (entry.get('msg') == 'Connection accepted' and 
                    entry.get('c') == 'NETWORK' and
                    entry.get('attr')):
                    attr = entry['attr']
                    if 'remote' in attr:
                        ip = attr['remote'].split(':')[0]
                        conn_id = attr.get('connectionId')
                        connection_count = attr.get('connectionCount', 0)
                        
                        connection_events.append({
                            'timestamp': timestamp,
                            'event_type': 'opened',
                            'ip': ip,
                            'connection_id': conn_id,
                            'total_connections': connection_count,
                            'log_message': line.strip()
                        })
                
                elif (entry.get('msg') == 'Connection ended' and 
                      entry.get('c') == 'NETWORK' and
                      entry.get('attr')):
                    attr = entry['attr']
                    conn_id = attr.get('connectionId')
                    
                    connection_events.append({
                        'timestamp': timestamp,
                        'event_type': 'closed',
                        'ip': 'unknown',
                        'connection_id': conn_id,
                        'total_connections': 0,
                        'log_message': line.strip()
                    })
                        
            except Exception:
                pass
    
    connection_events.sort(key=lambda x: x['timestamp'])
    
    cache_data = {'connection_events': connection_events}
    save_to_cache(cache_key, cache_data)
    
    return connection_events


def parse_queries(logfile: str, sample_percentage: Optional[int] = None) -> dict:
    """Parse query patterns and statistics from MongoDB log file, grouped by pattern."""
    total_lines = count_lines(logfile)
    cache_key = get_cache_key(
        logfile,
        'queries',
        variant=_build_sampling_cache_variant(total_lines, sample_percentage),
    )
    cached_result = load_from_cache(cache_key)
    if cached_result:
        click.echo("Using cached query data...")
        queries = cached_result['queries']
        for key, value in queries.items():
            if isinstance(value.get('operations'), list):
                value['operations'] = set(value['operations'])
            if isinstance(value.get('indexes'), list):
                value['indexes'] = set(value['indexes'])
            if 'indexes' not in value:
                value['indexes'] = set()
            for metric_key in (
                'keysExamined',
                'docsExamined',
                'nreturned',
                'hasSortStage',
                'usedDisk',
                'numYields',
                'reslen',
                'locksPresent',
            ):
                if metric_key not in value:
                    value[metric_key] = []
        return queries
    
    def default_query_data():
        return {
            'count': 0,
            'durations': [],
            'allowDiskUse': False,
            'operations': set(),
            'pattern': None,
            'indexes': set(),
            'keysExamined': [],
            'docsExamined': [],
            'nreturned': [],
            'hasSortStage': [],
            'usedDisk': [],
            'numYields': [],
            'reslen': [],
            'locksPresent': [],
        }
    
    queries = defaultdict(default_query_data)
    
    if sample_percentage is not None:
        sample_rate = get_sample_rate_from_percentage(sample_percentage, total_lines)
        is_sampled = sample_rate > 1
        if is_sampled:
            click.echo(f"Sampling {sample_percentage}% of file ({total_lines:,} lines). Processing every {sample_rate} lines...")
    else:
        sample_rate = get_sample_rate(total_lines)
        is_sampled = sample_rate > 1
        if is_sampled:
            click.echo(f"Large file detected ({total_lines:,} lines). Sampling every {sample_rate} lines for performance...")
    
    with open(logfile, 'r') as f:
        line_count = 0
        for line in tqdm(f, total=total_lines, desc="Parsing queries", unit="lines"):
            line_count += 1
            
            if is_sampled and line_count % sample_rate != 0:
                continue
            try:
                entry = json.loads(line)
                if (entry.get('c') == 'COMMAND' and 
                    entry.get('msg') in ('command', 'Slow query') and
                    entry.get('attr')):
                    attr = entry['attr']
                    namespace = attr.get('ns', '')
                    if not namespace:
                        continue
                    command = attr.get('command', {})
                    if not command:
                        continue
                    operation = list(command.keys())[0] if command else 'unknown'
                    pattern = extract_query_pattern(operation, command)
                    group_key = (namespace, operation, pattern)
                    duration_ms = attr.get('durationMillis', 0)
                    allow_disk_use = command.get('allowDiskUse', False)
                    plan_summary = attr.get('planSummary', '')
                    index_used = 'COLLSCAN' if plan_summary == 'COLLSCAN' else plan_summary
                    if not index_used:
                        index_used = 'N/A'
                    queries[group_key]['count'] += 1
                    queries[group_key]['durations'].append(duration_ms)
                    queries[group_key]['operations'].add(operation)
                    queries[group_key]['allowDiskUse'] = queries[group_key]['allowDiskUse'] or allow_disk_use
                    queries[group_key]['pattern'] = pattern
                    queries[group_key]['indexes'].add(index_used)
                    locks = attr.get('locks')
                    locks_present = bool(locks) if locks is not None else False
                    queries[group_key]['keysExamined'].append(int(attr.get('keysExamined') or 0))
                    queries[group_key]['docsExamined'].append(int(attr.get('docsExamined') or 0))
                    queries[group_key]['nreturned'].append(int(attr.get('nreturned') or 0))
                    queries[group_key]['hasSortStage'].append(bool(attr.get('hasSortStage', False)))
                    queries[group_key]['usedDisk'].append(bool(attr.get('usedDisk', False)))
                    queries[group_key]['numYields'].append(int(attr.get('numYields') or 0))
                    queries[group_key]['reslen'].append(int(attr.get('reslen') or 0))
                    queries[group_key]['locksPresent'].append(locks_present)
            except Exception:
                pass

    queries_dict = {}
    for key, value in queries.items():
        queries_dict[key] = {
            'count': value['count'],
            'durations': value['durations'],
            'allowDiskUse': value['allowDiskUse'],
            'operations': list(value['operations']),
            'pattern': value['pattern'],
            'indexes': list(value['indexes']),
            'keysExamined': value['keysExamined'],
            'docsExamined': value['docsExamined'],
            'nreturned': value['nreturned'],
            'hasSortStage': value['hasSortStage'],
            'usedDisk': value['usedDisk'],
            'numYields': value['numYields'],
            'reslen': value['reslen'],
            'locksPresent': value['locksPresent'],
        }
    cache_data = {
        'queries': queries_dict,
        'sampling_metadata': get_sampling_metadata(total_lines, sample_percentage)
    }
    save_to_cache(cache_key, cache_data)
    
    return queries
