import click
import json
import yaml
from collections import defaultdict

class CustomCommand(click.Command):
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
    def default_connection_data():
        return {'opened': 0, 'closed': 0, 'durations': []}
    
    connections = defaultdict(default_connection_data)
    total_opened = 0
    total_closed = 0
    connection_starts = {}  # Track connection start times by connection ID
    
    with open(logfile, 'r') as f:
        for line in f:
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
    
    return connections, total_opened, total_closed

def parse_replica_set_config(logfile):
    """Parse replica set configuration from MongoDB log file."""
    configs = []
    
    with open(logfile, 'r') as f:
        for line in f:
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
    
    return configs

def parse_replica_set_state(logfile):
    """Parse replica set state transitions and current node status from MongoDB log file."""
    states = []
    node_status = {}
    replica_set_config = None
    current_host = None
    state_transitions = {}  # Track state transitions per host
    
    with open(logfile, 'r') as f:
        for line in f:
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
    
    return states, node_status

def parse_clients(logfile):
    """Parse client/driver information from MongoDB log file."""
    clients = {}  # Group by driver info
    connection_drivers = {}  # Track which connections belong to which driver
    
    with open(logfile, 'r') as f:
        for line in f:
            try:
                entry = json.loads(line)
                
                # Connection accepted with driver info
                if (entry.get('msg') == 'Connection accepted' and 
                    entry.get('c') == 'NETWORK' and
                    entry.get('attr')):
                    attr = entry['attr']
                    if 'remote' in attr:
                        ip = attr['remote'].split(':')[0]
                        conn_id = attr.get('connectionId')
                        
                        # Extract driver information
                        driver_info = {}
                        if 'driver' in attr:
                            driver_info['name'] = attr['driver'].get('name', 'Unknown')
                            driver_info['version'] = attr['driver'].get('version', 'Unknown')
                            if 'app' in attr['driver']:
                                driver_info['app'] = attr['driver']['app']
                        
                        # Create driver key
                        if driver_info:
                            driver_key = f"{driver_info['name']} | Version: {driver_info['version']}"
                            if 'app' in driver_info:
                                driver_key += f" | App: {driver_info['app']}"
                            
                            if driver_key not in clients:
                                clients[driver_key] = {
                                    'driver_info': driver_info,
                                    'ips': set(),
                                    'connections': 0,
                                    'users': set()
                                }
                            
                            clients[driver_key]['ips'].add(ip)
                            clients[driver_key]['connections'] += 1
                            
                            # Track connection to driver mapping
                            if conn_id:
                                connection_drivers[conn_id] = driver_key
                
                # Authentication events to track users
                elif (entry.get('msg') == 'Authentication succeeded' and 
                      entry.get('c') == 'ACCESS' and
                      entry.get('attr')):
                    attr = entry['attr']
                    conn_id = attr.get('connectionId')
                    
                    if conn_id and conn_id in connection_drivers:
                        driver_key = connection_drivers[conn_id]
                        if 'user' in attr:
                            user = attr['user']
                            db = attr.get('db', 'admin')
                            user_key = f"{user}@{db}"
                            clients[driver_key]['users'].add(user_key)
                        
            except Exception:
                pass
    
    return clients

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

@click.command()
@click.option('--fetch', '-f', 'logfile', type=click.Path(exists=True), help='MongoDB log file to analyze.')
@click.option('--rs-conf', is_flag=True, help='Print replica set configuration(s) from the log.')
@click.option('--rs-state', is_flag=True, help='Print replica set node status from the log.')
@click.option('--connections', is_flag=True, help='Print connection information from the log.')
@click.option('--stats', is_flag=True, help='Include connection duration statistics (use with --connections).')
@click.option('--clients', is_flag=True, help='Print client/driver information from the log.')
def main(logfile, rs_conf, rs_state, connections, stats, clients):
    """pepi: MongoDB log analysis tool."""
    
    # Check if logfile is provided
    if not logfile:
        click.echo("Pepi didn't find anything to fetch")
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

    # Parse log file for basic information
    with open(logfile, 'r') as f:
        for line in f:
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
        
        click.echo("-" * 30)
        
        # Display per-IP details
        for ip, conn_info in connections_data.items():
            if stats and ip in ip_stats:
                stats_info = ip_stats[ip]
                click.echo(f"{ip} | opened:{conn_info['opened']} | closed:{conn_info['closed']} | dur-avg:{stats_info['avg']:.2f}s | dur-min:{stats_info['min']:.2f}s | dur-max:{stats_info['max']:.2f}s")
            else:
                click.echo(f"{ip} | opened:{conn_info['opened']} | closed:{conn_info['closed']}")
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
                driver_name = client_info['driver_info']['name']
                driver_version = client_info['driver_info']['version']
                app_info = client_info['driver_info'].get('app', '')
                
                # Format driver header
                if app_info:
                    click.echo(f"\n{driver_name} v{driver_version} ({app_info})")
                else:
                    click.echo(f"\n{driver_name} v{driver_version}")
                
                # Display connections count
                click.echo(f"├─ Connections: {client_info['connections']}")
                
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
