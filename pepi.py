import click
import json
import yaml

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

@click.command()
@click.argument('logfile', type=click.Path(exists=True))
@click.option('--rs-conf', is_flag=True, help='Print replica set configuration(s) from the log.')
@click.option('--rs-state', is_flag=True, help='Print replica set node status from the log.')
def main(logfile, rs_conf, rs_state):
    """pepi: MongoDB log analysis tool."""
    if rs_conf:
        click.echo("===== Replica Set Configuration =====")
        configs = parse_replica_set_config(logfile)
        if configs:
            for i, config_data in enumerate(configs, 1):
                click.echo(f"\nConfiguration #{i} (Timestamp: {config_data['timestamp']})")
                click.echo("=" * 50)
                yaml_str = yaml.dump(config_data['config'], sort_keys=False, default_flow_style=False)
                click.echo(yaml_str)
        else:
            click.echo("No replica set configuration found in the log.")
        click.echo("=" * 40)
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
                    state = state_data['new_state']
                    timestamp = state_data['timestamp']
                    click.echo(f"{state:<12} - {timestamp}")
                click.echo("-" * 30)
        else:
            click.echo("No replica set state transitions found in the log.")
        click.echo("=" * 40)
        return

    # Default: summary mode
    start_date = None
    end_date = None
    num_lines = 0
    os_version = None
    kernel_version = None
    db_version = None
    cmd_options = None

    # For efficiency, store last line
    last_line = None

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

    # Print reconstructed command line
    click.echo("===== Node Command Line Startup =====")
    command_line = reconstruct_command_line(cmd_options)
    if command_line:
        click.echo(command_line, nl=False)
        click.echo()
    else:
        click.echo("No command line options found.")
    click.echo("================================\n")

    # Prepare aligned output
    labels = [
        ("Log file", logfile),
        ("Start date", start_date if start_date else 'N/A'),
        ("End date", end_date if end_date else 'N/A'),
        ("Number of lines", num_lines),
        ("OS version", os_version if os_version else 'N/A'),
        ("Kernel version", kernel_version if kernel_version else 'N/A'),
        ("MongoDB version", db_version if db_version else 'N/A'),
    ]
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
    click.echo("================================\n")

if __name__ == '__main__':
    main()  # Let Click handle argument parsing
