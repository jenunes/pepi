"""Command-line interface for Pepi MongoDB log analyzer."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import threading
from pathlib import Path
from typing import Optional

import click
from tqdm import tqdm

from pepi.version import __version__
from pepi.cache import CACHE_DIR, get_cache_key, load_from_cache, save_to_cache
from pepi.formatters import generate_histogram, reconstruct_command_line
from pepi.parser import (
    parse_connections, parse_replica_set_config, parse_replica_set_state,
    parse_clients, parse_queries,
)
from pepi.stats import calculate_query_stats, calculate_connection_stats
from pepi.upgrade import check_version_async, perform_upgrade
from pepi.utils import count_lines, get_date_range, trim_log_file


class CustomCommand(click.Command):
    def format_help(self, ctx, formatter):
        """Custom help formatter with contextual help."""
        args = sys.argv
        if '--help' in args:
            help_index = args.index('--help')
            if help_index > 0:
                option = args[help_index - 1]
                if option.startswith('--'):
                    self.show_contextual_help(option, formatter)
                    return
        
        self.show_full_help(formatter)
    
    def show_contextual_help(self, option, formatter):
        """Show help for a specific option."""
        if option == '--connections':
            with formatter.section("Connection Analysis"):
                formatter.write_text("--connections       Print connection information and statistics")
                formatter.write_text("")
                formatter.write_text("Connection Sub-options:")
                formatter.write_text("  --stats          Include connection duration statistics")
                formatter.write_text("  --sort-by        Sort by: opened | closed")
                formatter.write_text("  --compare        Compare 2-3 specific hostnames/IPs")
                formatter.write_text("")
                formatter.write_text("Examples:")
                formatter.write_text("  pepi --fetch logfile --connections")
                formatter.write_text("  pepi --fetch logfile --connections --stats")
                formatter.write_text("  pepi --fetch logfile --connections --sort-by opened")
                formatter.write_text("  pepi --fetch logfile --connections --compare ip1 --compare ip2")
                formatter.write_text("  pepi --fetch logfile --connections --stats --sort-by opened --compare ip1 --compare ip2")
        
        elif option == '--queries':
            with formatter.section("Query Analysis"):
                formatter.write_text("--queries           Print query pattern statistics and performance analysis")
                formatter.write_text("")
                formatter.write_text("Query Sub-options:")
                formatter.write_text("  --sort-by        Sort by: count | min | max | 95% | sum | mean")
                formatter.write_text("  --report-full-patterns  Write complete patterns to file")
                formatter.write_text("  --namespace      Filter by namespace (e.g., 'database.collection')")
                formatter.write_text("  --operation      Filter by operation type (e.g., 'find', 'insert', 'update')")
                formatter.write_text("  --report-histogram  Show execution time distribution histogram")
                formatter.write_text("")
                formatter.write_text("Examples:")
                formatter.write_text("  pepi --fetch logfile --queries")
                formatter.write_text("  pepi --fetch logfile --queries --sort-by count")
                formatter.write_text("  pepi --fetch logfile --queries --sort-by mean")
                formatter.write_text("  pepi --fetch logfile --queries --sort-by 95%")
                formatter.write_text("  pepi --fetch logfile --queries --report-full-patterns report.txt")
                formatter.write_text("  pepi --fetch logfile --queries --namespace test.users")
                formatter.write_text("  pepi --fetch logfile --queries --operation find")
                formatter.write_text("  pepi --fetch logfile --queries --report-histogram")
        
        elif option == '--trim':
            with formatter.section("Log File Trimming"):
                formatter.write_text("--trim              Trim log file by date/time range")
                formatter.write_text("")
                formatter.write_text("Trim Options:")
                formatter.write_text("  --from DATE      Start date/time (format: DD/MM/YYYY HH:MM:SS:MS)")
                formatter.write_text("  --until DATE     End date/time (format: DD/MM/YYYY HH:MM:SS:MS)")
                formatter.write_text("")
                formatter.write_text("Flexible Format Examples:")
                formatter.write_text("  '25/12/2023'                    # Whole day")
                formatter.write_text("  '25/12/2023 14:30'              # From 14:30:00")
                formatter.write_text("  '25/12/2023 14:30:45'           # From 14:30:45")
                formatter.write_text("  '25/12/2023 14:30:45:123'       # From 14:30:45.123")
                formatter.write_text("")
                formatter.write_text("Smart Defaults:")
                formatter.write_text("  - If only date provided: assumes whole day (00:00:00 to 23:59:59)")
                formatter.write_text("  - If --from and --until are equal: full period of that timeframe")
                formatter.write_text("  - Missing time parts default to 0 (start) or maximum (end)")
                formatter.write_text("")
                formatter.write_text("Examples:")
                formatter.write_text("  pepi --fetch logfile --trim --from '25/12/2023'")
                formatter.write_text("  pepi --fetch logfile --trim --from '25/12/2023' --until '26/12/2023'")
                formatter.write_text("  pepi --fetch logfile --trim --from '25/12/2023 14:00' --until '25/12/2023 16:00'")
                formatter.write_text("  pepi --fetch logfile --trim --from '25/12/2023 14:30:45' --until '25/12/2023 14:30:45'")
        
        elif option == '--web-ui':
            with formatter.section("Web Interface"):
                formatter.write_text("--web-ui            Launch interactive web dashboard")
                formatter.write_text("")
                formatter.write_text("Features:")
                formatter.write_text("  - Drag & drop file upload")
                formatter.write_text("  - Interactive charts and visualizations")
                formatter.write_text("  - Real-time analysis with progress tracking")
                formatter.write_text("  - Query filtering and performance analysis")
                formatter.write_text("  - Log trimming with date range selection")
                formatter.write_text("  - File management (download, delete)")
                formatter.write_text("")
                formatter.write_text("Usage:")
                formatter.write_text("  pepi --web-ui                     # Launch with no file")
                formatter.write_text("  pepi --fetch logfile --web-ui     # Launch with pre-loaded file")
                formatter.write_text("")
                formatter.write_text("The web interface will open in your default browser at http://localhost:8000")
        
        elif option == '--ftdc':
            with formatter.section("FTDC Viewer"):
                formatter.write_text("--ftdc PATH         Launch FTDC Viewer for a specific diagnostic.data path")
                formatter.write_text("")
                formatter.write_text("Usage:")
                formatter.write_text("  pepi --ftdc /path/to/diagnostic.data")
                formatter.write_text("")
                formatter.write_text("The FTDC viewer will start Docker containers and open Grafana in your browser.")
        
        else:
            formatter.write_text(f"Unknown option: {option}")
            formatter.write_text("Use --help to see all available options.")
    
    def show_full_help(self, formatter):
        """Show the full help menu."""
        with formatter.section("Usage"):
            formatter.write_text("pepi --fetch <logfile> [OPTIONS]")
        
        with formatter.section("Description"):
            formatter.write_text("pepi: MongoDB log analysis tool for extracting insights from MongoDB log files.")
        
        with formatter.section("Required Options"):
            formatter.write_text("--fetch, -f PATH    MongoDB log file to analyze")
        
        with formatter.section("Main Analysis Modes"):
            formatter.write_text("--rs-conf           Print replica set configuration(s)")
            formatter.write_text("--rs-state          Print replica set node status and transitions")
            formatter.write_text("--clients           Print client/driver information")
            formatter.write_text("--queries           Print query pattern statistics and performance analysis")
            formatter.write_text("--connections       Print connection information and statistics")
            formatter.write_text("--ftdc              Launch FTDC Viewer for a given diagnostic.data path")
        
        with formatter.section("Log File Operations"):
            formatter.write_text("--trim              Trim log file by date/time range (use with --from and --until)")
            formatter.write_text("--web-ui            Launch web interface with specified log file pre-loaded")
            formatter.write_text("--clear-cache       Clear all cached data and re-parse files")
        
        with formatter.section("Default Behavior"):
            formatter.write_text("When no analysis mode is specified, shows MongoDB log summary and command line startup options.")
        
        with formatter.section("Contextual Help"):
            formatter.write_text("Use --option --help to see detailed help for specific options:")
            formatter.write_text("  --connections --help")
            formatter.write_text("  --queries --help")
            formatter.write_text("  --trim --help")
            formatter.write_text("  --web-ui --help")
            formatter.write_text("  --ftdc --help")

    def main(self, args=None, prog_name=None, complete_var=None, standalone_mode=True, **kwargs):
        try:
            return super().main(args, prog_name, complete_var, standalone_mode, **kwargs)
        except click.MissingParameter as e:
            if '--fetch' in str(e) or '-f' in str(e):
                click.echo("Pepi didn't find anything to fetch")
                return 1
            raise


def launch_web_ui(logfile: Optional[str] = None, sample_percentage: int = 100) -> None:
    """Launch the web interface with optional pre-loaded file and sampling."""
    web_api_path = Path(__file__).parent / "web_api.py"
    if not web_api_path.exists():
        click.echo("❌ Web interface not found. web_api.py is missing.")
        click.echo("   Make sure web_api.py is in the same package directory as pepi")
        return
    
    web_static_path = Path(__file__).parent / "web_static"
    if not web_static_path.exists():
        click.echo("❌ Web interface not found. web_static directory is missing.")
        click.echo("   Make sure web_static/ directory with HTML/CSS/JS files exists")
        return
    
    click.echo("🚀 Starting Pepi Web Interface...")
    
    env = os.environ.copy()
    if logfile:
        env['PEPI_PRELOAD_FILE'] = str(Path(logfile).absolute())
        click.echo(f"📁 Pre-loading file: {logfile}")
    
    env['PEPI_SAMPLE_PERCENTAGE'] = str(sample_percentage)
    if sample_percentage != 100:
        click.echo(f"📊 Sampling: {sample_percentage}% of log lines")
    
    try:
        process = subprocess.Popen(
            [sys.executable, "-m", "pepi.web_api"],
            env=env,
            text=True
        )
        
        time.sleep(3)
        
        if process.poll() is not None:
            click.echo("❌ Failed to start web server")
            return
        
        click.echo()
        
        server_port = 8000
        port_file = None
        try:
            import tempfile
            import psutil
            
            time.sleep(1)
            
            for child in psutil.Process(process.pid).children():
                if any('pepi.web_api' in arg for arg in child.cmdline()):
                    port_file = tempfile.gettempdir() + f"/pepi_port_{child.pid}.txt"
                    break
            
            if port_file and os.path.exists(port_file):
                with open(port_file, 'r') as f:
                    server_port = int(f.read().strip())
            else:
                newest_proc = None
                newest_time = 0
                for proc in psutil.process_iter(['pid', 'cmdline', 'create_time']):
                    if proc.info['cmdline'] and any('pepi.web_api' in cmd for cmd in proc.info['cmdline']):
                        if proc.info['create_time'] > newest_time:
                            newest_time = proc.info['create_time']
                            newest_proc = proc
                
                if newest_proc:
                    for conn in newest_proc.connections():
                        if conn.status == 'LISTEN' and conn.laddr.ip in ['0.0.0.0', '127.0.0.1']:
                            server_port = conn.laddr.port
                            break
        except Exception:
            pass
        
        click.echo(f"\n🌐 \033[4mhttp://localhost:{server_port}\033[0m ← click here")
        click.echo(f"📋 \033[4mhttp://localhost:{server_port}/docs\033[0m ← API docs")
        click.echo("\n💡 Press Ctrl+C to stop the web server")
        
        try:
            process.wait()
        except KeyboardInterrupt:
            click.echo("\n🛑 Stopping web server...")
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
            
            if 'port_file' in locals() and port_file and os.path.exists(port_file):
                try:
                    os.remove(port_file)
                except OSError:
                    pass
            
            click.echo("✅ Web server stopped")
            
    except Exception as e:
        click.echo(f"❌ Error starting web interface: {str(e)}")
        if 'process' in locals() and process.poll() is None:
            process.terminate()


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
@click.option('--sort-by', type=click.Choice(['opened', 'closed', 'count', 'min', 'max', '95%', 'sum', 'mean']), 
              help='Sort by specified metric (use with --connections or --queries).')
@click.option('--compare', multiple=True, 
              help='Compare 2-3 specific hostnames/IPs (use with --connections).')
@click.option('--clients', is_flag=True, 
              help='Print client/driver information and authentication details.')
@click.option('--queries', is_flag=True, 
              help='Print query pattern statistics and performance analysis.')
@click.option('--report-full-patterns', type=click.Path(), 
              help='Write complete query patterns to file (requires output file path).')
@click.option('--namespace', type=str, 
              help='Filter queries by namespace (e.g., "database.collection").')
@click.option('--operation', type=str, 
              help='Filter queries by operation type (e.g., "find", "insert", "update", "delete", "aggregate").')
@click.option('--report-histogram', is_flag=True, 
              help='Show histogram of execution time distribution.')
@click.option('--clear-cache', is_flag=True, 
              help='Clear all cached data and re-parse files.')
@click.option('--trim', is_flag=True,
              help='Trim log file by date/time range (use with --from and --until).')
@click.option('--from', 'from_date', type=str,
              help='Start date/time for trimming (format: DD/MM/YYYY HH:MM:SS:MS or partial).')
@click.option('--until', 'until_date', type=str,
              help='End date/time for trimming (format: DD/MM/YYYY HH:MM:SS:MS or partial).')
@click.option('--web-ui', is_flag=True,
              help='Launch web interface with the specified log file pre-loaded.')
@click.option('--version', is_flag=True,
              help='Show version information and exit.')
@click.option('--upgrade', is_flag=True,
              help='Check for updates and upgrade Pepi.')
@click.option('--sample', type=click.IntRange(0, 100), default=100,
              help='Percentage of log lines to sample (0-100). Default: 100 (no sampling)')
@click.option('--ftdc', type=click.Path(exists=True),
              help='Launch FTDC Viewer for a given diagnostic.data path.')
def main(logfile, rs_conf, rs_state, connections, stats, clients, sort_by, compare, queries, report_full_patterns, namespace, operation, report_histogram, clear_cache, trim, from_date, until_date, web_ui, version, upgrade, sample, ftdc):
    """pepi: MongoDB log analysis tool."""
    
    if version:
        click.echo(f"pepi version {__version__}")
        click.echo("MongoDB log analysis tool")
        click.echo("https://github.com/jenunes/pepi")
        return
    
    if upgrade:
        perform_upgrade()
        return
        
    if ftdc:
        from pepi.ftdc import launch_viewer
        launch_viewer(ftdc)
        return
    
    if not logfile and not web_ui:
        click.echo("Pepi didn't find anything to fetch")
        return
    
    if web_ui:
        launch_web_ui(logfile, sample)
        return
    
    if not web_ui and not upgrade:
        thread = threading.Thread(target=check_version_async, daemon=True)
        thread.start()
    
    if clear_cache:
        cache_files = list(CACHE_DIR.glob('*.pkl'))
        if cache_files:
            for cache_file in cache_files:
                cache_file.unlink()
            click.echo(f"Cleared {len(cache_files)} cached files.")
        else:
            click.echo("No cached files to clear.")
        return
    
    if trim:
        if not from_date and not until_date:
            click.echo("Error: --trim requires at least --from or --until to be specified.")
            return
        
        try:
            start_dt, end_dt = get_date_range(from_date, until_date)
            
            click.echo("===== Log File Trimming =====")
            click.echo(f"Original file: {logfile}")
            if start_dt:
                click.echo(f"From: {start_dt.strftime('%d/%m/%Y %H:%M:%S')}")
            if end_dt:
                click.echo(f"Until: {end_dt.strftime('%d/%m/%Y %H:%M:%S')}")
            click.echo()
            
            filtered_lines, total_lines, skipped_lines = trim_log_file(logfile, start_dt, end_dt)
            
            click.echo(f"\nTrimming completed:")
            click.echo(f"Total lines processed: {total_lines:,}")
            click.echo(f"Lines included: {len(filtered_lines):,}")
            click.echo(f"Lines skipped: {skipped_lines:,}")
            
            if not filtered_lines:
                click.echo("No lines found in the specified date range.")
                return
            
            save_file = click.confirm("Save trimmed log to file?", default=True)
            
            if save_file:
                base_name = Path(logfile).stem
                extension = Path(logfile).suffix
                
                date_suffix = ""
                if start_dt and end_dt:
                    if start_dt.date() == end_dt.date():
                        date_suffix = f"_{start_dt.strftime('%Y%m%d')}"
                    else:
                        date_suffix = f"_{start_dt.strftime('%Y%m%d')}-{end_dt.strftime('%Y%m%d')}"
                elif start_dt:
                    date_suffix = f"_from_{start_dt.strftime('%Y%m%d')}"
                elif end_dt:
                    date_suffix = f"_until_{end_dt.strftime('%Y%m%d')}"
                
                default_filename = f"{base_name}_trimmed{date_suffix}{extension}"
                
                output_file = click.prompt("Output filename", default=default_filename)
                
                if output_file == logfile:
                    output_file = f"{base_name}_trimmed{extension}"
                    click.echo(f"Cannot overwrite original file. Using: {output_file}")
                
                try:
                    with open(output_file, 'w') as f:
                        for line in tqdm(filtered_lines, desc="Writing trimmed file", unit="lines"):
                            f.write(line)
                    
                    click.echo(f"\n✅ Trimmed log saved as: {output_file}")
                    click.echo(f"Size: {len(filtered_lines):,} lines")
                    
                except Exception as e:
                    click.echo(f"❌ Error saving file: {str(e)}")
                    return
                    
            return
        
        except ValueError as e:
            click.echo(f"Error parsing date/time: {str(e)}")
            click.echo("Expected format: DD/MM/YYYY HH:MM:SS:MS (time parts are optional)")
            click.echo("Examples:")
            click.echo("  --from '25/12/2023'")
            click.echo("  --from '25/12/2023 14:30'")
            click.echo("  --from '25/12/2023 14:30:45'")
            click.echo("  --from '25/12/2023 14:30:45:123'")
            return
        except Exception as e:
            click.echo(f"Error during trimming: {str(e)}")
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
                if not os_version or not kernel_version:
                    try:
                        entry = json.loads(line)
                        if entry.get('msg') == 'Operating System':
                            os_version = entry.get('attr', {}).get('os', {}).get('name')
                            kernel_version = entry.get('attr', {}).get('os', {}).get('version')
                    except Exception:
                        pass
                if not db_version:
                    try:
                        entry = json.loads(line)
                        if entry.get('msg') == 'Build Info':
                            db_version = entry.get('attr', {}).get('buildInfo', {}).get('version')
                    except Exception:
                        pass
                if not cmd_options:
                    try:
                        entry = json.loads(line)
                        if entry.get('msg') == 'Options set by command line':
                            cmd_options = entry.get('attr', {}).get('options')
                    except Exception:
                        pass
                last_line = line

        if last_line:
            try:
                entry = json.loads(last_line)
                end_date = entry.get('t', {}).get('$date')
            except Exception:
                pass
        
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
        
        if node_status:
            click.echo("\nCurrent Node Status:")
            click.echo("-" * 30)
            for host_port, status in node_status.items():
                click.echo(f"{host_port} - {status['state']} - {status['timestamp']}")
            click.echo()
        
        if states:
            click.echo("State Transitions:")
            click.echo("-" * 30)
            states_by_host = {}
            for state_data in states:
                host = state_data['host']
                if host not in states_by_host:
                    states_by_host[host] = []
                states_by_host[host].append(state_data)
            
            for host, host_states in states_by_host.items():
                click.echo(f"------- {host} -------")
                for state_data in host_states:
                    click.echo(f"{state_data['new_state']:<12} - {state_data['timestamp']}")
                click.echo("-" * 30)
        else:
            click.echo("No replica set state transitions found in the log.")
        return
    elif connections:
        if sample == 100:
            try:
                with open(logfile, 'r') as f:
                    line_count = sum(1 for _ in f)
                if line_count > 50000:
                    click.echo(f"⚠️  Warning: Processing 100% of a large file ({line_count:,} lines) may be slow.", err=True)
                    click.echo(f"   Consider using --sample 50 for faster analysis.", err=True)
                    click.echo()
            except Exception:
                pass
        
        connections_data, total_opened, total_closed = parse_connections(logfile, sample_percentage=sample)
        
        overall_stats = None
        ip_stats = {}
        if stats:
            overall_stats, ip_stats = calculate_connection_stats(connections_data)
        
        click.echo("===== MongoDB Log Summary =====")
        labels = [
            ("Log file", logfile),
            ("Start date", start_date if start_date else 'N/A'),
            ("End date", end_date if end_date else 'N/A'),
            ("Number of lines", num_lines),
        ]
        
        host_info = None
        if cmd_options and 'net' in cmd_options and 'port' in cmd_options['net']:
            port = cmd_options['net']['port']
            hostname = "localhost"
            host_info = f"{hostname}:{port}"
        if host_info:
            labels.append(("Host", host_info))
        
        labels.extend([
            ("OS version", os_version if os_version else 'N/A'),
            ("Kernel version", kernel_version if kernel_version else 'N/A'),
            ("MongoDB version", db_version if db_version else 'N/A'),
        ])
        repl_name = None
        if cmd_options and 'replication' in cmd_options and 'replSet' in cmd_options['replication']:
            repl_name = cmd_options['replication']['replSet']
        if repl_name:
            labels.append(("ReplicaSet Name", repl_name))
        if repl_name:
            configs = parse_replica_set_config(logfile)
            if configs:
                latest_config = configs[-1]['config']
                if 'members' in latest_config:
                    num_nodes = len(latest_config['members'])
                    labels.append(("Nodes", str(num_nodes)))
        
        max_label_len = max(len(label) for label, _ in labels)
        for label, value in labels:
            click.echo(f"{label.ljust(max_label_len)} : {value}")
        
        click.echo("\n===== Connection Details =====")
        click.echo(f"Total Connections Opened: {total_opened}")
        click.echo(f"Total Connections Closed: {total_closed}")
        
        if stats and overall_stats:
            click.echo(f"Overall Average Connection Duration: {overall_stats['avg']:.2f}s")
            click.echo(f"Overall Minimum Connection Duration: {overall_stats['min']:.2f}s")
            click.echo(f"Overall Maximum Connection Duration: {overall_stats['max']:.2f}s")
        
        if sort_by:
            click.echo(f"--------Sorted by {sort_by}--------")
            if sort_by == 'opened':
                sorted_ips = sorted(connections_data.keys(), key=lambda ip: connections_data[ip]['opened'], reverse=True)
            elif sort_by == 'closed':
                sorted_ips = sorted(connections_data.keys(), key=lambda ip: connections_data[ip]['closed'], reverse=True)
        else:
            sorted_ips = list(connections_data.keys())
        
        if compare:
            if len(compare) < 2:
                click.echo("Error: --compare requires at least 2 hostnames/IPs to compare.")
                return
            elif len(compare) > 3:
                click.echo("Error: --compare accepts maximum 3 hostnames/IPs. Only the first 3 will be used.")
                compare = compare[:3]
            
            click.echo(f"--------Comparing {', '.join(compare)}--------")
            filtered_ips = []
            for host in compare:
                if host in connections_data:
                    filtered_ips.append(host)
                else:
                    click.echo(f"Warning: {host} not found in connection data")
            
            if filtered_ips:
                sorted_ips = [ip for ip in compare if ip in filtered_ips]
            else:
                click.echo("No matching hostnames/IPs found for comparison")
                return
        
        ip_list = list(sorted_ips)
        if ip_list:
            max_ip_len = max(len(ip) for ip in ip_list)
        else:
            max_ip_len = 15
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
        clients_data = parse_clients(logfile)
        
        click.echo("===== MongoDB Log Summary =====")
        labels = [
            ("Log file", logfile),
            ("Start date", start_date if start_date else 'N/A'),
            ("End date", end_date if end_date else 'N/A'),
            ("Number of lines", num_lines),
        ]
        
        host_info = None
        if cmd_options and 'net' in cmd_options and 'port' in cmd_options['net']:
            port = cmd_options['net']['port']
            hostname = "localhost"
            host_info = f"{hostname}:{port}"
        if host_info:
            labels.append(("Host", host_info))
        
        labels.extend([
            ("OS version", os_version if os_version else 'N/A'),
            ("Kernel version", kernel_version if kernel_version else 'N/A'),
            ("MongoDB version", db_version if db_version else 'N/A'),
        ])
        repl_name = None
        if cmd_options and 'replication' in cmd_options and 'replSet' in cmd_options['replication']:
            repl_name = cmd_options['replication']['replSet']
        if repl_name:
            labels.append(("ReplicaSet Name", repl_name))
        if repl_name:
            configs = parse_replica_set_config(logfile)
            if configs:
                latest_config = configs[-1]['config']
                if 'members' in latest_config:
                    num_nodes = len(latest_config['members'])
                    labels.append(("Nodes", str(num_nodes)))
        
        max_label_len = max(len(label) for label, _ in labels)
        for label, value in labels:
            click.echo(f"{label.ljust(max_label_len)} : {value}")
        
        click.echo("\n===== Client/Driver Information =====")
        if clients_data:
            for driver_key, client_info in clients_data.items():
                driver_name = client_info['driver_name']
                driver_version = client_info['driver_version']
                app_info = client_info['app_name']
                
                if app_info:
                    click.echo(f"\n{driver_name} v{driver_version} ({app_info})")
                else:
                    click.echo(f"\n{driver_name} v{driver_version}")
                
                click.echo(f"├─ Connections: {len(client_info['connections'])}")
                
                ips = sorted(client_info['ips'])
                if ips:
                    click.echo(f"├─ IP Addresses: {', '.join(ips)}")
                else:
                    click.echo("├─ IP Addresses: None")
                
                users = sorted(client_info['users'])
                if users:
                    click.echo(f"└─ Users: {', '.join(users)}")
                else:
                    click.echo("└─ Users: None")
        else:
            click.echo("No client/driver information found in the log.")
        return
    elif queries:
        if sample == 100:
            try:
                with open(logfile, 'r') as f:
                    line_count = sum(1 for _ in f)
                if line_count > 50000:
                    click.echo(f"⚠️  Warning: Processing 100% of a large file ({line_count:,} lines) may be slow.", err=True)
                    click.echo(f"   Consider using --sample 50 for faster analysis.", err=True)
                    click.echo()
            except Exception:
                pass
        
        queries_data = parse_queries(logfile, sample_percentage=sample)
        
        query_stats = calculate_query_stats(queries_data)
        
        if report_full_patterns:
            with open(report_full_patterns, 'w') as f:
                f.write("===== Complete Query Pattern Statistics =====\n")
                f.write(f"Log file: {logfile}\n")
                f.write(f"Analysis date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                if namespace:
                    filtered_queries = {}
                    for (ns, op, pattern), stats_info in query_stats.items():
                        if ns == namespace:
                            filtered_queries[(ns, op, pattern)] = stats_info
                    query_stats = filtered_queries
                    if not query_stats:
                        f.write(f"No queries found for namespace: {namespace}\n")
                        return
                
                if operation:
                    filtered_queries = {}
                    for (ns, op, pattern), stats_info in query_stats.items():
                        if op == operation:
                            filtered_queries[(ns, op, pattern)] = stats_info
                    query_stats = filtered_queries
                    if not query_stats:
                        f.write(f"No queries found for operation: {operation}\n")
                        return
                
                if sort_by:
                    if sort_by == 'count':
                        sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['count'], reverse=True)
                    elif sort_by == 'min':
                        sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['min'], reverse=True)
                    elif sort_by == 'max':
                        sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['max'], reverse=True)
                    elif sort_by == '95%':
                        sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['percentile_95'], reverse=True)
                    elif sort_by == 'sum':
                        sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['sum'], reverse=True)
                    elif sort_by == 'mean':
                        sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['mean'], reverse=True)
                    else:
                        sorted_queries = list(query_stats.items())
                else:
                    sorted_queries = list(query_stats.items())
                
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
                    max_index_len = max(len(', '.join(sorted(stats['indexes']))) for stats in query_stats.values())
                    
                    max_namespace_len = max(max_namespace_len, len("Namespace"))
                    max_operation_len = max(max_operation_len, len("Operation"))
                    max_pattern_len = max(max_pattern_len, len("Pattern"))
                    max_count_len = max(max_count_len, len("Count"))
                    max_min_len = max(max_min_len, len("Min(ms)"))
                    max_max_len = max(max_max_len, len("Max(ms)"))
                    max_percentile_len = max(max_percentile_len, len("95%(ms)"))
                    max_sum_len = max(max_sum_len, len("Sum(ms)"))
                    max_mean_len = max(max_mean_len, len("Mean(ms)"))
                    max_index_len = max(max_index_len, len("Index"))
                else:
                    max_namespace_len = len("Namespace")
                    max_operation_len = len("Operation")
                    max_pattern_len = len("Pattern")
                    max_count_len = len("Count")
                    max_min_len = len("Min(ms)")
                    max_max_len = len("Max(ms)")
                    max_percentile_len = len("95%(ms)")
                    max_sum_len = len("Sum(ms)")
                    max_mean_len = len("Mean(ms)")
                    max_index_len = len("Index")
                
                header_fmt = f"{{:<{max_namespace_len}}} | {{:<{max_operation_len}}} | {{:<{max_pattern_len}}} | {{:>{max_count_len}}} | {{:>{max_min_len}}} | {{:>{max_max_len}}} | {{:>{max_percentile_len}}} | {{:>{max_sum_len}}} | {{:>{max_mean_len}}} | {{:<8}} | {{:<{max_index_len}}}"
                f.write(header_fmt.format("Namespace", "Operation", "Pattern", "Count", "Min(ms)", "Max(ms)", "95%(ms)", "Sum(ms)", "Mean(ms)", "AllowDiskUse", "Index") + "\n")
                f.write("-" * (max_namespace_len + max_operation_len + max_pattern_len + max_count_len + max_min_len + max_max_len + max_percentile_len + max_sum_len + max_mean_len + max_index_len + 35))
                f.write("\n")
                
                for (namespace, operation, pattern), stats_info in sorted_queries:
                    indexes_display = ', '.join(sorted(stats_info['indexes'])) if stats_info['indexes'] else 'N/A'
                    row_fmt = f"{{:<{max_namespace_len}}} | {{:<{max_operation_len}}} | {{:<{max_pattern_len}}} | {{:>{max_count_len}}} | {{:>{max_min_len}}} | {{:>{max_max_len}}} | {{:>{max_percentile_len}}} | {{:>{max_sum_len}}} | {{:>{max_mean_len}}} | {{:<8}} | {{:<{max_index_len}}}"
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
                        'Yes' if stats_info['allowDiskUse'] else 'No',
                        indexes_display
                    ) + "\n")
            
            click.echo(f"Complete query patterns written to: {report_full_patterns}")
            return
        
        click.echo("===== MongoDB Log Summary =====")
        labels = [
            ("Log file", logfile),
            ("Start date", start_date if start_date else 'N/A'),
            ("End date", end_date if end_date else 'N/A'),
            ("Number of lines", num_lines),
        ]
        
        host_info = None
        if cmd_options and 'net' in cmd_options and 'port' in cmd_options['net']:
            port = cmd_options['net']['port']
            hostname = "localhost"
            host_info = f"{hostname}:{port}"
        if host_info:
            labels.append(("Host", host_info))
        
        labels.extend([
            ("OS version", os_version if os_version else 'N/A'),
            ("Kernel version", kernel_version if kernel_version else 'N/A'),
            ("MongoDB version", db_version if db_version else 'N/A'),
        ])
        repl_name = None
        if cmd_options and 'replication' in cmd_options and 'replSet' in cmd_options['replication']:
            repl_name = cmd_options['replication']['replSet']
        if repl_name:
            labels.append(("ReplicaSet Name", repl_name))
        if repl_name:
            configs = parse_replica_set_config(logfile)
            if configs:
                latest_config = configs[-1]['config']
                if 'members' in latest_config:
                    num_nodes = len(latest_config['members'])
                    labels.append(("Nodes", str(num_nodes)))
        
        max_label_len = max(len(label) for label, _ in labels)
        for label, value in labels:
            click.echo(f"{label.ljust(max_label_len)} : {value}")
        
        click.echo("\n===== Query Pattern Statistics =====")
        
        if namespace:
            filtered_queries = {}
            for (ns, op, pattern), stats_info in query_stats.items():
                if ns == namespace:
                    filtered_queries[(ns, op, pattern)] = stats_info
            query_stats = filtered_queries
            if not query_stats:
                click.echo(f"No queries found for namespace: {namespace}")
                return
        
        if operation:
            filtered_queries = {}
            for (ns, op, pattern), stats_info in query_stats.items():
                if op == operation:
                    filtered_queries[(ns, op, pattern)] = stats_info
            query_stats = filtered_queries
            if not query_stats:
                click.echo(f"No queries found for operation: {operation}")
                return
        
        if sort_by:
            if sort_by == 'count':
                sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['count'], reverse=True)
            elif sort_by == 'min':
                sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['min'], reverse=True)
            elif sort_by == 'max':
                sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['max'], reverse=True)
            elif sort_by == '95%':
                sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['percentile_95'], reverse=True)
            elif sort_by == 'sum':
                sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['sum'], reverse=True)
            elif sort_by == 'mean':
                sorted_queries = sorted(query_stats.items(), key=lambda x: x[1]['mean'], reverse=True)
            else:
                sorted_queries = list(query_stats.items())
        else:
            sorted_queries = list(query_stats.items())
        
        if sorted_queries:
            max_namespace_len = max(len(str(key[0])) for key in query_stats.keys())
            max_operation_len = max(len(str(key[1])) for key in query_stats.keys())
            max_pattern_len = max(len(pattern[:150] + "..." if len(pattern) > 150 else pattern) for (_, _, pattern) in query_stats.keys())
            max_count_len = max(len(str(stats['count'])) for stats in query_stats.values())
            max_min_len = max(len(f"{stats['min']:.1f}") for stats in query_stats.values())
            max_max_len = max(len(f"{stats['max']:.1f}") for stats in query_stats.values())
            max_percentile_len = max(len(f"{stats['percentile_95']:.1f}") for stats in query_stats.values())
            max_sum_len = max(len(f"{stats['sum']:.1f}") for stats in query_stats.values())
            max_mean_len = max(len(f"{stats['mean']:.1f}") for stats in query_stats.values())
            max_index_len = max(len(', '.join(sorted(stats['indexes']))) for stats in query_stats.values())
            
            max_namespace_len = max(max_namespace_len, len("Namespace"))
            max_operation_len = max(max_operation_len, len("Operation"))
            max_pattern_len = max(max_pattern_len, len("Pattern"))
            max_count_len = max(max_count_len, len("Count"))
            max_min_len = max(max_min_len, len("Min(ms)"))
            max_max_len = max(max_max_len, len("Max(ms)"))
            max_percentile_len = max(max_percentile_len, len("95%(ms)"))
            max_sum_len = max(max_sum_len, len("Sum(ms)"))
            max_mean_len = max(max_mean_len, len("Mean(ms)"))
            max_index_len = max(max_index_len, len("Index"))
        else:
            max_namespace_len = len("Namespace")
            max_operation_len = len("Operation")
            max_pattern_len = len("Pattern")
            max_count_len = len("Count")
            max_min_len = len("Min(ms)")
            max_max_len = len("Max(ms)")
            max_percentile_len = len("95%(ms)")
            max_sum_len = len("Sum(ms)")
            max_mean_len = len("Mean(ms)")
            max_index_len = len("Index")
        
        header_fmt = f"{{:<{max_namespace_len}}} | {{:<{max_operation_len}}} | {{:<{max_pattern_len}}} | {{:>{max_count_len}}} | {{:>{max_min_len}}} | {{:>{max_max_len}}} | {{:>{max_percentile_len}}} | {{:>{max_sum_len}}} | {{:>{max_mean_len}}} | {{:<{max_index_len}}}"
        click.echo(header_fmt.format("Namespace", "Operation", "Pattern", "Count", "Min(ms)", "Max(ms)", "95%(ms)", "Sum(ms)", "Mean(ms)", "Index"))
        click.echo("-" * (max_namespace_len + max_operation_len + max_pattern_len + max_count_len + max_min_len + max_max_len + max_percentile_len + max_sum_len + max_mean_len + max_index_len + 27))
        
        for (namespace, operation, pattern), stats_info in sorted_queries:
            display_pattern = pattern[:150] + "..." if len(pattern) > 150 else pattern
            indexes_display = ', '.join(sorted(stats_info['indexes'])) if stats_info['indexes'] else 'N/A'
            
            row_fmt = f"{{:<{max_namespace_len}}} | {{:<{max_operation_len}}} | {{:<{max_pattern_len}}} | {{:>{max_count_len}}} | {{:>{max_min_len}}} | {{:>{max_max_len}}} | {{:>{max_percentile_len}}} | {{:>{max_sum_len}}} | {{:>{max_mean_len}}} | {{:<{max_index_len}}}"
            click.echo(row_fmt.format(
                namespace,
                operation,
                display_pattern,
                stats_info['count'],
                f"{stats_info['min']:.1f}",
                f"{stats_info['max']:.1f}",
                f"{stats_info['percentile_95']:.1f}",
                f"{stats_info['sum']:.1f}",
                f"{stats_info['mean']:.1f}",
                indexes_display
            ))
        
        if not query_stats:
            click.echo("No query patterns found in the log.")
        else:
            if report_histogram:
                all_durations = []
                for stats_info in query_stats.values():
                    all_durations.extend(stats_info['durations'])
                
                if all_durations:
                    click.echo(generate_histogram(all_durations))
            
            click.echo("\n💡 For the full pattern report, use --report-full-patterns <output-file>.")
        return

    # Default: summary mode
    click.echo("===== Node Command Line Startup =====")
    command_line = reconstruct_command_line(cmd_options)
    if command_line:
        click.echo(command_line, nl=False)
        click.echo()
    else:
        click.echo("No command line options found.")
    click.echo()

    labels = [
        ("Log file", logfile),
        ("Start date", start_date if start_date else 'N/A'),
        ("End date", end_date if end_date else 'N/A'),
        ("Number of lines", num_lines),
    ]
    
    host_info = None
    if cmd_options and 'net' in cmd_options and 'port' in cmd_options['net']:
        port = cmd_options['net']['port']
        hostname = "localhost"
        host_info = f"{hostname}:{port}"
    if host_info:
        labels.append(("Host", host_info))
    
    labels.extend([
        ("OS version", os_version if os_version else 'N/A'),
        ("Kernel version", kernel_version if kernel_version else 'N/A'),
        ("MongoDB version", db_version if db_version else 'N/A'),
    ])
    repl_name = None
    if cmd_options and 'replication' in cmd_options:
        repl_opts = cmd_options['replication']
        if 'replSetName' in repl_opts:
            repl_name = repl_opts['replSetName']
        elif 'replSet' in repl_opts:
            repl_name = repl_opts['replSet']
    if repl_name:
        labels.append(("ReplicaSet Name", repl_name))
    if repl_name:
        configs = parse_replica_set_config(logfile)
        if configs:
            latest_config = configs[-1]['config']
            if 'members' in latest_config:
                num_nodes = len(latest_config['members'])
                labels.append(("Nodes", str(num_nodes)))
    
    max_label_len = max(len(label) for label, _ in labels)

    click.echo("===== MongoDB Log Summary =====")
    for label, value in labels:
        click.echo(f"{label.ljust(max_label_len)} : {value}")
