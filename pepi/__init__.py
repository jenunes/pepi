"""Pepi: MongoDB log analysis tool."""

from pepi.cache import CACHE_DIR, get_cache_key, load_from_cache, save_to_cache
from pepi.cli import main
from pepi.formatters import generate_histogram, reconstruct_command_line
from pepi.parser import (
    extract_query_pattern,
    parse_auth_failures,
    parse_clients,
    parse_collscan_trends,
    parse_connection_events,
    parse_connections,
    parse_connections_timeseries_by_ip,
    parse_errors_detail,
    parse_lock_contention,
    parse_queries,
    parse_repl_health,
    parse_replica_set_config,
    parse_replica_set_state,
    parse_timeseries_data,
    validate_connection_data_consistency,
)
from pepi.sampling import (
    get_sample_rate,
    get_sample_rate_from_percentage,
    get_sampling_metadata,
    should_sample_data,
)
from pepi.stats import calculate_connection_stats, calculate_query_stats
from pepi.upgrade import check_for_updates, check_version_async, perform_upgrade
from pepi.utils import count_lines, get_date_range, parse_flexible_datetime, trim_log_file
from pepi.version import __version__

__all__ = [
    "CACHE_DIR",
    "__version__",
    "calculate_connection_stats",
    "calculate_query_stats",
    "check_for_updates",
    "check_version_async",
    "count_lines",
    "extract_query_pattern",
    "generate_histogram",
    "get_cache_key",
    "get_date_range",
    "get_sample_rate",
    "get_sample_rate_from_percentage",
    "get_sampling_metadata",
    "load_from_cache",
    "main",
    "parse_auth_failures",
    "parse_clients",
    "parse_collscan_trends",
    "parse_connection_events",
    "parse_connections",
    "parse_connections_timeseries_by_ip",
    "parse_errors_detail",
    "parse_flexible_datetime",
    "parse_lock_contention",
    "parse_queries",
    "parse_repl_health",
    "parse_replica_set_config",
    "parse_replica_set_state",
    "parse_timeseries_data",
    "perform_upgrade",
    "reconstruct_command_line",
    "save_to_cache",
    "should_sample_data",
    "trim_log_file",
    "validate_connection_data_consistency",
]

