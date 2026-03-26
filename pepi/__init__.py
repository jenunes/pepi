"""Pepi: MongoDB log analysis tool."""

from pepi.version import __version__
from pepi.cache import CACHE_DIR, get_cache_key, load_from_cache, save_to_cache
from pepi.sampling import (
    get_sample_rate,
    get_sample_rate_from_percentage,
    get_sampling_metadata,
    should_sample_data,
)
from pepi.utils import count_lines, get_date_range, parse_flexible_datetime, trim_log_file
from pepi.stats import calculate_query_stats, calculate_connection_stats
from pepi.formatters import generate_histogram, reconstruct_command_line
from pepi.upgrade import check_for_updates, check_version_async, perform_upgrade
from pepi.parser import (
    parse_connections,
    parse_replica_set_config,
    parse_replica_set_state,
    parse_clients,
    parse_queries,
    parse_timeseries_data,
    parse_connections_timeseries_by_ip,
    validate_connection_data_consistency,
    parse_connection_events,
    extract_query_pattern,
)
from pepi.cli import main
