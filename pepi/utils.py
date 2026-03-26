"""Shared utility functions for Pepi."""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Optional

import click
from tqdm import tqdm


def count_lines(logfile: str | os.PathLike[str]) -> int:
    """Count lines in file for progress bar."""
    try:
        with open(logfile, "r") as f:
            return sum(1 for _ in f)
    except Exception:
        return 0


def parse_flexible_datetime(date_str: str | None) -> datetime | None:
    """Parse flexible date/time string with smart defaults."""
    if not date_str:
        return None

    # Remove any extra whitespace
    date_str = date_str.strip()

    # Split by space to separate date and time parts
    parts = date_str.split(" ")
    date_part = parts[0] if parts else ""
    time_part = parts[1] if len(parts) > 1 else ""

    # Parse date part (DD/MM/YYYY)
    date_components = date_part.split("/")
    if len(date_components) != 3:
        raise ValueError(f"Invalid date format: {date_part}. Expected DD/MM/YYYY")

    try:
        day = int(date_components[0])
        month = int(date_components[1])
        year = int(date_components[2])
    except ValueError:
        raise ValueError(f"Invalid date components: {date_part}")

    # Parse time part (HH:MM:SS:MS) with defaults
    hour = minute = second = microsecond = 0

    if time_part:
        time_components = time_part.split(":")
        try:
            if len(time_components) >= 1:
                hour = int(time_components[0])
            if len(time_components) >= 2:
                minute = int(time_components[1])
            if len(time_components) >= 3:
                second = int(time_components[2])
            if len(time_components) >= 4:
                microsecond = int(time_components[3]) * 1000  # Convert MS to microseconds
        except ValueError:
            raise ValueError(f"Invalid time components: {time_part}")

    return datetime(year, month, day, hour, minute, second, microsecond)


def get_date_range(
    from_str: Optional[str], until_str: Optional[str]
) -> tuple[datetime | None, datetime | None]:
    """Get start and end datetime objects from flexible input strings."""
    start_dt = end_dt = None

    if from_str:
        start_dt = parse_flexible_datetime(from_str)

        # If from and until are the same, assume from start to end of that period
        if until_str and from_str == until_str:
            # If only date provided, assume whole day
            if " " not in from_str:
                start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
                end_dt = start_dt.replace(hour=23, minute=59, second=59, microsecond=999000)
            # If time provided, assume from start to end of that minute/second/etc
            else:
                end_dt = start_dt.replace(second=59, microsecond=999000)
        else:
            # If only date provided in from_str, start from beginning of day
            if " " not in from_str:
                start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    if until_str and from_str != until_str:
        end_dt = parse_flexible_datetime(until_str)
        # If only date provided in until_str, end at end of day
        if " " not in until_str:
            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999000)

    return start_dt, end_dt


def _parse_timestamp_from_line(line: str) -> datetime | None:
    """Quickly extract and parse timestamp from a log line without full JSON parse if possible."""
    try:
        # Try to find timestamp in JSON structure
        # Look for "$date" pattern which is faster than full JSON parse
        if '"$date"' not in line and "'$date'" not in line:
            return None

        # Parse JSON to get timestamp
        entry = json.loads(line)
        timestamp_str = entry.get("t", {}).get("$date")

        if not timestamp_str:
            return None

        # Parse MongoDB timestamp format
        if timestamp_str.endswith("Z"):
            log_dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        elif "+" in timestamp_str or "-" in timestamp_str[-6:]:
            log_dt = datetime.fromisoformat(timestamp_str)
        else:
            log_dt = datetime.fromisoformat(timestamp_str)

        # Remove timezone info for comparison (assume local time)
        return log_dt.replace(tzinfo=None)
    except Exception:
        return None


def _find_start_position(logfile: str | os.PathLike[str], start_dt: datetime) -> int:
    """Use binary search to find the approximate start position in the file."""
    file_size = os.path.getsize(logfile)
    if file_size == 0:
        return 0

    left = 0
    right = file_size
    best_position = 0

    # Binary search with up to 20 iterations (covers files up to 2^20 * chunk_size)
    for _ in range(20):
        if right - left < 1024:  # Stop when range is small enough
            break

        mid = (left + right) // 2

        # Read from this position and find the next complete line
        with open(logfile, "rb") as f:
            f.seek(mid)
            # Skip to next newline to get a complete line
            f.readline()  # Discard partial line
            line_start = f.tell()

            # Read a few lines to find one with a timestamp
            for _ in range(10):  # Try up to 10 lines
                line_bytes = f.readline()
                if not line_bytes:
                    break

                try:
                    line = line_bytes.decode("utf-8", errors="ignore")
                    log_dt = _parse_timestamp_from_line(line)

                    if log_dt is not None:
                        if log_dt < start_dt:
                            # We're before the start, search right half
                            left = line_start
                            best_position = line_start
                        else:
                            # We're at or after start, search left half
                            right = line_start
                        break
                except Exception:
                    continue
            else:
                # No timestamp found, be conservative and search left
                right = line_start

    return best_position


def trim_log_file(
    logfile: str | os.PathLike[str],
    start_dt: datetime | None,
    end_dt: datetime | None,
) -> tuple[list[str], int, int]:
    """Trim log file by date/time range and return filtered lines.

    Optimized with binary search to jump to start date and early exit at end date.
    """
    filtered_lines = []
    skipped_lines = 0
    total_lines = 0

    # Early exit optimization: stop after seeing consecutive lines past end_dt
    consecutive_past_end = 0
    max_consecutive_past_end = 1000  # Safety margin for out-of-order entries

    # Use binary search to jump to start position if start_dt is specified
    start_position = 0
    if start_dt:
        click.echo("🔍 Finding start position...")
        start_position = _find_start_position(logfile, start_dt)
        click.echo(f"📍 Starting from position {start_position:,} bytes")

    # Open in binary mode to support byte-based seeking, then decode lines
    with open(logfile, "rb") as f:
        # Jump to start position
        if start_position > 0:
            f.seek(start_position)
            # Skip to next complete line (in case we landed in the middle)
            f.readline()  # Discard partial line

        # Skip line counting - use None for total to show rate but not percentage
        for line_bytes in tqdm(f, total=None, desc="Trimming log file", unit="lines"):
            # Decode the line from bytes to string
            try:
                line = line_bytes.decode("utf-8", errors="replace")
            except Exception:
                # Skip lines that can't be decoded
                continue
            total_lines += 1

            # Early exit: if we've seen many consecutive lines past end_dt, stop
            if end_dt and consecutive_past_end >= max_consecutive_past_end:
                break

            try:
                # Try to parse as JSON to get timestamp
                entry = json.loads(line)
                timestamp_str = entry.get("t", {}).get("$date")

                if timestamp_str:
                    # Parse MongoDB timestamp format
                    try:
                        # Handle both formats: with and without timezone
                        if timestamp_str.endswith("Z"):
                            log_dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                        elif "+" in timestamp_str or "-" in timestamp_str[-6:]:
                            log_dt = datetime.fromisoformat(timestamp_str)
                        else:
                            log_dt = datetime.fromisoformat(timestamp_str)

                        # Remove timezone info for comparison (assume local time)
                        log_dt = log_dt.replace(tzinfo=None)

                        # Check if within range
                        include_line = True

                        # Check start date (should be rare now since we jumped to start)
                        if start_dt and log_dt < start_dt:
                            include_line = False
                            consecutive_past_end = 0

                        # Check end date
                        if end_dt and log_dt > end_dt:
                            include_line = False
                            consecutive_past_end += 1
                        else:
                            # Reset counter if we're still in range
                            consecutive_past_end = 0

                        if include_line:
                            filtered_lines.append(line)
                            consecutive_past_end = 0  # Reset counter on match
                        else:
                            skipped_lines += 1
                    except Exception:
                        # If timestamp parsing fails, include the line
                        filtered_lines.append(line)
                        consecutive_past_end = 0
                else:
                    # If no timestamp, include the line
                    filtered_lines.append(line)
                    consecutive_past_end = 0

            except Exception:
                # If JSON parsing fails, include the line
                filtered_lines.append(line)
                consecutive_past_end = 0

    return filtered_lines, total_lines, skipped_lines
