"""Sampling logic for efficient processing of large MongoDB log files."""

from __future__ import annotations

from typing import Any, Optional


def should_sample_data(
    total_lines: int,
    threshold: int = 50000,
    user_sample_percentage: Optional[float] = None,
) -> bool:
    """Determine if we should sample data based on file size or user preference."""
    if user_sample_percentage is not None:
        return user_sample_percentage < 100
    return total_lines > threshold


def get_sample_rate_from_percentage(
    sample_percentage: int | float,
    total_lines: Optional[int] = None,
) -> int | float:
    """Convert percentage to sample rate and handle auto-sampling for large files."""
    if sample_percentage == 100:
        # Check if auto-sampling should kick in for large files
        if total_lines and total_lines > 50000:
            return get_sample_rate(total_lines)
        return 1  # No sampling
    elif sample_percentage == 0:
        return float('inf')  # Skip all
    else:
        # Convert percentage to sample rate: 50% = every 2nd line
        return int(100 / sample_percentage)


def get_sample_rate(total_lines: int) -> int:
    """Calculate sample rate for large files."""
    if total_lines < 50000:
        return 1  # No sampling
    elif total_lines < 200000:
        return 5  # Every 5th line
    elif total_lines < 500000:
        return 10  # Every 10th line
    else:
        return 20  # Every 20th line


def get_sampling_metadata(
    total_lines: int,
    user_sample_percentage: Optional[float] = None,
) -> dict[str, Any]:
    """Get metadata about sampling for large files."""
    if user_sample_percentage is not None:
        sample_rate = get_sample_rate_from_percentage(user_sample_percentage, total_lines)
        is_sampled = sample_rate > 1
        is_user_forced = user_sample_percentage < 100
    else:
        sample_rate = get_sample_rate(total_lines)
        is_sampled = sample_rate > 1
        is_user_forced = False

    # Calculate actual lines processed
    if is_sampled:
        sampled_lines = total_lines // sample_rate
    else:
        sampled_lines = total_lines

    return {
        'total_lines': total_lines,
        'is_sampled': is_sampled,
        'sample_rate': sample_rate,
        'sampled_lines': sampled_lines,
        'estimated_original_size': total_lines if not is_sampled else total_lines * sample_rate,
        'is_user_forced': is_user_forced,
        'user_percentage': user_sample_percentage
    }
