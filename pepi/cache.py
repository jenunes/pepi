from __future__ import annotations

"""Cache management for Pepi log analysis results."""

import hashlib
import json
import os
import pickle
import time
from pathlib import Path
from typing import Any, Optional

CACHE_DIR = Path.home() / '.pepi_cache'
CACHE_DIR.mkdir(exist_ok=True)
CACHE_TTL_SECONDS = 7 * 24 * 3600  # 7 days


def is_cache_expired(cache_file: Path) -> bool:
    mtime = os.path.getmtime(cache_file)
    return (time.time() - mtime) > CACHE_TTL_SECONDS


def get_file_hash(filepath: Path) -> str:
    """Calculate SHA256 hash of file for cache invalidation."""
    hash_sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


def build_cache_variant(params: dict[str, Any]) -> str:
    """Build a stable short variant hash from parameter payload."""
    canonical = json.dumps(params, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]


def get_cache_key(filepath: Path, analysis_type: str, variant: str = "") -> str:
    """Generate cache key for specific analysis type."""
    file_hash = get_file_hash(filepath)
    if variant:
        return f"{file_hash}_{analysis_type}_{variant}"
    return f"{file_hash}_{analysis_type}"


def get_cache_file(cache_key: str) -> Path:
    """Get cache file path for given key."""
    return CACHE_DIR / f"{cache_key}.pkl"


def load_from_cache(cache_key: str) -> Optional[Any]:
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


def save_to_cache(cache_key: str, data: Any) -> None:
    """Save results to cache."""
    cache_file = get_cache_file(cache_key)
    try:
        with open(cache_file, 'wb') as f:
            pickle.dump(data, f)
    except Exception:
        # If cache write fails, continue without caching
        pass
