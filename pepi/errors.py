"""
Custom error types and FastAPI dependencies for Pepi.

Provides consistent error handling across the CLI and web API layers.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from fastapi import HTTPException

if TYPE_CHECKING:
    pass


# ---------------------------------------------------------------------------
# Custom exception hierarchy
# ---------------------------------------------------------------------------


class PepiError(Exception):
    """Base exception for all Pepi errors."""


class FileNotFoundError(PepiError):
    """Raised when a referenced log file cannot be found."""

    def __init__(self, file_id: str | None = None, path: str | None = None):
        self.file_id = file_id
        self.path = path
        detail = f"File not found: {file_id or path}"
        super().__init__(detail)


class FileExpiredError(PepiError):
    """Raised when an uploaded file no longer exists on disk."""

    def __init__(self, file_id: str, path: str):
        self.file_id = file_id
        self.path = path
        super().__init__(f"File no longer exists on disk: {path}")


class AnalysisError(PepiError):
    """Raised when log analysis fails."""

    def __init__(self, analysis_type: str, detail: str):
        self.analysis_type = analysis_type
        super().__init__(f"{analysis_type} analysis failed: {detail}")


class ValidationError(PepiError):
    """Raised when input validation fails."""


class CacheError(PepiError):
    """Raised when cache operations fail (non-fatal)."""


class UpgradeError(PepiError):
    """Raised when the self-upgrade process fails."""


# ---------------------------------------------------------------------------
# FastAPI dependency: validate file_id and return path
# ---------------------------------------------------------------------------


def get_validated_file_path(file_id: str, upload_store: dict[str, dict]) -> str:
    """Validate that file_id exists in the store and the file is on disk.

    Intended to be called at the top of every route that receives a file_id.
    Raises HTTPException so the caller doesn't need try/except boilerplate.

    Returns the absolute file path.
    """
    if file_id not in upload_store:
        raise HTTPException(status_code=404, detail="File not found")

    file_path: str = upload_store[file_id]["path"]

    if not os.path.exists(file_path):
        del upload_store[file_id]
        raise HTTPException(status_code=404, detail="File no longer exists on disk")

    return file_path


def validate_sample_param(sample: int | None) -> None:
    """Guard clause for the sample query parameter (0-100)."""
    if sample is not None and (sample < 0 or sample > 100):
        raise HTTPException(
            status_code=400,
            detail="Sample percentage must be between 0 and 100",
        )
