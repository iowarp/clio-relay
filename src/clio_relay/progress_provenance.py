"""Helpers for progress provenance metadata."""

from __future__ import annotations

from typing import Any

from clio_relay.errors import ConfigurationError

PROTECTED_PROGRESS_METADATA_KEYS = frozenset(
    {"source", "package_name", "package_version", "run_id", "execution_id", "relay_progress_token"}
)


def external_progress_metadata(source: str, metadata: dict[str, Any]) -> dict[str, Any]:
    """Return observer metadata without allowing callers to spoof protected fields."""
    filtered = {
        key: value for key, value in metadata.items() if key not in PROTECTED_PROGRESS_METADATA_KEYS
    }
    return {**filtered, "source": source}


def package_progress_metadata(
    metadata: dict[str, Any],
    *,
    package_name: str,
    package_version: str,
    run_id: str,
) -> dict[str, Any]:
    """Return trusted package metadata with mandatory provenance fields."""
    filtered = {
        key: value
        for key, value in metadata.items()
        if key not in {"source", "relay_progress_token"}
    }
    return {
        **filtered,
        "source": "jarvis_package",
        "package_name": package_name,
        "package_version": package_version,
        "run_id": run_id,
        "execution_id": run_id,
    }


def validate_package_progress_metadata(metadata: dict[str, Any]) -> None:
    """Require progress metadata to identify the trusted package execution."""
    required = {
        "source": "jarvis_package",
        "package_name": None,
        "package_version": None,
        "run_id": None,
        "execution_id": None,
    }
    for key, expected in required.items():
        value = metadata.get(key)
        if expected is not None and value != expected:
            raise ConfigurationError(f"package progress metadata {key} must be {expected}")
        if expected is None and (not isinstance(value, str) or value == ""):
            raise ConfigurationError(f"package progress metadata {key} must be a non-empty string")
