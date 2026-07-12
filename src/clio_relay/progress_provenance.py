"""Helpers for progress provenance metadata."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from clio_relay.errors import ConfigurationError

PROTECTED_PROGRESS_METADATA_KEYS = frozenset(
    {
        "source",
        "package_name",
        "package_version",
        "run_id",
        "execution_id",
        "relay_progress_token",
        "adapter",
        "application_profile",
        "provider_entry_point",
        "provider_entry_point_value",
        "provider_distribution",
        "provider_distribution_version",
        "provider_source_authority",
        "provider_validated",
        "acceptance_validated",
        "provider_execution_id",
        "provider_pipeline_id",
        "provider_server_artifact_digest",
        "provider_notification_sequence",
        "provider_transport_source_authority",
        "provider_execution_validated",
        "mcp_progress_bridge",
    }
)


class PackageProgressSourceAuthority(StrEnum):
    """Exclusive worker input selected for one package progress provider."""

    PACKAGE_LOG = "package_log"
    JARVIS_STDOUT_FALLBACK = "jarvis_stdout_fallback"
    MCP_PROGRESS_NOTIFICATION = "mcp_progress_notification"


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


def package_progress_provider_metadata(
    metadata: dict[str, Any],
    *,
    package_name: str,
    package_version: str,
    run_id: str,
    adapter_name: str,
    provider_entry_point: str,
    provider_entry_point_value: str,
    provider_distribution: str,
    provider_distribution_version: str,
    source_authority: PackageProgressSourceAuthority,
    application_profile: str | None,
    provider_validated: bool,
    acceptance_validated: bool,
) -> dict[str, Any]:
    """Stamp relay-protected provider identity onto one progress candidate."""
    filtered = {
        key: value for key, value in metadata.items() if key not in PROTECTED_PROGRESS_METADATA_KEYS
    }
    trusted = package_progress_metadata(
        filtered,
        package_name=package_name,
        package_version=package_version,
        run_id=run_id,
    )
    trusted.update(
        {
            "adapter": adapter_name,
            "provider_entry_point": provider_entry_point,
            "provider_entry_point_value": provider_entry_point_value,
            "provider_distribution": provider_distribution,
            "provider_distribution_version": provider_distribution_version,
            "provider_source_authority": source_authority.value,
            "provider_validated": provider_validated,
            "acceptance_validated": acceptance_validated,
        }
    )
    if application_profile is not None:
        trusted["application_profile"] = application_profile
    return trusted


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


def validate_package_progress_provider_metadata(metadata: dict[str, Any]) -> None:
    """Require durable proof that an external provider approved the observation."""
    validate_package_progress_metadata(metadata)
    for key in (
        "adapter",
        "provider_entry_point",
        "provider_entry_point_value",
        "provider_distribution",
        "provider_distribution_version",
        "provider_source_authority",
    ):
        value = metadata.get(key)
        if not isinstance(value, str) or not value:
            raise ConfigurationError(f"package progress metadata {key} must be a non-empty string")
    profile = metadata.get("application_profile")
    if profile is not None and (not isinstance(profile, str) or not profile):
        raise ConfigurationError(
            "package progress metadata application_profile must be a non-empty string"
        )
    if metadata.get("provider_validated") is not True:
        raise ConfigurationError("package progress metadata provider_validated must be true")
    if not isinstance(metadata.get("acceptance_validated"), bool):
        raise ConfigurationError("package progress metadata acceptance_validated must be boolean")
    source_authority = metadata.get("provider_source_authority")
    if source_authority not in {
        PackageProgressSourceAuthority.PACKAGE_LOG.value,
        PackageProgressSourceAuthority.JARVIS_STDOUT_FALLBACK.value,
        PackageProgressSourceAuthority.MCP_PROGRESS_NOTIFICATION.value,
    }:
        raise ConfigurationError("package progress metadata provider_source_authority is invalid")


def validate_package_progress_acceptance_metadata(metadata: dict[str, Any]) -> None:
    """Require a provider-valid record that also satisfies the release acceptance predicate."""
    validate_package_progress_provider_metadata(metadata)
    if metadata.get("acceptance_validated") is not True:
        raise ConfigurationError("package progress metadata acceptance_validated must be true")
