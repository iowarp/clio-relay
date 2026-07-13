"""Helpers for progress provenance metadata."""

from __future__ import annotations

import math
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
        "mcp_native_progress_bridge",
        "pipeline_id",
        "package_id",
        "relay_job_id",
        "progress_schema_version",
        "progress_state",
        "progress_sequence",
        "progress_observed_at_epoch",
        "progress_determinate",
        "progress_event_count",
        "progress_skipped_event_count",
        "producer_validated",
        "execution_binding_validated",
        "execution_state",
        "execution_terminal",
        "progress_transport_sequence",
        "server_artifact_digest",
    }
)


class PackageProgressSourceAuthority(StrEnum):
    """Exclusive worker input selected for one package progress provider."""

    PACKAGE_LOG = "package_log"
    JARVIS_STDOUT_FALLBACK = "jarvis_stdout_fallback"
    MCP_PROGRESS_NOTIFICATION = "mcp_progress_notification"
    JARVIS_MCP_NOTIFICATION = "jarvis_mcp_progress_notification"


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


def jarvis_execution_progress_metadata(
    metadata: dict[str, Any],
    *,
    relay_job_id: str,
    execution_id: str,
    pipeline_id: str,
    package_name: str,
    package_id: str,
    progress_state: str,
    progress_sequence: int,
    observed_at_epoch: float,
    determinate: bool,
    event_count: int,
    skipped_event_count: int,
    execution_state: str,
    execution_terminal: bool,
    transport_sequence: int,
    server_artifact_digest: str,
    execution_binding_validated: bool,
) -> dict[str, Any]:
    """Stamp an exact native JARVIS event without application inference."""
    filtered = {
        key: value for key, value in metadata.items() if key not in PROTECTED_PROGRESS_METADATA_KEYS
    }
    return {
        **filtered,
        "source": "jarvis_execution",
        "relay_job_id": relay_job_id,
        "execution_id": execution_id,
        "run_id": execution_id,
        "pipeline_id": pipeline_id,
        "package_name": package_name,
        "package_id": package_id,
        "progress_schema_version": "jarvis.progress.v1",
        "progress_state": progress_state,
        "progress_sequence": progress_sequence,
        "progress_observed_at_epoch": observed_at_epoch,
        "progress_determinate": determinate,
        "progress_event_count": event_count,
        "progress_skipped_event_count": skipped_event_count,
        "execution_state": execution_state,
        "execution_terminal": execution_terminal,
        "progress_transport_sequence": transport_sequence,
        "server_artifact_digest": server_artifact_digest,
        "provider_source_authority": PackageProgressSourceAuthority.JARVIS_MCP_NOTIFICATION.value,
        "producer_validated": True,
        "execution_binding_validated": execution_binding_validated,
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


def validate_jarvis_execution_progress_metadata(metadata: dict[str, Any]) -> None:
    """Require exact JARVIS-owned progress provenance and truthful optional totals."""
    required_strings = {
        "source": "jarvis_execution",
        "relay_job_id": None,
        "execution_id": None,
        "run_id": None,
        "pipeline_id": None,
        "package_name": None,
        "package_id": None,
        "progress_schema_version": "jarvis.progress.v1",
        "progress_state": None,
        "execution_state": None,
        "server_artifact_digest": None,
        "provider_source_authority": (PackageProgressSourceAuthority.JARVIS_MCP_NOTIFICATION.value),
    }
    for key, expected in required_strings.items():
        value = metadata.get(key)
        if not isinstance(value, str) or not value:
            raise ConfigurationError(
                f"JARVIS execution progress metadata {key} must be a non-empty string"
            )
        if expected is not None and value != expected:
            raise ConfigurationError(f"JARVIS execution progress metadata {key} did not match")
    if metadata["run_id"] != metadata["execution_id"]:
        raise ConfigurationError("JARVIS execution progress run identity did not match")
    for key in (
        "progress_sequence",
        "progress_event_count",
        "progress_skipped_event_count",
        "progress_transport_sequence",
    ):
        value = metadata.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ConfigurationError(
                f"JARVIS execution progress metadata {key} must be nonnegative"
            )
    if metadata["progress_event_count"] < 1:
        raise ConfigurationError("JARVIS execution progress event count must be positive")
    if metadata["progress_skipped_event_count"] >= metadata["progress_event_count"]:
        raise ConfigurationError("JARVIS execution progress skipped event count was invalid")
    observed = metadata.get("progress_observed_at_epoch")
    if (
        isinstance(observed, bool)
        or not isinstance(observed, int | float)
        or not math.isfinite(float(observed))
        or observed < 0
    ):
        raise ConfigurationError("JARVIS execution progress observed time was invalid")
    if not isinstance(metadata.get("progress_determinate"), bool):
        raise ConfigurationError("JARVIS execution progress determinate flag was invalid")
    if not isinstance(metadata.get("execution_terminal"), bool):
        raise ConfigurationError("JARVIS execution progress terminal flag was invalid")
    progress_state = metadata["progress_state"]
    if progress_state not in {
        "pending",
        "starting",
        "running",
        "ready",
        "completed",
        "failed",
        "canceled",
    }:
        raise ConfigurationError("JARVIS execution progress state was invalid")
    execution_state = metadata["execution_state"]
    if execution_state not in {
        "preparing",
        "scripted",
        "submitting",
        "submitted",
        "running",
        "completed",
        "failed",
        "canceled",
        "unknown",
    }:
        raise ConfigurationError("JARVIS execution state was invalid")
    terminal = metadata["execution_terminal"]
    if terminal and execution_state not in {"scripted", "completed", "failed", "canceled"}:
        raise ConfigurationError("JARVIS terminal execution state was invalid")
    if execution_state in {"completed", "failed", "canceled"} and not terminal:
        raise ConfigurationError("JARVIS terminal execution omitted terminal=true")
    if (
        metadata["progress_transport_sequence"] == 0
        and metadata.get("execution_binding_validated") is not True
    ):
        raise ConfigurationError("JARVIS zero transport sequence was not final-result validated")
    if metadata.get("producer_validated") is not True:
        raise ConfigurationError("JARVIS execution progress producer was not validated")
    if not isinstance(metadata.get("execution_binding_validated"), bool):
        raise ConfigurationError("JARVIS execution progress binding flag was invalid")
