"""Merge partial runtime metadata observations without losing provenance.

Extracted from ``runtime_metadata.py`` (clio-relay split/runtime-metadata-w2):
:func:`merge_runtime_metadata` combines two
:class:`~clio_relay.runtime_metadata_core_model.JarvisRuntimeMetadata`
observations while preferring higher-trust structured sources and pinning
identity fields (execution_id, pipeline_id, scheduler_provider,
scheduler_job_id) once an authoritative source has set them -- an incoming
observation that would change a pinned identity is rejected rather than
silently overwriting it. This owns the trust-priority ordering
(``_source_priority``), the authoritative-source set
(``_AUTHORITATIVE_RUNTIME_SOURCES``), the scheduler-submission producer
contract check (``_trusted_producer_runtime_contract``), and the native
execution/progress lifecycle-regression guards
(``_validate_native_runtime_transition`` /
``_validate_native_progress_transition``) that reject an execution appearing
to move backward through its own state machine.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from clio_relay.runtime_metadata_coercion import _mapping
from clio_relay.runtime_metadata_core_model import JarvisRuntimeMetadata
from clio_relay.runtime_metadata_native_documents import (
    JarvisExecutionProgressDocument,
    JarvisNativeExecutionDocuments,
)
from clio_relay.runtime_metadata_types import (
    _JARVIS_REACHABLE_STATES,
    JARVIS_RUNTIME_METADATA_SCHEMA,
    JARVIS_SCHEDULER_SUBMISSION_SCHEMA,
    RuntimeMetadataIdentityConflictError,
    RuntimeMetadataSource,
)


def merge_runtime_metadata(
    current: JarvisRuntimeMetadata | None,
    incoming: JarvisRuntimeMetadata,
) -> JarvisRuntimeMetadata:
    """Merge partial observations while preferring higher-trust structured sources."""
    if current is None:
        return incoming
    _validate_native_runtime_transition(current, incoming)
    current_priority = _source_priority(current.source)
    incoming_priority = _source_priority(incoming.source)
    prefer_incoming = incoming_priority >= current_priority
    primary = incoming if prefer_incoming else current
    secondary = current if prefer_incoming else incoming
    update: dict[str, object] = {}
    field_sources: dict[str, RuntimeMetadataSource] = {}
    for field_name in (
        "execution_id",
        "pipeline_id",
        "scheduler_provider",
        "scheduler_type",
        "scheduler_job_id",
        "scheduler_phase",
        "script_path",
        "hostfile_path",
        "output_path",
        "error_path",
    ):
        primary_value = getattr(primary, field_name)
        secondary_value = getattr(secondary, field_name)
        pinned_source = _authoritative_field_source(current, field_name)
        pinned_value = getattr(current, field_name)
        incoming_source = _authoritative_field_source(incoming, field_name)
        incoming_value = getattr(incoming, field_name)
        if (
            field_name in _PINNED_RUNTIME_IDENTITY_FIELDS
            and pinned_source is not None
            and pinned_value is not None
        ):
            if (
                incoming_source is not None
                and incoming_value is not None
                and not _same_runtime_identity(field_name, pinned_value, incoming_value)
            ):
                raise RuntimeMetadataIdentityConflictError(
                    "authoritative runtime metadata changed pinned "
                    f"{field_name}: {pinned_value!r} != {incoming_value!r}"
                )
            update[field_name] = pinned_value
            field_sources[field_name] = pinned_source
            continue
        update[field_name] = primary_value if primary_value is not None else secondary_value
        selected = primary if primary_value is not None else secondary
        selected_source = selected.field_sources.get(field_name)
        if selected_source is not None:
            field_sources[field_name] = selected_source
    update["allocated_nodes"] = primary.allocated_nodes or secondary.allocated_nodes
    update["packages"] = primary.packages or secondary.packages
    nodes_source = primary if primary.allocated_nodes else secondary
    packages_source = primary if primary.packages else secondary
    if source := nodes_source.field_sources.get("allocated_nodes"):
        field_sources["allocated_nodes"] = source
    if source := packages_source.field_sources.get("packages"):
        field_sources["packages"] = source
    terminal_updates: dict[str, object] = {}
    for field_name in (
        "state",
        "terminal",
        "returncode",
        "reason",
        "started_at",
        "finished_at",
    ):
        primary_value = getattr(primary.terminal, field_name)
        secondary_value = getattr(secondary.terminal, field_name)
        terminal_updates[field_name] = (
            primary_value if primary_value is not None else secondary_value
        )
        selected = primary if primary_value is not None else secondary
        source_key = f"terminal.{field_name}"
        selected_source = selected.field_sources.get(source_key)
        if selected_source is not None:
            field_sources[source_key] = selected_source
    update["terminal"] = primary.terminal.model_copy(update=terminal_updates)
    update["field_sources"] = field_sources
    update["details"] = secondary.details | primary.details
    return primary.model_copy(update=update)


_PINNED_RUNTIME_IDENTITY_FIELDS = frozenset(
    {"execution_id", "pipeline_id", "scheduler_provider", "scheduler_job_id"}
)


def _validate_native_runtime_transition(
    current: JarvisRuntimeMetadata,
    incoming: JarvisRuntimeMetadata,
) -> None:
    """Reject identity or lifecycle regressions across exact native observations."""
    current_documents = _native_documents_from_runtime_metadata(current)
    incoming_documents = _native_documents_from_runtime_metadata(incoming)
    if current_documents is None or incoming_documents is None:
        return
    old_handle = current_documents.execution_handle
    new_handle = incoming_documents.execution_handle
    for field_name in ("execution_id", "pipeline_id", "mode"):
        if getattr(old_handle, field_name) != getattr(new_handle, field_name):
            raise RuntimeMetadataIdentityConflictError(
                f"native JARVIS execution changed {field_name}"
            )
    for field_name in ("scheduler_provider", "scheduler_native_id", "cluster"):
        old_value = getattr(old_handle, field_name)
        new_value = getattr(new_handle, field_name)
        if old_value is not None and new_value != old_value:
            raise RuntimeMetadataIdentityConflictError(
                f"native JARVIS execution changed assigned {field_name}"
            )

    old_record = current_documents.execution_record
    new_record = incoming_documents.execution_record
    if old_record.created_at != new_record.created_at:
        raise RuntimeMetadataIdentityConflictError(
            "native JARVIS execution changed its creation timestamp"
        )
    old_updated = datetime.fromisoformat(old_record.updated_at.replace("Z", "+00:00"))
    new_updated = datetime.fromisoformat(new_record.updated_at.replace("Z", "+00:00"))
    if new_updated < old_updated:
        raise RuntimeMetadataIdentityConflictError(
            "native JARVIS execution update timestamp regressed"
        )
    if old_record.submitted and not new_record.submitted:
        raise RuntimeMetadataIdentityConflictError(
            "native JARVIS execution submission flag regressed"
        )
    if old_record.state != new_record.state:
        if new_record.state not in _JARVIS_REACHABLE_STATES[old_record.state]:
            raise RuntimeMetadataIdentityConflictError(
                "native JARVIS execution lifecycle regressed"
            )
        if (
            old_record.state == "scripted"
            and new_record.state != "failed"
            and (
                new_handle.mode != "scheduler"
                or new_handle.scheduler_native_id is None
                or not new_record.submitted
            )
        ):
            raise RuntimeMetadataIdentityConflictError(
                "native JARVIS scripted activation lacked scheduler identity"
            )
    elif old_record.terminal is not new_record.terminal:
        raise RuntimeMetadataIdentityConflictError(
            "native JARVIS terminal flag changed without a lifecycle transition"
        )
    if old_record.return_code is not None and new_record.return_code != old_record.return_code:
        raise RuntimeMetadataIdentityConflictError("native JARVIS execution return code changed")
    if old_record.error is not None and new_record.error != old_record.error:
        raise RuntimeMetadataIdentityConflictError("native JARVIS execution error changed")
    _validate_native_progress_transition(
        current_documents.progress,
        incoming_documents.progress,
    )


def _native_documents_from_runtime_metadata(
    metadata: JarvisRuntimeMetadata,
) -> JarvisNativeExecutionDocuments | None:
    raw_documents = metadata.details.get("native_execution")
    if not isinstance(raw_documents, dict):
        return None
    try:
        return JarvisNativeExecutionDocuments.model_validate(raw_documents)
    except ValueError as exc:
        raise RuntimeMetadataIdentityConflictError(
            f"stored native JARVIS execution documents were invalid: {exc}"
        ) from exc


def _validate_native_progress_transition(
    current: JarvisExecutionProgressDocument,
    incoming: JarvisExecutionProgressDocument,
) -> None:
    """Reject package disappearance, count regression, or rewritten progress events."""
    incoming_packages = {package.package_id: package for package in incoming.packages}
    for current_package in current.packages:
        incoming_package = incoming_packages.get(current_package.package_id)
        if incoming_package is None:
            raise RuntimeMetadataIdentityConflictError(
                "native JARVIS progress dropped a package identity"
            )
        if incoming_package.package_name != current_package.package_name:
            raise RuntimeMetadataIdentityConflictError(
                "native JARVIS progress changed a package name"
            )
        if incoming_package.event_count < current_package.event_count:
            raise RuntimeMetadataIdentityConflictError(
                "native JARVIS progress event count regressed"
            )
        old_event = current_package.latest
        new_event = incoming_package.latest
        if old_event is None:
            continue
        if new_event is None or new_event.sequence < old_event.sequence:
            raise RuntimeMetadataIdentityConflictError(
                "native JARVIS progress event sequence regressed"
            )
        if new_event.sequence == old_event.sequence:
            if incoming_package.event_count != current_package.event_count or new_event.model_dump(
                mode="json"
            ) != old_event.model_dump(mode="json"):
                raise RuntimeMetadataIdentityConflictError(
                    "native JARVIS progress rewrote an existing event"
                )
        elif incoming_package.event_count == current_package.event_count:
            raise RuntimeMetadataIdentityConflictError(
                "native JARVIS progress changed an event without increasing its count"
            )


def _authoritative_field_source(
    metadata: JarvisRuntimeMetadata,
    field_name: str,
) -> RuntimeMetadataSource | None:
    """Return authoritative provenance for one populated metadata field."""
    if getattr(metadata, field_name) is None:
        return None
    source = metadata.field_sources.get(field_name, metadata.source)
    return source if source in _AUTHORITATIVE_RUNTIME_SOURCES else None


def _same_runtime_identity(field_name: str, current: object, incoming: object) -> bool:
    """Compare pinned identity fields using provider-name normalization only."""
    if (
        field_name == "scheduler_provider"
        and isinstance(current, str)
        and isinstance(incoming, str)
    ):
        return current.strip().lower().replace("_", "-") == incoming.strip().lower().replace(
            "_", "-"
        )
    return current == incoming


def _source_priority(source: RuntimeMetadataSource) -> int:
    return {
        RuntimeMetadataSource.LEGACY_STDOUT: 0,
        RuntimeMetadataSource.UNTRUSTED_COMPATIBILITY: 0,
        RuntimeMetadataSource.JARVIS_SIDECAR: 10,
        RuntimeMetadataSource.RELAY_RECONCILIATION: 15,
        RuntimeMetadataSource.JARVIS_MCP: 20,
    }[source]


_AUTHORITATIVE_RUNTIME_SOURCES = frozenset(
    {
        RuntimeMetadataSource.JARVIS_MCP,
        RuntimeMetadataSource.JARVIS_SIDECAR,
        RuntimeMetadataSource.RELAY_RECONCILIATION,
    }
)


def _trusted_producer_runtime_contract(
    runtime: dict[str, Any],
    metadata: JarvisRuntimeMetadata,
) -> tuple[bool, str]:
    """Validate the producer contract required for scheduler ownership."""
    if runtime.get("schema_version") != JARVIS_RUNTIME_METADATA_SCHEMA:
        return False, f"producer schema must be {JARVIS_RUNTIME_METADATA_SCHEMA}"
    if metadata.scheduler_job_id is None:
        return True, "producer schema matched and no scheduler identity was claimed"
    details = _mapping(runtime.get("details"))
    submission = _mapping(details.get("scheduler_submission")) if details else None
    if submission is None:
        return False, "scheduler identity omitted scheduler_submission proof"
    if submission.get("schema_version") != JARVIS_SCHEDULER_SUBMISSION_SCHEMA:
        return False, f"scheduler submission schema must be {JARVIS_SCHEDULER_SUBMISSION_SCHEMA}"
    if metadata.scheduler_provider is None or submission.get("provider") != (
        metadata.scheduler_provider
    ):
        return False, "scheduler submission provider did not match runtime metadata"
    if submission.get("scheduler_job_id") != metadata.scheduler_job_id:
        return False, "scheduler submission job id did not match runtime metadata"
    if submission.get("identity_source") != "scheduler_submit_api":
        return False, "scheduler submission identity source was not authoritative"
    if submission.get("submitted") is not True:
        return False, "scheduler submission did not confirm submission"
    return True, "producer and scheduler submission contracts matched"


def _has_runtime_identity(metadata: JarvisRuntimeMetadata) -> bool:
    return any(
        (
            metadata.execution_id,
            metadata.pipeline_id,
            metadata.scheduler_job_id,
            metadata.script_path,
            metadata.hostfile_path,
            metadata.output_path,
            metadata.error_path,
            metadata.allocated_nodes,
            metadata.packages,
            metadata.terminal.state,
            metadata.terminal.returncode is not None,
        )
    )
