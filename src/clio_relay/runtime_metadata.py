"""Structured runtime metadata emitted by JARVIS and remote MCP servers.

The relay stores this normalized contract without assuming a scheduler or an
application.  Producers may return the fields directly from an MCP tool, wrap
them in ``runtime_metadata``, or append authenticated observations to the
runtime sidecar advertised by the worker.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, model_validator

from clio_relay.models import utc_now

RUNTIME_METADATA_SCHEMA = "clio-relay.jarvis-runtime.v1"
JARVIS_RUNTIME_METADATA_SCHEMA = "jarvis.runtime.v1"
JARVIS_SCHEDULER_SUBMISSION_SCHEMA = "jarvis.scheduler.submission.v1"
JARVIS_EXECUTION_HANDLE_SCHEMA = "jarvis.execution.handle.v1"
JARVIS_EXECUTION_RECORD_SCHEMA = "jarvis.execution.record.v1"
JARVIS_EXECUTION_PROGRESS_SCHEMA = "jarvis.execution.progress.v1"
JARVIS_PROGRESS_EVENT_SCHEMA = "jarvis.progress.v1"
RUNTIME_SIDECAR_RECORD_SCHEMA = "clio-relay.runtime-sidecar-record.v1"

_JARVIS_EXECUTION_STATES = {
    "preparing",
    "scripted",
    "submitting",
    "submitted",
    "running",
    "completed",
    "failed",
    "canceled",
    "unknown",
}
_JARVIS_TERMINAL_STATES = {"scripted", "completed", "failed", "canceled"}
_JARVIS_PROGRESS_STATES = {
    "pending",
    "starting",
    "running",
    "ready",
    "completed",
    "failed",
    "canceled",
}
_WINDOWS_RESERVED_COMPONENTS = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "CLOCK$",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}
_JARVIS_REACHABLE_STATES: dict[str, set[str]] = {
    "preparing": _JARVIS_EXECUTION_STATES - {"preparing"},
    "scripted": {"running", "completed", "failed", "canceled", "unknown"},
    "submitting": {"submitted", "running", "completed", "failed", "canceled", "unknown"},
    "submitted": {"running", "completed", "failed", "canceled", "unknown"},
    "running": {"completed", "failed", "canceled", "unknown"},
    "completed": set(),
    "failed": set(),
    "canceled": set(),
    "unknown": {"submitted", "running", "completed", "failed", "canceled"},
}


class RuntimeMetadataIdentityConflictError(ValueError):
    """Raised when authoritative runtime metadata changes a pinned execution identity."""


class RuntimeMetadataSource(StrEnum):
    """Trust and compatibility source for a runtime observation."""

    JARVIS_MCP = "jarvis_mcp"
    JARVIS_SIDECAR = "jarvis_sidecar"
    RELAY_RECONCILIATION = "relay_reconciliation"
    UNTRUSTED_COMPATIBILITY = "untrusted_compatibility"
    LEGACY_STDOUT = "legacy_stdout"


class PackageProvenance(BaseModel):
    """Package identity captured by the execution owner."""

    model_config = ConfigDict(extra="forbid")

    name: str
    version: str | None = None
    package_type: str | None = None
    package_id: str | None = None
    source: str | None = None
    path: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class TerminalRuntimeMetadata(BaseModel):
    """Execution state reported by the runtime owner."""

    model_config = ConfigDict(extra="forbid")

    state: str | None = None
    terminal: bool | None = None
    returncode: int | None = None
    reason: str | None = None
    started_at: str | None = None
    finished_at: str | None = None


class JarvisExecutionHandleDocument(BaseModel):
    """Exact stable execution handle returned by JARVIS-CD."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["jarvis.execution.handle.v1"]
    execution_id: str
    pipeline_id: str
    mode: Literal["direct", "scheduler"]
    scheduler_provider: str | None
    scheduler_native_id: str | None
    cluster: str | None

    @model_validator(mode="after")
    def validate_identity(self) -> JarvisExecutionHandleDocument:
        """Require bounded identities and coherent scheduler fields."""
        _validate_native_identity(self.execution_id, "execution_id")
        _validate_native_identity(self.pipeline_id, "pipeline_id")
        for field_name in ("scheduler_provider", "scheduler_native_id", "cluster"):
            value = getattr(self, field_name)
            if value is not None:
                _validate_native_text(value, field_name)
        if self.mode == "direct" and any(
            value is not None
            for value in (self.scheduler_provider, self.scheduler_native_id, self.cluster)
        ):
            raise ValueError("direct JARVIS execution cannot claim scheduler identity")
        if self.mode == "scheduler" and self.scheduler_provider is None:
            raise ValueError("scheduler JARVIS execution requires scheduler_provider")
        if self.scheduler_provider == "slurm":
            if self.scheduler_native_id is not None and (
                len(self.scheduler_native_id) > 64
                or not self.scheduler_native_id.isascii()
                or not self.scheduler_native_id.isdigit()
            ):
                raise ValueError("SLURM JARVIS execution requires a numeric native identity")
            if self.cluster is not None and (
                len(self.cluster) > 255
                or any(
                    not (character.isascii() and (character.isalnum() or character in "._-"))
                    for character in self.cluster
                )
            ):
                raise ValueError("SLURM JARVIS execution cluster was invalid")
        return self


class JarvisExecutionRecordDocument(BaseModel):
    """Exact durable execution record returned by JARVIS-CD."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["jarvis.execution.record.v1"]
    execution_id: str
    pipeline_id: str
    pipeline_name: str
    mode: Literal["direct", "scheduler"]
    scheduler_provider: str | None
    scheduler_native_id: str | None
    cluster: str | None
    state: str
    submitted: bool
    terminal: bool
    created_at: str
    updated_at: str
    return_code: int | None
    error: str | None
    metadata: dict[str, Any]

    @model_validator(mode="after")
    def validate_record(self) -> JarvisExecutionRecordDocument:
        """Require a coherent lifecycle and scheduler ownership document."""
        JarvisExecutionHandleDocument(
            schema_version=JARVIS_EXECUTION_HANDLE_SCHEMA,
            execution_id=self.execution_id,
            pipeline_id=self.pipeline_id,
            mode=self.mode,
            scheduler_provider=self.scheduler_provider,
            scheduler_native_id=self.scheduler_native_id,
            cluster=self.cluster,
        )
        if self.pipeline_name != self.pipeline_id:
            raise ValueError("JARVIS execution record pipeline identity did not match")
        if self.state not in _JARVIS_EXECUTION_STATES:
            raise ValueError(f"unsupported JARVIS execution state: {self.state}")
        if self.terminal and self.state not in _JARVIS_TERMINAL_STATES:
            raise ValueError("terminal JARVIS record has a nonterminal state")
        if self.state in {"completed", "failed", "canceled"} and not self.terminal:
            raise ValueError("terminal JARVIS state must set terminal=true")
        if self.state == "completed" and self.return_code != 0:
            raise ValueError("completed JARVIS record requires return_code=0")
        if self.state == "failed" and (self.return_code is None or self.return_code == 0):
            raise ValueError("failed JARVIS record requires a nonzero return_code")
        _validate_native_timestamp(self.created_at, "created_at")
        _validate_native_timestamp(self.updated_at, "updated_at")
        if self.error is not None:
            _validate_native_text(self.error, "error", maximum=16_384, allow_newlines=True)
        _validate_native_json(self.metadata, "execution record metadata", maximum=48_000)
        _validate_native_submission(self)
        return self


class JarvisProgressEventDocument(BaseModel):
    """Exact application-independent JARVIS package progress event."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["jarvis.progress.v1"]
    package_name: str
    package_id: str
    execution_id: str
    label: str
    state: str
    current: float | int | None = None
    total: float | int | None = None
    unit: str | None = None
    message: str | None = None
    sequence: int
    observed_at_epoch: float | int
    determinate: bool
    metadata: dict[str, Any]

    @model_validator(mode="after")
    def validate_progress(self) -> JarvisProgressEventDocument:
        """Reject fabricated, non-finite, or incoherent progress values."""
        for field_name in ("package_name", "package_id", "execution_id", "label"):
            _validate_native_text(getattr(self, field_name), field_name, maximum=256)
        if self.state not in _JARVIS_PROGRESS_STATES:
            raise ValueError(f"unsupported JARVIS progress state: {self.state}")
        if self.sequence < 0:
            raise ValueError("JARVIS progress sequence cannot be negative")
        observed = float(self.observed_at_epoch)
        if not math.isfinite(observed) or observed < 0:
            raise ValueError("JARVIS progress observed_at_epoch must be finite and nonnegative")
        current = None if self.current is None else float(self.current)
        total = None if self.total is None else float(self.total)
        if current is not None and (not math.isfinite(current) or current < 0):
            raise ValueError("JARVIS progress current must be finite and nonnegative")
        if total is not None:
            if not math.isfinite(total) or total <= 0:
                raise ValueError("JARVIS progress total must be finite and positive")
            if current is None or current > total:
                raise ValueError("determinate JARVIS progress requires current within total")
        if self.determinate is not (current is not None and total is not None):
            raise ValueError("JARVIS progress determinate flag did not match current and total")
        if self.unit is not None:
            _validate_native_text(self.unit, "unit", maximum=256)
        if self.message is not None:
            _validate_native_text(self.message, "message")
        _validate_native_json(self.metadata, "progress metadata", maximum=48_000)
        return self


class JarvisPackageProgressSnapshotDocument(BaseModel):
    """Latest JARVIS progress event for one package alias."""

    model_config = ConfigDict(extra="forbid", strict=True)

    package_id: str
    package_name: str
    event_count: int
    latest: JarvisProgressEventDocument | None

    @model_validator(mode="after")
    def validate_package_snapshot(self) -> JarvisPackageProgressSnapshotDocument:
        """Bind the latest event to its package and count."""
        _validate_native_text(self.package_id, "package_id", maximum=256)
        _validate_native_text(self.package_name, "package_name", maximum=256)
        if self.event_count < 0:
            raise ValueError("JARVIS package progress event_count cannot be negative")
        if (self.event_count == 0) is not (self.latest is None):
            raise ValueError("JARVIS package progress event_count did not match latest event")
        if self.latest is not None and (
            self.latest.package_id != self.package_id
            or self.latest.package_name != self.package_name
        ):
            raise ValueError("JARVIS package progress identity did not match latest event")
        return self


class JarvisExecutionProgressDocument(BaseModel):
    """Exact queryable progress snapshot returned by JARVIS-CD."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["jarvis.execution.progress.v1"]
    execution_id: str
    pipeline_id: str
    execution_state: str
    terminal: bool
    packages: list[JarvisPackageProgressSnapshotDocument]

    @model_validator(mode="after")
    def validate_snapshot(self) -> JarvisExecutionProgressDocument:
        """Require unique packages and one immutable execution identity."""
        _validate_native_identity(self.execution_id, "execution_id")
        _validate_native_identity(self.pipeline_id, "pipeline_id")
        if self.execution_state not in _JARVIS_EXECUTION_STATES:
            raise ValueError(f"unsupported JARVIS execution state: {self.execution_state}")
        if self.terminal and self.execution_state not in _JARVIS_TERMINAL_STATES:
            raise ValueError("terminal JARVIS progress has a nonterminal execution state")
        if self.execution_state in {"completed", "failed", "canceled"} and not self.terminal:
            raise ValueError("terminal JARVIS progress state must set terminal=true")
        package_ids: set[str] = set()
        for package in self.packages:
            if package.package_id in package_ids:
                raise ValueError("JARVIS execution progress repeated a package_id")
            package_ids.add(package.package_id)
            if package.latest is not None and package.latest.execution_id != self.execution_id:
                raise ValueError("JARVIS progress event execution identity did not match snapshot")
        return self


class JarvisNativeExecutionDocuments(BaseModel):
    """Validated, mutually bound JARVIS handle, record, and progress snapshot."""

    model_config = ConfigDict(extra="forbid", strict=True)

    execution_handle: JarvisExecutionHandleDocument
    execution_record: JarvisExecutionRecordDocument
    progress: JarvisExecutionProgressDocument

    @model_validator(mode="after")
    def validate_documents(self) -> JarvisNativeExecutionDocuments:
        """Reject identity or lifecycle drift across native documents."""
        handle = self.execution_handle
        record = self.execution_record
        progress = self.progress
        if (
            handle.execution_id != record.execution_id
            or handle.pipeline_id != record.pipeline_id
            or handle.mode != record.mode
            or handle.scheduler_provider != record.scheduler_provider
            or handle.scheduler_native_id != record.scheduler_native_id
            or handle.cluster != record.cluster
        ):
            raise ValueError("JARVIS execution handle and record identities did not match")
        if (
            progress.execution_id != record.execution_id
            or progress.pipeline_id != record.pipeline_id
            or progress.execution_state != record.state
            or progress.terminal is not record.terminal
        ):
            raise ValueError("JARVIS execution record and progress lifecycle did not match")
        return self


class _JarvisRuntimeTerminalProjection(BaseModel):
    """Exact lifecycle projection emitted beside native JARVIS documents."""

    model_config = ConfigDict(extra="forbid", strict=True)

    state: str
    terminal: bool
    returncode: int | None
    reason: str | None
    started_at: str
    finished_at: str | None

    @model_validator(mode="after")
    def validate_projection(self) -> _JarvisRuntimeTerminalProjection:
        """Require bounded lifecycle values before comparing them with the record."""
        if self.state not in _JARVIS_EXECUTION_STATES:
            raise ValueError(f"unsupported JARVIS runtime state: {self.state}")
        _validate_native_timestamp(self.started_at, "runtime started_at")
        if self.finished_at is not None:
            _validate_native_timestamp(self.finished_at, "runtime finished_at")
        if self.reason is not None:
            _validate_native_text(
                self.reason,
                "runtime reason",
                maximum=16_384,
                allow_newlines=True,
            )
        return self


class _JarvisRuntimeProjectionDocument(BaseModel):
    """Structured clio-kit runtime projection paired with native JARVIS documents."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["jarvis.runtime.v1"]
    source: Literal["jarvis_mcp"]
    execution_id: str
    pipeline_id: str
    mode: Literal["direct", "scheduler"]
    scheduler_provider: str | None
    scheduler_native_id: str | None
    cluster: str | None
    scheduler_type: str | None
    scheduler_job_id: str | None
    scheduler_phase: str | None
    script_path: str | None
    hostfile_path: str | None
    output_path: str | None
    error_path: str | None
    package_provenance: list[dict[str, Any]] = Field(max_length=4_096)
    terminal: _JarvisRuntimeTerminalProjection
    details: dict[str, Any]

    @model_validator(mode="after")
    def validate_projection(self) -> _JarvisRuntimeProjectionDocument:
        """Require a bounded, portable producer projection before it is merged."""
        _validate_native_identity(self.execution_id, "runtime execution_id")
        _validate_native_identity(self.pipeline_id, "runtime pipeline_id")
        for field_name in (
            "scheduler_provider",
            "scheduler_native_id",
            "cluster",
            "scheduler_type",
            "scheduler_job_id",
            "scheduler_phase",
        ):
            value = getattr(self, field_name)
            if value is not None:
                _validate_native_text(value, f"runtime {field_name}")
        for field_name in (
            "script_path",
            "hostfile_path",
            "output_path",
            "error_path",
        ):
            value = getattr(self, field_name)
            if value is not None:
                _validate_native_text(value, f"runtime {field_name}", maximum=16_384)
        _validate_native_json(
            self.package_provenance,
            "runtime package provenance",
            maximum=1_048_576,
        )
        _validate_native_json(self.details, "runtime details", maximum=2_097_152)
        return self


class JarvisRuntimeMetadata(BaseModel):
    """Normalized, durable runtime metadata for one JARVIS-owned execution."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = RUNTIME_METADATA_SCHEMA
    source: RuntimeMetadataSource
    observed_at: datetime = Field(default_factory=utc_now)
    execution_id: str | None = None
    pipeline_id: str | None = None
    scheduler_provider: str | None = None
    scheduler_type: str | None = None
    scheduler_job_id: str | None = None
    scheduler_phase: str | None = None
    script_path: str | None = None
    hostfile_path: str | None = None
    output_path: str | None = None
    error_path: str | None = None
    allocated_nodes: list[str] = Field(default_factory=list)
    packages: list[PackageProvenance] = Field(default_factory=lambda: list[PackageProvenance]())
    terminal: TerminalRuntimeMetadata = Field(default_factory=TerminalRuntimeMetadata)
    field_sources: dict[str, RuntimeMetadataSource] = Field(default_factory=dict)
    details: dict[str, Any] = Field(default_factory=dict)


def native_execution_documents(
    payload: dict[str, Any],
) -> JarvisNativeExecutionDocuments | None:
    """Parse an exact native JARVIS result envelope when one is present.

    A producer that emits any native document must emit all three. Partial
    envelopes fail closed instead of falling back to the legacy synthesized
    runtime contract.
    """
    keys = {"execution_handle", "execution_record", "progress"}
    present = keys & set(payload)
    if not present:
        return None
    if present != keys:
        missing = sorted(keys - present)
        raise ValueError(f"native JARVIS result omitted documents: {missing}")
    return JarvisNativeExecutionDocuments.model_validate(
        {key: payload[key] for key in sorted(keys)}
    )


def runtime_metadata_from_native_documents(
    documents: JarvisNativeExecutionDocuments,
    *,
    source: RuntimeMetadataSource,
) -> JarvisRuntimeMetadata:
    """Normalize exact JARVIS-owned documents without scraping process output."""
    handle = documents.execution_handle
    record = documents.execution_record
    progress = documents.progress
    submission = _mapping(record.metadata.get("submission"))
    packages = [
        PackageProvenance(
            name=package.package_name,
            package_type=package.package_name,
            package_id=package.package_id,
            metadata={"progress_event_count": package.event_count},
        )
        for package in progress.packages
    ]
    metadata = JarvisRuntimeMetadata(
        source=source,
        execution_id=record.execution_id,
        pipeline_id=record.pipeline_id,
        scheduler_provider=record.scheduler_provider,
        scheduler_type=record.scheduler_provider,
        scheduler_job_id=record.scheduler_native_id,
        scheduler_phase=_native_scheduler_phase(record),
        script_path=_first_str(record.metadata, "script_path")
        or (_first_str(submission, "script_path") if submission is not None else None),
        hostfile_path=(_first_str(submission, "hostfile_path") if submission is not None else None),
        output_path=(
            _first_str(submission, "output_path", "output") if submission is not None else None
        ),
        error_path=(
            _first_str(submission, "error_path", "error") if submission is not None else None
        ),
        packages=packages,
        terminal=TerminalRuntimeMetadata(
            state=record.state,
            terminal=record.terminal,
            returncode=record.return_code,
            reason=record.error,
            started_at=record.created_at,
            finished_at=record.updated_at if record.terminal else None,
        ),
        details={
            "execution_mode": handle.mode,
            "submitted": record.submitted,
            "scheduler_cluster": record.cluster,
            "native_execution": documents.model_dump(mode="json"),
            "producer_contract": {
                "requested_source": source.value,
                "contract_kind": "native_execution",
                "producer_schema_version": record.schema_version,
                "handle_schema_version": handle.schema_version,
                "progress_schema_version": progress.schema_version,
                "trusted": True,
                "reason": "exact native JARVIS execution documents matched",
            },
        },
    )
    return metadata.model_copy(update={"field_sources": _field_sources(metadata, source)})


def _native_scheduler_phase(record: JarvisExecutionRecordDocument) -> str | None:
    """Return lifecycle state only when a scheduler owns a submitted native job."""

    if (
        record.mode != "scheduler"
        or record.submitted is not True
        or record.scheduler_provider is None
        or record.scheduler_native_id is None
    ):
        return None
    return record.state


def _merge_native_runtime_projection(
    metadata: JarvisRuntimeMetadata,
    documents: JarvisNativeExecutionDocuments,
    value: object,
) -> JarvisRuntimeMetadata:
    """Validate and merge non-authoritative detail from clio-kit's runtime projection."""
    if not isinstance(value, dict):
        raise ValueError("native JARVIS result omitted structured runtime_metadata")
    projection = _JarvisRuntimeProjectionDocument.model_validate(value)
    handle = documents.execution_handle
    record = documents.execution_record
    authoritative = {
        "execution_id": record.execution_id,
        "pipeline_id": record.pipeline_id,
        "mode": record.mode,
        "scheduler_provider": record.scheduler_provider,
        "scheduler_native_id": record.scheduler_native_id,
        "cluster": record.cluster,
        "scheduler_type": record.scheduler_provider,
        "scheduler_job_id": record.scheduler_native_id,
        "scheduler_phase": _native_scheduler_phase(record),
    }
    for field_name, expected in authoritative.items():
        if getattr(projection, field_name) != expected:
            raise ValueError(
                "native JARVIS runtime projection "
                f"{field_name} did not match authoritative execution documents"
            )

    terminal = projection.terminal
    authoritative_terminal = {
        "state": record.state,
        "terminal": record.terminal,
        "returncode": record.return_code,
        "reason": record.error,
        "started_at": record.created_at,
        "finished_at": record.updated_at if record.terminal else None,
    }
    for field_name, expected in authoritative_terminal.items():
        if getattr(terminal, field_name) != expected:
            raise ValueError(
                "native JARVIS runtime projection terminal."
                f"{field_name} did not match authoritative execution documents"
            )

    for field_name in ("script_path", "hostfile_path"):
        if getattr(projection, field_name) != getattr(metadata, field_name):
            raise ValueError(
                "native JARVIS runtime projection "
                f"{field_name} did not match authoritative execution documents"
            )

    handle_document = handle.model_dump(mode="json")
    record_document = record.model_dump(mode="json")
    runtime_details = projection.details
    required_detail_documents = {
        "execution_handle": handle_document,
        "execution_record": record_document,
        "scheduler_submission": record.metadata.get("submission"),
    }
    for field_name, expected in required_detail_documents.items():
        if field_name not in runtime_details or runtime_details[field_name] != expected:
            raise ValueError(
                "native JARVIS runtime projection details."
                f"{field_name} did not match authoritative execution documents"
            )

    output_path = _enriched_native_path(
        metadata.output_path,
        projection.output_path,
        field_name="output_path",
    )
    error_path = _enriched_native_path(
        metadata.error_path,
        projection.error_path,
        field_name="error_path",
    )
    packages = _merge_native_package_provenance(
        metadata.packages,
        projection.package_provenance,
    )

    merged_details = dict(runtime_details)
    for field_name, value in metadata.details.items():
        if field_name in merged_details and merged_details[field_name] != value:
            raise ValueError(
                "native JARVIS runtime projection details."
                f"{field_name} attempted to override authoritative relay metadata"
            )
        merged_details[field_name] = value
    producer_contract = cast(dict[str, Any], merged_details["producer_contract"])
    merged_details["producer_contract"] = {
        **producer_contract,
        "runtime_projection_schema_version": projection.schema_version,
        "runtime_projection_merged": True,
    }
    merged = metadata.model_copy(
        update={
            "output_path": output_path,
            "error_path": error_path,
            "packages": packages,
            "details": merged_details,
        }
    )
    return merged.model_copy(
        update={"field_sources": _field_sources(merged, RuntimeMetadataSource.JARVIS_MCP)}
    )


def _enriched_native_path(
    authoritative: str | None,
    projected: str | None,
    *,
    field_name: str,
) -> str | None:
    """Use a richer producer path only when it does not conflict with native evidence."""
    if authoritative is not None and projected not in {None, authoritative}:
        raise ValueError(
            f"native JARVIS runtime projection {field_name} conflicted with native evidence"
        )
    return authoritative or projected


def _merge_native_package_provenance(
    native_packages: list[PackageProvenance],
    projected_items: list[dict[str, Any]],
) -> list[PackageProvenance]:
    """Enrich progress identities with JARVIS package provenance without identity drift."""
    projected_packages = _package_provenance(projected_items)
    if len(projected_packages) != len(projected_items):
        raise ValueError("native JARVIS runtime package provenance contained an invalid entry")
    native_by_id = {
        package.package_id: package for package in native_packages if package.package_id is not None
    }
    consumed_native_ids: set[str] = set()
    observed_projected_ids: set[str] = set()
    merged: list[PackageProvenance] = []
    for raw_item, projected in zip(projected_items, projected_packages, strict=True):
        if projected.package_id is not None:
            if projected.package_id in observed_projected_ids:
                raise ValueError("native JARVIS runtime package provenance repeated a package_id")
            observed_projected_ids.add(projected.package_id)
        native = (
            native_by_id.get(projected.package_id) if projected.package_id is not None else None
        )
        metadata = dict(projected.metadata)
        global_id = raw_item.get("global_id")
        if global_id is not None:
            if not isinstance(global_id, str):
                raise ValueError("native JARVIS runtime package global_id was invalid")
            _validate_native_text(global_id, "runtime package global_id", maximum=256)
            metadata["global_id"] = global_id
        if native is not None:
            if projected.name != native.name or projected.package_type not in {
                None,
                native.package_type,
                native.name,
            }:
                raise ValueError(
                    "native JARVIS runtime package provenance conflicted with progress identity"
                )
            consumed_native_ids.add(cast(str, native.package_id))
            metadata = {**metadata, **native.metadata}
            projected = projected.model_copy(
                update={
                    "name": native.name,
                    "package_type": projected.package_type or native.package_type,
                    "metadata": metadata,
                }
            )
        else:
            projected = projected.model_copy(update={"metadata": metadata})
        merged.append(projected)
    merged.extend(
        package
        for package in native_packages
        if package.package_id is None or package.package_id not in consumed_native_ids
    )
    return merged


def runtime_metadata_from_mcp_result_document(
    document: object,
) -> JarvisRuntimeMetadata | None:
    """Extract JARVIS runtime metadata from a persisted MCP-call result."""
    if not isinstance(document, dict):
        return None
    typed = cast(dict[str, Any], document)
    tool = _optional_str(typed.get("tool"))
    structured = _mapping(typed.get("structured_result"))
    if structured is None:
        structured = structured_mcp_result(_mapping(typed.get("protocol_result")))
    if structured is None:
        return None
    if tool is None or not _is_jarvis_run_tool(tool):
        return None
    native = native_execution_documents(structured)
    if native is not None:
        metadata = runtime_metadata_from_native_documents(
            native,
            source=RuntimeMetadataSource.JARVIS_MCP,
        )
        return _merge_native_runtime_projection(
            metadata,
            native,
            structured.get("runtime_metadata"),
        )
    metadata = normalize_runtime_metadata(structured, source=RuntimeMetadataSource.JARVIS_MCP)
    if metadata is None:
        return metadata
    metadata = metadata.model_copy(
        update={
            "details": {
                **metadata.details,
                "compatibility_contract": {
                    "kind": "legacy_runtime_metadata",
                    "preferred_contract": JARVIS_EXECUTION_RECORD_SCHEMA,
                },
            }
        }
    )
    if metadata.source is not RuntimeMetadataSource.JARVIS_MCP:
        return metadata
    return _normalize_synchronous_jarvis_completion(metadata, typed, structured)


def structured_mcp_result(protocol_result: dict[str, Any] | None) -> dict[str, Any] | None:
    """Decode a structured MCP result, preferring ``structuredContent``."""
    if protocol_result is None:
        return None
    for key in ("structuredContent", "structured_content"):
        structured = protocol_result.get(key)
        if isinstance(structured, dict):
            return cast(dict[str, Any], structured)
    content = protocol_result.get("content")
    if isinstance(content, list):
        for item in cast(list[object], content):
            if not isinstance(item, dict):
                continue
            block = cast(dict[str, object], item)
            if block.get("type") != "text" or not isinstance(block.get("text"), str):
                continue
            try:
                decoded = json.loads(cast(str, block["text"]))
            except json.JSONDecodeError:
                continue
            if isinstance(decoded, dict):
                return cast(dict[str, Any], decoded)
    if _looks_like_runtime_payload(protocol_result):
        return protocol_result
    return None


def normalize_runtime_metadata(
    payload: dict[str, Any],
    *,
    source: RuntimeMetadataSource,
) -> JarvisRuntimeMetadata | None:
    """Normalize common JARVIS/clio-kit runtime result shapes."""
    outer = payload
    envelope = _mapping(payload.get("runtime_metadata"))
    nested_runtime = _mapping(payload.get("runtime"))
    runtime = envelope if envelope is not None else nested_runtime or payload
    scheduler = _mapping(runtime.get("scheduler")) or _mapping(outer.get("scheduler")) or {}
    paths = _mapping(runtime.get("paths")) or _mapping(outer.get("paths")) or {}
    terminal_payload = _mapping(runtime.get("terminal")) or _mapping(outer.get("terminal")) or {}

    scheduler_provider = _first_str(
        runtime,
        "scheduler_provider",
        "provider",
    ) or _first_str(scheduler, "provider", "name", "scheduler")
    scheduler_type = _first_str(runtime, "scheduler_type") or _first_str(
        scheduler,
        "type",
        "kind",
        "name",
        "provider",
        "scheduler",
    )
    if not scheduler and isinstance(runtime.get("scheduler"), str):
        scheduler_provider = scheduler_provider or _optional_str(runtime.get("scheduler"))
        scheduler_type = scheduler_type or _optional_str(runtime.get("scheduler"))
    scheduler_job_id = _first_str(runtime, "scheduler_job_id") or _first_str(
        scheduler,
        "scheduler_job_id",
        "job_id",
        "id",
    )
    scheduler_phase = _first_str(runtime, "scheduler_phase", "queue_state") or _first_str(
        scheduler,
        "phase",
        "state",
        "status",
    )

    state = _first_str(terminal_payload, "state", "status", "terminal_state") or _first_str(
        runtime,
        "terminal_state",
        "status",
        "state",
    )
    terminal_flag = _optional_bool(terminal_payload.get("terminal"))
    if terminal_flag is None:
        terminal_flag = _optional_bool(runtime.get("terminal"))
    if terminal_flag is None and state is not None:
        terminal_flag = state.lower() in {
            "succeeded",
            "success",
            "completed",
            "failed",
            "canceled",
            "cancelled",
            "timed_out",
            "timeout",
        }

    packages = _package_provenance(
        runtime.get("package_provenance")
        or runtime.get("packages")
        or runtime.get("pkgs")
        or outer.get("package_provenance")
        or outer.get("packages")
    )
    allocated_nodes = _nodes(
        runtime.get("allocated_nodes")
        or scheduler.get("allocated_nodes")
        or scheduler.get("nodes")
        or runtime.get("service_host")
        or runtime.get("node")
    )
    metadata = JarvisRuntimeMetadata(
        source=source,
        execution_id=_first_str(runtime, "execution_id", "run_id"),
        pipeline_id=_first_str(runtime, "pipeline_id", "pipeline", "pipeline_name")
        or _first_str(outer, "pipeline_id", "pipeline", "pipeline_name"),
        scheduler_provider=scheduler_provider,
        scheduler_type=scheduler_type,
        scheduler_job_id=scheduler_job_id,
        scheduler_phase=scheduler_phase,
        script_path=_path_value(runtime, paths, "script_path", "script"),
        hostfile_path=_path_value(runtime, paths, "hostfile_path", "hostfile"),
        output_path=_path_value(
            runtime,
            paths,
            "output_path",
            "stdout_path",
            "output",
            "stdout",
        ),
        error_path=_path_value(
            runtime,
            paths,
            "error_path",
            "stderr_path",
            "error",
            "stderr",
        ),
        allocated_nodes=allocated_nodes,
        packages=packages,
        terminal=TerminalRuntimeMetadata(
            state=state,
            terminal=terminal_flag,
            returncode=_optional_int(terminal_payload.get("returncode", runtime.get("returncode"))),
            reason=_first_str(terminal_payload, "reason", "message", "error")
            or _first_str(runtime, "terminal_reason"),
            started_at=_first_str(terminal_payload, "started_at")
            or _first_str(runtime, "started_at"),
            finished_at=_first_str(terminal_payload, "finished_at", "ended_at")
            or _first_str(runtime, "finished_at", "ended_at"),
        ),
        details=_json_object(outer),
    )
    if not _has_runtime_identity(metadata):
        return None
    effective_source = source
    producer_contract = {
        "requested_source": source.value,
        "producer_schema_version": runtime.get("schema_version"),
        "trusted": False,
        "reason": "source does not claim JARVIS producer authority",
    }
    if source in _AUTHORITATIVE_RUNTIME_SOURCES:
        producer_trusted, producer_reason = _trusted_producer_runtime_contract(runtime, metadata)
        producer_contract.update(
            {
                "trusted": producer_trusted,
                "reason": producer_reason,
            }
        )
        if not producer_trusted:
            effective_source = RuntimeMetadataSource.UNTRUSTED_COMPATIBILITY
    details = {
        **metadata.details,
        "producer_contract": producer_contract,
    }
    return metadata.model_copy(
        update={
            "source": effective_source,
            "field_sources": _field_sources(metadata, effective_source),
            "details": details,
        }
    )


def runtime_metadata_from_sidecar_record(
    record: object,
    *,
    expected_key: str,
    expected_sequence: int,
) -> JarvisRuntimeMetadata:
    """Verify one ordered HMAC-authenticated JARVIS runtime sidecar record."""
    if not isinstance(record, dict):
        raise ValueError("runtime metadata sidecar record must be an object")
    typed = cast(dict[str, Any], record)
    if set(typed) != {
        "schema_version",
        "sequence",
        "runtime_metadata",
        "runtime_metadata_hmac",
    }:
        raise ValueError("runtime metadata sidecar record fields did not match")
    if typed.get("schema_version") != RUNTIME_SIDECAR_RECORD_SCHEMA:
        raise ValueError("runtime metadata sidecar record schema did not match")
    sequence = typed.get("sequence")
    if isinstance(sequence, bool) or sequence != expected_sequence:
        raise ValueError("runtime metadata sidecar sequence did not match")
    payload = _mapping(typed.get("runtime_metadata"))
    if payload is None:
        raise ValueError("runtime metadata sidecar record omitted runtime metadata")
    observed_hmac = typed.get("runtime_metadata_hmac")
    if not isinstance(observed_hmac, str) or len(observed_hmac) != 64:
        raise ValueError("runtime metadata sidecar HMAC was invalid")
    expected_hmac = _runtime_sidecar_hmac(
        payload,
        key=expected_key,
        sequence=expected_sequence,
    )
    if not hmac.compare_digest(observed_hmac, expected_hmac):
        raise ValueError("runtime metadata sidecar HMAC did not match")
    native = native_execution_documents(payload)
    metadata = (
        runtime_metadata_from_native_documents(
            native,
            source=RuntimeMetadataSource.JARVIS_SIDECAR,
        )
        if native is not None
        else normalize_runtime_metadata(payload, source=RuntimeMetadataSource.JARVIS_SIDECAR)
    )
    if metadata is None:
        raise ValueError("runtime metadata sidecar did not contain runtime fields")
    return metadata


def runtime_sidecar_record(
    runtime_metadata: dict[str, Any],
    *,
    key: str,
    sequence: int,
) -> dict[str, object]:
    """Build one canonical ordered sidecar record without disclosing its HMAC key."""
    if not key:
        raise ValueError("runtime metadata sidecar key must not be empty")
    if sequence < 1:
        raise ValueError("runtime metadata sidecar sequence must be positive")
    return {
        "schema_version": RUNTIME_SIDECAR_RECORD_SCHEMA,
        "sequence": sequence,
        "runtime_metadata": runtime_metadata,
        "runtime_metadata_hmac": _runtime_sidecar_hmac(
            runtime_metadata,
            key=key,
            sequence=sequence,
        ),
    }


def _runtime_sidecar_hmac(
    runtime_metadata: dict[str, Any],
    *,
    key: str,
    sequence: int,
) -> str:
    signed = {
        "schema_version": RUNTIME_SIDECAR_RECORD_SCHEMA,
        "sequence": sequence,
        "runtime_metadata": runtime_metadata,
    }
    try:
        canonical = json.dumps(
            signed,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("runtime metadata sidecar payload was not canonical JSON") from exc
    return hmac.new(key.encode("utf-8"), canonical, hashlib.sha256).hexdigest()


def legacy_scheduler_runtime_metadata(
    *,
    scheduler_job_id: str,
    scheduler_provider: str,
) -> JarvisRuntimeMetadata:
    """Build an explicitly untrusted compatibility observation from log text."""
    return JarvisRuntimeMetadata(
        source=RuntimeMetadataSource.LEGACY_STDOUT,
        scheduler_provider=scheduler_provider,
        scheduler_type=scheduler_provider,
        scheduler_job_id=scheduler_job_id,
        field_sources={
            "scheduler_provider": RuntimeMetadataSource.LEGACY_STDOUT,
            "scheduler_type": RuntimeMetadataSource.LEGACY_STDOUT,
            "scheduler_job_id": RuntimeMetadataSource.LEGACY_STDOUT,
        },
        details={"fallback_reason": "structured JARVIS runtime metadata was not yet available"},
    )


def _normalize_synchronous_jarvis_completion(
    metadata: JarvisRuntimeMetadata,
    document: dict[str, Any],
    structured: dict[str, Any],
) -> JarvisRuntimeMetadata:
    """Record completion implied by a successful synchronous JARVIS MCP return."""
    if metadata.terminal.terminal is True:
        return metadata
    if (
        document.get("returncode") != 0
        or document.get("timed_out") is True
        or document.get("protocol_error") is not None
    ):
        return metadata
    mode = _optional_str(structured.get("mode"))
    arguments = _mapping(document.get("arguments")) or {}
    waited = _optional_bool(structured.get("wait")) is True or arguments.get("wait") is True
    synchronous = mode == "direct" or (mode == "scheduler" and waited)
    if not synchronous:
        return metadata
    raw_status = _optional_str(structured.get("status"))
    terminal = metadata.terminal.model_copy(
        update={
            "state": "completed",
            "terminal": True,
            "returncode": 0,
            "finished_at": _timestamp_string(document.get("finished_at")),
        }
    )
    field_sources = {
        **metadata.field_sources,
        "terminal.state": RuntimeMetadataSource.JARVIS_MCP,
        "terminal.terminal": RuntimeMetadataSource.JARVIS_MCP,
        "terminal.returncode": RuntimeMetadataSource.JARVIS_MCP,
    }
    if terminal.finished_at is not None:
        field_sources["terminal.finished_at"] = RuntimeMetadataSource.JARVIS_MCP
    details = {
        **metadata.details,
        "completion_normalization": {
            "basis": "successful synchronous jarvis_run MCP return",
            "mode": mode,
            "wait": waited,
            "reported_status": raw_status,
        },
    }
    return metadata.model_copy(
        update={"terminal": terminal, "field_sources": field_sources, "details": details}
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


def _field_sources(
    metadata: JarvisRuntimeMetadata,
    source: RuntimeMetadataSource,
) -> dict[str, RuntimeMetadataSource]:
    sources: dict[str, RuntimeMetadataSource] = {}
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
        if getattr(metadata, field_name) is not None:
            sources[field_name] = source
    if metadata.allocated_nodes:
        sources["allocated_nodes"] = source
    if metadata.packages:
        sources["packages"] = source
    for field_name in (
        "state",
        "terminal",
        "returncode",
        "reason",
        "started_at",
        "finished_at",
    ):
        if getattr(metadata.terminal, field_name) is not None:
            sources[f"terminal.{field_name}"] = source
    return sources


def _package_provenance(value: object) -> list[PackageProvenance]:
    if isinstance(value, dict):
        items: list[object] = [value]
    elif isinstance(value, list):
        items = cast(list[object], value)
    else:
        return []
    packages: list[PackageProvenance] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        typed = cast(dict[str, Any], item)
        name = _first_str(typed, "package_name", "name", "pkg_type", "pkg_name")
        if name is None:
            continue
        known = {
            "package_name",
            "name",
            "pkg_name",
            "version",
            "package_version",
            "pkg_version",
            "package_type",
            "pkg_type",
            "type",
            "package_id",
            "pkg_id",
            "global_id",
            "source",
            "path",
            "package_path",
            "config_path",
        }
        packages.append(
            PackageProvenance(
                name=name,
                version=_first_str(typed, "package_version", "version", "pkg_version"),
                package_type=_first_str(typed, "package_type", "pkg_type", "type"),
                package_id=_first_str(typed, "package_id", "pkg_id", "global_id"),
                source=_first_str(typed, "source"),
                path=_first_str(typed, "package_path", "path", "config_path"),
                metadata={key: value for key, value in typed.items() if key not in known},
            )
        )
    return packages


def _nodes(value: object) -> list[str]:
    if isinstance(value, str) and value:
        return [value]
    if isinstance(value, list):
        return [item for item in cast(list[object], value) if isinstance(item, str) and item]
    return []


def _path_value(
    runtime: dict[str, Any],
    paths: dict[str, Any],
    *keys: str,
) -> str | None:
    value = _first_str(runtime, *keys) or _first_str(paths, *keys)
    if value is not None:
        return value
    for key in keys:
        nested = _mapping(runtime.get(key)) or _mapping(paths.get(key))
        if nested is not None:
            nested_path = _first_str(nested, "path", "uri")
            if nested_path is not None:
                return nested_path
    return None


def _validate_native_identity(value: str, field_name: str) -> None:
    """Validate one portable JARVIS execution identity."""
    _validate_native_text(value, field_name, maximum=128)
    reserved_stem = value.split(".", 1)[0].upper()
    if (
        not value[0].isalnum()
        or value.endswith(".")
        or reserved_stem in _WINDOWS_RESERVED_COMPONENTS
        or any(
            not (character.isascii() and (character.isalnum() or character in "._-"))
            for character in value
        )
    ):
        raise ValueError(f"native JARVIS {field_name} must use portable ASCII identity characters")


def _validate_native_text(
    value: str,
    field_name: str,
    *,
    maximum: int = 4096,
    allow_newlines: bool = False,
) -> None:
    """Validate one bounded nonempty native producer string."""
    if not value or len(value.encode("utf-8")) > maximum:
        raise ValueError(f"native JARVIS {field_name} must be a bounded nonempty string")
    allowed_controls: set[str] = {"\n", "\r", "\t"} if allow_newlines else set()
    if any(
        (ord(character) < 32 and character not in allowed_controls) or ord(character) == 127
        for character in value
    ):
        raise ValueError(f"native JARVIS {field_name} contains control characters")


def _validate_native_timestamp(value: str, field_name: str) -> None:
    """Require a timezone-aware ISO-8601 producer timestamp."""
    _validate_native_text(value, field_name, maximum=64)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"native JARVIS {field_name} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"native JARVIS {field_name} must include a timezone")


def _validate_native_json(value: object, label: str, *, maximum: int) -> None:
    """Require finite, bounded JSON from a native producer."""
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ValueError(f"native JARVIS {label} must contain finite JSON") from exc
    if len(encoded) > maximum:
        raise ValueError(f"native JARVIS {label} exceeds its byte limit")


def _validate_native_submission(record: JarvisExecutionRecordDocument) -> None:
    """Validate any scheduler submission projection and bind claimed identity."""
    raw_submission = record.metadata.get("submission")
    if raw_submission is None:
        if record.scheduler_native_id is not None or record.submitted:
            raise ValueError("native JARVIS scheduler identity omitted submission proof")
        return
    submission = _mapping(raw_submission)
    if submission is None:
        raise ValueError("native JARVIS scheduler submission must be an object")
    if record.mode != "scheduler":
        raise ValueError("native JARVIS direct execution cannot carry scheduler submission proof")
    if submission.get("schema_version") != JARVIS_SCHEDULER_SUBMISSION_SCHEMA:
        raise ValueError("native JARVIS scheduler submission schema did not match")
    if submission.get("execution_id") != record.execution_id:
        raise ValueError("native JARVIS scheduler submission execution did not match")
    if _optional_str(submission.get("provider")) != record.scheduler_provider:
        raise ValueError("native JARVIS scheduler submission provider did not match")
    if _optional_str(submission.get("scheduler_job_id")) != record.scheduler_native_id:
        raise ValueError("native JARVIS scheduler submission identity did not match")
    if _optional_str(submission.get("scheduler_cluster")) != record.cluster:
        raise ValueError("native JARVIS scheduler submission cluster did not match")
    submitted = submission.get("submitted")
    if not isinstance(submitted, bool) or submitted is not record.submitted:
        raise ValueError("native JARVIS scheduler submission flag did not match")
    identity_source = submission.get("identity_source")
    if record.scheduler_native_id is not None and (
        identity_source != "scheduler_submit_api" or submitted is not True
    ):
        raise ValueError("native JARVIS scheduler identity source was not authoritative")
    if record.scheduler_native_id is None and identity_source is not None:
        raise ValueError("native JARVIS scheduler submission source claimed no native identity")
    for field_name in (
        "script_path",
        "hostfile_path",
        "pipeline_snapshot_path",
        "pipeline_input_path",
        "execution_root_path",
        "output_path",
        "error_path",
    ):
        value = submission.get(field_name)
        if value is not None:
            if not isinstance(value, str):
                raise ValueError(f"native JARVIS scheduler submission {field_name} was invalid")
            _validate_native_text(value, field_name, maximum=16_384)


def _looks_like_runtime_payload(value: dict[str, Any]) -> bool:
    return bool(
        {
            "runtime_metadata",
            "runtime",
            "pipeline_id",
            "scheduler",
            "scheduler_job_id",
            "script_path",
            "hostfile_path",
            "allocated_nodes",
            "package_provenance",
            "terminal",
            "execution_handle",
            "execution_record",
            "progress",
        }
        & set(value)
    )


def _is_jarvis_run_tool(tool: str) -> bool:
    normalized = tool.replace("-", "_").lower()
    return normalized == "jarvis_run" or normalized.endswith(".jarvis_run")


def _mapping(value: object) -> dict[str, Any] | None:
    return cast(dict[str, Any], value) if isinstance(value, dict) else None


def _first_str(value: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        candidate = _optional_str(value.get(key))
        if candidate is not None:
            return candidate
    return None


def _optional_str(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _optional_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _optional_int(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _timestamp_string(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        return datetime.fromtimestamp(value, tz=UTC).isoformat().replace("+00:00", "Z")
    except (OverflowError, OSError, ValueError):
        return None


def _json_object(value: dict[str, Any]) -> dict[str, Any]:
    """Round-trip producer metadata to guarantee durable JSON values."""
    try:
        decoded = json.loads(json.dumps(value, default=str))
    except (TypeError, ValueError):
        return {}
    return cast(dict[str, Any], decoded) if isinstance(decoded, dict) else {}
