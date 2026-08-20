"""The exact native JARVIS execution/progress document family, wire-locked.

Extracted from ``runtime_metadata.py`` (clio-relay split/runtime-metadata-w2):
these Pydantic models mirror JARVIS-CD's own producer contract byte-for-byte
(``strict=True``, ``extra="forbid"``) rather than tolerating the loose,
heterogeneous shapes the legacy compatibility flow accepts --
:class:`JarvisExecutionHandleDocument` (stable execution identity),
:class:`JarvisExecutionRecordDocument` (the durable lifecycle record),
:class:`JarvisProgressEventDocument` /
:class:`JarvisPackageProgressSnapshotDocument` /
:class:`JarvisExecutionProgressDocument` (per-package progress), and
:class:`JarvisNativeExecutionDocuments`, the mutually-bound triple every
native producer must emit as a whole or not at all. The clio-kit runtime
projection paired with them (``_JarvisRuntimeTerminalProjection`` /
``_JarvisRuntimeProjectionDocument``) lives here too -- it is validated
against the *same* native record it accompanies, so it belongs with the
documents it must never disagree with.
"""

from __future__ import annotations

import math
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from clio_relay.runtime_metadata_coercion import _mapping, _optional_str
from clio_relay.runtime_metadata_native_validators import (
    _validate_native_identity,
    _validate_native_json,
    _validate_native_text,
    _validate_native_timestamp,
)
from clio_relay.runtime_metadata_types import (
    _JARVIS_EXECUTION_STATES,
    _JARVIS_PROGRESS_STATES,
    _JARVIS_TERMINAL_STATES,
    JARVIS_EXECUTION_HANDLE_SCHEMA,
    JARVIS_SCHEDULER_SUBMISSION_SCHEMA,
)


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
