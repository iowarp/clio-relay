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
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, cast

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.models import utc_now

RUNTIME_METADATA_SCHEMA = "clio-relay.jarvis-runtime.v1"
JARVIS_RUNTIME_METADATA_SCHEMA = "jarvis.runtime.v1"
JARVIS_SCHEDULER_SUBMISSION_SCHEMA = "jarvis.scheduler.submission.v1"
RUNTIME_SIDECAR_RECORD_SCHEMA = "clio-relay.runtime-sidecar-record.v1"


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
    metadata = normalize_runtime_metadata(structured, source=RuntimeMetadataSource.JARVIS_MCP)
    if metadata is None:
        return metadata
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
    metadata = normalize_runtime_metadata(payload, source=RuntimeMetadataSource.JARVIS_SIDECAR)
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
    {"execution_id", "scheduler_provider", "scheduler_job_id"}
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
