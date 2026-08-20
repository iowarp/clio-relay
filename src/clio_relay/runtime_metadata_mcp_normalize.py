"""Decode a persisted MCP-call result into runtime metadata.

Extracted from ``runtime_metadata.py`` (clio-relay split/runtime-metadata-w2):
:func:`runtime_metadata_from_mcp_result_document` is the top-level entry
point that decodes one durable ``jarvis_run`` MCP-call result -- preferring
the exact native execution documents
(``runtime_metadata_native_normalize.native_execution_documents``) when a
producer emits them, and otherwise falling back to
:func:`normalize_runtime_metadata`'s loose JARVIS/clio-kit compatibility
parsing (:func:`_AUTHORITATIVE_RUNTIME_SOURCES` gates when that loose parse
is trusted enough to claim scheduler ownership).
:func:`legacy_scheduler_runtime_metadata` builds the explicitly untrusted
observation synthesized from stdout scraping before structured metadata was
available, and :func:`_normalize_synchronous_jarvis_completion` records
completion implied by a successful synchronous ``jarvis_run`` return that
never reported its own terminal state.
"""

from __future__ import annotations

import json
from typing import Any, cast

from clio_relay.runtime_metadata_coercion import (
    _first_str,
    _is_jarvis_run_tool,
    _json_object,
    _looks_like_runtime_payload,
    _mapping,
    _nodes,
    _optional_bool,
    _optional_int,
    _optional_str,
    _package_provenance,
    _path_value,
    _timestamp_string,
)
from clio_relay.runtime_metadata_core_model import (
    JarvisRuntimeMetadata,
    TerminalRuntimeMetadata,
    _field_sources,
)
from clio_relay.runtime_metadata_merge import (
    _AUTHORITATIVE_RUNTIME_SOURCES,
    _has_runtime_identity,
    _trusted_producer_runtime_contract,
)
from clio_relay.runtime_metadata_native_normalize import (
    _merge_native_runtime_projection,
    native_execution_documents,
    runtime_metadata_from_native_documents,
)
from clio_relay.runtime_metadata_types import JARVIS_EXECUTION_RECORD_SCHEMA, RuntimeMetadataSource


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
