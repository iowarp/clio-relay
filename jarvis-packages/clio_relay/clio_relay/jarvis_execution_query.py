"""Recognize and validate the unified JARVIS execution/progress/artifact query.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3). Pure
leaf validation that ties together
:mod:`clio_relay.jarvis_native_execution_documents` and
:mod:`clio_relay.jarvis_artifact_documents` -- no facade reach-back
needed.
"""

from __future__ import annotations

from typing import Any, cast

from clio_relay.constants import (
    _QUERY_CONTRACTS,
    MCP_JARVIS_EXECUTION_QUERY_SCHEMA,
    MCP_JARVIS_EXECUTION_SERVICE_RUNTIMES_SCHEMA,
)
from clio_relay.jarvis_artifact_documents import (
    _validated_jarvis_artifact_page,
    _validated_jarvis_artifact_query,
)
from clio_relay.jarvis_native_execution_documents import (
    _native_identity,
    _validated_native_execution_handle,
    _validated_native_execution_record,
    _validated_native_progress_snapshot,
)
from clio_relay.protocol_messages import _bounded_finite_json, _McpProtocolFailure


def _is_validated_jarvis_execution_query(
    *,
    operation: str,
    tool: str | None,
    expected_server_artifact_digest: str | None,
    expected_registered_contract: str | None,
    expected_jarvis_cd_lock_binding: dict[str, str] | None,
    observed_server_artifact_digest: str | None,
    server_artifact: dict[str, Any] | None,
) -> bool:
    """Return whether this is an artifact-bound, contract-identified JARVIS query."""
    if (
        operation != "tools/call"
        or tool != "jarvis_get_execution"
        or expected_server_artifact_digest is None
        or observed_server_artifact_digest != expected_server_artifact_digest
        or server_artifact is None
        or server_artifact.get("verified") is not True
    ):
        return False
    if expected_registered_contract in _QUERY_CONTRACTS and expected_jarvis_cd_lock_binding is None:
        return True
    if expected_jarvis_cd_lock_binding is None or expected_registered_contract is not None:
        return False
    nested_runtime = server_artifact.get("nested_runtime")
    return (
        isinstance(nested_runtime, dict)
        and cast(dict[str, Any], nested_runtime).get("server_name") == "jarvis"
        and cast(dict[str, Any], nested_runtime).get("locked_runtime_verified") is True
    )


def _validated_jarvis_execution_query_result(
    value: dict[str, Any] | None,
    *,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Validate one unified JARVIS execution, progress, and artifact result."""
    allowed_arguments = {
        "pipeline_id",
        "execution_id",
        "include_progress",
        "include_service_runtimes",
        "artifacts",
    }
    if not set(arguments).issubset(allowed_arguments):
        raise _McpProtocolFailure("MCP JARVIS execution query contained unknown arguments")
    pipeline_id = _native_identity(arguments.get("pipeline_id"), "pipeline_id")
    execution_id = _native_identity(arguments.get("execution_id"), "execution_id")
    include_progress = arguments.get("include_progress", True)
    if not isinstance(include_progress, bool):
        raise _McpProtocolFailure("MCP JARVIS include_progress must be boolean")
    include_service_runtimes = arguments.get("include_service_runtimes", False)
    if not isinstance(include_service_runtimes, bool):
        raise _McpProtocolFailure("MCP JARVIS include_service_runtimes must be boolean")
    artifact_query = _validated_jarvis_artifact_query(arguments.get("artifacts"))
    if value is None:
        raise _McpProtocolFailure("MCP JARVIS execution query omitted its structured result")
    expected_fields = {
        "schema_version",
        "pipeline_id",
        "execution_id",
        "execution_handle",
        "execution_record",
        "runtime_metadata",
        "progress",
        "artifact_page",
        "service_runtimes",
    }
    if set(value) != expected_fields or value.get("schema_version") != (
        MCP_JARVIS_EXECUTION_QUERY_SCHEMA
    ):
        raise _McpProtocolFailure("MCP JARVIS execution query envelope was invalid")
    if value.get("pipeline_id") != pipeline_id or value.get("execution_id") != execution_id:
        raise _McpProtocolFailure("MCP JARVIS execution query identity did not match its request")
    handle = _validated_native_execution_handle(value.get("execution_handle"))
    record = _validated_native_execution_record(value.get("execution_record"))
    identity_fields = (
        "execution_id",
        "pipeline_id",
        "mode",
        "scheduler_provider",
        "scheduler_native_id",
        "cluster",
    )
    if any(handle[field] != record[field] for field in identity_fields):
        raise _McpProtocolFailure("MCP native JARVIS handle and record identities did not match")
    if record["pipeline_id"] != pipeline_id or record["execution_id"] != execution_id:
        raise _McpProtocolFailure("MCP JARVIS execution documents did not match the query")
    runtime_metadata = value.get("runtime_metadata")
    if not isinstance(runtime_metadata, dict):
        raise _McpProtocolFailure("MCP JARVIS execution runtime_metadata must be an object")
    _bounded_finite_json(
        cast(dict[str, Any], runtime_metadata),
        "JARVIS execution runtime_metadata",
        4 * 1024 * 1024,
    )

    raw_progress = value.get("progress")
    progress: dict[str, Any] | None = None
    if include_progress:
        progress = _validated_native_progress_snapshot(raw_progress)
        if (
            progress["execution_id"] != execution_id
            or progress["pipeline_id"] != pipeline_id
            or progress["execution_state"] != record["state"]
            or progress["terminal"] is not record["terminal"]
        ):
            raise _McpProtocolFailure("MCP JARVIS query progress lifecycle did not match")
    elif raw_progress is not None:
        raise _McpProtocolFailure("MCP JARVIS query returned progress after it was omitted")

    raw_service_runtimes = value.get("service_runtimes")
    service_runtime_count = 0
    if include_service_runtimes:
        if not isinstance(raw_service_runtimes, dict):
            raise _McpProtocolFailure("MCP JARVIS query omitted requested service runtimes")
        service_document = cast(dict[str, Any], raw_service_runtimes)
        expected_service_fields = {
            "schema_version",
            "execution_id",
            "pipeline_id",
            "execution_state",
            "terminal",
            "service_runtimes",
        }
        raw_services = service_document.get("service_runtimes")
        if not isinstance(raw_services, list):
            raise _McpProtocolFailure("MCP JARVIS query service runtime envelope was invalid")
        typed_services = cast(list[object], raw_services)
        if (
            set(service_document) != expected_service_fields
            or service_document.get("schema_version")
            != MCP_JARVIS_EXECUTION_SERVICE_RUNTIMES_SCHEMA
            or service_document.get("execution_id") != execution_id
            or service_document.get("pipeline_id") != pipeline_id
            or service_document.get("execution_state") != record["state"]
            or service_document.get("terminal") is not record["terminal"]
            or len(typed_services) > 4_096
            or not all(isinstance(item, dict) for item in typed_services)
        ):
            raise _McpProtocolFailure("MCP JARVIS query service runtime envelope was invalid")
        _bounded_finite_json(
            service_document,
            "JARVIS execution service runtimes",
            4 * 1024 * 1024,
        )
        service_runtime_count = len(typed_services)
    elif raw_service_runtimes is not None:
        raise _McpProtocolFailure(
            "MCP JARVIS query returned service runtimes after they were omitted"
        )

    raw_artifact_page = value.get("artifact_page")
    artifact_page: dict[str, Any] | None = None
    if artifact_query is None:
        if raw_artifact_page is not None:
            raise _McpProtocolFailure(
                "MCP JARVIS query returned artifacts without an artifact request"
            )
    else:
        artifact_page = _validated_jarvis_artifact_page(
            raw_artifact_page,
            query=artifact_query,
            pipeline_id=pipeline_id,
            execution_id=execution_id,
            execution_state=cast(str, record["state"]),
            terminal=cast(bool, record["terminal"]),
        )
    return {
        "schema_version": "clio-relay.jarvis-execution-query-validation.v1",
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
        "include_progress": include_progress,
        "progress_included": progress is not None,
        "include_service_runtimes": include_service_runtimes,
        "service_runtimes_included": raw_service_runtimes is not None,
        "service_runtime_count": service_runtime_count,
        "artifacts_requested": artifact_query is not None,
        "artifact_filters": artifact_query or {},
        "returned_artifact_count": (
            artifact_page["returned_artifact_count"] if artifact_page is not None else 0
        ),
        "next_cursor_present": (
            artifact_page is not None and artifact_page["next_cursor"] is not None
        ),
    }
