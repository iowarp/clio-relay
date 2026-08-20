"""Live native JARVIS MCP progress-notification evidence (``jarvis_run``).

Owner module for the ``jarvis_mcp_validation.py`` split (clio-relay split/
jarvis-mcp-validation): binds one in-flight native progress notification
(the "warming" event) to a later, execution-bound "accepted" notification
carrying non-decreasing counters, cross-checks the MCP result's progress
bridge, and binds both to the durable structured runtime metadata's own final
package snapshot. Called by ``build_jarvis_mcp_validation_report`` in
``jarvis_mcp_validation_report.py`` for the non-resumable ``jarvis_run``
live-progress path.
"""

from __future__ import annotations

from typing import cast

from clio_relay.jarvis_mcp_validation_core import (
    JSON,
    _is_sha256,
    _mapping,
    _nonnegative_int,
    _positive_int,
)

_NATIVE_PROGRESS_IDENTITY_KEYS = (
    "execution_id",
    "pipeline_id",
    "package_id",
    "package_name",
    "server_artifact_digest",
)


def _jarvis_live_progress_evidence(
    *,
    progress: list[JSON],
    live_observation: JSON | None,
    call_job_id: str,
    pipeline_id: object,
    expected_server_artifact_digest: object,
    mcp_result: JSON | None,
    runtime_metadata: JSON | None,
) -> tuple[JSON, bool, JSON | None]:
    """Validate live and final progress owned by one native JARVIS execution."""
    candidates: list[tuple[int, JSON, JSON]] = []
    for index, record in enumerate(progress):
        metadata = _mapping(record.get("metadata"))
        if metadata is None or record.get("job_id") != call_job_id:
            continue
        if (
            metadata.get("source") != "jarvis_execution"
            or metadata.get("provider_source_authority") != "jarvis_mcp_progress_notification"
            or metadata.get("producer_validated") is not True
            or metadata.get("relay_job_id") != call_job_id
            or metadata.get("run_id") != metadata.get("execution_id")
            or metadata.get("progress_schema_version") != "jarvis.progress.v1"
        ):
            continue
        candidates.append((index, record, metadata))

    warming: tuple[int, JSON, JSON] | None = None
    accepted: tuple[int, JSON, JSON] | None = None
    for warming_candidate in candidates:
        warming_index, warming_record, warming_metadata = warming_candidate
        if warming_metadata.get("execution_binding_validated") is not False or not _positive_int(
            warming_metadata.get("progress_transport_sequence")
        ):
            continue
        for accepted_candidate in candidates:
            accepted_index, accepted_record, accepted_metadata = accepted_candidate
            if accepted_index <= warming_index:
                continue
            if (
                accepted_metadata.get("execution_binding_validated") is not True
                or not _same_native_progress_execution(
                    warming_metadata,
                    accepted_metadata,
                )
                or not _nondecreasing_native_progress(warming_metadata, accepted_metadata)
            ):
                continue
            warming = warming_candidate
            accepted = accepted_candidate
            break
        if warming is not None:
            break

    warming_record = warming[1] if warming is not None else None
    warming_metadata = warming[2] if warming is not None else None
    accepted_record = accepted[1] if accepted is not None else None
    accepted_metadata = accepted[2] if accepted is not None else None
    live_observed = (
        live_observation is not None
        and warming_record is not None
        and live_observation.get("progress_id") == warming_record.get("progress_id")
        and live_observation.get("job_state") == "running"
        and live_observation.get("terminal") is False
    )
    progress_binding_valid = (
        accepted_metadata is not None
        and isinstance(pipeline_id, str)
        and accepted_metadata.get("pipeline_id") == pipeline_id
        and _is_sha256(expected_server_artifact_digest)
        and accepted_metadata.get("server_artifact_digest") == expected_server_artifact_digest
        and all(
            isinstance(accepted_metadata.get(key), str) and bool(accepted_metadata.get(key))
            for key in _NATIVE_PROGRESS_IDENTITY_KEYS
        )
        and isinstance(accepted_metadata.get("progress_determinate"), bool)
        and _nonnegative_int(accepted_metadata.get("progress_event_count"))
        and _nonnegative_int(accepted_metadata.get("progress_sequence"))
        and _nonnegative_int(accepted_metadata.get("progress_transport_sequence"))
    )

    result_bridge = _mapping(mcp_result.get("package_progress_bridge")) if mcp_result else None
    bridge_valid = (
        result_bridge is not None
        and accepted_metadata is not None
        and result_bridge.get("schema_version") == "clio-relay.mcp-jarvis-progress-bridge.v1"
        and result_bridge.get("execution_validated") is True
        and _positive_int(result_bridge.get("notification_count"))
        and result_bridge.get("execution_id") == accepted_metadata.get("execution_id")
        and result_bridge.get("pipeline_id") == pipeline_id
        and result_bridge.get("expected_server_artifact_digest") == expected_server_artifact_digest
        and result_bridge.get("observed_server_artifact_digest") == expected_server_artifact_digest
        and isinstance(result_bridge.get("package_sequences"), dict)
    )

    runtime_details = _mapping(runtime_metadata.get("details")) if runtime_metadata else None
    native_execution = (
        _mapping(runtime_details.get("native_execution")) if runtime_details else None
    )
    native_progress = _mapping(native_execution.get("progress")) if native_execution else None
    native_packages = native_progress.get("packages") if native_progress else None
    runtime_package_bound = _native_runtime_package_bound(
        native_packages,
        accepted_metadata,
    )
    runtime_bound = (
        runtime_metadata is not None
        and accepted_metadata is not None
        and runtime_metadata.get("execution_id") == accepted_metadata.get("execution_id")
        and runtime_metadata.get("pipeline_id") == pipeline_id
        and native_progress is not None
        and native_progress.get("schema_version") == "jarvis.execution.progress.v1"
        and native_progress.get("execution_id") == accepted_metadata.get("execution_id")
        and runtime_package_bound
    )
    passed = bool(
        warming is not None
        and accepted is not None
        and live_observed
        and progress_binding_valid
        and bridge_valid
        and runtime_bound
    )
    evidence: JSON = {
        "execution_id": (accepted_metadata.get("execution_id") if accepted_metadata else None),
        "progress_record_count": len(progress),
        "warming_progress_id": warming_record.get("progress_id") if warming_record else None,
        "accepted_progress_id": accepted_record.get("progress_id") if accepted_record else None,
        "notification_sequence": (
            accepted_metadata.get("progress_transport_sequence") if accepted_metadata else None
        ),
        "live_observation": live_observation or {},
        "live_observed_while_running": live_observed,
        "expected_pipeline_id": pipeline_id,
        "expected_server_artifact_digest": expected_server_artifact_digest,
        "progress_binding_valid": progress_binding_valid,
        "bridge_valid": bridge_valid,
        "runtime_bound": runtime_bound,
        "bridge": result_bridge or {},
        "native_progress": (
            {key: accepted_metadata.get(key) for key in _NATIVE_PROGRESS_IDENTITY_KEYS}
            if accepted_metadata
            else {}
        ),
    }
    if accepted_record is None or accepted_metadata is None:
        return evidence, passed, None
    resource = {
        "resource_id": (
            f"{accepted_metadata.get('execution_id', 'execution')}:"
            f"{accepted_metadata.get('package_id', 'package')}"
        ),
        "provider": "jarvis-cd",
        "metadata": {
            **accepted_metadata,
            "warming_progress_id": warming_record.get("progress_id") if warming_record else None,
            "accepted_progress_id": accepted_record.get("progress_id"),
            "live_observed_while_running": live_observed,
            "bridge_validated": bridge_valid,
            "runtime_bound": runtime_bound,
        },
    }
    return evidence, passed, resource


def _same_native_progress_execution(
    warming_metadata: JSON,
    accepted_metadata: JSON,
) -> bool:
    """Return whether two observations belong to one native package execution."""
    return all(
        warming_metadata.get(key) == accepted_metadata.get(key)
        for key in _NATIVE_PROGRESS_IDENTITY_KEYS
    )


def _nondecreasing_native_progress(warming: JSON, accepted: JSON) -> bool:
    """Require final native progress counters not to regress from the live event."""
    return all(
        _nonnegative_int(warming.get(key))
        and _nonnegative_int(accepted.get(key))
        and cast(int, accepted[key]) >= cast(int, warming[key])
        for key in (
            "progress_sequence",
            "progress_event_count",
            "progress_transport_sequence",
        )
    )


def _native_runtime_package_bound(
    packages: object,
    metadata: JSON | None,
) -> bool:
    """Bind an accepted progress observation to the final native snapshot."""
    if not isinstance(packages, list) or metadata is None:
        return False
    for item in cast(list[object], packages):
        package = _mapping(item)
        if package is None:
            continue
        if (
            package.get("package_id") == metadata.get("package_id")
            and package.get("package_name") == metadata.get("package_name")
            and _nonnegative_int(package.get("event_count"))
            and cast(int, package["event_count"]) >= cast(int, metadata["progress_event_count"])
        ):
            return True
    return False
