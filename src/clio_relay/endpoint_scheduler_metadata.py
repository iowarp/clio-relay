"""Durable job/task/runtime-metadata classification predicates
(iowarp/clio-relay#231).

Owner module for the small, largely-independent predicates and readers
that classify durable job/task metadata: ``_job_timeout_seconds``,
scheduler-job-id extraction and ownership-proof verification
(``_scheduler_job_ids_from_metadata``, ``_owned_scheduler_job_ids_from_
metadata``), native-JARVIS runtime-metadata classification (``_runtime_
metadata_exact_marker_reconciliation``, ``_runtime_metadata_is_native``,
``_native_runtime_execution_mode``, ``_native_runtime_created_at``,
``_runtime_metadata_is_mcp_transport_wrapper``), task-state predicates
(``_task_direct_execution_pinned``, ``_task_scheduler_submission_refused``,
``_runtime_sidecar_channel_failed``), the durable scheduler-submission-
intent reader/validator (``_durable_scheduler_submission_intent``), and two
small task-list lookups (``_task_id_for_scheduler_job``, ``_task_scheduler_
status``) plus an isolated-subprocess-environment context manager
(``_job_subprocess_env``).

``_native_runtime_created_at`` depends on ``endpoint_recovery_directory.py``
(``_recovery_timestamp``, the strict timezone-aware ISO parser), a leaf
relative to this module, so it stays acyclic. ``EndpointWorker`` (still
resident in ``endpoint.py``) is this module's main caller.
"""

from __future__ import annotations

import os
import re
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime
from typing import Any, cast

from clio_relay.endpoint_recovery_directory import _recovery_timestamp
from clio_relay.endpoint_sidecar_types import RUNTIME_SIDECAR_CHANNEL_SCHEMA
from clio_relay.errors import RelayError
from clio_relay.models import JarvisRunSpec, McpCallSpec, RelayJob, RelayTask, RemoteAgentTaskSpec
from clio_relay.runtime_metadata import JarvisRuntimeMetadata, RuntimeMetadataSource


def _job_timeout_seconds(job: RelayJob) -> int | None:
    if isinstance(job.spec, (JarvisRunSpec, RemoteAgentTaskSpec, McpCallSpec)):
        return job.spec.timeout_seconds
    return None


def _scheduler_job_ids_from_metadata(metadata: dict[str, Any]) -> list[str]:
    stored = metadata.get("scheduler_job_ids")
    if not isinstance(stored, list):
        return []
    ids: list[str] = []
    for item in cast(list[object], stored):
        if isinstance(item, str) and item not in ids:
            ids.append(item)
    return ids


def _owned_scheduler_job_ids_from_metadata(
    metadata: dict[str, Any],
    *,
    relay_job_id: str,
    task_id: str | None = None,
) -> list[str]:
    records = metadata.get("scheduler_job_ownership")
    if not isinstance(records, list):
        return []
    owned: list[str] = []
    for item in cast(list[object], records):
        if not isinstance(item, dict):
            continue
        record = cast(dict[str, object], item)
        scheduler_job_id = record.get("scheduler_job_id")
        runtime_source = record.get("runtime_metadata_source")
        expected_proofs = {
            RuntimeMetadataSource.JARVIS_MCP.value: {"owned_jarvis_run_mcp_result"},
            RuntimeMetadataSource.JARVIS_SIDECAR.value: {
                "authenticated_runtime_sidecar",
                "exact_scheduler_marker_reconciliation",
            },
            RuntimeMetadataSource.RELAY_RECONCILIATION.value: {
                "exact_scheduler_marker_reconciliation"
            },
        }.get(runtime_source if isinstance(runtime_source, str) else "", set())
        if (
            not isinstance(scheduler_job_id, str)
            or not scheduler_job_id
            or not isinstance(record.get("scheduler_provider"), str)
            or not record.get("scheduler_provider")
            or not isinstance(record.get("execution_id"), str)
            or not record.get("execution_id")
            or record.get("ownership_verified") is not True
            or record.get("relay_job_id") != relay_job_id
            or (task_id is not None and record.get("task_id") != task_id)
            or record.get("proof") not in expected_proofs
        ):
            continue
        if scheduler_job_id not in owned:
            owned.append(scheduler_job_id)
    return owned


def _runtime_metadata_exact_marker_reconciliation(
    metadata: JarvisRuntimeMetadata,
) -> dict[str, Any] | None:
    raw = metadata.details.get("scheduler_marker_reconciliation")
    if not isinstance(raw, dict):
        return None
    reconciliation = cast(dict[str, Any], raw)
    if (
        reconciliation.get("schema_version") != "clio-relay.scheduler-marker-reconciliation.v1"
        or reconciliation.get("provider") != metadata.scheduler_provider
        or reconciliation.get("scheduler_job_id") != metadata.scheduler_job_id
        or reconciliation.get("match_count") != 1
        or not isinstance(reconciliation.get("marker"), str)
        or not cast(str, reconciliation["marker"]).startswith("clio-relay-")
    ):
        return None
    return reconciliation


def _runtime_metadata_is_native(metadata: JarvisRuntimeMetadata) -> bool:
    """Return whether exact JARVIS handle, record, and progress documents were validated."""
    producer_contract = metadata.details.get("producer_contract")
    native_execution = metadata.details.get("native_execution")
    return (
        isinstance(producer_contract, dict)
        and cast(dict[str, object], producer_contract).get("contract_kind") == "native_execution"
        and isinstance(native_execution, dict)
    )


def _native_runtime_execution_mode(metadata: JarvisRuntimeMetadata) -> str:
    """Return the matching mode from validated native handle and record documents."""
    raw = metadata.details.get("native_execution")
    if not isinstance(raw, dict):
        raise RelayError("native JARVIS runtime metadata omitted execution documents")
    native = cast(dict[str, object], raw)
    handle = native.get("execution_handle")
    record = native.get("execution_record")
    if not isinstance(handle, dict) or not isinstance(record, dict):
        raise RelayError("native JARVIS runtime metadata omitted handle or record")
    handle_mode = cast(dict[str, object], handle).get("mode")
    record_mode = cast(dict[str, object], record).get("mode")
    if handle_mode not in {"direct", "scheduler"} or record_mode != handle_mode:
        raise RelayError("native JARVIS execution mode was inconsistent")
    return cast(str, handle_mode)


def _native_runtime_created_at(metadata: JarvisRuntimeMetadata) -> datetime:
    """Return the authenticated native record creation time."""
    raw = metadata.details.get("native_execution")
    if not isinstance(raw, dict):
        raise RelayError("native JARVIS runtime metadata omitted execution documents")
    record = cast(dict[str, object], raw).get("execution_record")
    if not isinstance(record, dict):
        raise RelayError("native JARVIS runtime metadata omitted its execution record")
    raw_created_at = cast(dict[str, object], record).get("created_at")
    if not isinstance(raw_created_at, str):
        raise RelayError("native JARVIS execution record omitted created_at")
    created_at = _recovery_timestamp(raw_created_at)
    if created_at is None:
        raise RelayError("native JARVIS execution record created_at is invalid")
    return created_at


def _runtime_metadata_is_mcp_transport_wrapper(metadata: JarvisRuntimeMetadata) -> bool:
    """Return whether metadata describes the direct wrapper around one MCP call."""
    if (
        metadata.source is not RuntimeMetadataSource.JARVIS_SIDECAR
        or metadata.scheduler_provider is not None
        or metadata.scheduler_job_id is not None
    ):
        return False
    native_execution = metadata.details.get("native_execution")
    if isinstance(native_execution, dict):
        handle = cast(dict[str, object], native_execution).get("execution_handle")
        record = cast(dict[str, object], native_execution).get("execution_record")
        return (
            isinstance(handle, dict)
            and cast(dict[str, object], handle).get("mode") == "direct"
            and isinstance(record, dict)
            and cast(dict[str, object], record).get("submitted") is False
        )
    nested_details = metadata.details.get("details")
    return metadata.details.get("execution_mode") == "direct" or (
        isinstance(nested_details, dict)
        and cast(dict[str, object], nested_details).get("execution_mode") == "direct"
    )


def _task_direct_execution_pinned(task: RelayTask) -> bool:
    raw_sidecars = task.metadata.get("execution_sidecars")
    return (
        not _runtime_sidecar_channel_failed(task)
        and isinstance(raw_sidecars, dict)
        and cast(dict[str, object], raw_sidecars).get("scheduler_expected_resolved") is False
    )


def _task_scheduler_submission_refused(task: RelayTask) -> bool:
    raw_sidecars = task.metadata.get("execution_sidecars")
    return (
        not _runtime_sidecar_channel_failed(task)
        and isinstance(raw_sidecars, dict)
        and cast(dict[str, object], raw_sidecars).get("scheduler_submission_refused") is True
    )


def _runtime_sidecar_channel_failed(task: RelayTask) -> bool:
    """Return whether runtime authority is durably latched failed closed."""
    raw_channel = task.metadata.get("runtime_sidecar_channel")
    return (
        isinstance(raw_channel, dict)
        and cast(dict[str, object], raw_channel).get("schema_version")
        == RUNTIME_SIDECAR_CHANNEL_SCHEMA
        and cast(dict[str, object], raw_channel).get("state") == "failed_closed"
    )


def _durable_scheduler_submission_intent(task: RelayTask) -> dict[str, Any]:
    raw_sidecars = task.metadata.get("execution_sidecars")
    if not isinstance(raw_sidecars, dict):
        raise RelayError(f"scheduler submission intent is missing for task {task.task_id}")
    raw_intent = cast(dict[str, object], raw_sidecars).get("scheduler_submission_intent")
    if not isinstance(raw_intent, dict):
        raise RelayError(f"scheduler submission intent is missing for task {task.task_id}")
    intent = cast(dict[str, Any], raw_intent)
    if (
        set(intent)
        != {
            "schema_version",
            "execution_id",
            "marker",
            "created_at",
            "scheduler_user",
            "scheduler_expected",
            "direct_proof_sha256",
        }
        or intent.get("schema_version") != "clio-relay.scheduler-submission-intent.v1"
        or any(
            not isinstance(intent.get(field), str) or not intent[field]
            for field in ("execution_id", "marker", "created_at", "scheduler_user")
        )
        or not cast(str, intent["execution_id"]).startswith("jarvis_")
        or not cast(str, intent["marker"]).startswith("clio-relay-")
        or intent.get("scheduler_expected") not in {True, False, "unknown"}
        or not isinstance(intent.get("direct_proof_sha256"), str)
        or not re.fullmatch(r"[0-9a-f]{64}", cast(str, intent["direct_proof_sha256"]))
    ):
        raise RelayError(f"scheduler submission intent is invalid for task {task.task_id}")
    try:
        created_at = datetime.fromisoformat(cast(str, intent["created_at"]))
    except ValueError as exc:
        raise RelayError(f"scheduler submission intent time is invalid for {task.task_id}") from exc
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise RelayError(f"scheduler submission intent time is naive for {task.task_id}")
    return intent


def _task_id_for_scheduler_job(tasks: list[RelayTask], scheduler_job_id: str) -> str | None:
    for task in tasks:
        if scheduler_job_id in _scheduler_job_ids_from_metadata(task.metadata):
            return task.task_id
    return None


def _task_scheduler_status(
    tasks: list[RelayTask],
    task_id: str,
    scheduler_job_id: str,
) -> dict[str, Any] | None:
    for task in tasks:
        if task.task_id != task_id:
            continue
        stored = task.metadata.get("scheduler_status")
        if not isinstance(stored, dict):
            return None
        typed = cast(dict[str, Any], stored)
        if typed.get("scheduler_job_id") != scheduler_job_id:
            return None
        return typed
    return None


@contextmanager
def _job_subprocess_env(
    values: dict[str, str],
    *,
    inherit_parent: bool = True,
) -> Generator[dict[str, str], None, None]:
    """Yield an isolated child environment without mutating threaded process state."""
    yield {**os.environ, **values} if inherit_parent else dict(values)
