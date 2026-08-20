"""JARVIS run-dispatch completion (iowarp/clio-relay#231 continuation):
the retry-selector and completion helpers for one JARVIS run dispatch
attempt."""

from __future__ import annotations

from typing import Any, cast

from pydantic import ValidationError

import clio_relay.cli_jarvis_artifact_io as cli_jarvis_artifact_io
import clio_relay.cli_remote_collection_pagination as cli_remote_collection_pagination
import clio_relay.core_queue as core_queue
import clio_relay.remote_cli as remote_cli
from clio_relay.cluster_config import (
    ClusterDefinition,
)
from clio_relay.errors import RelayError
from clio_relay.runtime_metadata import RUNTIME_METADATA_SCHEMA, native_execution_documents


def _jarvis_execution_retry_selector_from_runtime_metadata(
    runtime_metadata: dict[str, Any],
    *,
    cluster: str,
    pipeline_id: str,
    execution_id: str,
) -> dict[str, object]:
    """Bind a query-only selector to JARVIS's structured native execution authority."""
    details = runtime_metadata.get("details")
    native_execution = (
        cast(dict[str, object], details).get("native_execution")
        if isinstance(details, dict)
        else None
    )
    if (
        runtime_metadata.get("schema_version") != RUNTIME_METADATA_SCHEMA
        or runtime_metadata.get("source") != "jarvis_mcp"
        or runtime_metadata.get("pipeline_id") != pipeline_id
        or runtime_metadata.get("execution_id") != execution_id
        or not isinstance(native_execution, dict)
    ):
        raise RelayError("JARVIS run metadata omitted its structured execution identity")
    try:
        documents = native_execution_documents(cast(dict[str, Any], native_execution))
    except (ValidationError, ValueError) as exc:
        raise RelayError(
            "JARVIS run metadata contains an invalid native execution identity"
        ) from exc
    if documents is None:
        raise RelayError("JARVIS run metadata omitted its native execution identity")
    handle = documents.execution_handle
    scheduler_provider = runtime_metadata.get("scheduler_provider")
    scheduler_native_id = runtime_metadata.get("scheduler_job_id")
    if (
        handle.pipeline_id != pipeline_id
        or handle.execution_id != execution_id
        or handle.scheduler_provider != scheduler_provider
        or handle.scheduler_native_id != scheduler_native_id
        or (scheduler_provider is None and scheduler_native_id is not None)
    ):
        raise RelayError("JARVIS run metadata contains inconsistent scheduler identity")
    return {
        "cluster": cluster,
        "scheduler_cluster": handle.cluster,
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
        "scheduler_provider": scheduler_provider,
        "scheduler_native_id": scheduler_native_id,
        "last_query_job_id": None,
    }


def _require_jarvis_run_dispatch_job_identity(
    status: dict[str, object],
    *,
    cluster: str,
    job_id: str,
    pipeline_id: str,
    idempotency_key: str,
) -> None:
    """Verify that a resumed receipt still denotes the exact accepted jarvis_run call."""
    raw_job = status.get("job")
    job = cast(dict[str, object], raw_job) if isinstance(raw_job, dict) else {}
    raw_spec = job.get("spec")
    spec = cast(dict[str, object], raw_spec) if isinstance(raw_spec, dict) else {}
    raw_arguments = spec.get("arguments")
    arguments = cast(dict[str, object], raw_arguments) if isinstance(raw_arguments, dict) else {}
    if (
        status.get("terminal") is not True
        or job.get("job_id") != job_id
        or job.get("cluster") != cluster
        or job.get("kind") != "mcp_call"
        or job.get("idempotency_key") != idempotency_key
        or spec.get("operation") != "tools/call"
        or spec.get("tool") != "jarvis_run"
        or arguments.get("pipeline_id") != pipeline_id
    ):
        raise RelayError("resumed JARVIS relay job changed its dispatch identity")


def _complete_jarvis_run_dispatch(
    *,
    definition: ClusterDefinition,
    queue: core_queue.ClioCoreQueue,
    checkpoint: dict[str, Any],
    wait_timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, Any]:
    """Wait and collect one exact relay dispatch without ever resubmitting it."""
    selector = cast(dict[str, object], checkpoint["retry_selector"])
    cluster = cast(str, selector["cluster"])
    job_id = cast(str, selector["relay_job_id"])
    pipeline_id = cast(str, selector["pipeline_id"])
    idempotency_key = cast(str, selector["idempotency_key"])
    if remote_cli.should_execute_on_cluster(definition):
        call_status = cli_remote_collection_pagination._wait_for_remote_job_terminal(
            definition,
            job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        _require_jarvis_run_dispatch_job_identity(
            call_status,
            cluster=cluster,
            job_id=job_id,
            pipeline_id=pipeline_id,
            idempotency_key=idempotency_key,
        )
        progress = cli_remote_collection_pagination._complete_remote_collection(
            definition,
            ["job", "progress", job_id],
            record_key="progress",
            label=f"JARVIS MCP dispatch progress for {job_id}",
        )
        artifacts = cli_jarvis_artifact_io._remote_artifact_records(definition, job_id)
        mcp_result = cli_jarvis_artifact_io._read_remote_json_artifact_kind(
            definition, artifacts, kind="mcp_result"
        )
        provenance = cli_jarvis_artifact_io._read_remote_json_artifact_kind(
            definition, artifacts, kind="provenance"
        )
        runtime_metadata = cli_jarvis_artifact_io._read_remote_json_artifact_kind(
            definition, artifacts, kind="runtime_metadata"
        )
    else:
        call_status = cli_remote_collection_pagination._wait_for_local_job_terminal(
            queue,
            job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        _require_jarvis_run_dispatch_job_identity(
            call_status,
            cluster=cluster,
            job_id=job_id,
            pipeline_id=pipeline_id,
            idempotency_key=idempotency_key,
        )
        progress = cli_remote_collection_pagination._complete_local_progress_records(queue, job_id)
        artifacts = cli_remote_collection_pagination._complete_local_artifact_records(queue, job_id)
        mcp_result = cli_jarvis_artifact_io._read_local_json_artifact_kind(
            queue, artifacts, kind="mcp_result"
        )
        provenance = cli_jarvis_artifact_io._read_local_json_artifact_kind(
            queue, artifacts, kind="provenance"
        )
        runtime_metadata = cli_jarvis_artifact_io._read_local_json_artifact_kind(
            queue, artifacts, kind="runtime_metadata"
        )
    return {
        **cast(dict[str, Any], checkpoint["builder_inputs"]),
        "call_status": call_status,
        "artifacts": artifacts,
        "mcp_result": mcp_result,
        "provenance": provenance,
        "runtime_metadata": runtime_metadata,
        "progress": progress,
        "live_progress_observation": None,
    }
