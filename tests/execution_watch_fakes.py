"""Shared fake native JARVIS document builders for clio-relay#266 test coverage.

Used by ``tests/test_execution_watch.py`` (the watch's own lifecycle
coverage) and by any other test that needs to fabricate a trusted
``jarvis_run`` dispatch or ``jarvis_get_execution`` poll response without a
real JARVIS MCP server -- e.g. an existing scheduler-mode fixture in
``test_endpoint.py`` that now also passes through clio-relay#266's watch on
its way to terminal.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from clio_relay.models import McpCallSpec


def native_execution_documents(
    *,
    pipeline_id: str,
    execution_id: str,
    state: str,
    terminal: bool,
    scheduler_job_id: str | None,
    created_at: str,
    execution_root: Path,
    return_code: int | None = None,
    error: str | None = None,
    scheduler_provider: str = "slurm",
    cluster: str = "watch-test",
) -> dict[str, object]:
    """Build one coherent native handle/record/progress/runtime envelope."""
    handle: dict[str, object] = {
        "schema_version": "jarvis.execution.handle.v1",
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "mode": "scheduler",
        "scheduler_provider": scheduler_provider,
        "scheduler_native_id": scheduler_job_id,
        "cluster": cluster,
    }
    script_path = str(execution_root / "submit.sh")
    submitted = scheduler_job_id is not None
    submission: dict[str, object] | None = (
        {
            "schema_version": "jarvis.scheduler.submission.v1",
            "execution_id": execution_id,
            "provider": scheduler_provider,
            "scheduler_job_id": scheduler_job_id,
            "scheduler_cluster": cluster,
            "submitted": submitted,
            "identity_source": "scheduler_submit_api" if submitted else None,
            "script_path": script_path,
            "hostfile_path": None,
            "pipeline_snapshot_path": None,
            "pipeline_input_path": None,
            "execution_root_path": None,
            "output_path": None,
            "error_path": None,
        }
        if submitted
        else None
    )
    record: dict[str, object] = {
        "schema_version": "jarvis.execution.record.v1",
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "pipeline_name": pipeline_id,
        "mode": "scheduler",
        "scheduler_provider": scheduler_provider,
        "scheduler_native_id": scheduler_job_id,
        "cluster": cluster,
        "state": state,
        "submitted": submitted,
        "terminal": terminal,
        "created_at": created_at,
        "updated_at": created_at,
        "return_code": return_code,
        "error": error,
        "metadata": {"script_path": script_path, "submission": submission},
    }
    progress: dict[str, object] = {
        "schema_version": "jarvis.execution.progress.v1",
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "execution_state": state,
        "terminal": terminal,
        "packages": [],
    }
    runtime: dict[str, object] = {
        "schema_version": "jarvis.runtime.v1",
        "source": "jarvis_mcp",
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "mode": "scheduler",
        "scheduler_provider": scheduler_provider,
        "scheduler_native_id": scheduler_job_id,
        "cluster": cluster,
        "scheduler_type": scheduler_provider,
        "scheduler_job_id": scheduler_job_id,
        "scheduler_phase": state if scheduler_job_id is not None else None,
        "script_path": script_path,
        "hostfile_path": None,
        "output_path": None,
        "error_path": None,
        "package_provenance": [],
        "terminal": {
            "state": state,
            "terminal": terminal,
            "returncode": return_code,
            "reason": error,
            "started_at": created_at,
            "finished_at": created_at if terminal else None,
        },
        "details": {
            "execution_handle": handle,
            "execution_record": record,
            "scheduler_submission": submission,
        },
    }
    return {
        "execution_handle": handle,
        "execution_record": record,
        "progress": progress,
        "runtime_metadata": runtime,
    }


def envelope(
    *,
    spec: McpCallSpec,
    server_artifact: dict[str, Any],
    operation: str,
    tool: str,
    structured_result: dict[str, object],
) -> dict[str, object]:
    """Wrap one structured result in the exact trusted mcp-result.json shape."""
    return {
        "server": spec.server,
        "server_args": spec.server_args,
        "expected_server_artifact_digest": spec.expected_server_artifact_digest,
        "expected_registered_contract": spec.expected_registered_contract,
        "expected_jarvis_cd_lock_binding": spec.expected_jarvis_cd_lock_binding,
        "observed_server_artifact_digest": spec.expected_server_artifact_digest,
        "server_artifact": server_artifact,
        "operation": operation,
        "tool": tool,
        "arguments": spec.arguments,
        "env_from": spec.env_from,
        "protocol_result": {"structuredContent": structured_result},
        "structured_result": structured_result,
        "stdout": "",
        "stderr": "",
        "returncode": 0,
        "timed_out": False,
        "protocol_error": None,
        "stdout_truncation": None,
        "stderr_truncation": None,
        "result_validation": None,
        "package_progress_bridge": None,
    }


def run_dispatch_document(
    *,
    run_spec: McpCallSpec,
    server_artifact: dict[str, Any],
    native: dict[str, object],
) -> dict[str, object]:
    """Build the ``jarvis_run`` dispatch's mcp-result.json (clio-kit.jarvis-run.v1)."""
    record = cast(dict[str, Any], native["execution_record"])
    structured: dict[str, object] = {
        "schema_version": "clio-kit.jarvis-run.v1",
        "pipeline_id": record["pipeline_id"],
        "execution_id": record["execution_id"],
        "status": record["state"],
        "mode": "scheduler",
        "scheduler": None,
        "script_path": record["metadata"]["script_path"],
        "wait": False,
        **native,
    }
    return envelope(
        spec=run_spec,
        server_artifact=server_artifact,
        operation="tools/call",
        tool="jarvis_run",
        structured_result=structured,
    )


def query_result_document(
    *,
    query_spec: McpCallSpec,
    server_artifact: dict[str, Any],
    native: dict[str, object],
    include_artifacts: bool,
) -> dict[str, object]:
    """Build one ``jarvis_get_execution`` poll's mcp-result.json."""
    record = cast(dict[str, Any], native["execution_record"])
    structured: dict[str, object] = {
        "schema_version": "clio-kit.jarvis-execution.v2",
        "pipeline_id": record["pipeline_id"],
        "execution_id": record["execution_id"],
        **native,
        "artifact_page": (
            {"artifacts": [], "terminal": record["terminal"]} if include_artifacts else None
        ),
        "service_runtimes": None,
    }
    document = envelope(
        spec=query_spec,
        server_artifact=server_artifact,
        operation="tools/call",
        tool="jarvis_get_execution",
        structured_result=structured,
    )
    if not include_artifacts:
        document["result_validation"] = {
            "schema_version": "clio-relay.jarvis-execution-query-validation.v1",
            "pipeline_id": record["pipeline_id"],
            "execution_id": record["execution_id"],
            "include_progress": True,
            "progress_included": True,
            "include_service_runtimes": False,
            "service_runtimes_included": False,
            "service_runtime_count": 0,
            "artifacts_requested": False,
            "artifact_filters": {},
            "returned_artifact_count": 0,
            "next_cursor_present": False,
        }
    return document
