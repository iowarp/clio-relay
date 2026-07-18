"""Convergence coverage for an artifact-pinned JARVIS run with a lost response."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from pytest import MonkeyPatch

from clio_relay import endpoint as endpoint_module
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import (
    Cursor,
    EndpointRole,
    JobKind,
    JobState,
    McpCallSpec,
    RelayJob,
    SchedulerPhase,
    SchedulerStatus,
    deterministic_jarvis_execution_id,
)
from clio_relay.remote_mcp import remote_mcp_server_artifact_digest
from clio_relay.spool import JobSpool
from tests.jarvis_mcp_fakes import verified_jarvis_server_artifact


class _LostRunResponseProvider(JarvisCdProvider):
    """Execute one outer JARVIS transport whose MCP result never arrives."""

    def __init__(self, *, release_dispatch: bool = True) -> None:
        super().__init__(jarvis_bin="jarvis")
        self.dispatch_count = 0
        self.release_dispatch = release_dispatch

    def run_pipeline_streaming(
        self,
        pipeline_path: Path,
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        on_start: Callable[[int], None] | None = None,
        should_cancel: Callable[[], bool] | None = None,
        on_poll: Callable[[], None] | None = None,
        timeout_seconds: int | None = None,
        on_timeout: Callable[[], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Return a failed transport without fabricating a workload result."""
        del pipeline_path, cwd, should_cancel, timeout_seconds, on_timeout
        self.dispatch_count += 1
        assert env is not None
        Path(env["CLIO_RELAY_PROGRESS_FILE"]).write_text("", encoding="utf-8")
        Path(env["CLIO_RELAY_RUNTIME_METADATA_FILE"]).write_text("", encoding="utf-8")
        if on_start is not None and self.release_dispatch:
            probe = subprocess.Popen([sys.executable, "-c", "pass"])
            on_start(probe.pid)
            probe.wait(timeout=10)
        if on_stdout is not None:
            on_stdout("JARVIS accepted the run before the transport response was lost\n")
        if on_stderr is not None:
            on_stderr("outer MCP response unavailable\n")
        if on_poll is not None:
            on_poll()
        return subprocess.CompletedProcess(
            ["jarvis", "ppl", "run"],
            1,
            "",
            "outer MCP response unavailable\n",
        )


class _RecordingSlurmProvider:
    """Record scheduler observation and cancellation calls made by the worker."""

    name = "slurm"

    def __init__(self) -> None:
        self.polled: list[str] = []
        self.canceled: list[str] = []

    def scheduler_cluster_name(self) -> str:
        """Return the native scheduler cluster identity in the JARVIS document."""
        return "scheduler-test"

    def poll(self, scheduler_job_id: str) -> SchedulerStatus:
        """Return an exact nonterminal status for the recovered scheduler job."""
        self.polled.append(scheduler_job_id)
        return SchedulerStatus(
            scheduler=self.name,
            scheduler_job_id=scheduler_job_id,
            phase=SchedulerPhase.SUBMITTED,
            record_found=True,
            active_record_found=True,
            raw_state="PENDING",
        )

    def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        """Record an unexpected cancellation attempt."""
        self.canceled.append(scheduler_job_id)
        return subprocess.CompletedProcess(["scancel", scheduler_job_id], 0, "", "")

    def find_job_ids_by_marker(
        self,
        marker: str,
        *,
        submitted_after: datetime,
        scheduler_user: str,
    ) -> list[str]:
        """Return no legacy marker matches; native JARVIS identity owns this path."""
        del marker, submitted_after, scheduler_user
        return []


def _scheduled_execution_documents(
    *,
    pipeline_id: str,
    execution_id: str,
    scheduler_job_id: str | None,
    created_at: str,
) -> dict[str, object]:
    """Build one coherent native scheduler observation from JARVIS-CD."""
    submitted = scheduler_job_id is not None
    state = "submitted" if submitted else "submitting"
    handle: dict[str, object] = {
        "schema_version": "jarvis.execution.handle.v1",
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "mode": "scheduler",
        "scheduler_provider": "slurm",
        "scheduler_native_id": scheduler_job_id,
        "cluster": "scheduler-test",
    }
    submission: dict[str, object] = {
        "schema_version": "jarvis.scheduler.submission.v1",
        "execution_id": execution_id,
        "provider": "slurm",
        "scheduler_job_id": scheduler_job_id,
        "scheduler_cluster": "scheduler-test",
        "submitted": submitted,
        "identity_source": "scheduler_submit_api" if submitted else None,
        "script_path": f"/runs/{execution_id}/submit.sh",
        "hostfile_path": None,
        "pipeline_snapshot_path": f"/runs/{execution_id}/pipeline.yaml",
        "pipeline_input_path": None,
        "execution_root_path": f"/runs/{execution_id}",
        "output_path": f"/runs/{execution_id}/stdout.log",
        "error_path": f"/runs/{execution_id}/stderr.log",
    }
    record: dict[str, object] = {
        "schema_version": "jarvis.execution.record.v1",
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "pipeline_name": pipeline_id,
        "mode": "scheduler",
        "scheduler_provider": "slurm",
        "scheduler_native_id": scheduler_job_id,
        "cluster": "scheduler-test",
        "state": state,
        "submitted": submitted,
        "terminal": False,
        "created_at": created_at,
        "updated_at": created_at,
        "return_code": None,
        "error": None,
        "metadata": {"submission": submission},
    }
    progress: dict[str, object] = {
        "schema_version": "jarvis.execution.progress.v1",
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "execution_state": state,
        "terminal": False,
        "packages": [],
    }
    runtime: dict[str, object] = {
        "schema_version": "jarvis.runtime.v1",
        "source": "jarvis_mcp",
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "mode": "scheduler",
        "scheduler_provider": "slurm",
        "scheduler_native_id": scheduler_job_id,
        "cluster": "scheduler-test",
        "scheduler_type": "slurm",
        "scheduler_job_id": scheduler_job_id,
        "scheduler_phase": state if submitted else None,
        "script_path": submission["script_path"],
        "hostfile_path": None,
        "output_path": submission["output_path"],
        "error_path": submission["error_path"],
        "package_provenance": [],
        "terminal": {
            "state": state,
            "terminal": False,
            "returncode": None,
            "reason": None,
            "started_at": record["created_at"],
            "finished_at": None,
        },
        "details": {
            "execution_handle": handle,
            "execution_record": record,
            "scheduler_submission": submission,
        },
    }
    return {
        "schema_version": "clio-kit.jarvis-execution.v2",
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
        "execution_handle": handle,
        "execution_record": record,
        "progress": progress,
        "runtime_metadata": runtime,
        "artifact_page": None,
        "service_runtimes": None,
    }


def _execution_query_document(
    *,
    spec: McpCallSpec,
    server_artifact: dict[str, Any],
    scheduler_job_id: str | None,
    created_at: str,
) -> dict[str, object]:
    """Wrap one native observation in the exact pinned runner result contract."""
    pipeline_id = cast(str, spec.arguments["pipeline_id"])
    execution_id = cast(str, spec.arguments["execution_id"])
    structured = _scheduled_execution_documents(
        pipeline_id=pipeline_id,
        execution_id=execution_id,
        scheduler_job_id=scheduler_job_id,
        created_at=created_at,
    )
    return {
        "server": spec.server,
        "server_args": spec.server_args,
        "expected_server_artifact_digest": spec.expected_server_artifact_digest,
        "expected_jarvis_cd_lock_binding": spec.expected_jarvis_cd_lock_binding,
        "observed_server_artifact_digest": spec.expected_server_artifact_digest,
        "server_artifact": server_artifact,
        "operation": "tools/call",
        "tool": "jarvis_get_execution",
        "arguments": spec.arguments,
        "env_from": spec.env_from,
        "protocol_result": {"structuredContent": structured},
        "structured_result": structured,
        "stdout": "",
        "stderr": "",
        "returncode": 0,
        "timed_out": False,
        "protocol_error": None,
        "result_validation": {
            "schema_version": "clio-relay.jarvis-execution-query-validation.v1",
            "pipeline_id": pipeline_id,
            "execution_id": execution_id,
            "include_progress": True,
            "progress_included": True,
            "include_service_runtimes": False,
            "service_runtimes_included": False,
            "service_runtime_count": 0,
            "artifacts_requested": False,
            "artifact_filters": {},
            "returned_artifact_count": 0,
            "next_cursor_present": False,
        },
        "package_progress_bridge": None,
    }


def test_lost_run_response_converges_after_scheduler_assigns_native_id(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A null-to-assigned SLURM identity eventually finalizes only the relay call."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    command = ["locked-clio-kit", "mcp-server", "jarvis"]
    server_artifact = verified_jarvis_server_artifact()
    digest = remote_mcp_server_artifact_digest(server_artifact)
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    submission = RelayJob(
        cluster="research-cluster",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server=command[0],
            server_args=command[1:],
            expected_server_artifact_digest=digest,
            expected_jarvis_cd_lock_binding=(endpoint_module.jarvis_cd_lock_binding_expectation()),
            tool="jarvis_run",
            arguments={"pipeline_id": "durable-science"},
        ),
        idempotency_key="lost-response-stable-execution",
    )
    job = queue.submit_job(submission)
    assert isinstance(job.spec, McpCallSpec)
    execution_id = deterministic_jarvis_execution_id(
        cluster=job.cluster,
        idempotency_key=job.idempotency_key,
        job_id=job.job_id,
    )
    assert job.spec.arguments["execution_id"] == execution_id

    transport = _LostRunResponseProvider()
    scheduler = _RecordingSlurmProvider()
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster=job.cluster,
        queue=queue,
        provider=transport,
        scheduler_provider=scheduler,
    )
    observations: list[str | None] = [None, "21999"]
    query_specs: list[McpCallSpec] = []

    def run_recovery_query(
        _job: RelayJob,
        *,
        task_id: str,
        spool: JobSpool,
        intent: dict[str, Any],
        recovery_spec: McpCallSpec,
        attempt: int,
    ) -> tuple[subprocess.CompletedProcess[str], bytes, Path]:
        del task_id, attempt
        assert intent["execution_id"] == execution_id
        query_specs.append(recovery_spec)
        scheduler_job_id = observations[len(query_specs) - 1]
        document = _execution_query_document(
            spec=recovery_spec,
            server_artifact=server_artifact,
            scheduler_job_id=scheduler_job_id,
            created_at=cast(str, intent["dispatch_started_at"]),
        )
        payload = json.dumps(document, sort_keys=True).encode("utf-8")
        result_path = spool.path / cast(str, intent["result_relative_path"])
        return subprocess.CompletedProcess(["recovery-query"], 0, "", ""), payload, result_path

    monkeypatch.setattr(worker, "_run_jarvis_execution_recovery_query", run_recovery_query)

    first = worker.run_once()

    assert first is not None
    assert first.state is JobState.RUNNING
    assert transport.dispatch_count == 1
    assert len(query_specs) == 1
    assert query_specs[0].tool == "jarvis_get_execution"
    task = queue.list_tasks(job.job_id)[0]
    assert task.state is JobState.RUNNING
    recovery = cast(dict[str, Any], task.metadata["jarvis_execution_recovery"])
    assert recovery["state"] == "pending"
    assert recovery["attempts"] == 1
    assert recovery["last_error"] == (
        "scheduled JARVIS execution is durable but its scheduler identity is not available yet"
    )
    assert recovery["next_retry_at"] is not None
    assert recovery["scheduler_provider"] is None
    assert recovery["scheduler_job_id"] is None
    runtime = cast(dict[str, Any], task.metadata["runtime_metadata"])
    assert runtime["execution_id"] == execution_id
    assert runtime["scheduler_provider"] == "slurm"
    assert runtime["scheduler_job_id"] is None
    assert runtime["details"]["execution_mode"] == "scheduler"
    assert task.metadata["scheduler_job_ids"] == []
    pending, truncated = queue.scan_execution_cleanup(cluster=job.cluster, limit=10)
    assert truncated is False
    assert [marker.task_id for marker in pending] == [task.task_id]
    assert not (settings.spool_dir / job.job_id / "mcp-result.json").exists()
    assert scheduler.polled == []
    assert scheduler.canceled == []

    # A restart scan before the durable deadline must preserve the attempt and backoff.
    cast(Any, worker)._reconcile_pending_execution_cleanup()
    assert len(query_specs) == 1
    assert queue.get_job(job.job_id).state is JobState.RUNNING
    assert queue.get_task(task.task_id).state is JobState.RUNNING

    queue.update_task_metadata(
        task.task_id,
        {
            "jarvis_execution_recovery": {
                **recovery,
                "next_retry_at": recovery["last_attempt_at"],
            }
        },
    )
    cast(Any, worker)._reconcile_pending_execution_cleanup()

    completed_job = queue.get_job(job.job_id)
    completed_task = queue.get_task(task.task_id)
    assert completed_job.state is JobState.SUCCEEDED
    assert completed_task.state is JobState.SUCCEEDED
    assert transport.dispatch_count == 1
    assert len(query_specs) == 2
    assert all(spec.tool == "jarvis_get_execution" for spec in query_specs)
    resolved = cast(dict[str, Any], completed_task.metadata["jarvis_execution_recovery"])
    assert resolved["state"] == "resolved"
    assert resolved["attempts"] == 2
    assert resolved["execution_id"] == execution_id
    assert resolved["scheduler_provider"] == "slurm"
    assert resolved["scheduler_job_id"] == "21999"
    assert resolved["next_retry_at"] is None
    assert completed_task.metadata["scheduler_job_ids"] == ["21999"]
    ownership = cast(list[dict[str, Any]], completed_task.metadata["scheduler_job_ownership"])
    assert len(ownership) == 1
    assert ownership[0]["execution_id"] == execution_id
    assert ownership[0]["scheduler_provider"] == "slurm"
    assert ownership[0]["scheduler_job_id"] == "21999"
    assert ownership[0]["ownership_verified"] is True
    assert scheduler.polled == ["21999"]
    assert scheduler.canceled == []
    assert "cancellation_request" not in completed_job.metadata
    assert queue.get_scheduler_cancel_pending(job.job_id, cluster=job.cluster) is None

    artifact_kinds = [artifact.kind for artifact in queue.list_artifacts(job.job_id)]
    for required_kind in ("mcp_result", "runtime_metadata", "provenance"):
        assert artifact_kinds.count(required_kind) == 1
    mcp_result = json.loads(
        (settings.spool_dir / job.job_id / "mcp-result.json").read_text(encoding="utf-8")
    )
    assert mcp_result["structured_result"]["execution_id"] == execution_id
    assert mcp_result["structured_result"]["scheduler"] is None
    assert mcp_result["structured_result"]["execution_handle"]["scheduler_native_id"] == ("21999")
    query_sha256 = hashlib.sha256(
        json.dumps(
            _execution_query_document(
                spec=query_specs[1],
                server_artifact=server_artifact,
                scheduler_job_id="21999",
                created_at=cast(str, recovery["dispatch_started_at"]),
            ),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    assert mcp_result["relay_recovery"]["source_result_sha256"] == query_sha256
    provenance = json.loads(
        (settings.spool_dir / job.job_id / "provenance.json").read_text(encoding="utf-8")
    )
    assert provenance["runtime_metadata"]["execution_id"] == execution_id
    assert provenance["runtime_metadata"]["scheduler_provider"] == "slurm"
    assert provenance["runtime_metadata"]["scheduler_job_id"] == "21999"
    assert completed_task.metadata["restart_cleanup_acknowledged"] is True
    pending, truncated = queue.scan_execution_cleanup(cluster=job.cluster, limit=10)
    assert pending == []
    assert truncated is False
    events, next_cursor = queue.drain_events(Cursor(job_id=job.job_id), limit=200)
    assert next_cursor.next_seq == events[-1].seq + 1
    event_types = [event.event_type for event in events]
    assert event_types.count("jarvis.execution_recovery_pending") == 1
    assert event_types.count("jarvis.execution_recovered") == 1
    assert event_types.count("mcp.dispatch_recovered") == 1
    assert event_types.count("execution.restart_reconciled") == 1
    assert not any(event_type.startswith("scheduler.cancel") for event_type in event_types)
    pending_event = next(
        event for event in events if event.event_type == "jarvis.execution_recovery_pending"
    )
    assert pending_event.payload["scheduler_cancel_requested"] is False


def test_prelaunch_crash_never_queries_or_adopts_remote_execution(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A prepared but unreleased child is terminal locally without recovery query."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    command = ["locked-clio-kit", "mcp-server", "jarvis"]
    server_artifact = verified_jarvis_server_artifact()
    digest = remote_mcp_server_artifact_digest(server_artifact)
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    job = queue.submit_job(
        RelayJob(
            cluster="research-cluster",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=command[0],
                server_args=command[1:],
                expected_server_artifact_digest=digest,
                expected_jarvis_cd_lock_binding=(
                    endpoint_module.jarvis_cd_lock_binding_expectation()
                ),
                tool="jarvis_run",
                arguments={"pipeline_id": "never-released"},
            ),
            idempotency_key="never-released-dispatch",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster=job.cluster,
        queue=queue,
        provider=_LostRunResponseProvider(release_dispatch=False),
        scheduler_provider=_RecordingSlurmProvider(),
    )
    query_attempts: list[object] = []

    def record_unexpected_query(*args: object, **kwargs: object) -> None:
        query_attempts.append((args, kwargs))

    monkeypatch.setattr(
        worker,
        "_run_jarvis_execution_recovery_query",
        record_unexpected_query,
    )

    first = worker.run_once()
    assert first is not None
    assert first.state is JobState.RUNNING
    task = queue.list_tasks(job.job_id)[0]
    intent = cast(dict[str, object], task.metadata["jarvis_execution_recovery"])
    assert intent["dispatch_state"] == "prepared"
    assert intent["dispatch_started_at"] is None

    cast(Any, worker)._reconcile_pending_execution_cleanup()

    assert query_attempts == []
    assert queue.get_task(task.task_id).state is JobState.FAILED
    assert queue.get_job(job.job_id).state is JobState.FAILED
    pending, truncated = queue.scan_execution_cleanup(cluster=job.cluster, limit=10)
    assert pending == []
    assert truncated is False
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    assert "jarvis.dispatch_not_released" in {event.event_type for event in events}
