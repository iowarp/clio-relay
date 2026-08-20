"""clio-relay#266: the job IS the run -- watch a deferred jarvis_run to terminal.

Two layers:

* Pure-function unit coverage for ``clio_relay.execution_watch`` (phase
  mapping, deferred detection, deadline, failure detail/text).
* End-to-end ``EndpointWorker`` lifecycle coverage using a fake transport
  provider (same pattern as ``test_jarvis_lost_response_recovery.py``'s
  ``_LostRunResponseProvider``) that fabricates the ``jarvis_run`` dispatch
  and every subsequent ``jarvis_get_execution`` poll without a real JARVIS
  MCP server, driving a fake execution through
  queued -> running -> completed (and, in a second test, -> failed).
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from pytest import MonkeyPatch

from clio_relay import endpoint_jarvis_recovery, execution_watch
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
    deterministic_jarvis_execution_id,
)
from clio_relay.remote_mcp import remote_mcp_server_artifact_digest
from clio_relay.runtime_metadata import JarvisRuntimeMetadata, TerminalRuntimeMetadata
from tests.execution_watch_fakes import (
    native_execution_documents,
    query_result_document,
    run_dispatch_document,
)
from tests.jarvis_mcp_fakes import verified_jarvis_server_artifact

# --------------------------------------------------------------------------
# Pure-function unit coverage.
# --------------------------------------------------------------------------


def _metadata(
    *,
    state: str | None,
    terminal: bool | None,
    **terminal_kwargs: Any,
) -> JarvisRuntimeMetadata:
    return JarvisRuntimeMetadata(
        source="jarvis_mcp",
        execution_id="jarvis_exec",
        pipeline_id="pipe",
        scheduler_provider="slurm",
        scheduler_job_id="9001",
        terminal=TerminalRuntimeMetadata(state=state, terminal=terminal, **terminal_kwargs),
    )


def test_deferred_jarvis_execution_detects_nonterminal_only() -> None:
    """Only ``terminal.terminal is False`` starts a watch."""
    deferred = execution_watch.deferred_jarvis_execution(
        _metadata(state="submitted", terminal=False)
    )
    assert deferred == execution_watch.DeferredJarvisExecution(
        pipeline_id="pipe", execution_id="jarvis_exec"
    )


@pytest.mark.parametrize("terminal", [True, None])
def test_deferred_jarvis_execution_ignores_terminal_or_unknown(terminal: bool | None) -> None:
    """A terminal (today's fast path) or unset terminal flag never starts a watch."""
    assert (
        execution_watch.deferred_jarvis_execution(_metadata(state="completed", terminal=terminal))
        is None
    )


def test_deferred_jarvis_execution_requires_identity() -> None:
    """A native snapshot missing execution/pipeline identity is never watched."""
    metadata = _metadata(state="submitted", terminal=False).model_copy(
        update={"execution_id": None}
    )
    assert execution_watch.deferred_jarvis_execution(metadata) is None


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ("preparing", "queued"),
        ("scripted", "queued"),
        ("submitting", "queued"),
        ("submitted", "queued"),
        ("running", "running"),
        (None, "jarvis_state:unknown"),
        ("a_future_jarvis_state", "jarvis_state:a_future_jarvis_state"),
    ],
)
def test_execution_phase_for_state_typed_passthrough(
    state: str | None,
    expected: str,
) -> None:
    """Every JARVIS state maps to a typed phase; unknown states pass through, never drop."""
    assert execution_watch.execution_phase_for_state(state) == expected


def test_execution_phase_job_metadata_shape() -> None:
    """The job-metadata payload is schema-versioned and carries the raw jarvis state."""
    metadata = _metadata(state="running", terminal=False)
    payload = execution_watch.execution_phase_job_metadata(
        metadata,
        poll_count=3,
        observed_at=datetime(2026, 8, 20, tzinfo=UTC),
    )
    assert payload["schema_version"] == execution_watch.EXECUTION_PHASE_SCHEMA
    assert payload["phase"] == "running"
    assert payload["jarvis_state"] == "running"
    assert payload["terminal"] is False
    assert payload["execution_id"] == "jarvis_exec"
    assert payload["pipeline_id"] == "pipe"
    assert payload["scheduler_provider"] == "slurm"
    assert payload["scheduler_job_id"] == "9001"
    assert payload["poll_count"] == 3
    assert payload["observed_at"] == "2026-08-20T00:00:00+00:00"


def test_execution_watch_query_spec_omits_artifacts_by_default() -> None:
    """Intermediate polls never request artifacts; the terminal poll does."""
    base = McpCallSpec(
        server="clio-kit",
        server_args=["mcp-server", "jarvis"],
        env_from={"TOKEN": "SOURCE"},
        expected_server_artifact_digest="a" * 64,
        tool="jarvis_run",
        arguments={"pipeline_id": "p", "execution_id": "e"},
    )
    poll = execution_watch.execution_watch_query_spec(
        base,
        pipeline_id="p",
        execution_id="e",
        include_artifacts=False,
        timeout_seconds=60,
    )
    assert poll.tool == "jarvis_get_execution"
    assert poll.arguments == {"pipeline_id": "p", "execution_id": "e"}
    assert poll.env_from == {"TOKEN": "SOURCE"}
    assert poll.expected_server_artifact_digest == "a" * 64

    final = execution_watch.execution_watch_query_spec(
        base,
        pipeline_id="p",
        execution_id="e",
        include_artifacts=True,
        timeout_seconds=60,
    )
    assert final.arguments == {
        "pipeline_id": "p",
        "execution_id": "e",
        "artifacts": {"page_size": execution_watch.EXECUTION_WATCH_TERMINAL_ARTIFACT_PAGE_SIZE},
    }


def test_execution_watch_deadline_anchors_to_supplied_time() -> None:
    """The deadline is anchor + ceiling, not tied to wall-clock call time."""
    anchor = datetime(2026, 8, 20, 12, 0, 0, tzinfo=UTC)
    deadline = execution_watch.execution_watch_deadline(anchor, ceiling_seconds=3600)
    assert deadline == anchor + timedelta(seconds=3600)


def test_execution_watch_succeeded_only_for_completed() -> None:
    assert execution_watch.execution_watch_succeeded(_metadata(state="completed", terminal=True))
    assert not execution_watch.execution_watch_succeeded(_metadata(state="failed", terminal=True))
    assert not execution_watch.execution_watch_succeeded(_metadata(state="canceled", terminal=True))


def test_execution_watch_failure_detail_and_error_text() -> None:
    metadata = _metadata(
        state="failed",
        terminal=True,
        returncode=1,
        reason="application exited nonzero",
    )
    detail = execution_watch.execution_watch_failure_detail(metadata)
    assert detail == {
        "schema_version": execution_watch.EXECUTION_WATCH_FAILURE_SCHEMA,
        "pipeline_id": "pipe",
        "execution_id": "jarvis_exec",
        "state": "failed",
        "returncode": 1,
        "reason": "application exited nonzero",
    }
    assert execution_watch.execution_watch_error_text(detail) == (
        "JARVIS execution jarvis_exec ended in failed: application exited nonzero"
    )
    detail_no_reason = {**detail, "reason": None}
    assert execution_watch.execution_watch_error_text(detail_no_reason) == (
        "JARVIS execution jarvis_exec ended in failed"
    )


def test_execution_cancel_unsupported_payload() -> None:
    payload = execution_watch.execution_cancel_unsupported_payload(
        pipeline_id="pipe",
        execution_id="jarvis_exec",
    )
    assert payload["reason"] == execution_watch.CANCEL_UNSUPPORTED_REASON
    assert payload["schema_version"] == execution_watch.EXECUTION_CANCEL_REFUSAL_SCHEMA


# --------------------------------------------------------------------------
# End-to-end EndpointWorker lifecycle coverage.
# --------------------------------------------------------------------------


class _WatchTransportProvider(JarvisCdProvider):
    """Fabricate a ``jarvis_run`` dispatch and every ``jarvis_get_execution`` poll.

    ``states`` is the sequence of ``(state, terminal, scheduler_job_id)``
    tuples the fake execution moves through on each poll (the FIRST entry
    is what the initial ``jarvis_run`` dispatch itself reports); polling
    past the end of the list repeats the last entry.
    """

    def __init__(
        self,
        *,
        pipeline_id: str,
        execution_id: str,
        states: list[tuple[str, bool, str | None]],
        server_artifact: dict[str, Any],
        execution_root: Path,
        created_at: str,
        return_code: int | None = 0,
        error: str | None = None,
        cancel_on_poll_index: int | None = None,
        queue: ClioCoreQueue | None = None,
        job_id: str | None = None,
    ) -> None:
        super().__init__(jarvis_bin="jarvis")
        self.pipeline_id = pipeline_id
        self.execution_id = execution_id
        self.states = states
        self.server_artifact = server_artifact
        self.execution_root = execution_root
        self.created_at = created_at
        self.return_code = return_code
        self.error = error
        self.cancel_on_poll_index = cancel_on_poll_index
        self.queue = queue
        self.job_id = job_id
        self.dispatch_count = 0
        self.poll_count = 0
        self.poll_specs: list[McpCallSpec] = []

    def _native(self, index: int) -> dict[str, object]:
        state, terminal, scheduler_job_id = self.states[min(index, len(self.states) - 1)]
        return native_execution_documents(
            pipeline_id=self.pipeline_id,
            execution_id=self.execution_id,
            state=state,
            terminal=terminal,
            scheduler_job_id=scheduler_job_id,
            created_at=self.created_at,
            execution_root=self.execution_root,
            return_code=self.return_code if terminal else None,
            error=self.error if terminal else None,
        )

    def run_command_streaming(
        self,
        command: list[str],
        *,
        process_label: str = "JARVIS-CD",
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        credential_payload: str | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        on_start: Callable[[int], None] | None = None,
        should_cancel: Callable[[], bool] | None = None,
        on_poll: Callable[[], None] | None = None,
        timeout_seconds: int | None = None,
        on_timeout: Callable[[], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        del command, credential_payload, should_cancel, timeout_seconds, on_timeout
        assert cwd is not None
        if on_start is not None:
            probe = subprocess.Popen([sys.executable, "-c", "pass"])
            on_start(probe.pid)
            probe.wait(timeout=10)
        if process_label == "endpoint MCP operation":
            self.dispatch_count += 1
            request = json.loads((cwd / "mcp-request.json").read_text(encoding="utf-8"))
            run_spec = McpCallSpec.model_validate(request)
            document = run_dispatch_document(
                run_spec=run_spec,
                server_artifact=self.server_artifact,
                native=self._native(0),
            )
            (cwd / "mcp-result.json").write_text(json.dumps(document), encoding="utf-8")
            if on_stdout is not None:
                on_stdout("jarvis_run accepted; execution submitted\n")
            if on_poll is not None:
                on_poll()
            return subprocess.CompletedProcess(["jarvis-run"], 0, "", "")
        assert process_label == "jarvis execution watch query"
        params = json.loads((cwd / "params.json").read_text(encoding="utf-8"))
        query_spec = McpCallSpec.model_validate(params)
        self.poll_specs.append(query_spec)
        include_artifacts = "artifacts" in query_spec.arguments
        index = self.poll_count if not include_artifacts else self.poll_count - 1
        if (
            self.cancel_on_poll_index is not None
            and self.poll_count == self.cancel_on_poll_index
            and self.queue is not None
            and self.job_id is not None
        ):
            self.queue.cancel_job_if_active(self.job_id, cancel_scheduler=False)
        self.poll_count += 1
        document = query_result_document(
            query_spec=query_spec,
            server_artifact=self.server_artifact,
            native=self._native(index),
            include_artifacts=include_artifacts,
        )
        (cwd / "mcp-result.json").write_text(json.dumps(document), encoding="utf-8")
        return subprocess.CompletedProcess(["jarvis-get-execution"], 0, "", "")


def _event_types(queue: ClioCoreQueue, job_id: str) -> list[str]:
    events, _cursor = queue.drain_events(Cursor(job_id=job_id), limit=500)
    return [event.event_type for event in events]


def _submit_watch_job(
    queue: ClioCoreQueue,
    *,
    command: list[str],
    digest: str,
) -> tuple[RelayJob, str]:
    submission = RelayJob(
        cluster="watch-test",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server=command[0],
            server_args=command[1:],
            expected_server_artifact_digest=digest,
            expected_jarvis_cd_lock_binding=endpoint_jarvis_recovery.jarvis_cd_lock_binding_expectation(),
            tool="jarvis_run",
            arguments={"pipeline_id": "watch-pipeline"},
        ),
        idempotency_key="watch-stable-execution",
    )
    job = queue.submit_job(submission)
    assert isinstance(job.spec, McpCallSpec)
    execution_id = deterministic_jarvis_execution_id(
        cluster=job.cluster,
        idempotency_key=job.idempotency_key,
        job_id=job.job_id,
    )
    assert job.spec.arguments["execution_id"] == execution_id
    return job, execution_id


@pytest.fixture
def _watch_env(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str]:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        execution_watch_poll_interval_seconds=0.01,
    )
    queue = ClioCoreQueue(settings.core_dir)
    command = ["locked-clio-kit", "mcp-server", "jarvis"]
    server_artifact = verified_jarvis_server_artifact()
    digest = remote_mcp_server_artifact_digest(server_artifact)
    monkeypatch.setattr(endpoint_jarvis_recovery, "jarvis_mcp_command", lambda: command)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    return settings, queue, command, server_artifact, digest


def test_deferred_execution_watched_to_success(
    tmp_path: Path,
    _watch_env: tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str],
) -> None:
    """Queued -> running -> completed: job stays working, console accumulates, terminal maps."""
    settings, queue, command, server_artifact, digest = _watch_env
    job, execution_id = _submit_watch_job(queue, command=command, digest=digest)
    execution_root = tmp_path / "execution-root"
    execution_root.mkdir()
    (execution_root / "stdout.log").write_bytes(b"application booted\napplication running\n")
    created_at = job.created_at.isoformat()
    transport = _WatchTransportProvider(
        pipeline_id="watch-pipeline",
        execution_id=execution_id,
        states=[
            ("submitted", False, None),
            ("submitted", False, "9001"),
            ("running", False, "9001"),
            ("running", False, "9001"),
            ("completed", True, "9001"),
        ],
        server_artifact=server_artifact,
        execution_root=execution_root,
        created_at=created_at,
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster=job.cluster,
        queue=queue,
        provider=transport,
    )

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.SUCCEEDED
    assert transport.dispatch_count == 1
    # 4 intermediate polls (submitted, submitted, running, running) that
    # observe the loop-breaking terminal "completed" record on the 5th
    # intermediate poll, plus one FINAL artifact-bearing poll -- 6 total.
    assert transport.poll_count == 6
    assert all(spec.tool == "jarvis_get_execution" for spec in transport.poll_specs)
    final_phase = cast(dict[str, Any], queue.get_job(job.job_id).metadata["execution_phase"])
    assert final_phase["jarvis_state"] == "completed"
    assert final_phase["terminal"] is True
    events = _event_types(queue, job.job_id)
    assert events.count("execution.watch_started") == 1
    assert "execution.queued" in events
    assert "execution.running" in events
    assert events.count("execution.watch_resolved") == 1
    artifact_kinds = [artifact.kind for artifact in queue.list_artifacts(job.job_id)]
    for required_kind in ("mcp_result", "runtime_metadata", "provenance", "console"):
        assert artifact_kinds.count(required_kind) == 1
    console_bytes = (settings.spool_dir / job.job_id / "console.log").read_bytes()
    assert b"application running" in console_bytes
    mcp_result = json.loads(
        (settings.spool_dir / job.job_id / "mcp-result.json").read_text(encoding="utf-8")
    )
    assert mcp_result["structured_result"]["execution_record"]["state"] == "completed"
    assert mcp_result["structured_result"]["execution_id"] == execution_id


def test_deferred_execution_watched_to_failure(
    tmp_path: Path,
    _watch_env: tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str],
) -> None:
    """#265's negative path: a failed execution lands the job FAILED with a typed reason."""
    settings, queue, command, server_artifact, digest = _watch_env
    job, execution_id = _submit_watch_job(queue, command=command, digest=digest)
    execution_root = tmp_path / "execution-root"
    execution_root.mkdir()
    (execution_root / "stdout.log").write_bytes(b"application crashed\n")
    transport = _WatchTransportProvider(
        pipeline_id="watch-pipeline",
        execution_id=execution_id,
        states=[
            ("submitted", False, "9002"),
            ("running", False, "9002"),
            ("failed", True, "9002"),
        ],
        server_artifact=server_artifact,
        execution_root=execution_root,
        created_at=job.created_at.isoformat(),
        return_code=1,
        error="application exited with code 137",
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster=job.cluster,
        queue=queue,
        provider=transport,
    )

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.FAILED
    assert result.last_error is not None
    assert "application exited with code 137" in result.last_error
    task = queue.list_tasks(job.job_id)[0]
    assert task.state is JobState.FAILED
    watch_failure = cast(dict[str, Any], task.metadata["execution_watch_failure"])
    assert watch_failure["state"] == "failed"
    assert watch_failure["reason"] == "application exited with code 137"


def test_synchronous_terminal_dispatch_skips_watch(
    tmp_path: Path,
    _watch_env: tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str],
) -> None:
    """Sabotage twin: an already-terminal jarvis_run result keeps today's fast path."""
    settings, queue, command, server_artifact, digest = _watch_env
    job, execution_id = _submit_watch_job(queue, command=command, digest=digest)
    execution_root = tmp_path / "execution-root"
    execution_root.mkdir()
    (execution_root / "stdout.log").write_bytes(b"ran and finished synchronously\n")
    transport = _WatchTransportProvider(
        pipeline_id="watch-pipeline",
        execution_id=execution_id,
        states=[("completed", True, "9099")],
        server_artifact=server_artifact,
        execution_root=execution_root,
        created_at=job.created_at.isoformat(),
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster=job.cluster,
        queue=queue,
        provider=transport,
    )

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.SUCCEEDED
    assert transport.dispatch_count == 1
    assert transport.poll_count == 0  # the watch never engaged
    events = _event_types(queue, job.job_id)
    assert "execution.watch_started" not in events
    assert "execution_phase" not in queue.get_job(job.job_id).metadata


def test_watch_ceiling_exceeded_fails_the_job(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    _watch_env: tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str],
) -> None:
    """A watch that never reaches terminal within its ceiling fails typed, not silently."""
    settings, queue, command, server_artifact, digest = _watch_env
    job, execution_id = _submit_watch_job(queue, command=command, digest=digest)
    execution_root = tmp_path / "execution-root"
    execution_root.mkdir()
    (execution_root / "stdout.log").write_bytes(b"still running\n")
    transport = _WatchTransportProvider(
        pipeline_id="watch-pipeline",
        execution_id=execution_id,
        states=[("running", False, "9003")],
        server_artifact=server_artifact,
        execution_root=execution_root,
        created_at=job.created_at.isoformat(),
    )
    monkeypatch.setattr(
        execution_watch,
        "execution_watch_deadline",
        lambda anchor, *, ceiling_seconds: anchor - timedelta(seconds=1),
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster=job.cluster,
        queue=queue,
        provider=transport,
    )

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.FAILED
    assert "ExecutionWatchCeilingExceeded" in (result.last_error or "")
    events = _event_types(queue, job.job_id)
    assert "execution.watch_ceiling_exceeded" in events
    # The ceiling trips before any poll is dispatched.
    assert transport.poll_count == 0


def test_cancellation_during_watch_is_refused_typed_and_watch_continues(
    tmp_path: Path,
    _watch_env: tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str],
) -> None:
    """JARVIS exposes no cancel surface: refuse typed, keep watching to the real outcome."""
    settings, queue, command, server_artifact, digest = _watch_env
    job, execution_id = _submit_watch_job(queue, command=command, digest=digest)
    execution_root = tmp_path / "execution-root"
    execution_root.mkdir()
    (execution_root / "stdout.log").write_bytes(b"running despite a cancel request\n")
    transport = _WatchTransportProvider(
        pipeline_id="watch-pipeline",
        execution_id=execution_id,
        states=[
            ("submitted", False, "9004"),
            ("running", False, "9004"),
            ("completed", True, "9004"),
        ],
        server_artifact=server_artifact,
        execution_root=execution_root,
        created_at=job.created_at.isoformat(),
        # Request cancellation from INSIDE the watch's 1st poll (the job is
        # already RUNNING by then), so the loop's next iteration observes
        # it and refuses it typed BEFORE the terminal (3rd) poll -- the
        # watch must still observe every remaining poll and resolve on the
        # REAL outcome rather than silently reporting the job canceled.
        cancel_on_poll_index=1,
        queue=queue,
        job_id=job.job_id,
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster=job.cluster,
        queue=queue,
        provider=transport,
    )

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.SUCCEEDED
    events = _event_types(queue, job.job_id)
    assert f"execution.{execution_watch.CANCEL_UNSUPPORTED_REASON}" in events
    assert events.count(f"execution.{execution_watch.CANCEL_UNSUPPORTED_REASON}") == 1
    assert "execution.canceled" not in events
