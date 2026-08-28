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

import hashlib
import json
import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from pytest import MonkeyPatch

from clio_relay import application_runtime_prediction, endpoint_jarvis_recovery, execution_watch
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.jarvis_execution_artifacts import resolve_jarvis_run_owner_by_execution_id
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import (
    Cursor,
    EndpointRole,
    JobKind,
    JobState,
    McpCallSpec,
    ProgressRecord,
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
    # clio-relay#265 + #183 residual: a DISTINCT application_verdict key,
    # never conflated with the scheduler-only "phase"/"jarvis_state" above.
    verdict = cast(dict[str, Any], payload["application_verdict"])
    assert verdict["schema_version"] == execution_watch.APPLICATION_VERDICT_SCHEMA
    assert verdict["status"] == "unknown"
    assert verdict["reason"] == "execution_not_terminal"
    # clio-relay#214: the restored runtime-prediction capability rides beside
    # application_verdict on the same payload; no progress_history was
    # supplied, so it is a typed absence, never an error or a fabrication.
    prediction = cast(dict[str, Any], payload["application_runtime_prediction"])
    assert (
        prediction["schema_version"]
        == application_runtime_prediction.APPLICATION_RUNTIME_PREDICTION_SCHEMA
    )
    assert prediction["status"] == "absent"
    assert prediction["reason"] == application_runtime_prediction.NO_PROGRESS_OBSERVATIONS_REASON


def test_execution_phase_job_metadata_composes_runtime_prediction_from_progress() -> None:
    """A job's own structured progress history, when supplied, grounds a real prediction.

    clio-relay#214: the runtime prediction is composed onto the SAME phase
    payload as ``application_verdict`` -- additive, never replacing the
    phase/jarvis_state fields ``test_execution_phase_job_metadata_shape``
    above already covers.
    """
    metadata = _metadata(state="running", terminal=False)
    epoch = datetime(2026, 8, 20, tzinfo=UTC).timestamp()
    progress_history = [
        ProgressRecord(
            job_id="job-execphase-prediction",
            label="timestep",
            current=step,
            total=100,
            # clio-relay#214 review D3: created_at is a batch-persistence
            # instant here (all identical); the source-reported
            # progress_observed_at_epoch is the real, spread-out clock.
            created_at=datetime(2026, 8, 20, tzinfo=UTC),
            metadata={"progress_observed_at_epoch": epoch + step},
        )
        for step in (0, 10, 20, 30, 40)
    ]
    runtime_prediction = application_runtime_prediction.application_runtime_prediction_for_progress(
        progress_history
    )
    payload = execution_watch.execution_phase_job_metadata(
        metadata,
        poll_count=5,
        observed_at=datetime(2026, 8, 20, tzinfo=UTC),
        runtime_prediction=runtime_prediction,
    )
    prediction = cast(dict[str, Any], payload["application_runtime_prediction"])
    assert prediction["status"] == "predicted"
    assert prediction["reason"] is None
    assert prediction["predicted_remaining_seconds"] == 60.0
    assert prediction["confidence"] == "observed"
    basis = cast(dict[str, Any], prediction["basis"])
    assert basis["clock"] == "progress_observed_at_epoch"
    # application_verdict is still present and unaffected by the addition.
    assert cast(dict[str, Any], payload["application_verdict"])["status"] == "unknown"


@pytest.mark.parametrize(
    ("state", "terminal", "returncode", "reason", "expected_status", "expected_reason"),
    [
        ("completed", True, 0, None, "success", None),
        ("failed", True, 1, "application exited nonzero", "failed", "application exited nonzero"),
        ("canceled", True, None, None, "failed", "jarvis_state:canceled"),
        ("running", False, None, None, "unknown", "execution_not_terminal"),
        (None, None, None, None, "unknown", "execution_not_terminal"),
        ("a_future_jarvis_state", True, 0, None, "unknown", "jarvis_state:a_future_jarvis_state"),
        # Adversarial-review Ruling A: state says "completed" (launcher
        # exited cleanly) but returncode disagrees -- a self-contradiction
        # loose/legacy metadata can produce (the strict native-document
        # contract's own cross-field validator forbids this shape outright
        # on the main watched path). Must never read as "success".
        ("completed", True, 3, None, "failed", "returncode_conflict"),
    ],
)
def test_application_verdict_for_metadata_is_distinct_from_scheduler_state(
    state: str | None,
    terminal: bool | None,
    returncode: int | None,
    reason: str | None,
    expected_status: str,
    expected_reason: str | None,
) -> None:
    """#265's own issue text: scheduler rc=0 must never read as application success.

    ``application_verdict_for_metadata`` answers a DIFFERENT question than
    ``execution_watch_succeeded`` (below) -- it never fabricates a signal
    JARVIS did not report, so a terminal-but-unrecognized state (a future
    JARVIS state) or a non-terminal execution both report "unknown", not a
    guessed success/failure.
    """
    metadata = _metadata(state=state, terminal=terminal, returncode=returncode, reason=reason)
    verdict = execution_watch.application_verdict_for_metadata(metadata)
    assert verdict == {
        "schema_version": execution_watch.APPLICATION_VERDICT_SCHEMA,
        "status": expected_status,
        "application_returncode": returncode,
        "reason": expected_reason,
    }


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


def _clean_verdict(status: str = "success") -> dict[str, object]:
    """A non-conflicting application_verdict for outputs_missing-focused cases."""
    return {
        "schema_version": execution_watch.APPLICATION_VERDICT_SCHEMA,
        "status": status,
        "application_returncode": 0 if status == "success" else 1,
        "reason": None if status == "success" else "application exited nonzero",
    }


@pytest.mark.parametrize(
    (
        "watch_succeeded",
        "outputs_missing",
        "expected_returncode",
        "expected_cancellation",
    ),
    [
        # Legacy/pre-Ruling-B shape (no "reason" key at all): a SURFACED
        # SIGNAL only, same as every other reason below -- never forces
        # failure. existence/size heuristics deciding success/failure are
        # banned; only the returncode/application_verdict may decide that.
        (True, {"schema_version": "clio-relay.execution-outputs-missing.v1"}, 0, False),
        # declared_outputs_missing (one or more DECLARED outputs found
        # missing/empty on disk -- the exact LAMMPS live-defect shape: a
        # clean rc=0 run whose declared stderr.log is legitimately empty)
        # is ALSO signal-only now -- the owner ruling that superseded this
        # module's own earlier "producing declared outputs is part of what
        # completed means" stance (jarvis_execution_artifacts.py's module
        # docstring narrates the full history).
        (True, {"reason": "declared_outputs_missing"}, 0, False),
        # no_outputs_declared: unchanged -- was already signal-only.
        (True, {"reason": "no_outputs_declared"}, 0, False),
        # No outputs_missing verdict: the watch's own success stands.
        (True, None, 0, False),
        # A genuinely failed watch stays failed regardless of outputs_missing
        # -- the guard: a real application failure is never masked BY, nor
        # dependent on, the outputs-missing signal.
        (False, None, 1, False),
        (False, {"reason": "declared_outputs_missing"}, 1, False),
    ],
)
def test_resolve_execution_outcome_folds_outputs_missing(
    watch_succeeded: bool,
    outputs_missing: dict[str, object] | None,
    expected_returncode: int,
    expected_cancellation: bool,
) -> None:
    resolution = execution_watch.ExecutionWatchResolution(
        succeeded=watch_succeeded,
        failure_detail=None if watch_succeeded else {"schema_version": "x"},
        application_verdict=_clean_verdict("success" if watch_succeeded else "failed"),
    )
    outcome = execution_watch.resolve_execution_outcome(
        dispatch_recovered=False,
        watch_resolution=resolution,
        dispatch_refusal_present=False,
        transport_returncode=0,
        cancellation_requested=True,
        outputs_missing=outputs_missing,
    )
    assert outcome.effective_returncode == expected_returncode
    assert outcome.cancellation_honored is expected_cancellation
    # The RAW signal is always carried unconditionally -- it never drives
    # effective_returncode, but it must never be silently dropped either.
    assert outcome.outputs_missing == outputs_missing


def test_resolve_execution_outcome_folds_returncode_conflict() -> None:
    """Ruling A: a returncode_conflict verdict fails the job even though the
    watch's own state-only ``succeeded`` said otherwise -- defense-in-depth
    for loose/legacy metadata.
    """
    resolution = execution_watch.ExecutionWatchResolution(
        succeeded=True,
        failure_detail=None,
        application_verdict={
            "schema_version": execution_watch.APPLICATION_VERDICT_SCHEMA,
            "status": "failed",
            "application_returncode": 3,
            "reason": execution_watch.RETURNCODE_CONFLICT_REASON,
        },
    )
    outcome = execution_watch.resolve_execution_outcome(
        dispatch_recovered=False,
        watch_resolution=resolution,
        dispatch_refusal_present=False,
        transport_returncode=0,
        cancellation_requested=True,
        outputs_missing=None,
    )
    assert outcome.effective_returncode == 1
    assert outcome.cancellation_honored is False


def test_execution_phase_status_message_names_a_failed_application_verdict() -> None:
    """Ruling A: a run card polling mid-run sees the application's own
    trouble, not only the scheduler phase.
    """
    phase_ok = {
        "phase": "running",
        "application_verdict": _clean_verdict("success"),
    }
    assert execution_watch.execution_phase_status_message("running", phase_ok) == (
        "Relay job is running; jarvis execution is running"
    )
    phase_conflicted = {
        "phase": "running",
        "application_verdict": {
            "schema_version": execution_watch.APPLICATION_VERDICT_SCHEMA,
            "status": "failed",
            "application_returncode": 3,
            "reason": execution_watch.RETURNCODE_CONFLICT_REASON,
        },
    }
    message = execution_watch.execution_phase_status_message("running", phase_conflicted)
    assert message == (
        "Relay job is running; jarvis execution is running; application failed "
        "(returncode_conflict)"
    )


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
        terminal_artifacts: list[dict[str, object]] | None = None,
        dispatch_returncode: int = 0,
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
        # clio-relay#265: declared execution-file entries the FINAL,
        # artifact-bearing poll reports -- None keeps the pre-#265 empty
        # ``artifacts: []`` default.
        self.terminal_artifacts = terminal_artifacts
        # clio-relay#265 item 2 (adversarial-review): the OUTER transport
        # returncode (this subprocess's own exit code) is a SEPARATE concept
        # from the document-internal ``return_code`` above -- the document
        # can be a trusted, terminal, successful native execution while the
        # dispatch subprocess itself still exits nonzero. Defaults to 0
        # (today's behavior) for every other test using this fixture.
        self.dispatch_returncode = dispatch_returncode
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
        if process_label == "jarvis pipeline precheck query":
            # clio-relay#162: every jarvis_run dispatch is preceded by a
            # pipeline-emptiness precheck query. This fake never answers it
            # (no mcp-result.json written, nonzero returncode), so the
            # precheck is always INCONCLUSIVE and #266's watch behavior
            # below -- what this fixture exists to test -- is unaffected.
            return subprocess.CompletedProcess(["jarvis-describe"], 1, "", "")
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
            dispatch_record = cast(dict[str, Any], self._native(0)["execution_record"])
            if self.terminal_artifacts is not None and dispatch_record["terminal"]:
                # clio-relay#265 item 2 (adversarial-review failing-first
                # coverage): a real ``jarvis_run`` dispatch response never
                # carries ``artifact_page`` (only ``jarvis_get_execution``
                # declares it -- see jarvis_execution_artifacts.py's own
                # module docstring), so this branch is normally dead for
                # every OTHER test using this fixture (``terminal_artifacts``
                # is ``None`` there). Injecting it here, ONLY when a test
                # opts in, proves ``ingest_jarvis_execution_outputs`` /
                # ``resolve_execution_outcome`` / the renderers handle a
                # synchronously-terminal dispatch (``watch_resolution is
                # None``, so ``effective_returncode`` comes from the
                # TRANSPORT returncode, not any watch/application verdict)
                # that ALSO happens to declare a terminal artifact page --
                # the one combination that lets a nonzero transport
                # returncode and a present-but-non-forcing
                # ``no_outputs_declared`` signal coexist, which is exactly
                # the shape the proven Ruling B hijack bug needed.
                structured_result = cast(dict[str, object], document["structured_result"])
                structured_result["artifact_page"] = {
                    "artifacts": self.terminal_artifacts,
                    "terminal": True,
                }
            (cwd / "mcp-result.json").write_text(json.dumps(document), encoding="utf-8")
            if on_stdout is not None:
                on_stdout("jarvis_run accepted; execution submitted\n")
            if on_poll is not None:
                on_poll()
            return subprocess.CompletedProcess(["jarvis-run"], self.dispatch_returncode, "", "")
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
            artifacts=self.terminal_artifacts if include_artifacts else None,
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


def _present_output_artifacts(path: Path, *, relative: str) -> list[dict[str, object]]:
    """Declare ``path`` (already written) as one present, non-empty output.

    clio-relay#265 D1: a "completed" terminal poll with NO declared
    execution-file entries is itself now a typed outputs_missing verdict
    (``no_outputs_declared``) -- every watch test in this file whose fake
    execution genuinely succeeds must declare at least one real, present
    output, or it now (correctly) fails typed instead of succeeding.
    """
    payload = path.read_bytes()
    return [
        {
            "package_id": "jarvis.execution",
            "kind": "execution-file",
            "role": "log",
            "location": {"kind": "execution_path", "value": relative},
            "size_bytes": len(payload),
            "checksum": f"sha256:{hashlib.sha256(payload).hexdigest()}",
        }
    ]


@pytest.fixture
def _watch_env(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str]:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        execution_watch_poll_interval_seconds=0.01,
        # CI-hang guard (not a production ceiling change -- this settings
        # instance is test-local; the real DEFAULT_EXECUTION_WATCH_CEILING_
        # SECONDS, 24h, is untouched everywhere else). Every watch test in
        # this file except test_watch_ceiling_exceeded_fails_the_job relies
        # ENTIRELY on its fake's ``states`` list reaching terminal within a
        # handful of polls -- nothing previously bounded a test whose fake
        # (present or future, known bug or not) fails to converge. Without
        # this, `now=utc_now` (the REAL wall clock, unmocked here) plus
        # ``run_execution_watch``'s ``sleep`` parameter -- which defaults to
        # the real ``time.sleep`` bound at function-definition time, NOT the
        # patched one below, since a default argument is evaluated once at
        # import time and this fixture's own monkeypatch cannot reach back
        # into it -- means a non-converging fake would tick toward the REAL
        # 24h default for up to 24 real hours, exactly the CI-hour-hang
        # symptom this value exists to convert into a fast, loud pytest
        # failure instead. 10s gives >100x headroom over every test's
        # actual poll count (<=6) at the 0.01s poll interval above, even
        # under a slow/loaded CI runner.
        execution_watch_ceiling_seconds=10,
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
    # clio-relay#259 residual: the application's stderr must be live-tailed
    # and terminal-flushed the SAME way stdout is, into its own channel.
    (execution_root / "stderr.log").write_bytes(b"warning: low disk\n")
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
        # clio-relay#265 D1: a genuinely successful run declares its real
        # output, or a zero-declared-outputs "completed" now (correctly)
        # fails typed instead of reading green.
        terminal_artifacts=_present_output_artifacts(
            execution_root / "stdout.log", relative="stdout.log"
        ),
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
    # clio-relay#214: the real end-to-end watch loop reaches the bounded
    # ExecutionWatchPredictionTracker without error; this fake job never
    # appends progress, so a typed absence (not a crash, not a fabricated
    # number) is what a real deferred job with no progress adapter wired
    # in yet gets today.
    prediction = cast(dict[str, Any], final_phase["application_runtime_prediction"])
    assert prediction["status"] == "absent"
    assert prediction["reason"] == application_runtime_prediction.NO_PROGRESS_OBSERVATIONS_REASON
    events = _event_types(queue, job.job_id)
    assert events.count("execution.watch_started") == 1
    assert "execution.queued" in events
    assert "execution.running" in events
    assert events.count("execution.watch_resolved") == 1
    # clio-relay#214 review D2: an unchanging (still-absent) prediction
    # across polls is never material -- the tracker's cheap probe skips
    # recompute entirely once nothing has changed, so this event never
    # fires for a job with no progress history at all.
    assert "execution.runtime_prediction_updated" not in events
    artifact_kinds = [artifact.kind for artifact in queue.list_artifacts(job.job_id)]
    for required_kind in (
        "mcp_result",
        "runtime_metadata",
        "provenance",
        "console",
        "console_stderr",
    ):
        assert artifact_kinds.count(required_kind) == 1
    console_bytes = (settings.spool_dir / job.job_id / "console.log").read_bytes()
    assert b"application running" in console_bytes
    console_stderr_bytes = (settings.spool_dir / job.job_id / "console_stderr.log").read_bytes()
    assert b"warning: low disk" in console_stderr_bytes
    assert console_stderr_bytes != console_bytes
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


def test_deferred_execution_completed_with_missing_declared_output_succeeds_with_signal(
    tmp_path: Path,
    _watch_env: tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str],
) -> None:
    """Owner ruling, current (supersedes this module's own earlier #265/
    Ruling-B stance -- see jarvis_execution_artifacts.py's module docstring
    for the full history): a missing/empty DECLARED output is a SURFACED
    TYPED SIGNAL, never itself a reason to fail a job whose application
    genuinely returned success. JARVIS itself reports the execution
    ``completed`` (rc=0) here; a declared output never landed on disk --
    the job must still reach SUCCEEDED, carrying the typed ``outputs_missing``
    signal naming exactly which declared output is missing.
    """
    settings, queue, command, server_artifact, digest = _watch_env
    job, execution_id = _submit_watch_job(queue, command=command, digest=digest)
    execution_root = tmp_path / "execution-root"
    execution_root.mkdir()
    (execution_root / "stdout.log").write_bytes(b"application ran to completion\n")
    transport = _WatchTransportProvider(
        pipeline_id="watch-pipeline",
        execution_id=execution_id,
        states=[
            ("submitted", False, "9005"),
            ("running", False, "9005"),
            ("completed", True, "9005"),
        ],
        server_artifact=server_artifact,
        execution_root=execution_root,
        created_at=job.created_at.isoformat(),
        terminal_artifacts=[
            {
                "package_id": "jarvis.execution",
                "kind": "execution-file",
                "role": "output",
                "location": {"kind": "execution_path", "value": "dump.h5"},
                "size_bytes": 2048,
                "checksum": f"sha256:{'b' * 64}",
            }
        ],
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
    assert result.last_error is None
    task = queue.list_tasks(job.job_id)[0]
    assert task.state is JobState.SUCCEEDED
    outputs_missing = cast(dict[str, Any], task.metadata["execution_outputs_missing"])
    assert outputs_missing["schema_version"] == "clio-relay.execution-outputs-missing.v1"
    assert outputs_missing["reason"] == "declared_outputs_missing"
    assert outputs_missing["declared_count"] == 1
    assert outputs_missing["missing"] == [
        {
            "relative_path": "dump.h5",
            "role": "output",
            "reason": "absent",
            "declared_size_bytes": 2048,
        }
    ]
    # JARVIS's own execution record genuinely reached "completed" -- the
    # watch resolved normally; the signal is surfaced but never forces a
    # fabricated execution-level failure.
    events = _event_types(queue, job.job_id)
    assert "execution.watch_resolved" in events
    assert "jarvis.execution_output_missing" in events
    assert "jarvis.execution_outputs_missing" in events


def test_deferred_execution_completed_with_empty_declared_output_succeeds_with_signal(
    tmp_path: Path,
    _watch_env: tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str],
) -> None:
    """The exact live defect this fix slice closes (2026-08-27 ares LAMMPS
    run, jarvis_70633ea9d168bb28191178a4a1ced5ce / real slurm job 23723 /
    relay job_d5466728059642baa293e72c2379e50d): the application completed
    1000/1000 steps with return_code=0, but its declared ``stderr.log``
    output was PRESENT and legitimately empty (a clean run writes nothing to
    stderr) -- the wrapping job was wrongly marked FAILED. This is the EMPTY
    counterpart of the sibling ABSENT test above -- the file genuinely
    exists on disk (unlike ``dump.h5`` there), proving the fix does not
    merely special-case "file not found" but the declared-size_bytes==0
    case too, which flows through the SAME ``declared_outputs_missing``
    reason with per-item ``reason: "empty"`` (not ``"absent"``).
    """
    settings, queue, command, server_artifact, digest = _watch_env
    job, execution_id = _submit_watch_job(queue, command=command, digest=digest)
    execution_root = tmp_path / "execution-root"
    execution_root.mkdir()
    (execution_root / "stdout.log").write_bytes(b"1000/1000 steps\n")
    # The declared output genuinely EXISTS on disk -- 0 bytes, not absent.
    (execution_root / "stderr.log").write_bytes(b"")
    empty_sha256 = hashlib.sha256(b"").hexdigest()
    transport = _WatchTransportProvider(
        pipeline_id="watch-pipeline",
        execution_id=execution_id,
        states=[
            ("submitted", False, "23723"),
            ("running", False, "23723"),
            ("completed", True, "23723"),
        ],
        server_artifact=server_artifact,
        execution_root=execution_root,
        created_at=job.created_at.isoformat(),
        return_code=0,
        terminal_artifacts=[
            {
                "package_id": "jarvis.execution",
                "kind": "execution-file",
                "role": "log",
                "location": {"kind": "execution_path", "value": "stderr.log"},
                "size_bytes": 0,
                "checksum": f"sha256:{empty_sha256}",
            }
        ],
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
    assert result.last_error is None
    task = queue.list_tasks(job.job_id)[0]
    assert task.state is JobState.SUCCEEDED
    outputs_missing = cast(dict[str, Any], task.metadata["execution_outputs_missing"])
    assert outputs_missing["schema_version"] == "clio-relay.execution-outputs-missing.v1"
    assert outputs_missing["reason"] == "declared_outputs_missing"
    assert outputs_missing["declared_count"] == 1
    assert outputs_missing["missing"] == [
        {
            "relative_path": "stderr.log",
            "role": "log",
            "reason": "empty",
            "declared_size_bytes": 0,
        }
    ]
    events = _event_types(queue, job.job_id)
    assert "execution.watch_resolved" in events
    assert "jarvis.execution_output_empty" in events
    assert "jarvis.execution_outputs_missing" in events


def test_deferred_execution_with_empty_declared_output_stays_resolvable_by_execution_id(
    tmp_path: Path,
    _watch_env: tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str],
) -> None:
    """Second consumer of the SAME defect (live evidence, same LAMMPS run):
    ``relay_list_artifacts {"execution_id": ...}`` on the wrongly-FAILED
    job returned a typed ``execution_not_found`` -- the wrongly-failed job
    made its own execution's artifacts unreachable by execution_id. This
    pins the cascade end to end through the REAL admission path
    ``relay_list_artifacts``/the door's execution_id route both use
    (``jarvis_execution_artifacts.resolve_jarvis_run_owner_by_execution_id``,
    the exact function ``artifact_routing.list_artifacts`` calls): the SAME
    rc=0-with-empty-declared-output execution must (a) SUCCEED, (b) carry
    the ``outputs_missing`` signal, AND (c) still resolve by execution_id
    with its artifacts listable.

    Note (investigated, not fixed): the admission lookup itself
    (``resolve_jarvis_run_owner_by_execution_id`` ->
    ``_jarvis_run_matches`` -> ``_is_jarvis_run``) applies NO job-state
    filter at all -- confirmed by direct probe, a FAILED job's execution_id
    resolves through it exactly like a SUCCEEDED one's. The live
    not-found symptom was entirely downstream of THIS fix's own defect (the
    wrongly-FAILED job state), not a second, independent heuristic in the
    lookup -- so no separate admission-path change is warranted.
    """
    settings, queue, command, server_artifact, digest = _watch_env
    job, execution_id = _submit_watch_job(queue, command=command, digest=digest)
    execution_root = tmp_path / "execution-root"
    execution_root.mkdir()
    (execution_root / "stdout.log").write_bytes(b"1000/1000 steps\n")
    (execution_root / "stderr.log").write_bytes(b"")
    empty_sha256 = hashlib.sha256(b"").hexdigest()
    transport = _WatchTransportProvider(
        pipeline_id="watch-pipeline",
        execution_id=execution_id,
        states=[
            ("submitted", False, "23723"),
            ("running", False, "23723"),
            ("completed", True, "23723"),
        ],
        server_artifact=server_artifact,
        execution_root=execution_root,
        created_at=job.created_at.isoformat(),
        return_code=0,
        terminal_artifacts=[
            {
                "package_id": "jarvis.execution",
                "kind": "execution-file",
                "role": "log",
                "location": {"kind": "execution_path", "value": "stderr.log"},
                "size_bytes": 0,
                "checksum": f"sha256:{empty_sha256}",
            }
        ],
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
    task = queue.list_tasks(job.job_id)[0]
    outputs_missing = cast(dict[str, Any], task.metadata["execution_outputs_missing"])
    assert outputs_missing["reason"] == "declared_outputs_missing"

    # The exact admission path relay_list_artifacts / the door's
    # GET /executions/{execution_id}/artifacts route both call.
    owner = resolve_jarvis_run_owner_by_execution_id(
        queue,
        execution_id,
        cluster=job.cluster,
        owns_job=None,
    )
    assert owner.job_id == job.job_id
    assert owner.state is JobState.SUCCEEDED
    artifacts, _next_cursor, total = queue.list_artifacts_page(owner.job_id, cursor=1, limit=100)
    assert total > 0
    assert artifacts


def test_deferred_execution_genuinely_failed_with_missing_declared_output_stays_failed(
    tmp_path: Path,
    _watch_env: tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str],
) -> None:
    """Guard: a NONZERO returncode (a real application failure) must still
    fail the job even when a declared output also happens to be missing --
    the outputs_missing signal never masks, dilutes, or substitutes for the
    real failure reason. The watch's own ``execution_watch_failure`` must
    remain the terminal-failure driver, and the outputs_missing signal must
    still reach the durable record unconditionally alongside it.
    """
    settings, queue, command, server_artifact, digest = _watch_env
    job, execution_id = _submit_watch_job(queue, command=command, digest=digest)
    execution_root = tmp_path / "execution-root"
    execution_root.mkdir()
    (execution_root / "stdout.log").write_bytes(b"application crashed\n")
    transport = _WatchTransportProvider(
        pipeline_id="watch-pipeline",
        execution_id=execution_id,
        states=[
            ("submitted", False, "9007"),
            ("running", False, "9007"),
            ("failed", True, "9007"),
        ],
        server_artifact=server_artifact,
        execution_root=execution_root,
        created_at=job.created_at.isoformat(),
        return_code=1,
        error="application exited with code 137",
        terminal_artifacts=[
            {
                "package_id": "jarvis.execution",
                "kind": "execution-file",
                "role": "output",
                "location": {"kind": "execution_path", "value": "dump.h5"},
                "size_bytes": 2048,
                "checksum": f"sha256:{'e' * 64}",
            }
        ],
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
    # The REAL failure reason names the crash, not the outputs signal.
    assert "application exited with code 137" in result.last_error
    task = queue.list_tasks(job.job_id)[0]
    assert task.state is JobState.FAILED
    watch_failure = cast(dict[str, Any], task.metadata["execution_watch_failure"])
    assert watch_failure["reason"] == "application exited with code 137"
    # The outputs_missing signal is still folded into the durable record
    # unconditionally, even though it did not drive the failure.
    outputs_missing = cast(dict[str, Any], task.metadata["execution_outputs_missing"])
    assert outputs_missing["reason"] == "declared_outputs_missing"
    assert outputs_missing["missing"][0]["relative_path"] == "dump.h5"


def test_deferred_execution_with_only_a_pipeline_snapshot_artifact_succeeds(
    tmp_path: Path,
    _watch_env: tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str],
) -> None:
    """Adversarial-review Ruling B probe case (flagged for owner review): a
    terminal page declaring one real, non-execution-file artifact (a
    pipeline snapshot -- the same shape the relay-flushed console.log or a
    pure-stdout application's page would take) and ZERO execution-file
    entries must still SUCCEED. no_outputs_declared is a typed signal, not
    an automatic failure -- #265's own "never invent a heuristic about
    which files should exist" instruction (jarvis_execution_artifacts.py)
    forbids treating "declared no execution-file outputs" as proof nothing
    real happened.
    """
    settings, queue, command, server_artifact, digest = _watch_env
    job, execution_id = _submit_watch_job(queue, command=command, digest=digest)
    execution_root = tmp_path / "execution-root"
    execution_root.mkdir()
    (execution_root / "stdout.log").write_bytes(b"application ran to completion\n")
    transport = _WatchTransportProvider(
        pipeline_id="watch-pipeline",
        execution_id=execution_id,
        states=[
            ("submitted", False, "9006"),
            ("running", False, "9006"),
            ("completed", True, "9006"),
        ],
        server_artifact=server_artifact,
        execution_root=execution_root,
        created_at=job.created_at.isoformat(),
        terminal_artifacts=[
            {
                "package_id": "jarvis.pipeline",
                "kind": "pipeline-snapshot",
                "role": "log",
                "location": {"kind": "execution_path", "value": "pipeline.yaml"},
                "size_bytes": 512,
                "checksum": f"sha256:{'c' * 64}",
            }
        ],
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
    task = queue.list_tasks(job.job_id)[0]
    assert task.state is JobState.SUCCEEDED
    outputs_missing = cast(dict[str, Any], task.metadata["execution_outputs_missing"])
    assert outputs_missing["schema_version"] == "clio-relay.execution-outputs-missing.v1"
    assert outputs_missing["reason"] == "no_outputs_declared"
    assert outputs_missing["declared_count"] == 0
    events = _event_types(queue, job.job_id)
    assert "execution.watch_resolved" in events
    assert "jarvis.execution_outputs_missing" in events


def test_transport_failure_with_signal_only_outputs_missing_names_the_real_reason(
    tmp_path: Path,
    _watch_env: tuple[RelaySettings, ClioCoreQueue, list[str], dict[str, Any], str],
) -> None:
    """Failing-first e2e for the proven Ruling B hijack (adversarial-review
    item 2): a nonzero TRANSPORT returncode is only ever consulted by
    ``resolve_execution_outcome`` when no watch resolution exists (the
    synchronous-terminal-dispatch fast path -- ``test_synchronous_terminal_
    dispatch_skips_watch``'s own twin), and that is the ONE combination
    that lets a present-but-non-forcing ``no_outputs_declared`` signal sit
    alongside a real, unrelated FAILED verdict it must not hijack. Proven
    live (2026-08-26): the pre-fix code rendered ``last_error`` as
    "completed but declared zero outputs are missing or empty" -- naming
    the SIGNAL, not the actual transport failure -- and suppressed the
    #183/#248 ``mcp_call_result_error`` tier's own guard from ever running.

    The fixed code must instead: (1) never let the raw signal win priority
    over the real, lower-tier reason (here, the bare "exit code 1" last
    resort -- structurally correct, since this fabricated document itself
    carries no MCP-protocol-level error to name more specifically, see
    ``mcp_call_result_error``'s own docstring on what it can and cannot
    invent), and (2) still fold the raw signal into task metadata
    unconditionally, so it is never silently lost.
    """
    settings, queue, command, server_artifact, digest = _watch_env
    job, execution_id = _submit_watch_job(queue, command=command, digest=digest)
    execution_root = tmp_path / "execution-root"
    execution_root.mkdir()
    (execution_root / "stdout.log").write_bytes(b"ran and finished synchronously\n")
    transport = _WatchTransportProvider(
        pipeline_id="watch-pipeline",
        execution_id=execution_id,
        # A single, already-terminal state: the dispatch's OWN document
        # reports success (state=="completed", return_code=0, no Ruling A
        # conflict) -- the watch never engages (mirrors ``test_synchronous_
        # terminal_dispatch_skips_watch``), so ``resolve_execution_outcome``
        # falls through to ``effective_returncode = transport_returncode``.
        states=[("completed", True, "9100")],
        server_artifact=server_artifact,
        execution_root=execution_root,
        created_at=job.created_at.isoformat(),
        # The TRANSPORT-level dispatch subprocess itself exits nonzero --
        # independent of the document-internal fields above, see
        # ``dispatch_returncode``'s own docstring on this fixture.
        dispatch_returncode=1,
        terminal_artifacts=[
            {
                "package_id": "jarvis.pipeline",
                "kind": "pipeline-snapshot",
                "role": "log",
                "location": {"kind": "execution_path", "value": "pipeline.yaml"},
                "size_bytes": 512,
                "checksum": f"sha256:{'d' * 64}",
            }
        ],
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
    assert transport.dispatch_count == 1
    assert transport.poll_count == 0  # the watch never engaged
    assert result.last_error is not None
    # The real reason (a bare transport exit code -- no structural MCP
    # error exists in this document to name more specifically) must
    # survive, never overwritten by a "completed"/"declared zero outputs"
    # message the signal-only reason must not drive.
    assert result.last_error == "exit code 1"
    assert "declared" not in result.last_error
    assert "completed" not in result.last_error
    task = queue.list_tasks(job.job_id)[0]
    assert task.state is JobState.FAILED
    assert "mcp_dispatch_failure" not in task.metadata
    assert "application_verdict_failure" not in task.metadata
    assert "execution_watch_failure" not in task.metadata
    # The raw signal itself is NEVER lost -- folded into the durable
    # record unconditionally, exactly as the success branch already does.
    outputs_missing = cast(dict[str, Any], task.metadata["execution_outputs_missing"])
    assert outputs_missing["schema_version"] == "clio-relay.execution-outputs-missing.v1"
    assert outputs_missing["reason"] == "no_outputs_declared"
    assert outputs_missing["declared_count"] == 0
    events = _event_types(queue, job.job_id)
    assert "jarvis.execution_outputs_missing" in events


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
        # clio-relay#265 D1: declare the real output so this genuinely
        # successful run does not (correctly) fail on zero declared outputs.
        terminal_artifacts=_present_output_artifacts(
            execution_root / "stdout.log", relative="stdout.log"
        ),
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
