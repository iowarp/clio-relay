"""The job IS the run: watch a scheduler-deferred ``jarvis_run`` execution
to real terminal (clio-relay#266).

Run 14's live evidence: a scheduler-backed ``jarvis_run`` dispatch returns
in seconds carrying a *queued/submitted* execution -- the JARVIS MCP tool
call answers as soon as the workload is handed to the scheduler, not when
it finishes. Pre-#266 the relay job went terminal on that dispatch
response, so #259's console tailer covered only the dispatch's own
lifetime and the client had no single task spanning the real run.

This module is the owner of the *typed* decisions this fixes:

* :func:`deferred_jarvis_execution` -- is a trusted dispatch result a
  scheduler execution that has not reached terminal yet? The only signal
  is JARVIS's own ``execution_record.terminal`` (via the already-validated
  :class:`~clio_relay.runtime_metadata.JarvisRuntimeMetadata`) -- never a
  keyword/phrase match on prose.
* :func:`execution_phase_for_state` -- JARVIS's own execution-state
  vocabulary (the closed set validated in ``runtime_metadata.py``) mapped
  into the small typed phase the job record and ``tasks/get`` surface.
  Owner ruling (#266): the phase is JARVIS's state mapped through, not a
  relay-side scheduler (squeue/sacct) read -- JARVIS owns the scheduler
  handle it submitted, relay only watches JARVIS's own execution record
  (the same one ``jarvis_get_execution`` reports).
* :func:`execution_watch_query_spec` -- the bounded ``jarvis_get_execution``
  poll request, reusing the exact MCP dispatch contract
  ``endpoint_jarvis_recovery`` already proved for its lost-response
  recovery query, but WITHOUT that module's crash-recovery durable-intent
  state machine -- this is not a lost response, it is the ordinary
  in-progress case, and needs none of the pending/resolved bookkeeping
  ``_durable_jarvis_execution_recovery`` enforces for the outage it owns.
* Ceiling and cancellation-refusal helpers -- see
  :class:`ExecutionWatchCeilingExceeded` and
  :data:`CANCEL_UNSUPPORTED_REASON`.

:func:`run_execution_watch` is the poll loop itself (subprocess dispatch,
console tailing, lease renewal, typed phase/event bookkeeping). It lives
here -- not on ``EndpointWorker`` -- per the cleanup program's
owner-module/no-accretion rule (``scripts/check_file_size.py``, #774/#775):
``endpoint.py`` is already over its ratcheted baseline and may not regrow.
The handful of operations that are genuinely endpoint-worker state (lease
renewal, and materializing the terminal result through the already-tested
``_write_recovered_jarvis_run_result``) are injected as callbacks; every
MCP dispatch/trust/parse step is a plain, directly testable call into
``endpoint_jarvis_recovery``/``runtime_metadata`` from here.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast

from clio_relay.command_evidence import bounded_error_detail
from clio_relay.endpoint_jarvis_recovery import (
    _endpoint_mcp_runner_command,
    _minimal_mcp_runner_environment,
    _trusted_jarvis_execution_query_validation,
    _trusted_jarvis_mcp_result,
)
from clio_relay.endpoint_recovery_directory import _write_private_json_file
from clio_relay.endpoint_scheduler_metadata import _runtime_metadata_is_native
from clio_relay.errors import RelayError
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.models import McpCallSpec
from clio_relay.runtime_metadata import runtime_metadata_from_mcp_result_document

if TYPE_CHECKING:
    from clio_relay.console_stream import ConsoleLiveTailer
    from clio_relay.core_queue import ClioCoreQueue
    from clio_relay.jarvis_provider import JarvisCdProvider
    from clio_relay.models import RelayJob
    from clio_relay.runtime_metadata import JarvisRuntimeMetadata

#: MCP timeouts for one watch poll -- matches the existing lost-response
#: recovery query's bounds (``endpoint_sidecar_types.py``); a watch poll is
#: the same kind of bounded ``jarvis_get_execution`` dispatch.
EXECUTION_WATCH_QUERY_TIMEOUT_SECONDS = 60
EXECUTION_WATCH_QUERY_PROCESS_TIMEOUT_SECONDS = 75

EXECUTION_PHASE_SCHEMA = "clio-relay.execution-phase.v1"
EXECUTION_CANCEL_REFUSAL_SCHEMA = "clio-relay.execution-cancel-refusal.v1"
EXECUTION_WATCH_FAILURE_SCHEMA = "clio-relay.execution-watch-failure.v1"

#: Page size for the ONE terminal, artifact-bearing poll -- matches the
#: existing lost-response recovery query's page size
#: (``endpoint.py::_recover_jarvis_execution``); #252's
#: ``ingest_jarvis_execution_outputs`` truncates beyond
#: ``MAX_RELAY_EXECUTION_OUTPUTS`` (128) the same way regardless of caller.
EXECUTION_WATCH_TERMINAL_ARTIFACT_PAGE_SIZE = 100

DEFAULT_EXECUTION_WATCH_POLL_INTERVAL_SECONDS = 7.0
DEFAULT_EXECUTION_WATCH_CEILING_SECONDS = 24 * 60 * 60

#: Typed reason recorded when a client asks to cancel a job mid-watch.
#: JARVIS's registered user contract (``_contracts/jarvis-user-v3.7.2.json``)
#: exposes no cancel tool alongside ``jarvis_run``/``jarvis_get_execution``
#: -- owner ruling (#266): relay must never issue a raw ``scancel`` against
#: a scheduler job JARVIS submitted and owns. Refuse the cancel request
#: with this typed reason and keep watching, so the job's terminal state
#: stays faithful to what actually happened on the cluster.
CANCEL_UNSUPPORTED_REASON = "execution_cancel_unsupported"

#: JARVIS's own non-terminal execution states
#: (``clio_relay.runtime_metadata._JARVIS_EXECUTION_STATES`` minus the
#: terminal subset) mapped to the small typed phase vocabulary. Anything
#: this table has not been taught -- including a future JARVIS state --
#: passes through typed as ``jarvis_state:<raw>`` rather than being
#: silently dropped or guessed at.
_NON_TERMINAL_STATE_PHASE: dict[str, str] = {
    "preparing": "queued",
    "scripted": "queued",
    "submitting": "queued",
    "submitted": "queued",
    "running": "running",
}


class ExecutionWatchCeilingExceeded(RelayError):
    """One deferred execution watch exceeded its configured ceiling.

    Typed, never silent: the ceiling default is generous (24h), but a
    watch that outlives it must fail the job with a structured reason
    rather than hang the worker slot forever.
    """


@dataclass(frozen=True, slots=True)
class DeferredJarvisExecution:
    """One scheduler-backed ``jarvis_run`` execution not yet at terminal."""

    pipeline_id: str
    execution_id: str


@dataclass(frozen=True, slots=True)
class ExecutionWatchResolution:
    """The watch's terminal outcome, already mapped to success/failure."""

    succeeded: bool
    failure_detail: dict[str, object] | None


def deferred_jarvis_execution(
    metadata: JarvisRuntimeMetadata,
) -> DeferredJarvisExecution | None:
    """Return the watch target, or ``None`` when today's fast path applies.

    ``metadata`` must already be a *trusted*, native runtime snapshot --
    see ``_trusted_jarvis_mcp_result`` and
    ``runtime_metadata_from_mcp_result_document`` in ``endpoint.py``, both
    called by the caller before this function ever runs. This makes no
    trust decision of its own: it is a pure typed-state read.
    ``terminal.terminal is False`` (a scheduler-backed submission still in
    flight) is the ONLY condition that starts a watch -- a synchronous or
    already-terminal dispatch (today's behavior) returns ``None``
    unchanged, exactly satisfying design constraint 1 (scope the extended
    lifetime to deferred executions only).
    """
    if metadata.execution_id is None or metadata.pipeline_id is None:
        return None
    if metadata.terminal.terminal is not False:
        return None
    return DeferredJarvisExecution(
        pipeline_id=metadata.pipeline_id,
        execution_id=metadata.execution_id,
    )


def execution_phase_for_state(state: str | None) -> str:
    """Map one JARVIS execution state to the small typed phase vocabulary."""
    if state is None:
        return "jarvis_state:unknown"
    return _NON_TERMINAL_STATE_PHASE.get(state, f"jarvis_state:{state}")


def execution_phase_job_metadata(
    metadata: JarvisRuntimeMetadata,
    *,
    poll_count: int,
    observed_at: datetime,
) -> dict[str, object]:
    """Build the typed payload merged into ``job.metadata["execution_phase"]``.

    Carried on the durable job record the door serves (``RelayJob.metadata``,
    merged via ``ClioCoreQueue.update_job_metadata``) so a run card can read
    a queued/running phase without polling the console or task events.
    """
    state = metadata.terminal.state
    return {
        "schema_version": EXECUTION_PHASE_SCHEMA,
        "phase": execution_phase_for_state(state),
        "jarvis_state": state,
        "terminal": metadata.terminal.terminal,
        "pipeline_id": metadata.pipeline_id,
        "execution_id": metadata.execution_id,
        "scheduler_provider": metadata.scheduler_provider,
        "scheduler_job_id": metadata.scheduler_job_id,
        "poll_count": poll_count,
        "observed_at": observed_at.isoformat(),
    }


def execution_watch_query_spec(
    base: McpCallSpec,
    *,
    pipeline_id: str,
    execution_id: str,
    include_artifacts: bool,
    timeout_seconds: int,
) -> McpCallSpec:
    """Build one bounded ``jarvis_get_execution`` poll request.

    ``include_artifacts`` is set ONLY on the final, terminal poll: #252's
    ``ingest_jarvis_execution_outputs`` reads
    ``structured_result.artifact_page``, which JARVIS populates only when
    ``artifacts`` is requested. Every intermediate poll omits it -- cheaper
    per tick, and it lets each intermediate response be checked with
    ``_trusted_jarvis_execution_query_validation``'s fixed
    ``artifacts_requested is False`` expectation.
    """
    arguments: dict[str, object] = {
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
    }
    if include_artifacts:
        arguments["artifacts"] = {
            "page_size": EXECUTION_WATCH_TERMINAL_ARTIFACT_PAGE_SIZE,
        }
    return McpCallSpec(
        server=base.server,
        server_args=base.server_args,
        env_from=base.env_from,
        expected_server_artifact_digest=base.expected_server_artifact_digest,
        expected_registered_contract=base.expected_registered_contract,
        expected_jarvis_cd_lock_binding=base.expected_jarvis_cd_lock_binding,
        tool="jarvis_get_execution",
        arguments=arguments,
        timeout_seconds=timeout_seconds,
    )


def execution_watch_deadline(anchor: datetime, *, ceiling_seconds: float) -> datetime:
    """Return one watch's absolute deadline, anchored to a stable time.

    Anchored to the relay job's own ``created_at`` rather than to when a
    particular watch loop instance started: ``execution_id`` is durable
    and pre-generated before the first dispatch
    (``endpoint_jarvis_recovery._jarvis_execution_recovery_intent``
    requires it), so a worker-restart requeue-and-redispatch reaches the
    SAME execution and should not reset the ceiling clock.
    """
    return anchor + timedelta(seconds=ceiling_seconds)


def execution_cancel_unsupported_payload(
    *,
    pipeline_id: str,
    execution_id: str,
) -> dict[str, object]:
    """Typed payload recorded when a cancel request is refused mid-watch."""
    return {
        "schema_version": EXECUTION_CANCEL_REFUSAL_SCHEMA,
        "reason": CANCEL_UNSUPPORTED_REASON,
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
    }


def execution_watch_succeeded(metadata: JarvisRuntimeMetadata) -> bool:
    """Return whether a terminal watch observation is the run's success."""
    return metadata.terminal.state == "completed"


def execution_watch_failure_detail(
    metadata: JarvisRuntimeMetadata,
) -> dict[str, object]:
    """Typed failure detail for a terminal non-completed execution.

    This is #265's negative path: a crashed/canceled application must
    land the relay job as ``failed`` with a typed error, not a bare exit
    code -- JARVIS's own ``execution_record.error`` (``terminal.reason``
    here) is the faithful reason, when JARVIS reported one.
    """
    return {
        "schema_version": EXECUTION_WATCH_FAILURE_SCHEMA,
        "pipeline_id": metadata.pipeline_id,
        "execution_id": metadata.execution_id,
        "state": metadata.terminal.state,
        "returncode": metadata.terminal.returncode,
        "reason": metadata.terminal.reason,
    }


def execution_watch_error_text(failure_detail: dict[str, object]) -> str:
    """Render one bounded, human-readable error from a typed failure detail."""
    execution_id = failure_detail.get("execution_id")
    state = failure_detail.get("state")
    reason = failure_detail.get("reason")
    if isinstance(reason, str) and reason:
        return f"JARVIS execution {execution_id} ended in {state}: {reason}"
    return f"JARVIS execution {execution_id} ended in {state}"


def execution_phase_status_message(
    job_state: str,
    execution_phase: object,
) -> str:
    """Render the one honest ``tasks/get`` ``statusMessage`` slot (SEP-2663).

    ``GetTaskResult``'s schema reserves ``result`` for the ``completed``
    status and forbids additionalProperties on the task arm
    (``fastmcp_tasks.models._TaskFields``) -- so ``statusMessage`` is the
    only place left to surface the typed ``execution_phase`` while a job
    is still working. ``execution_phase`` is expected to be
    ``job.metadata.get("execution_phase")`` (a dict shaped by
    :func:`execution_phase_job_metadata`, or ``None``/anything else before
    the first poll); this stays a thin string render of an already-typed
    value, never a keyword/phrase decision of its own.
    """
    base = f"Relay job is {job_state}"
    if not isinstance(execution_phase, dict):
        return base
    phase = cast(dict[str, object], execution_phase).get("phase")
    if not isinstance(phase, str) or not phase:
        return base
    return f"{base}; jarvis execution is {phase}"


def execution_phase_status_message_for_job(job: RelayJob) -> str:
    """Short call-site form of :func:`execution_phase_status_message` for one job."""
    return execution_phase_status_message(job.state.value, job.metadata.get("execution_phase"))


def deferred_jarvis_execution_from_document(
    job: RelayJob,
    document: object,
) -> DeferredJarvisExecution | None:
    """Detect a watch target directly from one ``mcp-result.json`` document.

    Owns the trust-and-parse chain (identity, then native runtime
    metadata) endpoint.py would otherwise have to inline at its call
    site -- kept here, not there, per the ratchet (endpoint.py may not
    regrow, #774/#775). Returns ``None`` for every case where today's fast
    path applies unchanged: not a trusted result, not native, or already
    terminal.
    """
    trusted, _reason = _trusted_jarvis_mcp_result(job, document)
    if not trusted:
        return None
    metadata = runtime_metadata_from_mcp_result_document(
        {**cast(dict[str, object], document), "tool": "jarvis_run"}
    )
    if metadata is None or not _runtime_metadata_is_native(metadata):
        return None
    return deferred_jarvis_execution(metadata)


@dataclass(frozen=True, slots=True)
class ExecutionOutcome:
    """The three ``_run_job_impl`` terminal-mapping decisions #266 touches."""

    effective_returncode: int
    cancellation_honored: bool
    watch_failure: dict[str, object] | None


def resolve_execution_outcome(
    *,
    dispatch_recovered: bool,
    watch_resolution: ExecutionWatchResolution | None,
    dispatch_refusal_present: bool,
    transport_returncode: int,
    cancellation_requested: bool,
) -> ExecutionOutcome:
    """Fold a resolved watch into ``_run_job_impl``'s existing outcome logic.

    A resolved watch means the run's REAL terminal outcome was observed
    (and, if cancellation was requested mid-watch, explicitly refused --
    JARVIS exposes no cancel surface, see :data:`CANCEL_UNSUPPORTED_REASON`).
    Reporting the job canceled anyway would contradict that typed refusal
    and hide the real outcome, so a resolved watch always wins over a
    pending cancellation request for terminal-state purposes.
    """
    if dispatch_recovered:
        effective_returncode = 0
    elif watch_resolution is not None:
        effective_returncode = 0 if watch_resolution.succeeded else 1
    elif dispatch_refusal_present and transport_returncode == 0:
        effective_returncode = 1
    else:
        effective_returncode = transport_returncode
    return ExecutionOutcome(
        effective_returncode=effective_returncode,
        cancellation_honored=watch_resolution is None and cancellation_requested,
        watch_failure=(watch_resolution.failure_detail if watch_resolution is not None else None),
    )


def _tail_console(
    queue: ClioCoreQueue,
    job: RelayJob,
    console_tailer: ConsoleLiveTailer | None,
) -> None:
    """Advance one #259 console-tail increment; mirrors ``endpoint.py``'s own helper."""
    if console_tailer is None:
        return
    step = console_tailer.poll()
    if step.reason is not None:
        queue.append_event(
            job.job_id,
            f"console.{step.reason}",
            step.message or "console live-tail reason",
            payload={"stream": "console", "reason": step.reason},
        )


def dispatch_execution_watch_query(
    job: RelayJob,
    *,
    base_spec: McpCallSpec,
    provider: JarvisCdProvider,
    watch_dir: Path,
    deferred: DeferredJarvisExecution,
    include_artifacts: bool,
) -> tuple[JarvisRuntimeMetadata, bytes]:
    """Dispatch one bounded ``jarvis_get_execution`` poll and return trusted bytes."""
    query_spec = execution_watch_query_spec(
        base_spec,
        pipeline_id=deferred.pipeline_id,
        execution_id=deferred.execution_id,
        include_artifacts=include_artifacts,
        timeout_seconds=EXECUTION_WATCH_QUERY_TIMEOUT_SECONDS,
    )
    query_job = job.model_copy(update={"spec": query_spec})
    params_path = watch_dir / "params.json"
    result_path = watch_dir / "mcp-result.json"
    with suppress(FileNotFoundError):
        internal_filesystem_path(result_path).unlink()
    _write_private_json_file(params_path, query_spec.model_dump(mode="json", exclude_none=True))
    completed = provider.run_command_streaming(
        _endpoint_mcp_runner_command(params_path),
        process_label="jarvis execution watch query",
        cwd=internal_filesystem_path(watch_dir),
        env=_minimal_mcp_runner_environment(base_spec.env_from),
        timeout_seconds=EXECUTION_WATCH_QUERY_PROCESS_TIMEOUT_SECONDS,
    )
    if completed.returncode != 0:
        detail = bounded_error_detail(completed.stderr or completed.stdout or "no detail")
        raise RelayError(f"jarvis execution watch query exited {completed.returncode}: {detail}")
    payload = internal_filesystem_path(result_path).read_bytes()
    document = json.loads(payload.decode("utf-8"))
    trusted, reason = _trusted_jarvis_mcp_result(
        query_job,
        document,
        expected_tool="jarvis_get_execution",
    )
    if not trusted:
        raise RelayError(f"jarvis execution watch query result was not trusted: {reason}")
    if not include_artifacts and not _trusted_jarvis_execution_query_validation(
        document,
        pipeline_id=deferred.pipeline_id,
        execution_id=deferred.execution_id,
    ):
        raise RelayError("jarvis execution watch query validation did not match")
    parser_document = {**cast(dict[str, object], document), "tool": "jarvis_run"}
    metadata = runtime_metadata_from_mcp_result_document(parser_document)
    if (
        metadata is None
        or not _runtime_metadata_is_native(metadata)
        or metadata.pipeline_id != deferred.pipeline_id
        or metadata.execution_id != deferred.execution_id
    ):
        raise RelayError("jarvis execution watch query returned an unexpected execution identity")
    return metadata, payload


def run_execution_watch(
    job: RelayJob,
    *,
    base_spec: McpCallSpec,
    deferred: DeferredJarvisExecution,
    queue: ClioCoreQueue,
    provider: JarvisCdProvider,
    watch_dir: Path,
    console_tailer: ConsoleLiveTailer | None,
    poll_interval_seconds: float,
    ceiling_seconds: float,
    renew_lease: Callable[[], None],
    is_cancellation_requested: Callable[[], bool],
    write_terminal_result: Callable[[dict[str, object], str], None],
    now: Callable[[], datetime],
    sleep: Callable[[float], None] = time.sleep,
) -> ExecutionWatchResolution:
    """Poll ``jarvis_get_execution`` until JARVIS's own record is terminal.

    The console tailer keeps advancing and the lease keeps renewing on
    every tick -- the job's (now correct) lifetime -- and a typed
    ceiling/cancellation-refusal reason is recorded rather than ever
    hanging the worker slot or issuing a raw scheduler cancel (JARVIS owns
    the scheduler handle it submitted; see :data:`CANCEL_UNSUPPORTED_REASON`).
    ``write_terminal_result`` is called exactly once, with the raw terminal
    ``jarvis_get_execution`` document and its sha256, to materialize the
    job's ``mcp-result.json`` before this returns.
    """
    internal_filesystem_path(watch_dir).mkdir(parents=True, exist_ok=True)
    deadline = execution_watch_deadline(job.created_at, ceiling_seconds=ceiling_seconds)
    queue.append_event(
        job.job_id,
        "execution.watch_started",
        "jarvis_run execution is scheduler-deferred; watching until terminal",
        payload={
            "pipeline_id": deferred.pipeline_id,
            "execution_id": deferred.execution_id,
            "poll_interval_seconds": poll_interval_seconds,
            "ceiling_seconds": ceiling_seconds,
        },
    )
    poll_count = 0
    cancel_refusal_reported = False
    last_reported_phase: str | None = None
    while True:
        if now() >= deadline:
            queue.append_event(
                job.job_id,
                "execution.watch_ceiling_exceeded",
                "Deferred jarvis_run execution watch exceeded its configured ceiling",
                payload={
                    "pipeline_id": deferred.pipeline_id,
                    "execution_id": deferred.execution_id,
                    "ceiling_seconds": ceiling_seconds,
                    "poll_count": poll_count,
                },
            )
            raise ExecutionWatchCeilingExceeded(
                f"execution {deferred.execution_id} did not reach terminal within "
                f"{ceiling_seconds:g}s"
            )
        if not cancel_refusal_reported and is_cancellation_requested():
            cancel_refusal_reported = True
            queue.append_event(
                job.job_id,
                f"execution.{CANCEL_UNSUPPORTED_REASON}",
                "Cancellation was requested but JARVIS exposes no execution cancel "
                "surface; the watch continues so the real terminal outcome is observed",
                payload=execution_cancel_unsupported_payload(
                    pipeline_id=deferred.pipeline_id,
                    execution_id=deferred.execution_id,
                ),
            )
        poll_count += 1
        metadata, _payload = dispatch_execution_watch_query(
            job,
            base_spec=base_spec,
            provider=provider,
            watch_dir=watch_dir,
            deferred=deferred,
            include_artifacts=False,
        )
        _tail_console(queue, job, console_tailer)
        renew_lease()
        phase_metadata = execution_phase_job_metadata(
            metadata,
            poll_count=poll_count,
            observed_at=now(),
        )
        phase = cast(str, phase_metadata["phase"])
        if phase != last_reported_phase:
            last_reported_phase = phase
            queue.update_job_metadata(job.job_id, {"execution_phase": phase_metadata})
            queue.append_event(
                job.job_id,
                f"execution.{phase}",
                f"jarvis execution {deferred.execution_id} is {phase}",
                payload=phase_metadata,
            )
        if metadata.terminal.terminal is True:
            break
        sleep(poll_interval_seconds)
    final_metadata, final_payload = dispatch_execution_watch_query(
        job,
        base_spec=base_spec,
        provider=provider,
        watch_dir=watch_dir,
        deferred=deferred,
        include_artifacts=True,
    )
    if final_metadata.terminal.terminal is not True:
        raise RelayError(
            "deferred jarvis_run execution watch observed a non-terminal record on its "
            "artifact-bearing terminal query"
        )
    write_terminal_result(
        cast(dict[str, object], json.loads(final_payload.decode("utf-8"))),
        hashlib.sha256(final_payload).hexdigest(),
    )
    _tail_console(queue, job, console_tailer)
    final_phase_metadata = execution_phase_job_metadata(
        final_metadata,
        poll_count=poll_count + 1,
        observed_at=now(),
    )
    queue.update_job_metadata(job.job_id, {"execution_phase": final_phase_metadata})
    queue.append_event(
        job.job_id,
        "execution.watch_resolved",
        f"jarvis_run execution reached terminal: {final_metadata.terminal.state}",
        payload=final_phase_metadata,
    )
    succeeded = execution_watch_succeeded(final_metadata)
    failure_detail = None if succeeded else execution_watch_failure_detail(final_metadata)
    return ExecutionWatchResolution(succeeded=succeeded, failure_detail=failure_detail)
