"""Worker-slot lane resilience: quarantine poisoned recovery records, never
die silently (clio-relay#238).

**Root cause (issue #238's two faces, one mechanism).** A daemon-mode worker
slot runs ``EndpointWorker._serve_worker_slot``'s ``while True:
worker.run_once(...)`` loop with no exception handling of its own, inside a
``ThreadPoolExecutor`` future. ``run_once`` wraps per-job execution
(``_run_job``) in its own ``try/except Exception``, so a job-scoped failure
never escapes -- but the *between-jobs housekeeping* it also calls,
``_reconcile_pending_execution_cleanup``, is not job-scoped: it scans every
pending execution-cleanup marker for the cluster looking for stale executions
to reconcile. Its first call to
``endpoint._durable_jarvis_execution_recovery(job, task)`` per marker sits
*outside* that method's own per-marker ``try/except`` (the ``try:`` begins
several lines later, at ``eligible += 1`` -- everything from the lease scan
through the first recovery-intent fetch runs unguarded). A validator finding
one poisoned ``jarvis_execution_recovery`` record therefore raises an
unguarded ``RelayError`` straight out of ``run_once``, out of the per-slot
``while`` loop, and the thread that was serving that slot dies -- silently,
because nothing on that path ever logs or records anything. In daemon mode
the workload slot is the one with ``reconcile_execution_cleanup=True``
(``_serve_worker_slots`` sets it only for ``index == 0`` of the workload
group), and this scan runs on *every* iteration where no lease was acquired
-- i.e. continuously whenever jobs are not leasing -- while the sibling
control-query slot never calls it at all and stays alive. That split exactly
matches face 1's reported shape (``worker_count: 1``, only the control-query
slot registered) as well as face 2's exact reproduction (``--once`` surfaces
the same unguarded ``RelayError`` because there is no per-slot loop to hide
it behind). The process itself does not exit: the still-running control-query
slot's future never completes, so ``ThreadPoolExecutor.__exit__``'s
``shutdown(wait=True)`` blocks forever waiting for it once the dead
workload-slot future's exception propagates out of ``_serve_worker_slots``'s
``for future in as_completed(...): future.result()`` -- explaining "worker
process alive throughout (Ssl, low CPU), NOT crash-looping" with 0-byte
``worker.out.log``/``worker.err.log``: the exception is captured, but never
reaches anything that prints it.

**Fix, two layers.**

1. :func:`quarantine_relay_error` wraps the one unguarded call site
   (``endpoint.py``'s ``_reconcile_pending_execution_cleanup``, the fetch
   immediately before the lease-scan's first ``continue``). A poisoned
   record can never be trusted enough to drive a recovery decision, so
   quarantining it and falling back to the same "absent record" path every
   caller already takes for ``None`` is the existing safe default -- this
   makes a poisoned record behave like a merely-absent one instead of a
   process-killing crash, while making the quarantine itself a typed,
   queryable durable event instead of a silent downgrade.
2. :func:`run_worker_lane_iteration` is a defense-in-depth barrier around
   the per-slot loop body itself (``_serve_worker_slot``): *any* exception
   any future failure mode raises from one iteration is caught, logged with
   ``exc_info`` (Python's logging module writes WARNING-and-above to stderr
   through its last-resort handler even with no ``logging.basicConfig`` --
   this is what makes the 0-byte-log failure mode structurally impossible
   from here on, independent of whatever redirects the daemon's own
   stdout/stderr), recorded on the endpoint's own registry metadata as
   ``lane_last_error`` (so ``worker status``/diagnostics can see it without
   reading a log file), and the loop keeps going rather than letting the
   thread -- and with it, the whole daemon process's shutdown -- die quietly.

Both typed reasons below are worker-lane-internal lifecycle events, not
door-surface classifications: they never reach ``fastmcp_server.py``,
``http_api.py``, ``browser_gateway.py``, or ``mcp_server.py`` (the four
surfaces ``door_errors.py`` owns per docs/design/relay-architecture-2026-08.md
§6), so they are their own small frozen catalog in the same *style* --
schema-versioned, typed, queryable -- rather than a new row in
``door_errors.REASONS`` (whose ``retryable``/``mcp_code``/``http_status``
triple has no meaning for a background reconciliation event nobody's request
is waiting on).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from clio_relay.errors import RelayError
from clio_relay.models import EndpointRegistration, utc_now

logger = logging.getLogger(__name__)

#: Schema of the durable event payload recording one quarantined recovery record.
WORKER_LANE_QUARANTINE_SCHEMA = "clio-relay.worker-lane-recovery-quarantine.v1"

#: Schema of the durable event payload / endpoint metadata recording one
#: otherwise-unhandled worker-slot lane iteration failure.
WORKER_LANE_ITERATION_FAILURE_SCHEMA = "clio-relay.worker-lane-iteration-failure.v1"

#: Typed reason: a durable JARVIS execution-recovery record failed
#: validation and was quarantined rather than raised.
RECOVERY_RECORD_QUARANTINED = "jarvis_execution_recovery_quarantined"

#: Typed reason: one worker-slot poll iteration raised an otherwise-unhandled
#: exception and was contained by the per-slot lane barrier.
WORKER_LANE_ITERATION_FAILED = "worker_lane_iteration_failed"

#: Endpoint registration metadata key carrying the most recent lane failure
#: (cleared once an iteration succeeds again).
LANE_LAST_ERROR_METADATA_KEY = "lane_last_error"

#: Bound on the exception detail text folded into a typed event/metadata
#: record -- mirrors the byte-budget doctrine other typed-reason payloads in
#: this codebase already apply (door_errors.py's MAX_MESSAGE_CHARS), sized
#: down for a field that is diagnostic context, not the primary message.
_MAX_DETAIL_CHARS = 2_000


def _bounded_detail(text: str) -> str:
    """Clip an exception's ``str()`` so one lane failure cannot bloat records."""
    if len(text) <= _MAX_DETAIL_CHARS:
        return text
    return text[: _MAX_DETAIL_CHARS - 1] + "…"


class WorkerLaneEventQueue(Protocol):
    """The one durable-queue capability quarantine needs: append a typed event."""

    def append_event(
        self,
        job_id: str,
        event_type: str,
        message: str,
        *,
        payload: dict[str, object] | None = None,
    ) -> object: ...


class WorkerLaneRegistrationQueue(Protocol):
    """The one durable-queue capability the lane barrier needs: re-register."""

    def register_endpoint(self, endpoint: EndpointRegistration) -> EndpointRegistration: ...


@dataclass(frozen=True, slots=True)
class PoisonedRecoveryQuarantine:
    """One poisoned durable JARVIS execution-recovery record, quarantined."""

    task_id: str
    job_id: str
    context: str
    detail: str
    quarantined_at: datetime

    def as_event_payload(self) -> dict[str, object]:
        """Return the durable event payload recording this quarantine."""
        return {
            "schema_version": WORKER_LANE_QUARANTINE_SCHEMA,
            "reason": RECOVERY_RECORD_QUARANTINED,
            "task_id": self.task_id,
            "job_id": self.job_id,
            "context": self.context,
            "detail": self.detail,
            "quarantined_at": self.quarantined_at.isoformat(),
        }


def quarantine_relay_error[T](
    action: Callable[[], T],
    *,
    queue: WorkerLaneEventQueue,
    job_id: str,
    task_id: str,
    context: str,
) -> T | None:
    """Run ``action``; a :class:`~clio_relay.errors.RelayError` is quarantined.

    clio-relay#238: a poisoned durable ``jarvis_execution_recovery`` record
    previously raised an unguarded ``RelayError`` out of
    ``EndpointWorker._reconcile_pending_execution_cleanup``'s first
    recovery-intent fetch per marker -- a call outside that method's own
    per-marker ``try/except`` -- which killed the daemon-mode worker slot
    calling it with no log line (see the module docstring for the full
    mechanism). Every caller of the wrapped validator already treats
    ``None`` as "no recovery to reconcile, fall back to standard cleanup" --
    the same safe default a record too corrupt to trust deserves, so
    quarantining converts a process-killing crash into that existing,
    well-trodden fallback plus a typed, queryable durable event.

    Args:
        action: The validator call to run, e.g. ``lambda:
            _durable_jarvis_execution_recovery(job, task)``.
        queue: The durable queue to record the quarantine event on.
        job_id: The job the poisoned task belongs to.
        task_id: The task carrying the poisoned record.
        context: Short label for which call site quarantined this record
            (folded into the event payload for triage).

    Returns:
        ``action()``'s result, or ``None`` if it raised ``RelayError``.
    """
    try:
        return action()
    except RelayError as exc:
        record = PoisonedRecoveryQuarantine(
            task_id=task_id,
            job_id=job_id,
            context=context,
            detail=_bounded_detail(f"{type(exc).__name__}: {exc}"),
            quarantined_at=utc_now(),
        )
        queue.append_event(
            job_id,
            "jarvis.execution_recovery_quarantined",
            f"Poisoned JARVIS execution-recovery record quarantined ({context})",
            payload=record.as_event_payload(),
        )
        logger.warning(
            "clio-relay#238: quarantined poisoned JARVIS execution-recovery record "
            "task=%s job=%s context=%s detail=%s",
            task_id,
            job_id,
            context,
            record.detail,
        )
        return None


@dataclass(frozen=True, slots=True)
class WorkerLaneIterationFailure:
    """One otherwise-unhandled exception from a worker-slot poll iteration."""

    endpoint_id: str
    detail: str
    failed_at: datetime

    def as_payload(self) -> dict[str, object]:
        """Return the typed payload recorded on the event log and registry."""
        return {
            "schema_version": WORKER_LANE_ITERATION_FAILURE_SCHEMA,
            "reason": WORKER_LANE_ITERATION_FAILED,
            "endpoint_id": self.endpoint_id,
            "detail": self.detail,
            "failed_at": self.failed_at.isoformat(),
        }


def run_worker_lane_iteration(
    iterate: Callable[[], object],
    *,
    queue: WorkerLaneRegistrationQueue,
    endpoint: EndpointRegistration,
) -> EndpointRegistration:
    """Run one worker-slot poll iteration; never let it die silently.

    clio-relay#238 acceptance item 3: any slot/lane death must emit a typed
    reason to the worker log *and* the endpoint registry -- the 0-byte-log
    failure mode must be structurally impossible. This is the last-resort
    barrier: :func:`quarantine_relay_error` already prevents the one known
    poisoned-record crash from reaching here, but this wrapper contains
    *any* exception a future failure mode raises, so a daemon-mode slot can
    never again die with zero evidence.

    Args:
        iterate: One worker-slot iteration, e.g. ``lambda: worker.run_once(
            mcp_admission_class=..., mcp_admission_limit=...)``. Its return
            value is discarded; it runs for effect.
        queue: The durable queue to heartbeat/re-register the endpoint on.
        endpoint: The current registration for this slot.

    Returns:
        The endpoint registration to keep looping with -- unchanged on
        success (unless a prior failure's ``lane_last_error`` needed
        clearing), or updated with a typed ``lane_last_error`` on failure.
    """
    try:
        iterate()
    except Exception as exc:  # noqa: BLE001 - the lane's last-resort barrier, #238
        failure = WorkerLaneIterationFailure(
            endpoint_id=endpoint.endpoint_id,
            detail=_bounded_detail(f"{type(exc).__name__}: {exc}"),
            failed_at=utc_now(),
        )
        logger.error(
            "clio-relay#238: worker lane iteration failed endpoint=%s detail=%s",
            endpoint.endpoint_id,
            failure.detail,
            exc_info=True,
        )
        updated_metadata = {**endpoint.metadata, LANE_LAST_ERROR_METADATA_KEY: failure.as_payload()}
        return queue.register_endpoint(endpoint.model_copy(update={"metadata": updated_metadata}))
    if LANE_LAST_ERROR_METADATA_KEY in endpoint.metadata:
        cleared_metadata = {
            key: value
            for key, value in endpoint.metadata.items()
            if key != LANE_LAST_ERROR_METADATA_KEY
        }
        return queue.register_endpoint(endpoint.model_copy(update={"metadata": cleared_metadata}))
    return endpoint
