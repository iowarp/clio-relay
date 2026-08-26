"""Worker-side sweep for expired owned-session client-liveness leases.

iowarp/clio-relay#277: joins the SAME per-cycle machinery ``run_once``
already runs the job-lease sweep through (``queue.recover_stale_jobs``, see
``endpoint_serve_loop.py``) rather than minting a second scheduler -- this
module is called once more, right next to that call, on every worker cycle.
Because ``run_once`` runs on the FIRST cycle after a restart too, a worker
that comes back up after being down replays/validates owned-session leases
before doing anything else, exactly as design point 4 (worker restart
replays leases) asks -- for the owned-session-lease dimension specifically;
see this campaign's report for the JARVIS-recovery-record shard of
#238/#240, which this sweep does not touch.

For each lease gone quiet past its own TTL:

1. Re-check the durable generation-admission state. A concurrent client-
   driven ``session teardown`` (or a previous, partially-applied sweep
   retried after a crash) may have already moved the generation on; in that
   case only the lease bookkeeping needs to catch up, and nothing else runs.
2. Reuse :func:`clio_relay.session_cleanup_execution.execute_owned_session_teardown`
   -- the SAME cluster-local primitive the desktop's own ``session
   teardown`` invokes (over SSH, as ``session teardown-owned``) -- to kill
   the owned-session API process's systemd-contained scope and sanitize its
   local session files. No parallel cleanup machinery.
3. Reuse ``queue.set_owner_session_closed`` -- the SAME durable-admission
   primitive the desktop's own final teardown step (``session mark-closed``)
   calls -- to close generation admission. This mirrors today's DEFAULT
   teardown behavior (``--keep-jobs``, the safe default): running jobs are
   left alone; admission closes regardless of whether any are still active.
4. Marks the lease ``expired`` (a status DISTINCT from ``closed``, which is
   reserved for an explicit, successful client-driven teardown -- see
   ``cli_session_owned.session_teardown_owned``) and, when jobs were still
   non-terminal at sweep time, stamps ``expired_with_running_jobs`` plus
   their ids for a later ``session attach`` to recover.
5. Appends a typed ``owner_session.lease_expired`` event onto each such
   still-running job's OWN event stream -- the SAME durable, queryable
   per-job event mechanism ``queue_lease_recovery.py`` already uses for
   job-lease expiry (``job.requeued``/``job.failed``); the no-silent-fallback
   trail for "why is this job's owning session gone".

Deliberately NOT reused: the two-sided "coordinator report" ceremony
(``cli_session_teardown_finalize.py``'s SSH round trip back to the desktop
to persist a signed evidence artifact, and the desktop-local
``owned_session_record``/validation-report bookkeeping around it). That
ceremony exists to hand the DESKTOP a receipt; when this sweep runs, the
desktop is exactly the thing that is gone -- there is no coordinator to hand
a receipt to. Skipping it is not a parallel mechanism; it is the same
mechanism minus the one step that requires a coordinator which, by
definition, is absent here.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import uuid4

from clio_relay.errors import RelayError
from clio_relay.models_owner_session_lease import OwnerSessionLease
from clio_relay.session_cleanup_execution import execute_owned_session_teardown
from clio_relay.session_lifecycle import OwnedSessionTeardownRequest

if TYPE_CHECKING:
    from clio_relay.endpoint import EndpointWorker

logger = logging.getLogger(__name__)

#: Bounds one sweep cycle's worth of due leases (the storage layer's own
#: `due_expired_owner_session_leases` default already matches this; kept as
#: an explicit constant here so a future caller cannot forget the bound).
MAX_OWNER_SESSION_SWEEP_LEASES_PER_CYCLE = 256

#: Bounds how many non-terminal job ids one swept session's job listing walks
#: (pages beyond this are simply not tracked in expired_with_running_jobs'
#: id list -- the boolean itself only needs at least one).
MAX_OWNER_SESSION_SWEEP_JOB_LISTING_PAGES = 4


def sweep_expired_owner_session_leases(worker: EndpointWorker, *, cluster: str) -> int:
    """Reap owned-session leases that have gone quiet past their own TTL.

    Returns the number of leases swept (0 in the common, steady-state case).
    Never raises for one session's teardown failure -- logs a structured
    warning and continues, so one poisoned session can never wedge the rest
    of the worker's cycle (mirrors ``run_worker_lane_iteration``'s per-slot
    containment doctrine, clio-relay#238).
    """
    queue = worker.queue
    due = queue.due_expired_owner_session_leases(
        cluster=cluster,
        limit=MAX_OWNER_SESSION_SWEEP_LEASES_PER_CYCLE,
    )
    swept = 0
    for lease in due:
        try:
            _sweep_one_owner_session_lease(worker, lease)
            swept += 1
        except Exception:
            logger.warning(
                "owner_session.lease_sweep_failed",
                extra={
                    "owner_session_id": lease.owner_session_id,
                    "session_generation_id": lease.session_generation_id,
                    "cluster": cluster,
                },
                exc_info=True,
            )
    return swept


def _sweep_one_owner_session_lease(worker: EndpointWorker, lease: OwnerSessionLease) -> None:
    queue = worker.queue
    owner_session_id = lease.owner_session_id
    session_generation_id = lease.session_generation_id

    admission = queue.owner_session_generation_status(
        owner_session_id,
        session_generation_id=session_generation_id,
    )
    if admission.get("active_generation_id") != session_generation_id:
        # Something else already moved this generation on (a concurrent
        # client teardown, or a previous sweep attempt that got this far
        # before a worker restart interrupted it). Only the lease
        # bookkeeping is still stale; reap that and stop -- no double
        # teardown, no double closure.
        queue.close_owner_session_lease(
            owner_session_id,
            session_generation_id=session_generation_id,
            reason="lease_expired",
            running_job_ids=(),
        )
        return

    running_job_ids = _owned_generation_running_job_ids(
        worker,
        owner_session_id=owner_session_id,
        session_generation_id=session_generation_id,
    )

    # Quiesce explicitly, BEFORE calling teardown -- mirroring the client's
    # own flow (cli_session_teardown_jobs._quiesce_owner_session_intake runs
    # before session_lifecycle.teardown_remote_session). execute_owned_
    # session_teardown also quiesces internally, but this sweep must not
    # depend on that: if teardown fails before reaching its own quiesce
    # call, set_owner_session_closed below would otherwise refuse ("must be
    # closing before it can be closed"). set_owner_session_closing is
    # idempotent for a retry with the SAME policy, so calling it here and
    # having teardown call it again is a no-op, not a double transition.
    queue.set_owner_session_closing(
        owner_session_id,
        session_generation_id=session_generation_id,
        stop_worker=False,
        cancel_jobs=False,
        cancel_scheduler_jobs=False,
    )

    try:
        execute_owned_session_teardown(
            OwnedSessionTeardownRequest(
                cluster=lease.cluster,
                session_id=owner_session_id,
                expected_session_generation_id=session_generation_id,
                expected_cleanup_operation_id=f"cleanup_{uuid4().hex}",
                stop_worker=False,
                cancel_jobs=False,
                cancel_scheduler_jobs=False,
            ),
            core_dir=worker.settings.core_dir,
        )
    except RelayError:
        # Not fatal to the sweep: the process may already be gone (the
        # ordinary crash case), or a concurrent path already tore it down.
        # The generation-admission closure below is what actually matters
        # for "no manual action" -- log the structured reason and continue.
        logger.warning(
            "owner_session.lease_expiry_teardown_failed",
            extra={
                "owner_session_id": owner_session_id,
                "session_generation_id": session_generation_id,
                "cluster": lease.cluster,
            },
            exc_info=True,
        )

    admission = queue.owner_session_generation_status(
        owner_session_id,
        session_generation_id=session_generation_id,
    )
    if not admission.get("closed"):
        queue.set_owner_session_closed(
            owner_session_id,
            session_generation_id=session_generation_id,
            residual_resource_ids=[],
        )

    queue.close_owner_session_lease(
        owner_session_id,
        session_generation_id=session_generation_id,
        reason="lease_expired",
        running_job_ids=running_job_ids,
    )
    for job_id in running_job_ids:
        queue.append_event(
            job_id,
            "owner_session.lease_expired",
            "Owned-session client-liveness lease expired; job continues under queue ownership",
            payload={
                "owner_session_id": owner_session_id,
                "session_generation_id": session_generation_id,
                "expired_with_running_jobs": True,
            },
        )


def _owned_generation_running_job_ids(
    worker: EndpointWorker,
    *,
    owner_session_id: str,
    session_generation_id: str,
) -> list[str]:
    """Return this generation's non-terminal job ids (bounded, page-walked)."""
    queue = worker.queue
    job_ids: list[str] = []
    cursor: str | None = None
    for _ in range(MAX_OWNER_SESSION_SWEEP_JOB_LISTING_PAGES):
        jobs, next_cursor, _source_total, _scan_count = queue.list_owner_session_jobs_page(
            owner_session_id,
            session_generation_id=session_generation_id,
            cursor=cursor,
            include_terminal=False,
        )
        job_ids.extend(job.job_id for job in jobs)
        if next_cursor is None:
            break
        cursor = next_cursor
    return sorted(set(job_ids))
