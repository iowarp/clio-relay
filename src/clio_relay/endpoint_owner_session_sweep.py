"""Worker-side sweep for expired owned-session client-liveness leases.

iowarp/clio-relay#277: joins the SAME per-cycle machinery ``run_once``
already runs the job-lease sweep through (``queue.recover_stale_jobs``, see
``endpoint_serve_loop.py``) rather than minting a second scheduler -- this
module is called once more, right next to that call, on every worker cycle.
Because ``run_once`` runs on the FIRST cycle after a restart too, a worker
that comes back up after being down replays/validates owned-session leases
before doing anything else -- for the owned-session-lease dimension of the
#238/#240 "worker restart replays leases" ask specifically (the JARVIS
execution-recovery-record shard of those issues is untouched; see this
campaign's report). This claim now rests on more than wiring: BLOCKER 2's
bounded-retry quarantine and MEDIUM 7's scan/prune containment are what
make a REAL restart-into-a-backlog survive rather than wedge or spin --
``test_run_once_sweeps_owner_session_leases_every_cycle`` proves the wiring,
and the sweep-failure/containment tests in ``test_owner_session_lease.py``
prove the survival properties around it.

For each lease gone quiet past its own TTL:

1. Re-check the durable generation-admission state. A concurrent client-
   driven ``session teardown`` (or a previous, partially-applied sweep
   retried after a crash) may have already moved the generation on; in that
   case only the lease bookkeeping needs to catch up (CAS-protected, see
   below), and nothing else runs.
2. Resolve the cleanup policy: a client that recorded a cleanup intent
   before crashing (e.g. ``session quiesce-intake --cancel-jobs``) already
   made a durable promise about ``stop_worker``/``cancel_jobs``/
   ``cancel_scheduler_jobs`` -- the sweep HONORS that recorded intent
   (``queue.get_owner_session_cleanup_intent``) instead of hardcoding its
   own policy, and reuses the recorded ``operation_id`` for teardown instead
   of minting a fresh one (adversarial-review BLOCKER 1: a fresh id there
   made ``execute_owned_session_teardown``'s own internal quiesce call
   refuse as "operation changed during retry" -- a guaranteed no-op). No
   session ever had a recorded intent AND is a fresh crash gets the sweep's
   own conservative all-``False`` policy with a freshly minted operation id.
3. Quiesces via ``queue.set_owner_session_closing`` with the resolved
   policy/operation id, then RE-READS the lease and re-checks ``is_due``
   before doing anything further (BLOCKER 3: the same expected-state
   discipline ``set_owner_session_closing`` itself applies) -- a renewal
   landing between the due-scan and this point aborts the sweep for this
   lease before anything destructive runs. Residual: the durable
   "closing" quiescence set moments ago is NOT rolled back on this abort
   path (there is no primitive to safely undo it) -- a session that proves
   alive right after being quiesced stays quiesced (blocks new job
   submissions) until an operator resumes it. Documented, not hidden: this
   is a vastly smaller blast radius than killing a live process, which is
   the alternative this exists to prevent.
4. Reuses :func:`clio_relay.session_cleanup_execution.execute_owned_session_teardown`
   -- the SAME cluster-local primitive the desktop's own ``session
   teardown`` invokes (over SSH, as ``session teardown-owned``) -- to kill
   the owned-session API process's systemd-contained scope and sanitize its
   local session files. No parallel cleanup machinery.
5. Reuses ``queue.set_owner_session_closed`` -- the SAME durable-admission
   primitive the desktop's own final teardown step (``session mark-closed``)
   calls -- to close generation admission.
6. Marks the lease ``expired`` (CAS-protected against a renewal landing
   during teardown) and, when jobs were still non-terminal at sweep time,
   stamps ``expired_with_running_jobs`` plus their ids (bounded; truncated
   is recorded honestly) for a later ``session attach`` to recover.
7. Appends a typed ``owner_session.lease_expired`` event onto each such
   still-running job's OWN event stream.

Two more belts against a runaway sweep (MEDIUM 6): a wall-clock-vs-monotonic
divergence check skips a whole cycle with a typed reason instead of treating
a system clock jump as "every lease is suddenly due", and a per-cycle reap
cap bounds the blast radius even if that check somehow misses one. MEDIUM 7:
the due-scan itself, and best-effort terminal-record pruning, run inside
this module's own containment -- a storage-layer safety-bound refusal
degrades to "swept nothing this cycle", never an uncaught exception that
would take the worker's ``while True`` loop down with it.

BLOCKER 2's bounded retry: any per-lease failure (including one this module
does not anticipate) is recorded via
``queue.record_owner_session_lease_sweep_failure`` instead of just logged --
after enough consecutive failures the lease reaches the typed terminal
``quarantined`` status and the due-scan stops selecting it, instead of
retrying forever with a fresh traceback every ~2s.

Deliberately NOT reused: the two-sided "coordinator report" ceremony
(``cli_session_teardown_finalize.py``'s SSH round trip back to the desktop
to persist a signed evidence artifact, and the desktop-local
``owned_session_record``/validation-report bookkeeping around it). That
ceremony exists to hand the DESKTOP a receipt; when this sweep runs, the
desktop is exactly the thing that is gone. Skipping it is not a parallel
mechanism; it is the same mechanism minus the one step that requires a
coordinator which, by definition, is absent here.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, cast

from clio_relay.errors import QueueConflictError, RelayError
from clio_relay.models_owner_session_lease import OwnerSessionLease
from clio_relay.session_cleanup_execution import execute_owned_session_teardown
from clio_relay.session_lifecycle import OwnedSessionTeardownRequest

if TYPE_CHECKING:
    from clio_relay.endpoint import EndpointWorker

logger = logging.getLogger(__name__)

#: Bounds one sweep cycle's worth of due leases the QUERY returns (the
#: storage layer's own `due_expired_owner_session_leases` default already
#: matches this; kept as an explicit constant here so a future caller
#: cannot forget the bound).
MAX_OWNER_SESSION_SWEEP_LEASES_PER_CYCLE = 256

#: MEDIUM 6, second belt: bounds how many of those due leases are actually
#: REAPED in one cycle, independent of the clock-jump check above it. A
#: legitimate mass-expiry (a network outage affecting many sessions at
#: once) still drains within a handful of cycles at ~2s apiece.
MAX_OWNER_SESSION_SWEEP_REAP_PER_CYCLE = 32

#: Bounds how many non-terminal job ids one swept session's job listing walks
#: (pages beyond this stop -- `running_job_ids_truncated` records that the
#: id list is a partial snapshot; `expired_with_running_jobs` stays correct
#: either way, since it only needs to know "at least one").
MAX_OWNER_SESSION_SWEEP_JOB_LISTING_PAGES = 4

#: MEDIUM 7: terminal lease records are pruned once safely past this
#: retention window (24h) -- long enough for an operator to observe
#: `expired_with_running_jobs` on a real incident before the record
#: disappears, short enough that steady churn never approaches the
#: directory-scan safety bound.
OWNER_SESSION_LEASE_PRUNE_RETENTION_SECONDS = 86_400

#: MEDIUM 6: generous slack against ordinary scheduling jitter between the
#: two clock reads themselves -- NOT related to poll_seconds or the lease
#: TTL. A genuine wall-clock jump (NTP correction, manual date change)
#: diverges the wall clock from the monotonic clock by far more than this;
#: a slow cycle (a long-running job between polls) advances both together
#: and never trips it.
CLOCK_JUMP_SLACK_SECONDS = 10.0

#: Per-cluster (wall_time, monotonic_time) of the last sweep call, used by
#: the clock-jump check. Module-level by design: an EndpointWorker instance
#: (and the single cluster it serves) lives for the whole `while True` loop,
#: so this needs no more durability than the process itself -- a restart
#: naturally re-baselines, which is exactly what should happen.
_LAST_SWEEP_CLOCKS: dict[str, tuple[float, float]] = {}


def sweep_expired_owner_session_leases(worker: EndpointWorker, *, cluster: str) -> int:
    """Reap owned-session leases that have gone quiet past their own TTL.

    Returns the number of leases swept (0 in the common, steady-state case,
    and 0 whenever a cycle is skipped for a typed reason -- clock jump,
    scan failure). Never raises: every failure mode this function itself
    anticipates is caught and degrades to "swept nothing this cycle" with a
    structured log line, so one poisoned session, one storage hiccup, or one
    clock anomaly can never wedge the worker's own ``while True`` loop
    (MEDIUM 6, MEDIUM 7).
    """
    if _clock_jump_detected(cluster):
        logger.warning(
            "owner_session.lease_sweep_clock_jump_detected",
            extra={"cluster": cluster},
        )
        return 0
    queue = worker.queue
    try:
        due = queue.due_expired_owner_session_leases(
            cluster=cluster,
            limit=MAX_OWNER_SESSION_SWEEP_LEASES_PER_CYCLE,
        )
    except Exception:
        # MEDIUM 7: the due-scan's own safety bound (or any other storage
        # failure) must degrade this ONE cycle, not the worker's loop.
        logger.warning(
            "owner_session.lease_sweep_scan_failed",
            extra={"cluster": cluster},
            exc_info=True,
        )
        return 0
    swept = 0
    for lease in due[:MAX_OWNER_SESSION_SWEEP_REAP_PER_CYCLE]:
        try:
            if _sweep_one_owner_session_lease(worker, lease):
                swept += 1
        except Exception as exc:
            # BLOCKER 2: bounded retry, not an eternal per-cycle traceback --
            # record the typed failure and let the queue layer decide
            # whether this lease has now exhausted its attempts and must
            # quarantine.
            _record_sweep_attempt_failure(worker, lease, exc)
    try:
        queue.prune_terminal_owner_session_leases(
            cluster=cluster,
            older_than_seconds=OWNER_SESSION_LEASE_PRUNE_RETENTION_SECONDS,
        )
    except Exception:
        logger.warning(
            "owner_session.lease_prune_failed",
            extra={"cluster": cluster},
            exc_info=True,
        )
    return swept


def _clock_jump_detected(cluster: str) -> bool:
    """Return whether the wall clock jumped relative to the monotonic clock.

    MEDIUM 6: compares the DELTA of both clocks since the last call for this
    cluster, not the absolute elapsed time -- a legitimately slow cycle (a
    long-running job between polls) advances both clocks together and never
    trips this; only a genuine discontinuity in the wall clock (NTP
    correction, a manual date change) diverges them.
    """
    wall_now = time.time()
    mono_now = time.monotonic()
    previous = _LAST_SWEEP_CLOCKS.get(cluster)
    _LAST_SWEEP_CLOCKS[cluster] = (wall_now, mono_now)
    if previous is None:
        return False
    previous_wall, previous_mono = previous
    wall_delta = wall_now - previous_wall
    mono_delta = mono_now - previous_mono
    return wall_delta - mono_delta > CLOCK_JUMP_SLACK_SECONDS


def _record_sweep_attempt_failure(
    worker: EndpointWorker,
    lease: OwnerSessionLease,
    exc: Exception,
) -> None:
    logger.warning(
        "owner_session.lease_sweep_failed",
        extra={
            "owner_session_id": lease.owner_session_id,
            "session_generation_id": lease.session_generation_id,
            "cluster": lease.cluster,
        },
        exc_info=True,
    )
    try:
        worker.queue.record_owner_session_lease_sweep_failure(
            lease.owner_session_id,
            session_generation_id=lease.session_generation_id,
            reason=f"{type(exc).__name__}: {exc}",
        )
    except Exception:
        # The failure-bookkeeping call itself failing is not allowed to
        # propagate either -- it is already inside the sweep's own per-lease
        # containment, but this is the last line of defense.
        logger.warning(
            "owner_session.lease_sweep_failure_bookkeeping_failed",
            extra={
                "owner_session_id": lease.owner_session_id,
                "session_generation_id": lease.session_generation_id,
                "cluster": lease.cluster,
            },
            exc_info=True,
        )


def _sweep_one_owner_session_lease(worker: EndpointWorker, lease: OwnerSessionLease) -> bool:
    """Reap one due lease. Returns whether it actually reaped anything.

    ``False`` (never counted in ``sweep_expired_owner_session_leases``'s
    return value) covers every abort path where a concurrent renewal was
    detected and nothing was touched -- BLOCKER 3's CAS-refused close and
    its post-quiesce re-check both return here without acting. A caller
    that wants to know "did this cycle make progress" reads the return
    value, not merely "did this raise".
    """
    queue = worker.queue
    owner_session_id = lease.owner_session_id
    session_generation_id = lease.session_generation_id
    # BLOCKER 3: anchors every close call below to the EXACT last_seen_at
    # this due-scan observed -- a renewal landing after this point never
    # gets silently overwritten by a close that is now stale.
    expected_last_seen_at = lease.last_seen_at

    admission = queue.owner_session_generation_status(
        owner_session_id,
        session_generation_id=session_generation_id,
    )
    if admission.get("active_generation_id") != session_generation_id:
        # Something else already moved this generation on (a concurrent
        # client teardown, or a previous sweep attempt that got this far
        # before a worker restart interrupted it). Only the lease
        # bookkeeping is still stale; reap that and stop -- no double
        # teardown, no double closure. CAS-protected the same as every
        # other close call: if this lease was ALSO just renewed, do not
        # overwrite that.
        result = queue.close_owner_session_lease(
            owner_session_id,
            session_generation_id=session_generation_id,
            reason="lease_expired",
            running_job_ids=(),
            expected_last_seen_at=expected_last_seen_at,
        )
        return result is not None and result.status != "open"

    running_job_ids, running_job_ids_truncated = _owned_generation_running_job_ids(
        worker,
        owner_session_id=owner_session_id,
        session_generation_id=session_generation_id,
    )

    # BLOCKER 2: honor a RECORDED cleanup intent -- a client that already
    # durably declared "stop_worker=X/cancel_jobs=Y/cancel_scheduler_jobs=Z"
    # (e.g. via `session quiesce-intake --cancel-jobs`) before crashing made
    # a promise the sweep must keep the SAME shape as, or
    # `set_owner_session_closing`'s own idempotent-intent check refuses
    # every retry forever ("policy changed during retry"). Only a session
    # with NO recorded intent gets the sweep's own conservative default.
    try:
        existing_intent = queue.get_owner_session_cleanup_intent(
            owner_session_id,
            session_generation_id=session_generation_id,
        )
    except QueueConflictError:
        # An unexpected stale/mismatched closing record for a DIFFERENT
        # generation than the one just confirmed active above -- should not
        # happen (prepare_owner_session_start clears it on a new
        # generation), but this read must never abort the sweep; fall back
        # to the sweep's own default policy exactly as if no intent existed.
        existing_intent = None
    if existing_intent is not None:
        stop_worker = bool(existing_intent["stop_worker"])
        cancel_jobs = bool(existing_intent["cancel_jobs"])
        cancel_scheduler_jobs = bool(existing_intent["cancel_scheduler_jobs"])
    else:
        stop_worker = False
        cancel_jobs = False
        cancel_scheduler_jobs = False

    # Quiesce explicitly, BEFORE calling teardown -- mirroring the client's
    # own flow (cli_session_teardown_jobs._quiesce_owner_session_intake runs
    # before session_lifecycle.teardown_remote_session). execute_owned_
    # session_teardown also quiesces internally, but this sweep must not
    # depend on that: if teardown fails before reaching its own quiesce
    # call, set_owner_session_closed below would otherwise refuse ("must be
    # closing before it can be closed"). set_owner_session_closing is
    # idempotent for a retry with the SAME policy, so calling it here and
    # having teardown call it again is a no-op, not a double transition.
    cleanup_intent = queue.set_owner_session_closing(
        owner_session_id,
        session_generation_id=session_generation_id,
        stop_worker=stop_worker,
        cancel_jobs=cancel_jobs,
        cancel_scheduler_jobs=cancel_scheduler_jobs,
    )
    # BLOCKER 1: reuse the operation_id THIS quiesce call returned (whether
    # freshly minted or the already-recorded one) for teardown's
    # `expected_cleanup_operation_id`. A fresh, unrelated uuid here made
    # execute_owned_session_teardown's own internal quiesce call refuse as
    # "operation changed during retry" -- a guaranteed no-op that silently
    # left the process running forever.
    operation_id = cast(str, cleanup_intent["operation_id"])

    # BLOCKER 3: re-read + re-check due-ness AFTER quiesce, BEFORE teardown
    # (which kills the live process) -- the same expected-state discipline
    # set_owner_session_closing itself applies. See the module docstring
    # for the documented residual when this aborts.
    current_lease = queue.owner_session_lease_status(
        owner_session_id,
        session_generation_id=session_generation_id,
    )
    if current_lease is None or not current_lease.is_due():
        logger.info(
            "owner_session.lease_sweep_aborted_renewed",
            extra={
                "owner_session_id": owner_session_id,
                "session_generation_id": session_generation_id,
                "cluster": lease.cluster,
            },
        )
        return False

    try:
        execute_owned_session_teardown(
            OwnedSessionTeardownRequest(
                cluster=lease.cluster,
                session_id=owner_session_id,
                expected_session_generation_id=session_generation_id,
                expected_cleanup_operation_id=operation_id,
                stop_worker=stop_worker,
                cancel_jobs=cancel_jobs,
                cancel_scheduler_jobs=cancel_scheduler_jobs,
            ),
            core_dir=worker.settings.core_dir,
        )
    except RelayError:
        # Not fatal to the sweep: the process may already be gone (the
        # ordinary crash case), or a concurrent path already tore it down.
        # A precondition mismatch (the guaranteed-no-op BLOCKER 1 bug) is no
        # longer reachable here -- operation_id and policy both come from
        # the SAME resolved intent both calls share. The generation-
        # admission closure below is what actually matters for "no manual
        # action" -- log the structured reason and continue.
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

    closed = queue.close_owner_session_lease(
        owner_session_id,
        session_generation_id=session_generation_id,
        reason="lease_expired",
        running_job_ids=running_job_ids,
        running_job_ids_truncated=running_job_ids_truncated,
        expected_last_seen_at=expected_last_seen_at,
    )
    if closed is None or closed.status == "open":
        # By this point the process is already dead and admission already
        # closed -- a CAS refusal here would mean a renewal raced in after
        # teardown killed the process, which cannot happen (no live process,
        # no more HTTP traffic to renew it). Logged, not asserted, in case
        # that invariant is ever wrong.
        logger.warning(
            "owner_session.lease_sweep_final_close_unexpectedly_no_op",
            extra={
                "owner_session_id": owner_session_id,
                "session_generation_id": session_generation_id,
                "cluster": lease.cluster,
            },
        )
        return False
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
    return True


def _owned_generation_running_job_ids(
    worker: EndpointWorker,
    *,
    owner_session_id: str,
    session_generation_id: str,
) -> tuple[list[str], bool]:
    """Return this generation's non-terminal job ids (bounded, page-walked).

    The second element is ``True`` when the bounded page walk hit its own
    cap before exhausting every non-terminal job -- the returned id list is
    then a PARTIAL snapshot, not a complete one (MINOR, adversarial review).
    """
    queue = worker.queue
    job_ids: list[str] = []
    cursor: str | None = None
    truncated = True
    for _ in range(MAX_OWNER_SESSION_SWEEP_JOB_LISTING_PAGES):
        jobs, next_cursor, _source_total, _scan_count = queue.list_owner_session_jobs_page(
            owner_session_id,
            session_generation_id=session_generation_id,
            cursor=cursor,
            include_terminal=False,
        )
        job_ids.extend(job.job_id for job in jobs)
        if next_cursor is None:
            truncated = False
            break
        cursor = next_cursor
    return sorted(set(job_ids)), truncated
