"""Server-side owned-session client-liveness lease (iowarp/clio-relay#277).

When the desktop client walks away (crash, network loss, or an ordinary
``session detach``), nothing on the remote side today notices: session-level
cleanup is client-initiated only. This lease is the typed, bounded contract
that lets the remote side notice and clean itself up:

* Opened by the FIRST authenticated request the owned-session HTTP API
  receives (``http_api_auth._require_api_token`` /
  ``http_api_owner_session_lease_renewal.renew_owner_session_lease``) --
  NOT at session bring-up. A session whose desktop crashes before making
  even one authenticated request (attach's status probe, a poll, a
  submission) never gets a lease record at all and is therefore never swept
  by this mechanism (adversarial-review MINOR: this is a real, currently
  undocumented functional gap, not merely a docstring nuance -- there is no
  clean server-side bring-up hook to close it yet; ``session
  recovery-status``/the SSH-driven start path do not share the HTTP
  request/response cycle this lease's renewal chokepoint depends on).
* Renewed by ordinary authenticated HTTP traffic to the owned-session API --
  attach, polls, submissions all pass through the SAME dependency
  (``http_api_auth._require_api_token``), and long-lived SSE/WebSocket
  streams renew on every poll tick
  (``http_api_owner_session_lease_renewal.renew_owner_session_lease``,
  called from ``http_api_streaming.py``'s poll loops) -- so renewal costs
  the client nothing new and a stream held open longer than the TTL is not
  reaped mid-stream.
* Swept by the worker's own periodic cycle
  (``endpoint_owner_session_sweep.sweep_expired_owner_session_leases``,
  joined onto ``run_once`` next to the existing job-lease sweep) once it has
  gone quiet past its own TTL.

Two DISTINCT terminal states for a normal close, never conflated: ``closed``
(an explicit, successful ``session teardown``) and ``expired`` (the sweep
reaped it after the TTL elapsed with no renewal) -- a caller reading this
record, or ``session recovery-status``'s ``owner_session_lease_status``
projection of it, can always tell which happened. A THIRD terminal state,
``quarantined``, exists for the sweep's own bounded-retry discipline: a
session whose recorded cleanup intent conflicts with what the sweep can
safely do (adversarial-review BLOCKER 2) stops retrying forever after
``MAX_OWNER_SESSION_SWEEP_ATTEMPTS`` failures instead of spinning a
traceback every cycle -- see ``sweep_failure_count``/``last_sweep_error``.

``expired_with_running_jobs`` records whether the queue still had
non-terminal jobs for this generation at the moment the lease was reaped:
leases never kill jobs (they are owned by the queue, not by the session's
liveness), so those jobs keep running and their ids are preserved here
(``running_job_ids_at_close``, capped at
``MAX_OWNER_SESSION_LEASE_RUNNING_JOB_IDS`` -- ``running_job_ids_truncated``
records when the sweep's own bounded job-listing walk hit that cap without
finishing) for a later ``session attach`` to recover.

Storage: one more small JSON sibling file per owner session
(``owner_sessions/<label>.lease.json``), in the SAME record family
``queue_owner_session_records.py`` already owns for
``.active.json``/``.closing.json``/``.closed.json``. This is not a new
store -- it is one more record kind in the existing one (house rule: never
add a fifth materialization of session state, iowarp/clio-relay
CLAUDE.md Rule 4).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.identifiers import DurableRecordId

OWNER_SESSION_LEASE_SCHEMA = "clio-relay.owner-session-lease.v1"

#: Bounds the running-job-id snapshot taken at close time. Matches the order
#: of magnitude of other owner-session job-membership bounds
#: (``queue_layout.MAX_ACTIVE_JOB_RECORDS``) without importing that module
#: purely for one constant.
MAX_OWNER_SESSION_LEASE_RUNNING_JOB_IDS = 1_000

#: Default TTL: 0 -- the lease NEVER expires unless a deployment opts in.
#: Owner ruling (2026-08-27, direct): a timeout must never destroy a live
#: session by default -- on the ssh transport a reaped session costs the user
#: a VPN + 2FA re-authentication, and a 30-minute reaper made the product
#: unusable ("i will stop using it if i have to reconnect every 30 min").
#: Bounded self-clean is an OPT-IN deployment choice
#: (``CLIO_RELAY_OWNER_SESSION_LEASE_TTL_SECONDS`` >= 30), not a default.
#: The authoritative value is ``RelaySettings.owner_session_lease_ttl_seconds``
#: (config.py); this is only the fallback that field itself defaults to.
DEFAULT_OWNER_SESSION_LEASE_TTL_SECONDS = 0

#: Bounds the sweep's own retry loop (BLOCKER 2): a session whose recorded
#: cleanup intent the sweep cannot safely honor stops retrying after this
#: many consecutive failed attempts and reaches ``quarantined`` instead of
#: retrying forever with a fresh traceback every worker cycle.
MAX_OWNER_SESSION_SWEEP_ATTEMPTS = 5

#: Bounds the typed last-failure detail string kept for diagnosis.
MAX_OWNER_SESSION_LEASE_SWEEP_ERROR_CHARS = 2_048

OwnerSessionLeaseStatus = Literal["open", "closed", "expired", "quarantined"]
OwnerSessionLeaseCloseReason = Literal["client_close", "lease_expired", "expiry_quarantined"]


class OwnerSessionLease(BaseModel):
    """One owned-session generation's server-side client-liveness lease.

    ``status`` carries the whole state machine: ``open`` while renewed,
    ``closed`` after an explicit clean teardown, ``expired`` after the
    worker's sweep reaps a lease that went quiet past ``ttl_seconds``,
    ``quarantined`` after the sweep gives up retrying a session it cannot
    safely reap (BLOCKER 2). Once terminal, a lease never reopens -- a NEW
    owned-session generation for the same ``owner_session_id`` gets its OWN
    fresh lease (``queue_owner_session_lease.touch_owner_session_lease``
    overwrites the sibling file for a differing ``session_generation_id``,
    exactly mirroring how ``.active.json`` moves across generations).
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.owner-session-lease.v1"] = OWNER_SESSION_LEASE_SCHEMA
    owner_session_id: str = Field(min_length=1, max_length=256)
    session_generation_id: DurableRecordId
    cluster: str = Field(min_length=1, max_length=256)
    #: 0 = the lease never expires (the default posture); positive values
    #: opt this generation into bounded self-clean.
    ttl_seconds: int = Field(ge=0, le=86_400)
    status: OwnerSessionLeaseStatus = "open"
    opened_at: datetime
    last_seen_at: datetime
    closed_at: datetime | None = None
    close_reason: OwnerSessionLeaseCloseReason | None = None
    expired_with_running_jobs: bool = False
    running_job_ids_at_close: list[str] = Field(
        default_factory=list[str],
        max_length=MAX_OWNER_SESSION_LEASE_RUNNING_JOB_IDS,
    )
    #: MINOR (adversarial review): true when the sweep's bounded job-listing
    #: walk hit its own page cap before exhausting every non-terminal job --
    #: ``running_job_ids_at_close`` is then a PARTIAL snapshot, not a
    #: complete one. ``expired_with_running_jobs`` stays accurate either way
    #: (it only needs to know "at least one").
    running_job_ids_truncated: bool = False
    #: Review residual 1: an expiry whose teardown call FAILED (with the
    #: owned-session process possibly still alive) must never be recorded as
    #: a clean reap -- the record itself would overclaim. ``teardown_failed``
    #: marks the reap as degraded; ``teardown_error`` carries the bounded
    #: typed reason so a later ``session attach`` projection can surface it.
    teardown_failed: bool = False
    teardown_error: str | None = Field(default=None, max_length=512)
    #: BLOCKER 2: how many consecutive sweep attempts have failed for this
    #: still-open lease. Reset implicitly by never incrementing once the
    #: lease reaches a terminal status.
    sweep_failure_count: int = Field(default=0, ge=0, le=MAX_OWNER_SESSION_SWEEP_ATTEMPTS + 1)
    #: The typed detail of the most recent sweep failure, kept for
    #: diagnosis -- never silently dropped (no-silent-fallback doctrine).
    last_sweep_error: str | None = Field(
        default=None,
        max_length=MAX_OWNER_SESSION_LEASE_SWEEP_ERROR_CHARS,
    )

    def is_due(self, *, now: datetime | None = None) -> bool:
        """Return whether an OPEN lease has gone quiet past its own TTL.

        Always ``False`` for a lease that is already terminal (``closed``,
        ``expired``, or ``quarantined``) -- due-ness is only ever asked of a
        still-open lease.
        """
        if self.status != "open":
            return False
        if self.ttl_seconds == 0:
            # Disabled lease (the default): never due, no matter how quiet.
            # Only an explicit close (clean teardown) ever terminates it.
            return False
        current = now or datetime.now(UTC)
        return current >= self.last_seen_at + timedelta(seconds=self.ttl_seconds)
