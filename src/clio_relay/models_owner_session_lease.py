"""Server-side owned-session client-liveness lease (iowarp/clio-relay#277).

When the desktop client walks away (crash, network loss, or an ordinary
``session detach``), nothing on the remote side today notices: session-level
cleanup is client-initiated only. This lease is the typed, bounded contract
that lets the remote side notice and clean itself up:

* Opened when an owned-session generation starts (mirrors the existing
  ``owner_sessions/<label>.active.json`` record's lifetime).
* Renewed by ordinary authenticated HTTP traffic to the owned-session API --
  attach, polls, submissions all pass through the SAME dependency
  (``http_api_auth._require_api_token``), so renewal costs the client
  nothing new.
* Swept by the worker's own periodic cycle
  (``endpoint_owner_session_sweep.sweep_expired_owner_session_leases``,
  joined onto ``run_once`` next to the existing job-lease sweep) once it has
  gone quiet past its own TTL.

Two DISTINCT terminal states, not one: ``closed`` (an explicit, successful
``session teardown``) and ``expired`` (the sweep reaped it after the TTL
elapsed with no renewal). They are never conflated -- a caller reading this
record, or ``session recovery-status``'s ``owner_session_lease_status``
projection of it, can always tell which happened. ``expired_with_running_jobs``
records whether the queue still had non-terminal jobs for this generation at
the moment the lease was reaped: leases never kill jobs (they are owned by
the queue, not by the session's liveness), so those jobs keep running and
their ids are preserved here for a later ``session attach`` to recover.

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

#: A sane, non-hardcoded-in-scattered-constants default TTL (30 minutes).
#: The authoritative value is ``RelaySettings.owner_session_lease_ttl_seconds``
#: (config.py); this is only the fallback that field itself defaults to.
DEFAULT_OWNER_SESSION_LEASE_TTL_SECONDS = 1_800

OwnerSessionLeaseStatus = Literal["open", "closed", "expired"]
OwnerSessionLeaseCloseReason = Literal["client_close", "lease_expired"]


class OwnerSessionLease(BaseModel):
    """One owned-session generation's server-side client-liveness lease.

    ``status`` carries the whole state machine: ``open`` while renewed,
    ``closed`` after an explicit clean teardown, ``expired`` after the
    worker's sweep reaps a lease that went quiet past ``ttl_seconds``. Once
    terminal (``closed`` or ``expired``), a lease never reopens -- a NEW
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
    ttl_seconds: int = Field(gt=0, le=86_400)
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

    def is_due(self, *, now: datetime | None = None) -> bool:
        """Return whether an OPEN lease has gone quiet past its own TTL.

        Always ``False`` for a lease that is already terminal (``closed`` or
        ``expired``) -- due-ness is only ever asked of a still-open lease.
        """
        if self.status != "open":
            return False
        current = now or datetime.now(UTC)
        return current >= self.last_seen_at + timedelta(seconds=self.ttl_seconds)
