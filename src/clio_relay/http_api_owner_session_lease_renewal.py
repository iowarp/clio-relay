"""Shared owned-session client-liveness lease renewal helper (iowarp/clio-relay#277).

One function, reused by every owned-session traffic path that must renew the
lease: the ordinary per-request auth dependency
(:func:`clio_relay.http_api_auth._require_api_token`), and the long-lived
SSE/WebSocket poll loops (:mod:`clio_relay.http_api_streaming`) that would
otherwise never touch it again after their first request -- a stream open
longer than the lease TTL would get reaped mid-stream (adversarial-review
HIGH 4). Both call sites need the identical policy:

* Renewal is a NO-OP when the process is not bound to an owned session at
  all (``settings.owner_session_id is None``) -- never construct a lease for
  an unowned/local API.
* Renewal NEVER raises. A bookkeeping failure here must never turn a valid
  client request into a 500, and must never kill a long-lived stream --
  MEDIUM 5. The typed structured log line is the no-silent-fallback trail.
* Debouncing (skip the write when the lease was already renewed recently)
  is owned by :meth:`~clio_relay.queue_owner_session_lease.
  QueueOwnerSessionLeaseMixin.touch_owner_session_lease` itself, not
  duplicated here -- every call site can call this on every request/poll
  tick without needing its own throttle.
"""

from __future__ import annotations

import logging

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue

logger = logging.getLogger(__name__)


def renew_owner_session_lease(settings: RelaySettings, queue: ClioCoreQueue) -> None:
    """Best-effort owned-session lease renewal. Never raises."""
    cluster = settings.resolved_owner_session_cluster()
    session_id = settings.owner_session_id
    generation_id = settings.owner_session_generation_id
    if cluster is None or session_id is None or generation_id is None:
        return
    try:
        queue.touch_owner_session_lease(
            session_id,
            session_generation_id=generation_id,
            cluster=cluster,
            ttl_seconds=settings.owner_session_lease_ttl_seconds,
        )
    except Exception:
        logger.warning(
            "owner_session.lease_renewal_failed",
            extra={
                "owner_session_id": session_id,
                "session_generation_id": generation_id,
                "cluster": cluster,
            },
            exc_info=True,
        )
