"""The typed owned-session client-liveness lease-expiry error (iowarp/clio-relay#277).

Split out of :mod:`clio_relay.remote_connection` to keep that file under its
800-line ratchet cap (`scripts/check_file_size.py`) -- the same "MOVE, not a
rewrite" discipline that file's own docstring already documents for
:mod:`clio_relay.remote_connection_stream_io` and
:mod:`clio_relay.remote_connection_registry`. ``remote_connection.py``
re-exports :class:`SessionLeaseExpiredError` under its original name so
every existing import (:mod:`clio_relay.session_attach`, tests) keeps
resolving unchanged.
"""

from __future__ import annotations

from typing import cast

from clio_relay.errors import RelayError


class SessionLeaseExpiredError(RelayError):
    """The owned session's server-side client-liveness lease already expired.

    iowarp/clio-relay#277: distinct from a generic bring-up/bootstrap-
    verification failure -- the remote generation was not merely dead or
    torn down for an ordinary reason; the worker's own TTL sweep reaped it
    after this desktop stopped renewing its lease (crash, network loss, or
    an ordinary walk-away). Raised by
    :func:`~clio_relay.remote_connection_registry.verify_bootstrap` (as its
    FIRST statement -- position matters: an expired session also fails the
    generic ``running is not True`` check right after it, so appending this
    check later would silently lose the typed reason) from the SAME
    single-dial bootstrap document every bring-up already reads (``session
    recovery-status``'s ``owner_session_lease_status`` projection, embedded
    via ``control_channel.owned_session_channel_bootstrap_script`` -- zero
    extra dials).

    When ``expired_with_running_jobs`` is true, at least one relay job was
    still non-terminal at sweep time; leases never kill jobs (they are owned
    by the queue, not by the session's liveness), so ``running_job_ids``
    names what to look for via ``queue``/a future attach rather than
    treating that work as lost.
    """

    reason = "owner_session_lease_expired"

    def __init__(
        self,
        *,
        cluster: str,
        session_id: str,
        session_generation_id: str | None,
        expired_with_running_jobs: bool,
        running_job_ids: tuple[str, ...],
    ) -> None:
        self.cluster = cluster
        self.session_id = session_id
        self.session_generation_id = session_generation_id
        self.expired_with_running_jobs = expired_with_running_jobs
        self.running_job_ids = running_job_ids
        generation_detail = (
            f" (generation {session_generation_id!r})" if session_generation_id else ""
        )
        detail = (
            f"owned session {session_id!r}{generation_detail} on cluster {cluster!r} was reaped "
            "by its client-liveness lease TTL after this desktop stopped talking to it"
        )
        if expired_with_running_jobs:
            detail += (
                f"; {len(running_job_ids)} job(s) were still running when it expired and "
                "were left running under queue ownership"
            )
        super().__init__(detail)


def raise_if_lease_expired(
    *,
    cluster: str,
    session_id: str,
    session_generation_id: str,
    status: dict[str, object],
) -> None:
    """Raise :class:`SessionLeaseExpiredError` before the generic verification failure.

    Called as the FIRST statement of
    :func:`~clio_relay.remote_connection_registry.verify_bootstrap`, with the
    SAME bootstrap ``status`` document that function goes on to verify.
    iowarp/clio-relay#277: an expired lease already means the process is
    gone and the generation already closed -- the generic checks right
    after this call would fail anyway, just with a reason that does not say
    WHY. Scoped to exactly the given connection identity so a status
    document that does not even claim to describe this session/cluster falls
    through to the ordinary generic verification unchanged (a no-op return).
    """
    if status.get("cluster") != cluster or status.get("session_id") != session_id:
        return
    raw_lease_status = status.get("owner_session_lease_status")
    if not isinstance(raw_lease_status, dict):
        return
    lease_status = cast(dict[str, object], raw_lease_status)
    if lease_status.get("status") != "expired":
        return
    raw_running_job_ids = lease_status.get("running_job_ids_at_close")
    running_job_ids = (
        tuple(
            job_id for job_id in cast(list[object], raw_running_job_ids) if isinstance(job_id, str)
        )
        if isinstance(raw_running_job_ids, list)
        else ()
    )
    raise SessionLeaseExpiredError(
        cluster=cluster,
        session_id=session_id,
        session_generation_id=session_generation_id,
        expired_with_running_jobs=bool(lease_status.get("expired_with_running_jobs")),
        running_job_ids=running_job_ids,
    )
