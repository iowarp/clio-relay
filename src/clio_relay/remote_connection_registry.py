"""Registry of held :class:`~clio_relay.remote_connection.RemoteConnection` links.

The client-facing MCP endpoint stays single and stable while connections in
here come and go; a cluster's connection is created on first use and reused
by every later operation for that cluster.  This module owns only the
one-local-relay-to-many-remote-relays bookkeeping -- which held connection
serves which cluster, and the retired-connection ledger the deployment-gate
acceptance measurement (:meth:`RemoteConnectionRegistry.event_report`) reads
-- never the channel/stream mechanics a held connection itself owns (see
:mod:`clio_relay.remote_connection`).
"""

from __future__ import annotations

import atexit
import threading
from typing import TYPE_CHECKING, Final, cast

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings, TransportMode
from clio_relay.control_channel import (
    ChannelEventSink,
    ChannelProcessFactory,
    OwnedSessionChannelBootstrap,
    RelayTransport,
    channel_event,
)
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.remote_connection_lease import raise_if_lease_expired

if TYPE_CHECKING:
    from clio_relay.remote_connection import RemoteConnection

CHANNEL_EVENT_REPORT_SCHEMA: Final = "clio-relay.control-channel-report.v1"

# The evidence class the live owned-session API stamps on its own
# ``/session-status`` self-report (``http_api_routes_session.py``). Kept as a
# named constant here because :func:`verify_bootstrap` branches on it.
LIVE_API_SELF_REPORT_EVIDENCE: Final = "live_api_self_report"


def verify_bootstrap(
    bootstrap: OwnedSessionChannelBootstrap,
    *,
    definition: ClusterDefinition,
    session_id: str,
    generation_id: str,
    remote_api_port: int,
) -> None:
    """Require the remote relay to be the exact, live, owned generation.

    clio-relay#221/#259 adversarial review (D13, file-size housekeeping):
    moved out of ``RemoteConnection`` -- this check carries no state beyond
    its arguments, so it moves as a free function to a module with headroom
    rather than staying resident purely to hold ``RemoteConnection`` under
    its own file-size ratchet. Its one call site is
    ``RemoteConnection._establish``.

    iowarp/clio-relay#277 (post-merge adversarial-review fix): the typed
    lease-expiry check runs FIRST, before the generic checks below --
    position matters. An expired session also fails the plain
    ``running is not True`` check right after, so appending the lease check
    later would silently lose the typed reason behind the generic
    ``RelayError`` this function raises next.

    Verification is evidence-class aware. The ssh-carried status executor
    performs the cluster-local filesystem/process ownership audit and reports
    ``ownership_verified``; the brokered modes fetch the live API's
    ``/session-status`` self-report instead (``evidence:
    live_api_self_report``), which deliberately does NOT claim that audit --
    for that evidence class the audit fact is not demanded here, because the
    responder's identity is proven by the identity-first challenge plus the
    identity-bound stream re-proof establishment always runs. Every other
    fact (owner, cluster, session, generation, running, port) is demanded of
    both classes, and the refusal names exactly which check(s) failed.
    """
    status = bootstrap.status
    raise_if_lease_expired(
        cluster=definition.name,
        session_id=session_id,
        session_generation_id=generation_id,
        status=status,
    )
    remote_api_port_reported = status.get("remote_api_port")
    checks: list[tuple[str, bool]] = [
        ("owner", status.get("owner") == "clio-relay"),
        ("cluster", status.get("cluster") == definition.name),
        ("session_id", status.get("session_id") == session_id),
        ("session_generation_id", status.get("session_generation_id") == generation_id),
        ("running", status.get("running") is True),
        (
            "remote_api_port",
            not isinstance(remote_api_port_reported, bool)
            and isinstance(remote_api_port_reported, int)
            and 1 <= remote_api_port_reported <= 65_535,
        ),
    ]
    # Evidence-class-aware ownership check: ``ownership_verified`` is a
    # cluster-local filesystem/process audit fact that only the ssh-carried
    # status executor can produce. The brokered modes' status document is the
    # live API describing itself (``http_api_routes_session.py``'s
    # ``/session-status``, ``evidence: live_api_self_report``) -- it honestly
    # refuses to claim the audit it cannot perform, and its identity is proven
    # instead by the identity-first challenge plus the identity-bound stream
    # re-proof that establishment always runs. Demanding the audit fact of the
    # self-report evidence class made brokered attach permanently unverifiable.
    if status.get("evidence") != LIVE_API_SELF_REPORT_EVIDENCE:
        checks.append(("ownership_verified", status.get("ownership_verified") is True))
    failed_checks = [name for name, passed in checks if not passed]
    if failed_checks:
        raise RelayError(
            "remote relay session is not the active, ownership-verified generation requested "
            f"for {definition.name}/{session_id}; failed check(s): {', '.join(failed_checks)}"
        )
    if remote_api_port_reported != remote_api_port:
        raise RelayError(
            "remote relay session reported owned API port "
            f"{remote_api_port_reported}, but the held channel maps {remote_api_port}; "
            "configure CLIO_RELAY_OWNER_SESSION_API_PORT for this connection"
        )


class RemoteConnectionRegistry:
    """The one local relay's connections to many remote relays.

    The client-facing MCP endpoint stays single and stable while connections in
    here come and go; a cluster's connection is created on first use and reused
    by every later operation for that cluster.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._connections: dict[str, RemoteConnection] = {}
        self._retired: list[dict[str, object]] = []
        self._atexit_registered = False

    def connection(
        self,
        *,
        definition: ClusterDefinition,
        settings: RelaySettings,
        remote_api_port: int | None = None,
        transport_mode: TransportMode | None = None,
        process_factory: ChannelProcessFactory | None = None,
        timeout_seconds: float = 30.0,
        event_sink: ChannelEventSink | None = None,
        allow_interactive_authorization: bool | None = None,
    ) -> RemoteConnection:
        """Return the held connection for one cluster, establishing it once.

        Bring-up can block for as long as a user takes to authorize it, so the
        registry lock is never held across it: one cluster connecting must not
        stall every other cluster's operations, nor the acceptance report.
        """
        # Function-scope: clio_relay.remote_connection imports this module at
        # its own top level to re-export RemoteConnectionRegistry, so a
        # module-scope import here would be circular. By the time this method
        # actually runs, that module is always fully loaded.
        from clio_relay.remote_connection import RemoteConnection

        self._ensure_atexit_registered()
        with self._lock:
            existing = self._connections.get(definition.name)
            if existing is not None and existing.matches(
                settings=settings,
                remote_api_port=remote_api_port,
            ):
                held = existing
            else:
                held = None
                if existing is not None:
                    # The pinned identity changed, so this is a different
                    # connection, not a retry -- pop it now, but defer
                    # reporting it retired until AFTER it is actually closed
                    # below (#285 D4: an earlier revision retired it here,
                    # BEFORE close(), so its terminal `closed` event -- and
                    # any events close() itself records -- never reached the
                    # retired report at all).
                    self._connections.pop(definition.name, None)
        if held is not None:
            held.connect()
            return held
        if existing is not None:
            existing.close()
            with self._lock:
                self._retired.append(_retired_report(existing))
        created = RemoteConnection(
            definition=definition,
            settings=settings,
            remote_api_port=remote_api_port,
            transport_mode=transport_mode,
            process_factory=process_factory,
            timeout_seconds=timeout_seconds,
            event_sink=event_sink,
            allow_interactive_authorization=allow_interactive_authorization,
        )
        created.connect()
        with self._lock:
            raced = self._connections.get(definition.name)
            if raced is None:
                self._connections[definition.name] = created
        if raced is not None:
            # Same D4 ordering fix as above: close the redundant connection
            # BEFORE reporting it retired, not after.
            created.close()
            with self._lock:
                self._retired.append(_retired_report(created))
            return raced
        return created

    def get(self, cluster: str) -> RemoteConnection | None:
        """Return the existing connection for one cluster without creating it."""
        with self._lock:
            return self._connections.get(cluster)

    def reconnect(self, cluster: str) -> RemoteConnection:
        """Re-establish one cluster's dropped channel on explicit instruction.

        This is the only way a replacement transport is ever opened.  In
        ``ssh_forward`` mode calling it is what the present user authorizes, so
        it must be reached from an operator action and never from a retry.
        """
        with self._lock:
            connection = self._connections.get(cluster)
        if connection is None:
            raise ConfigurationError(f"no owned session connection is held for cluster {cluster}")
        connection.reconnect()
        return connection

    def disconnect(self, cluster: str) -> None:
        """Close and forget one cluster's connection.

        Retires AFTER close() (#285 D4 ordering fix), not before -- see
        ``connection()``'s matching fix and ``_retired_report``'s docstring.
        """
        with self._lock:
            connection = self._connections.pop(cluster, None)
        if connection is not None:
            connection.close()
            with self._lock:
                self._retired.append(_retired_report(connection))

    def close_all(self, *, at_exit: bool = False) -> None:
        """Close every held connection this local relay owns.

        ``at_exit=True`` (iowarp/clio-relay#285) is the interpreter-exit
        self-clean path: each connection records ``closed_at_exit`` instead
        of ``closed``, and a per-connection close failure is caught and
        folded into that connection's own typed event instead of raised --
        one connection's close failing must never stop every OTHER held
        connection from being released, nor raise back into an atexit hook.
        The default, ordinary-caller path is unchanged: a close failure
        there still propagates, exactly as before #285.

        Retires every connection AFTER it is closed (#285 D4 ordering fix),
        not before -- see ``_retired_report``'s docstring.
        """
        with self._lock:
            connections = list(self._connections.values())
            self._connections.clear()
        for connection in connections:
            if not at_exit:
                connection.close()
                continue
            try:
                connection.close(at_exit=True)
            except BaseException as exc:  # noqa: BLE001 -- must never raise at exit; see docstring
                _record_exit_close_failure(connection, detail=str(exc))
        with self._lock:
            self._retired.extend(_retired_report(item) for item in connections)

    def _ensure_atexit_registered(self) -> None:
        """Register this registry's exit-path close exactly once (#285).

        Lazy, idempotent: armed on this registry's first ``connection()``
        call (which, for a fresh registry, is also its first-ever connection
        creation -- nothing is held yet to reuse), not at import time or
        module construction -- a registry that never holds a connection
        registers nothing. A second (or later) call is a no-op: the flag is
        set before ``atexit.register`` is even called, so a hook is armed at
        most once per registry instance, never once per connection.
        """
        with self._lock:
            if self._atexit_registered:
                return
            self._atexit_registered = True
        atexit.register(_atexit_close_all_connections, self)

    @property
    def clusters(self) -> tuple[str, ...]:
        """Return the clusters this local relay currently holds a channel to."""
        with self._lock:
            return tuple(sorted(self._connections))

    def event_report(self) -> dict[str, object]:
        """Return the client half of the one-held-channel acceptance measurement.

        ``established`` plus ``reestablished`` is exactly the number of new
        transport connections this local relay opened, which is what a desktop
        process sampler and the cluster's ``sshd`` session log independently
        count in the deployment gate.  Reading it costs no transport.
        """
        with self._lock:
            connections = dict(self._connections)
            retired = list(self._retired)
        clusters: dict[str, object] = {}
        for cluster, connection in connections.items():
            events = connection.events
            clusters[cluster] = {
                "transport_mode": connection.transport_mode,
                "identity_anchor": connection.identity_anchor,
                "session_id": connection.session_id,
                "session_generation_id": connection.session_generation_id,
                "remote_api_port": connection.remote_api_port,
                "connected": connection.connected,
                "transport_connections_opened": sum(
                    1 for event in events if event.event in {"established", "reestablished"}
                ),
                "events": [event.model_dump(mode="json") for event in events],
            }
        live = sum(
            cast(int, cast(dict[str, object], value)["transport_connections_opened"])
            for value in clusters.values()
        )
        retired_total = sum(cast(int, item["transport_connections_opened"]) for item in retired)
        return {
            "schema_version": CHANNEL_EVENT_REPORT_SCHEMA,
            "clusters": clusters,
            "retired": retired,
            "transport_connections_opened": live + retired_total,
        }


def record_reconciliation_events(connection: RemoteConnection, transport: RelayTransport) -> None:
    """Record every typed event ONE visitor spawn's reconciliation produced (#285).

    Called from ``RemoteConnection._establish`` in BOTH outcomes -- right
    after ``transport.establish`` succeeds, AND (D5 adversarial-review fix)
    from the except-handler when ``transport.establish`` itself raises.
    Reconciliation runs as the FIRST action inside ``establish`` (before
    anything that can fail), so the attributes read below are already
    populated on ``transport`` by the time either call site reaches them --
    a reap (or a skipped snapshot, D2) must never be lost from the ledger
    just because the rest of THIS attempt went on to fail. Only frp-based
    transports carry these attributes at all (``ssh_forward`` is
    self-cleaning by construction, so it has no equivalent); plain
    optional-attribute reads, not an isinstance/mode branch.

    Records one ``visitor_orphan_reaped`` event per reaped pid (``detail``
    is the pid), and -- when the reconciliation's own process-table
    snapshot could not be read at all -- one ``visitor_reconciliation_skipped``
    event naming the typed reason, so a leak that could never even be
    DETECTED on a PowerShell-constrained host is still visible in the
    acceptance report (D2).
    """
    reaped_pids = cast("tuple[int, ...]", getattr(transport, "reaped_orphan_visitor_pids", ()))
    for pid in reaped_pids:
        connection._record(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            channel_event(
                cluster=connection.cluster,
                mode=connection.transport_mode,
                event="visitor_orphan_reaped",
                attempt=connection._attempt,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                reason="prior_cli_exited",
                detail=str(pid),
                identity_anchor=connection.identity_anchor,
            )
        )
    skipped_reason = cast("str | None", getattr(transport, "reconciliation_skipped_reason", None))
    if skipped_reason is not None:
        connection._record(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            channel_event(
                cluster=connection.cluster,
                mode=connection.transport_mode,
                event="visitor_reconciliation_skipped",
                attempt=connection._attempt,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                reason=skipped_reason,
                identity_anchor=connection.identity_anchor,
            )
        )


def _record_exit_close_failure(connection: RemoteConnection, *, detail: str) -> None:
    """Fold an unexpected atexit close failure into the SAME typed event (#285).

    Every currently-implemented transport's own ``close()`` already swallows
    its internal errors, so ``close_all(at_exit=True)`` is not expected to
    reach this in practice -- it exists so an unanticipated failure still
    lands on the connection's own ledger as a ``closed_at_exit`` event
    (reason ``"close_failed"``) instead of the terminal event being lost
    (no-silent-fallback applies to a best-effort exit teardown too).
    Reaches ``connection``'s own private ledger the same way this module's
    ``connection()`` already reaches ``RemoteConnection`` internals -- these
    two modules are one coupled connection-lifecycle concern split only for
    file size (see this module's own docstring).
    """
    connection._record(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        channel_event(
            cluster=connection.cluster,
            mode=connection.transport_mode,
            event="closed_at_exit",
            attempt=connection._attempt,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            reason="close_failed",
            detail=detail,
            identity_anchor=connection.identity_anchor,
        )
    )


def _retired_report(connection: RemoteConnection) -> dict[str, object]:
    """Keep a retired connection's transport count AND terminal events (#285 D4).

    Every call site calls this AFTER the connection has already been
    closed -- an earlier revision called it BEFORE close() at every one of
    them, so a retired connection's terminal ``closed``/``closed_at_exit``
    event (and any ``visitor_orphan_reaped``/``visitor_reconciliation_skipped``
    events its own establish recorded) never reached ``event_report()`` at
    all once the connection left the live ``clusters`` dict: the count
    below only ever summed ``established``/``reestablished``, and no
    ``events`` key existed here at all. Now mirrors the live cluster
    entry's ``events`` shape exactly, so a caller does not need two
    different shapes depending on whether a connection is still held or
    already retired.
    """
    events = connection.events
    return {
        "cluster": connection.cluster,
        "session_id": connection.session_id,
        "session_generation_id": connection.session_generation_id,
        "transport_mode": connection.transport_mode,
        "identity_anchor": connection.identity_anchor,
        "transport_connections_opened": sum(
            1 for event in events if event.event in {"established", "reestablished"}
        ),
        "events": [event.model_dump(mode="json") for event in events],
    }


def _atexit_close_all_connections(registry: RemoteConnectionRegistry) -> None:
    """Close every connection one registry still holds, at interpreter exit.

    iowarp/clio-relay#285: authenticated ``brokered_tcp``/``udp_rendezvous``
    tunnels have no self-tearing-down tether the way ``ssh_forward``'s held
    stdin pipe does (``control_channel.py``'s own module docstring) --
    nothing else closes their held frpc visitor child when the owning CLI
    process exits, so it orphans, still logged into the frps edge, until
    this hook runs. Registered lazily, once per registry, by
    ``RemoteConnectionRegistry._ensure_atexit_registered`` on that
    registry's own first ``connection()`` call -- never at import time.

    This is a thin, directly-callable wrapper around
    ``RemoteConnectionRegistry.close_all(at_exit=True)``, which is itself
    the one place that guarantees no per-connection failure raises back out
    (see its own docstring) -- kept as a bare module-level function, rather
    than a bound method passed straight to ``atexit.register``, only so
    tests can invoke this exact hook directly (per its own module's test
    suite) without reaching through the private ``atexit`` callback queue.
    """
    registry.close_all(at_exit=True)


_REGISTRY = RemoteConnectionRegistry()


def connection_registry() -> RemoteConnectionRegistry:
    """Return the process-wide registry of remote relay connections."""
    return _REGISTRY
