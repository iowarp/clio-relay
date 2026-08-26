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

import threading
from typing import TYPE_CHECKING, Final, cast

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings, TransportMode
from clio_relay.control_channel import (
    ChannelEventSink,
    ChannelProcessFactory,
    OwnedSessionChannelBootstrap,
)
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.remote_connection_lease import raise_if_lease_expired

if TYPE_CHECKING:
    from clio_relay.remote_connection import RemoteConnection

CHANNEL_EVENT_REPORT_SCHEMA: Final = "clio-relay.control-channel-report.v1"


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
    """
    status = bootstrap.status
    raise_if_lease_expired(
        cluster=definition.name,
        session_id=session_id,
        session_generation_id=generation_id,
        status=status,
    )
    remote_api_port_reported = status.get("remote_api_port")
    if (
        status.get("owner") != "clio-relay"
        or status.get("cluster") != definition.name
        or status.get("session_id") != session_id
        or status.get("session_generation_id") != generation_id
        or status.get("running") is not True
        or status.get("ownership_verified") is not True
        or isinstance(remote_api_port_reported, bool)
        or not isinstance(remote_api_port_reported, int)
        or not 1 <= remote_api_port_reported <= 65_535
    ):
        raise RelayError(
            "remote relay session is not the active, ownership-verified generation requested "
            f"for {definition.name}/{session_id}"
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
                    # connection, not a retry. Retire the old one but keep its
                    # transport count, or the acceptance measurement loses it.
                    self._connections.pop(definition.name, None)
                    self._retired.append(_retired_report(existing))
        if held is not None:
            held.connect()
            return held
        if existing is not None:
            existing.close()
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
            if raced is not None:
                self._retired.append(_retired_report(created))
                created.close()
                return raced
            self._connections[definition.name] = created
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
        """Close and forget one cluster's connection."""
        with self._lock:
            connection = self._connections.pop(cluster, None)
            if connection is not None:
                self._retired.append(_retired_report(connection))
        if connection is not None:
            connection.close()

    def close_all(self) -> None:
        """Close every held connection this local relay owns."""
        with self._lock:
            connections = list(self._connections.values())
            self._connections.clear()
            self._retired.extend(_retired_report(item) for item in connections)
        for connection in connections:
            connection.close()

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


def _retired_report(connection: RemoteConnection) -> dict[str, object]:
    """Keep a retired connection's transport count for the acceptance report."""
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
    }


_REGISTRY = RemoteConnectionRegistry()


def connection_registry() -> RemoteConnectionRegistry:
    """Return the process-wide registry of remote relay connections."""
    return _REGISTRY
