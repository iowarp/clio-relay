"""``RelayTransport`` implementations for modes (a) ``brokered_tcp`` and (b)
``udp_rendezvous`` (iowarp/clio-relay#231 R5, tracked by iowarp/clio-relay#188
items 1-3+5; the design is ``docs/design/relay-architecture-2026-08.md`` §8).

``control_channel.py``'s ``SshForwardTransport`` is the reference lifecycle for
mode (c): dial once, hold the process for the connection's lifetime, prove the
bring-up identity document out of band, refuse to redial internally. Both
classes here hold exactly one :class:`~clio_relay.frp_link.HeldFrpVisitor`
(the R4 substrate) instead -- a local ``frpc`` process holding an stcp/xtcp
visitor tunnel through the cluster's configured relay point -- and follow the
same discipline: ``establish`` is callable once, ``open_stream_channel``
refuses (multiplexing onto the held link is not built for any mode yet), and
``close`` releases the held visitor.

Modes (a)/(b) have no ssh-authenticated act to carry the bring-up identity
document over the way mode (c)'s held SSH session does, so ``establish`` fetches
the exact same two facts (``session-status``, ``session-identity``) as plain
authenticated HTTP requests over the tunnel that is already held -- the same
requests every later owned-session operation makes
(``remote_connection.py``'s ``session_status``/``_open_identity_bound_stream``).
That is why §8.3 requires a cluster to explicitly opt into the weaker
``preshared_link_secret`` identity anchor before either mode may be used at all;
``control_channel.build_transport`` enforces that refusal before this module is
even imported (a lazy import there, to avoid a reverse circular import: this
module needs ``ChannelLink``/``ChannelEndpoint``/``OwnedSessionChannelBootstrap``
from ``control_channel.py``).

``udp_rendezvous``'s hole-punch failure is a typed :class:`TransportPunchFailed`
refusal in this slice, not the automatic in-mode fallback to ``brokered_tcp``'s
stcp visitor that ``docs/connection-model.md`` and §8.4 describe as the mode's
eventual sanctioned behavior -- see that class's docstring and
``docs/connection-model.md``'s "Still deviating" entry for the tracked
follow-up. This path must never render or spawn an stcp visitor:
``transport_probe.py``'s probe-only ``allow_stcp_fallback`` has no equivalent
here.
"""

from __future__ import annotations

import http.client
import json
import socket
import urllib.parse
from typing import ClassVar, Final

from clio_relay.cluster_config import ClusterDefinition, IdentityAnchor
from clio_relay.config import TransportMode
from clio_relay.control_channel import (
    DEFAULT_CHANNEL_READY_TIMEOUT_SECONDS,
    MAX_CHANNEL_BOOTSTRAP_BYTES,
    ChannelBootstrapError,
    ChannelEndpoint,
    ChannelLink,
    OwnedSessionChannelBootstrap,
    StreamChannelsUnavailable,
)
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.frp_link import (
    FrpLinkConfig,
    FrpProcessFactory,
    FrpVisitorType,
    HeldFrpVisitor,
)
from clio_relay.job_identity import OWNER_SESSION_ID_HEADER, SESSION_GENERATION_ID_HEADER

MAX_BRING_UP_FETCH_BYTES: Final = MAX_CHANNEL_BOOTSTRAP_BYTES


class TransportPunchFailed(RelayError):
    """The ``udp_rendezvous`` hole-punch handshake did not establish a tunnel.

    This is a typed refusal, not the "fall back to brokered_tcp" degradation
    ``docs/connection-model.md`` and §8.4 describe as the mode's eventual
    sanctioned behavior: automatically switching this held visitor's proxy type
    mid-connection is a bigger, separate change this slice does not build, and
    this path must never render or spawn an stcp visitor to simulate it.
    Reconnecting after this error retries the same xtcp handshake in the same
    configured mode; it does not try another one.
    """


class _FrpChannelTransport:
    """Shared held-frp-visitor lifecycle for ``brokered_tcp``/``udp_rendezvous``.

    Both modes hold exactly one :class:`~clio_relay.frp_link.HeldFrpVisitor` for
    the connection's lifetime. ``establish`` writes its rendered visitor config,
    spawns ``frpc`` once, waits for the mapped local port to answer, then
    fetches the bring-up document over that SAME held link. Subclasses fix
    ``_mode``/``_visitor_type`` and may override :meth:`_translate_tunnel_failure`
    to turn a failed tunnel into a mode-specific typed error.
    """

    _mode: ClassVar[TransportMode]
    _visitor_type: ClassVar[FrpVisitorType]

    def __init__(
        self,
        *,
        definition: ClusterDefinition,
        cluster: str,
        session_id: str,
        session_generation_id: str,
        remote_api_port: int,
        api_token: str,
        identity_anchor: IdentityAnchor,
        frpc_bin: str,
        process_factory: FrpProcessFactory | None = None,
        local_bind_port: int | None = None,
        ready_timeout_seconds: float = DEFAULT_CHANNEL_READY_TIMEOUT_SECONDS,
    ) -> None:
        if remote_api_port <= 0 or remote_api_port > 65_535:
            raise ValueError("remote_api_port must be a valid TCP port")
        if not api_token:
            raise ValueError("api_token must not be empty")
        if ready_timeout_seconds <= 0:
            raise ValueError("ready_timeout_seconds must be positive")
        self._definition = definition
        self._cluster = cluster
        self._session_id = session_id
        self._session_generation_id = session_generation_id
        # Carried for interface parity with SshForwardTransport and a later
        # multiplexed stream-channel slice; the authoritative cross-check against
        # the remote relay's own report happens in
        # RemoteConnection._verify_bootstrap, not here.
        self._remote_api_port = remote_api_port
        self._api_token = api_token
        self._identity_anchor: IdentityAnchor = identity_anchor
        self._frpc_bin = frpc_bin
        self._process_factory = process_factory
        self._local_bind_port = local_bind_port
        self._ready_timeout_seconds = ready_timeout_seconds
        self._proxy_name = definition.frp_transport.proxy_name or f"{cluster}-owned-session"
        self._visitor: HeldFrpVisitor | None = None
        self._established = False
        self._failure_detail: str | None = None

    @property
    def mode(self) -> TransportMode:
        """Return the declared transport mode."""
        return self._mode

    @property
    def requires_user_authorization(self) -> bool:
        """Neither mode blocks on an interactive prompt.

        Both relays simply dial out to the already-deployed relay point
        (§8.4's dial-count invariants; ``connection-model.md``'s "at most 1
        [ssh]... skipped when already deployed" covers only the separate,
        skippable deploy step, not this tunnel).
        """
        return False

    def establish(self, *, nonce: str) -> ChannelLink:
        """Hold one frp visitor tunnel and fetch the bring-up document over it."""
        if self._established:
            raise RelayError(f"{self._mode} transport was already established")
        _validate_channel_nonce(nonce)
        local_bind_port = self._local_bind_port or _select_loopback_port()
        _assert_bind_port_available(local_bind_port)
        config = FrpLinkConfig.from_cluster(
            self._definition,
            cluster=self._cluster,
            proxy_name=self._proxy_name,
        )
        visitor = HeldFrpVisitor(
            frpc_bin=self._frpc_bin,
            config=config,
            local_bind_port=local_bind_port,
            visitor_type=self._visitor_type,
            keep_tunnel_open=self._visitor_type == "xtcp",
            process_factory=self._process_factory,
        )
        self._visitor = visitor
        try:
            self._establish_visitor(visitor)
        except BaseException as exc:
            self._failure_detail = visitor.failure_detail()
            visitor.close()
            self._visitor = None
            translated = self._translate_tunnel_failure(exc)
            if translated is exc:
                raise
            raise translated from exc
        endpoint = ChannelEndpoint(host="127.0.0.1", port=local_bind_port)
        try:
            bootstrap = _fetch_channel_bootstrap(
                endpoint=endpoint,
                api_token=self._api_token,
                session_id=self._session_id,
                session_generation_id=self._session_generation_id,
                nonce=nonce,
                timeout_seconds=self._ready_timeout_seconds,
            )
        except BaseException:
            self._failure_detail = visitor.failure_detail()
            visitor.close()
            self._visitor = None
            raise
        self._established = True
        return ChannelLink(
            control_endpoint=endpoint,
            bootstrap=bootstrap,
            identity_anchor=self._identity_anchor,
        )

    def _establish_visitor(self, visitor: HeldFrpVisitor) -> None:
        """Spawn the held visitor and wait for its mapped local port to answer."""
        visitor.establish()
        if not visitor.is_alive():
            raise RelayError(visitor.failure_detail() or f"{self._mode} visitor exited immediately")
        # Names the CONNECTION, not the process holding it (contrast
        # HeldFrpVisitor.wait_healthy's own "frp {type} visitor" default) --
        # #231 R4 opus review F4.
        visitor.wait_healthy(
            timeout_seconds=self._ready_timeout_seconds,
            subject=f"frp {self._visitor_type} link",
        )

    def _translate_tunnel_failure(self, exc: BaseException) -> BaseException:
        """Return the exception to raise for a failed tunnel establish.

        The default passes it through unchanged (``BrokeredTcpTransport``);
        ``UdpRendezvousTransport`` overrides this to translate into
        :class:`TransportPunchFailed`. Returning the SAME object (identity,
        not equality) tells :meth:`establish` to plain-``raise`` rather than
        chain a new exception.
        """
        return exc

    def open_stream_channel(self, *, name: str, remote_port: int) -> ChannelEndpoint:
        """Refuse: multiplexing additional channels onto the held link is not built."""
        raise StreamChannelsUnavailable(
            f"{self._mode} cannot yet carry the {name!r} stream channel to remote port "
            f"{remote_port}; live service streams must ride the one held link, not a new one"
        )

    def is_alive(self) -> bool:
        """Return whether the held frp visitor process is still running."""
        visitor = self._visitor
        return visitor is not None and visitor.is_alive()

    def failure_detail(self) -> str | None:
        """Return bounded stderr captured when the channel failed."""
        return self._failure_detail

    def close(self) -> None:
        """Release the held visitor (terminate -> kill escalation) and its config."""
        visitor = self._visitor
        self._visitor = None
        if visitor is not None:
            visitor.close()


class BrokeredTcpTransport(_FrpChannelTransport):
    """Mode (a): one held stcp visitor tunnel through the relay point.

    Both relays dial out; the relay point brokers the handshake that joins
    them. Bring-up costs exactly one frp visitor pair and zero ssh connections
    (§8.4), held for the connection's lifetime -- the same one-held-process
    dial-count shape as ``ssh_forward``, just over ``frpc`` instead of ``ssh``.
    """

    _mode: ClassVar[TransportMode] = "brokered_tcp"
    _visitor_type: ClassVar[FrpVisitorType] = "stcp"


class UdpRendezvousTransport(_FrpChannelTransport):
    """Mode (b): the same rendezvous with a UDP hole-punching (xtcp) handshake.

    A failed punch -- the visitor exiting during bring-up, or its mapped port
    never answering within the health-wait deadline -- is a typed
    :class:`TransportPunchFailed`. See that class's docstring: this never
    substitutes ``brokered_tcp``'s stcp visitor automatically.
    """

    _mode: ClassVar[TransportMode] = "udp_rendezvous"
    _visitor_type: ClassVar[FrpVisitorType] = "xtcp"

    def _translate_tunnel_failure(self, exc: BaseException) -> BaseException:
        if isinstance(exc, TransportPunchFailed):
            return exc
        detail = self._failure_detail or str(exc)
        return TransportPunchFailed(
            f"udp_rendezvous hole punch failed for cluster {self._cluster!r}: {detail}. "
            "Reconnecting retries the same xtcp handshake in the same configured mode; it "
            "never substitutes brokered_tcp's stcp visitor automatically. If punching keeps "
            "failing, configure this cluster's remote_transport_mode as brokered_tcp instead, "
            "or verify UDP reachability/firewall rules between both relays and the relay point."
        )


def _validate_channel_nonce(nonce: str) -> None:
    """Require the exact 256-bit lowercase hex nonce shape ``SshForwardTransport`` does."""
    if len(nonce) != 64 or any(character not in "0123456789abcdef" for character in nonce):
        raise ValueError("channel bootstrap nonce must be a lowercase 256-bit hex value")


def _select_loopback_port() -> int:
    """Select an unused loopback port for the local end of the held visitor."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    if not isinstance(port, int) or port <= 0:
        raise RelayError("could not select a loopback port for the held frp visitor")
    return port


def _assert_bind_port_available(port: int) -> None:
    """Fail loudly, before spawning anything, when the local bind port is occupied."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("127.0.0.1", port))
    except OSError as exc:
        raise ConfigurationError(f"local frp visitor port is already occupied: {port}") from exc


def _fetch_channel_bootstrap(
    *,
    endpoint: ChannelEndpoint,
    api_token: str,
    session_id: str,
    session_generation_id: str,
    nonce: str,
    timeout_seconds: float,
) -> OwnedSessionChannelBootstrap:
    """Fetch the bring-up document OVER the held link -- no ssh act exists here.

    Mode (c) composes this document from an ssh-authenticated cluster-local
    executor. Modes (a)/(b) have no such act, so this fetches the same two
    facts as plain HTTP over the tunnel that is already held: an authenticated
    ``GET /session-status`` (the same request ``RemoteConnection.session_status``
    makes once the channel is up) and an unauthenticated
    ``GET /session-identity?nonce=`` (the same identity challenge
    ``_open_identity_bound_stream`` proves before any credential is sent).
    """
    status = _get_bounded_json(
        endpoint,
        path="/session-status",
        timeout_seconds=timeout_seconds,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {api_token}",
            OWNER_SESSION_ID_HEADER: session_id,
            SESSION_GENERATION_ID_HEADER: session_generation_id,
        },
    )
    identity = _get_bounded_json(
        endpoint,
        path="/session-identity?" + urllib.parse.urlencode({"nonce": nonce}),
        timeout_seconds=timeout_seconds,
        headers={"Accept": "application/json"},
    )
    if not isinstance(status, dict) or not isinstance(identity, dict):
        raise ChannelBootstrapError(
            "owned session channel bring-up did not report a JSON status/identity document"
        )
    try:
        return OwnedSessionChannelBootstrap.model_validate({"status": status, "identity": identity})
    except ValueError as exc:
        raise ChannelBootstrapError(
            f"owned session channel bootstrap is not the exact contract: {exc}"
        ) from exc


def _get_bounded_json(
    endpoint: ChannelEndpoint,
    *,
    path: str,
    timeout_seconds: float,
    headers: dict[str, str],
) -> object:
    """Issue one GET over the held link and return its bounded JSON body."""
    connection = http.client.HTTPConnection(endpoint.host, endpoint.port, timeout=timeout_seconds)
    try:
        connection.connect()
        connection.request("GET", path, headers=headers)
        response = connection.getresponse()
        payload = response.read(MAX_BRING_UP_FETCH_BYTES + 1)
        if len(payload) > MAX_BRING_UP_FETCH_BYTES:
            raise ChannelBootstrapError(
                f"owned session channel bring-up {path} response exceeded its byte limit"
            )
        if response.status != 200:
            raise ChannelBootstrapError(
                f"owned session channel bring-up {path} failed: HTTP {response.status}"
            )
        try:
            return json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ChannelBootstrapError(
                f"owned session channel bring-up {path} response was not UTF-8 JSON"
            ) from exc
    except (OSError, http.client.HTTPException) as exc:
        raise ChannelBootstrapError(
            f"owned session channel bring-up {path} request failed: {exc}"
        ) from exc
    finally:
        connection.close()
