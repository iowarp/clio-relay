"""Connection-scoped owned-session control plane over one held channel.

One local relay process manages a connection to each remote relay it is
connected to.  A connection establishes its transport once, at bring-up, and
holds it for the connection's lifetime.  Every owned-session operation is plain
HTTP over the mapped port of that held channel.

Nothing here may re-establish *transport* implicitly.  A dropped channel raises
:class:`~clio_relay.control_channel.ChannelDropped`; replacing it is an explicit
:meth:`RemoteConnection.reconnect` call that emits typed, visible events -- in
``ssh_forward`` mode that call is what a present user authorizes.

One level below the transport, a pooled HTTP *stream* over an already-held
channel is a cheaper, narrower thing: :meth:`RemoteConnection.request_json`
identity-bound reconnects a stream that died between requests (an OS-level
idle close it cannot see coming) exactly once, re-proving it against the same
bring-up identity, before surfacing anything (clio-relay#213). That is never a
silent redial -- it costs no new transport and is itself a typed, visible
``stream_reproven`` event -- and it never substitutes for the explicit
channel-level reconnect above.
"""

from __future__ import annotations

import hmac
import http.client
import json
import math
import secrets
import threading
import urllib.parse
from contextlib import suppress
from typing import Final, Literal, cast

from clio_relay.bounded_payload import describe_delivery_refusal, is_delivery_refusal
from clio_relay.cluster_config import ClusterDefinition, IdentityAnchor
from clio_relay.config import RelaySettings, TransportMode
from clio_relay.control_channel import (
    ChannelDropped,
    ChannelEndpoint,
    ChannelEvent,
    ChannelEventSink,
    ChannelLink,
    ChannelNotEstablished,
    ChannelProcessFactory,
    OwnedSessionChannelBootstrap,
    RelayTransport,
    build_transport,
    channel_event,
)
from clio_relay.errors import ConfigurationError, ObservationTimeoutError, RelayError
from clio_relay.job_identity import (
    OWNER_SESSION_ID_HEADER,
    SESSION_GENERATION_ID_HEADER,
)

MAX_SESSION_API_RESPONSE_BYTES: Final = 8 * 1024 * 1024
MAX_RECORDED_CHANNEL_EVENTS: Final = 256
# Idle streams held over the one channel; more concurrent operations simply open
# more streams through the same forward, which costs no new transport.
MAX_POOLED_CHANNEL_STREAMS: Final = 8
DEFAULT_OWNED_SESSION_API_PORT: Final = 8765
CHANNEL_EVENT_REPORT_SCHEMA: Final = "clio-relay.control-channel-report.v1"

# clio-relay#213: the closed set of exceptions `_request_json_on_stream` wraps and
# chains that mean the *stream* died at the OS level (idle-closed, reset, a bad
# status line) rather than the request being genuinely rejected. Deliberately
# narrow: an HTTP-status failure or a slow-but-live server (ObservationTimeoutError)
# must never retry under a second identity-bound stream.
_STALE_STREAM_ERROR_TYPES: Final = (ConnectionError, http.client.BadStatusLine)


def _is_stale_stream_error(exc: BaseException) -> bool:
    """Return True only for the narrow, OS-observed dead-pooled-stream signature."""
    return isinstance(exc.__cause__, _STALE_STREAM_ERROR_TYPES)


_IDENTITY_FIELDS: Final = (
    "schema_version",
    "cluster",
    "session_id",
    "session_generation_id",
    "nonce",
)


def validate_channel_request(*, method: str, path: str) -> str:
    """Validate one owned-session request line before it reaches the channel."""
    normalized_method = method.upper()
    if normalized_method not in {"GET", "POST"}:
        raise ValueError("owned session API method must be GET or POST")
    if (
        not path.startswith("/")
        or path.startswith("//")
        or any(character in path for character in ("\r", "\n", "?"))
    ):
        raise ValueError("owned session API path must be an absolute path without a query")
    return normalized_method


def resolve_remote_api_port(
    *,
    settings: RelaySettings,
    remote_api_port: int | None = None,
) -> int:
    """Resolve the remote relay's owned-session API port for this connection.

    The port is connection configuration, not a per-operation discovery: the
    held forward has to be pointed at it before the channel exists.  Whatever
    is resolved here is verified against the remote relay's own report during
    bring-up, so a wrong port fails visibly instead of binding to a stranger.
    """
    resolved = remote_api_port or settings.owner_session_api_port
    port = resolved or DEFAULT_OWNED_SESSION_API_PORT
    if not 1 <= port <= 65_535:
        raise ConfigurationError("owned session remote API port must be a valid TCP port")
    return port


class RemoteConnection:
    """One local relay's held link to one remote relay, for its whole lifetime."""

    def __init__(
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
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        session_id, generation_id, api_token = owned_session_credentials(
            definition=definition,
            settings=settings,
        )
        self._definition = definition
        self._settings = settings
        self._session_id = session_id
        self._generation_id = generation_id
        self._api_token = api_token
        self._remote_api_port = resolve_remote_api_port(
            settings=settings,
            remote_api_port=remote_api_port,
        )
        # The mode is configuration, never a runtime choice: it is fixed for the
        # connection's whole life and every reconnect re-establishes this same
        # mode. Nothing here probes a mode or substitutes another one.
        self._transport_mode: TransportMode = transport_mode or settings.remote_transport_mode
        self._process_factory = process_factory
        self._timeout_seconds = timeout_seconds
        self._event_sink = event_sink
        # A headless deployment must be able to refuse the prompt rather than
        # burn its whole bring-up deadline waiting for a tty that is not there.
        self._allow_interactive_authorization = (
            settings.remote_transport_interactive
            if allow_interactive_authorization is None
            else allow_interactive_authorization
        )
        self._lock = threading.RLock()
        self._transport: RelayTransport | None = None
        self._endpoint: ChannelEndpoint | None = None
        self._bootstrap: OwnedSessionChannelBootstrap | None = None
        self._link: ChannelLink | None = None
        self._nonce: str | None = None
        self._idle_streams: list[http.client.HTTPConnection] = []
        self._open_streams = 0
        # Counts proofs, not live streams: the event means "another stream was
        # proven after bring-up", which is true both when one replaces a dead
        # stream and when concurrency needs an extra one.
        self._streams_proven = 0
        self._transport_generation = 0
        self._attempt = 0
        self._events: list[ChannelEvent] = []

    @property
    def cluster(self) -> str:
        """Return the remote relay's cluster name."""
        return self._definition.name

    @property
    def session_id(self) -> str:
        """Return the exact owned session this connection is pinned to."""
        return self._session_id

    @property
    def session_generation_id(self) -> str:
        """Return the exact owned session generation this connection is pinned to."""
        return self._generation_id

    @property
    def remote_api_port(self) -> int:
        """Return the remote relay port the held channel maps."""
        return self._remote_api_port

    @property
    def transport_mode(self) -> TransportMode:
        """Return the declared transport mode of the held channel."""
        return self._transport_mode

    @property
    def identity_anchor(self) -> IdentityAnchor | None:
        """Return this connection's identity anchor (§8.3).

        Prefers the HELD link's own snapshot (``ChannelLink.identity_anchor``,
        captured when ``establish`` last succeeded) over live cluster config:
        the audit trail must describe the link that is actually held, not
        whatever the on-disk cluster definition says right now, which could
        have drifted since bring-up (#231 R5 opus review item R9). Before the
        first ``establish`` succeeds -- no link yet to describe -- falls back
        to what config currently declares for this mode.
        ``brokered_tcp``/``udp_rendezvous`` declare ``"preshared_link_secret"``;
        ``ssh_forward`` has none: its identity document is carried by the
        ssh-authenticated bootstrap act itself.
        """
        link = self._link
        if link is not None:
            return link.identity_anchor
        if self._transport_mode in ("brokered_tcp", "udp_rendezvous"):
            return self._definition.frp_transport.identity_anchor
        return None

    @property
    def connected(self) -> bool:
        """Return whether the channel is currently held."""
        transport = self._transport
        return transport is not None and transport.is_alive()

    @property
    def events(self) -> tuple[ChannelEvent, ...]:
        """Return the bounded, typed transport lifecycle record."""
        return tuple(self._events)

    @property
    def bootstrap(self) -> OwnedSessionChannelBootstrap | None:
        """Return the out-of-band bring-up document proven for this channel."""
        return self._bootstrap

    @property
    def link(self) -> ChannelLink | None:
        """Return the established link, or None when no channel is held.

        The link is what every kind of traffic rides, not just owned-session
        request/response; a later slice adds multiplexed stream channels to it.
        """
        return self._link

    def connect(self) -> None:
        """Establish the channel once.

        Calling this on an already-connected connection is a no-op and costs no
        new transport.  It never silently replaces a dropped channel; use
        :meth:`reconnect` for that.
        """
        with self._lock:
            if self.connected:
                return
            if self._transport is not None:
                from clio_relay.dev_mode import dev_mode_enabled

                if not dev_mode_enabled():
                    raise ChannelDropped(
                        f"owned session channel for {self.cluster} dropped; "
                        "call reconnect() to re-establish it"
                    )
                # Dev channel: auto-replace the dropped channel (recorded, one
                # attempt) instead of requiring the explicit reconnect().
                previous = self._transport
                detail = previous.failure_detail()
                self._release_locked(reason="dev_mode_auto_reconnect")
                self._record(
                    channel_event(
                        cluster=self.cluster,
                        mode=self._transport_mode,
                        event="reestablishing",
                        attempt=self._attempt,
                        reason="channel_dropped_dev_mode_auto",
                        detail=detail,
                        user_authorization_required=self._allow_interactive_authorization,
                        identity_anchor=self.identity_anchor,
                    )
                )
                self._establish(event="reestablished")
                return
            self._establish(event="established")

    def reconnect(self) -> None:
        """Explicitly replace a dropped channel with exactly one new transport."""
        with self._lock:
            previous = self._transport
            detail = None if previous is None else previous.failure_detail()
            self._release_locked(reason="reconnect_requested")
            self._record(
                channel_event(
                    cluster=self.cluster,
                    mode=self._transport_mode,
                    event="reestablishing",
                    attempt=self._attempt,
                    reason="channel_dropped",
                    detail=detail,
                    user_authorization_required=self._allow_interactive_authorization,
                    identity_anchor=self.identity_anchor,
                )
            )
            self._establish(event="reestablished")

    def request_json(
        self,
        *,
        method: str,
        path: str,
        query: dict[str, object] | None = None,
        body: dict[str, object] | None = None,
        response_timeout_seconds: float | None = None,
    ) -> object:
        """Issue one authenticated JSON request over the held channel.

        clio-relay#213: a pooled stream that looked live when it left the pool
        but died at the OS level while idle (WinError 10053/10054, a reset, a
        bad status line -- the kind of loss nothing sees coming until the next
        real I/O) is retried exactly once. The dead stream is discarded, a
        replacement is proven fresh against this connection's already-held
        bring-up identity (`_acquire_stream` re-runs the same per-stream
        identity challenge it always does), and the request is reissued. A
        second consecutive failure on that freshly proven stream surfaces
        unchanged. This is a narrow, typed, visible *stream* retry over the
        channel that is already held -- never a silent redial: the channel
        itself only ever replaces via the explicit, user-authorized
        `reconnect()`.
        """
        normalized_method = validate_channel_request(method=method, path=path)
        if response_timeout_seconds is not None and (
            not math.isfinite(response_timeout_seconds) or response_timeout_seconds <= 0
        ):
            raise ValueError("response_timeout_seconds must be positive and finite")
        retried = False
        while True:
            stream = self._acquire_stream(
                reason="stale_pooled_stream" if retried else "http_stream_opened"
            )
            try:
                document = _request_json_on_stream(
                    stream=stream,
                    method=normalized_method,
                    path=path,
                    query=query,
                    body=body,
                    api_token=self._api_token,
                    session_id=self._session_id,
                    generation_id=self._generation_id,
                    response_timeout_seconds=response_timeout_seconds,
                )
            except BaseException as exc:
                self._discard_stream(stream)
                if retried or not _is_stale_stream_error(exc):
                    raise
                retried = True
                continue
            self._release_stream(stream)
            return document

    def session_status(self) -> dict[str, object]:
        """Read the remote relay session's status over the held channel.

        This replaces the per-operation ``ssh ... bash -s`` status probe.  The
        remote report is cross-checked against the identity this connection is
        pinned to, so a channel that started answering for a different session
        or generation fails loudly instead of being used.
        """
        document = self.request_json(method="GET", path="/session-status")
        if not isinstance(document, dict):
            raise RelayError("owned session status response is not a JSON object")
        status = cast(dict[str, object], document)
        if (
            status.get("owner") != "clio-relay"
            or status.get("cluster") != self.cluster
            or status.get("session_id") != self._session_id
            or status.get("session_generation_id") != self._generation_id
            or status.get("running") is not True
        ):
            raise RelayError(
                "owned session status does not describe the exact connected generation for "
                f"{self.cluster}/{self._session_id}"
            )
        return status

    def close(self) -> None:
        """Release the held channel and record the closure.

        ``transport.failure_detail()`` is read AFTER ``_release_locked``
        (which calls ``transport.close()``), not before: for the frp-based
        transports that is when a residual, secret-bearing config-cleanup
        failure would be folded in (``HeldFrpVisitor.config_cleanup_error``,
        #231 R5 opus review item R3) -- a normal close leaves it ``None`` and
        the event is unchanged, but a residual is never silently dropped from
        the ledger.
        """
        with self._lock:
            if self._transport is None:
                return
            transport = self._transport
            self._release_locked(reason="closed")
            residual_detail = transport.failure_detail()
            self._record(
                channel_event(
                    cluster=self.cluster,
                    mode=self._transport_mode,
                    event="closed",
                    attempt=self._attempt,
                    reason="config_cleanup_error" if residual_detail else None,
                    detail=residual_detail,
                    identity_anchor=self.identity_anchor,
                )
            )

    def matches(
        self,
        *,
        settings: RelaySettings,
        remote_api_port: int | None = None,
    ) -> bool:
        """Return whether a held connection still serves this exact identity."""
        try:
            session_id, generation_id, api_token = owned_session_credentials(
                definition=self._definition,
                settings=settings,
            )
        except ConfigurationError:
            return False
        resolved_port = resolve_remote_api_port(
            settings=settings,
            remote_api_port=remote_api_port,
        )
        return (
            session_id == self._session_id
            and generation_id == self._generation_id
            and api_token == self._api_token
            and resolved_port == self._remote_api_port
        )

    def _establish(self, *, event: Literal["established", "reestablished"]) -> None:
        """Bring one transport up and prove the remote relay behind it.

        ``build_transport`` runs INSIDE the try (#231 R5 opus review item R2):
        a typed refusal it raises (``TransportIdentityAnchorRequired``, a
        missing ``CLIO_RELAY_API_TOKEN``, ...) must still reach the ledger as
        a terminal ``establish_failed`` event, not propagate with a dangling
        ``establishing`` and no terminal event at all. It also means the
        transport object exists before ``authorization_required`` is decided,
        which is what item R7 needs (see below).
        """
        self._attempt += 1
        nonce = secrets.token_hex(32)
        transport: RelayTransport | None = None
        try:
            transport = build_transport(
                mode=self._transport_mode,
                definition=self._definition,
                session_id=self._session_id,
                session_generation_id=self._generation_id,
                remote_api_port=self._remote_api_port,
                nonce=nonce,
                api_token=self._api_token,
                frpc_bin=self._settings.frpc_bin,
                process_factory=self._process_factory,
                ready_timeout_seconds=self._timeout_seconds,
                allow_interactive_authorization=self._allow_interactive_authorization,
            )
            # Gated on the TRANSPORT's own declared property, not on
            # ``self._allow_interactive_authorization`` (#231 R5 opus review
            # item R7): that connection-level setting only controls whether an
            # ssh_forward dial may prompt (it becomes
            # ``SshForwardTransport.requires_user_authorization`` verbatim, so
            # ssh_forward's event is unchanged), but a default-configured
            # brokered_tcp/udp_rendezvous connection has no prompt to announce
            # at all -- gating on the transport's own answer is what makes
            # "no authorization event" a structural property of the mode
            # rather than a fixture/settings choice.
            if transport.requires_user_authorization:
                self._record(
                    channel_event(
                        cluster=self.cluster,
                        mode=self._transport_mode,
                        event="authorization_required",
                        attempt=self._attempt,
                        reason="transport_requires_user_authorization",
                        user_authorization_required=True,
                        identity_anchor=self.identity_anchor,
                    )
                )
            self._record(
                channel_event(
                    cluster=self.cluster,
                    mode=self._transport_mode,
                    event="establishing",
                    attempt=self._attempt,
                    identity_anchor=self.identity_anchor,
                )
            )
            link = transport.establish(nonce=nonce)
            endpoint = link.control_endpoint
            bootstrap = link.bootstrap
            self._verify_bootstrap(bootstrap)
            stream = _open_identity_bound_stream(
                endpoint=endpoint,
                nonce=nonce,
                expected_identity=bootstrap.identity,
                timeout_seconds=self._timeout_seconds,
            )
        except BaseException as exc:
            if transport is not None:
                transport.close()
            self._record(
                channel_event(
                    cluster=self.cluster,
                    mode=self._transport_mode,
                    event="establish_failed",
                    attempt=self._attempt,
                    reason=type(exc).__name__,
                    detail=str(exc),
                    identity_anchor=self.identity_anchor,
                )
            )
            raise
        self._transport = transport
        self._endpoint = endpoint
        self._bootstrap = bootstrap
        self._link = link
        self._nonce = nonce
        self._idle_streams = [stream]
        self._open_streams = 1
        self._streams_proven = 1
        self._record(
            channel_event(
                cluster=self.cluster,
                mode=self._transport_mode,
                event=event,
                attempt=self._attempt,
                identity_anchor=self.identity_anchor,
            )
        )

    def _verify_bootstrap(self, bootstrap: OwnedSessionChannelBootstrap) -> None:
        """Require the remote relay to be the exact, live, owned generation."""
        status = bootstrap.status
        remote_api_port = status.get("remote_api_port")
        if (
            status.get("owner") != "clio-relay"
            or status.get("cluster") != self._definition.name
            or status.get("session_id") != self._session_id
            or status.get("session_generation_id") != self._generation_id
            or status.get("running") is not True
            or status.get("ownership_verified") is not True
            or isinstance(remote_api_port, bool)
            or not isinstance(remote_api_port, int)
            or not 1 <= remote_api_port <= 65_535
        ):
            raise RelayError(
                "remote relay session is not the active, ownership-verified generation requested "
                f"for {self._definition.name}/{self._session_id}"
            )
        if remote_api_port != self._remote_api_port:
            raise RelayError(
                "remote relay session reported owned API port "
                f"{remote_api_port}, but the held channel maps {self._remote_api_port}; "
                "configure CLIO_RELAY_OWNER_SESSION_API_PORT for this connection"
            )

    def _acquire_stream(
        self,
        *,
        reason: str = "http_stream_opened",
    ) -> http.client.HTTPConnection:
        """Take one proven HTTP stream over the held channel.

        Streams are pooled, not shared: a long poll on one operation must not
        block every other operation on the same cluster.  Opening another stream
        is another TCP connection *through the forward that is already held*, so
        it costs no new transport -- the dial count is unchanged.  Every stream
        is proven against the same bring-up identity document -- carried
        out-of-band in ``ssh_forward`` mode, fetched identity-first over the
        held link itself under the weaker ``preshared_link_secret`` anchor in
        modes (a)/(b) (§8.3; ``RemoteConnection.identity_anchor``) -- before
        any credential is sent, so none of them talks to an unproven listener.

        ``reason`` labels *why* a freshly proven stream's ``stream_reproven``
        event fired: ordinary concurrency (the default) versus `request_json`
        retrying a stream that died between requests (clio-relay#213).
        """
        with self._lock:
            transport = self._transport
            if transport is None:
                raise ChannelNotEstablished(
                    f"owned session connection for {self.cluster} has no channel; call connect()"
                )
            if not transport.is_alive():
                self._record(
                    channel_event(
                        cluster=self.cluster,
                        mode=self._transport_mode,
                        event="dropped",
                        attempt=self._attempt,
                        reason="transport_exited",
                        detail=transport.failure_detail(),
                        identity_anchor=self.identity_anchor,
                    )
                )
                raise ChannelDropped(
                    f"owned session channel for {self.cluster} dropped; "
                    "call reconnect() to re-establish it"
                )
            while self._idle_streams:
                candidate = self._idle_streams.pop()
                if candidate.sock is not None:
                    return candidate
                self._close_stream_locked(candidate)
            endpoint = self._endpoint
            bootstrap = self._bootstrap
            nonce = self._nonce
            generation = self._transport_generation
            if endpoint is None or bootstrap is None or nonce is None:
                raise ChannelNotEstablished(
                    f"owned session connection for {self.cluster} lost its proven bring-up identity"
                )
        proven = _open_identity_bound_stream(
            endpoint=endpoint,
            nonce=nonce,
            expected_identity=bootstrap.identity,
            timeout_seconds=self._timeout_seconds,
        )
        with self._lock:
            if generation != self._transport_generation:
                # The channel was replaced while this stream was being proven;
                # it belongs to a link that no longer exists.
                with suppress(OSError):
                    proven.close()
                raise ChannelDropped(
                    f"owned session channel for {self.cluster} was replaced while "
                    "a stream was being proven"
                )
            self._open_streams += 1
            self._streams_proven += 1
            if self._streams_proven > 1:
                self._record(
                    channel_event(
                        cluster=self.cluster,
                        mode=self._transport_mode,
                        event="stream_reproven",
                        attempt=self._attempt,
                        reason=reason,
                        identity_anchor=self.identity_anchor,
                    )
                )
        return proven

    def _release_stream(self, stream: http.client.HTTPConnection) -> None:
        """Return one still-usable stream to the pool."""
        with self._lock:
            if stream.sock is None or self._transport is None:
                self._close_stream_locked(stream)
                return
            if len(self._idle_streams) >= MAX_POOLED_CHANNEL_STREAMS:
                self._close_stream_locked(stream)
                return
            self._idle_streams.append(stream)

    def _discard_stream(self, stream: http.client.HTTPConnection) -> None:
        """Drop one stream that failed; the channel itself is unaffected."""
        with self._lock:
            self._close_stream_locked(stream)

    def _close_stream_locked(self, stream: http.client.HTTPConnection) -> None:
        with suppress(OSError):
            stream.close()
        self._open_streams = max(0, self._open_streams - 1)

    def _release_locked(self, *, reason: str) -> None:
        """Drop every stream and the transport without recording an event."""
        del reason
        streams = self._idle_streams
        self._idle_streams = []
        for stream in streams:
            with suppress(OSError):
                stream.close()
        self._open_streams = 0
        self._streams_proven = 0
        self._transport_generation += 1
        transport = self._transport
        self._transport = None
        self._endpoint = None
        self._bootstrap = None
        if transport is not None:
            transport.close()

    def _record(self, event: ChannelEvent) -> None:
        """Append one bounded typed event and forward it to the sink."""
        self._events.append(event)
        if len(self._events) > MAX_RECORDED_CHANNEL_EVENTS:
            del self._events[: len(self._events) - MAX_RECORDED_CHANNEL_EVENTS]
        sink = self._event_sink
        if sink is not None:
            sink(event)


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


def owned_session_credentials(
    *,
    definition: ClusterDefinition,
    settings: RelaySettings,
) -> tuple[str, str, str]:
    """Return the exact owned session identity and token for one connection."""
    session_id = settings.owner_session_id
    generation_id = settings.owner_session_generation_id
    api_token = settings.api_token
    if session_id is None or generation_id is None:
        raise ConfigurationError(
            "owned remote request requires CLIO_RELAY_OWNER_SESSION_ID and "
            "CLIO_RELAY_SESSION_GENERATION_ID"
        )
    if settings.resolved_owner_session_cluster() != definition.name:
        raise ConfigurationError(
            "owned remote request requires CLIO_RELAY_OWNER_SESSION_CLUSTER to match the "
            "selected route"
        )
    if not api_token:
        raise ConfigurationError(
            "owned remote request requires CLIO_RELAY_API_TOKEN for authentication"
        )
    return session_id, generation_id, api_token


def _open_identity_bound_stream(
    *,
    endpoint: ChannelEndpoint,
    nonce: str,
    expected_identity: dict[str, object],
    timeout_seconds: float,
) -> http.client.HTTPConnection:
    """Prove one non-reconnecting TCP stream before any credential is sent."""
    stream = http.client.HTTPConnection(endpoint.host, endpoint.port, timeout=timeout_seconds)
    try:
        stream.connect()
        stream.auto_open = 0
        stream.request(
            "GET",
            "/session-identity?" + urllib.parse.urlencode({"nonce": nonce}),
            headers={"Accept": "application/json", "Connection": "keep-alive"},
        )
        proof_response = stream.getresponse()
        proof_document = read_json_response(proof_response, label="session identity challenge")
        if proof_response.status != 200 or not isinstance(proof_document, dict):
            raise RelayError("owned session API did not return a valid server identity challenge")
        verify_session_identity(
            cast(dict[str, object], proof_document),
            expected=expected_identity,
        )
        if proof_response.will_close or stream.sock is None:
            raise RelayError(
                "owned session API closed the identity-proven connection before authentication"
            )
        return stream
    except (OSError, http.client.HTTPException) as exc:
        stream.close()
        raise RelayError("owned session API identity challenge failed") from exc
    except BaseException:
        stream.close()
        raise


def _request_json_on_stream(
    *,
    stream: http.client.HTTPConnection,
    method: str,
    path: str,
    query: dict[str, object] | None,
    body: dict[str, object] | None,
    api_token: str,
    session_id: str,
    generation_id: str,
    response_timeout_seconds: float | None,
) -> object:
    """Issue one request without permitting HTTPConnection to reconnect."""
    encoded_query = "" if query is None else "?" + urllib.parse.urlencode(query)
    encoded_body = None if body is None else json.dumps(body).encode("utf-8")
    proven_socket = stream.sock
    prior_connection_timeout = stream.timeout
    prior_socket_timeout: float | None = None
    response_timeout_applied = False
    try:
        if response_timeout_seconds is not None:
            if proven_socket is None:
                raise RelayError("owned session API identity-proven connection is not open")
            prior_socket_timeout = proven_socket.gettimeout()
            stream.timeout = response_timeout_seconds
            proven_socket.settimeout(response_timeout_seconds)
            response_timeout_applied = True
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {api_token}",
            OWNER_SESSION_ID_HEADER: session_id,
            SESSION_GENERATION_ID_HEADER: generation_id,
        }
        if encoded_body is not None:
            headers["Content-Type"] = "application/json"
        stream.request(
            method,
            path + encoded_query,
            body=encoded_body,
            headers=headers,
        )
        response = stream.getresponse()
        document = read_json_response(response, label=f"{method} {path}")
        if not 200 <= response.status < 300:
            # A1 (#231 R6 review): door_errors.as_http_problem spreads the
            # original T2 refusal document's fields (doc §6.4) into the 413
            # problem body (fault.data, F4) -- recognized here so its own
            # typed code/message surfaces instead of the generic "HTTP
            # {status}: {raw json blob}" that discards the structure.
            typed_document = (
                cast(dict[str, object], document) if isinstance(document, dict) else None
            )
            if typed_document is not None and is_delivery_refusal(typed_document):
                code = cast(dict[str, object], typed_document.get("delivery", {})).get("code")
                raise RelayError(
                    f"owned session API request refused delivery ({code}): "
                    f"{describe_delivery_refusal(typed_document)}"
                )
            detail = json.dumps(document, ensure_ascii=False)[:2_000]
            raise RelayError(
                f"owned session API request failed: {method} {path}: "
                f"HTTP {response.status}: {detail}"
            )
        return document
    except (OSError, http.client.HTTPException) as exc:
        if response_timeout_applied and isinstance(exc, TimeoutError):
            raise ObservationTimeoutError(
                f"owned session API response observation timed out for {method} {path}"
            ) from exc
        raise RelayError(
            f"owned session API identity-bound request failed for {method} {path}: {exc}"
        ) from exc
    finally:
        if response_timeout_seconds is not None:
            stream.timeout = prior_connection_timeout
            if (
                response_timeout_applied
                and stream.sock is proven_socket
                and proven_socket is not None
            ):
                try:
                    proven_socket.settimeout(prior_socket_timeout)
                except OSError:
                    # The response may have closed the proven socket.  Never let
                    # HTTPConnection reconnect it under the authenticated client.
                    stream.close()


def read_json_response(response: http.client.HTTPResponse, *, label: str) -> object:
    """Read one bounded JSON document from a channel response."""
    payload = response.read(MAX_SESSION_API_RESPONSE_BYTES + 1)
    if len(payload) > MAX_SESSION_API_RESPONSE_BYTES:
        raise RelayError(f"owned session API {label} response exceeded its byte limit")
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RelayError(f"owned session API {label} response was not UTF-8 JSON") from exc


def verify_session_identity(
    observed: dict[str, object],
    *,
    expected: dict[str, object],
) -> None:
    """Require the reached listener to be the bring-up-proven session.

    ``expected`` is proven under whatever identity anchor this connection's
    mode declares (§8.3) -- the ssh-authenticated bootstrap act in
    ``ssh_forward`` mode, the weaker ``preshared_link_secret`` anchor in
    modes (a)/(b) -- never assumed to be ssh-specific here.
    """
    if any(observed.get(field) != expected.get(field) for field in _IDENTITY_FIELDS):
        raise RelayError(
            "owned session API server identity did not match the bring-up-proven session"
        )
    observed_signature = observed.get("hmac_sha256")
    expected_signature = expected.get("hmac_sha256")
    if (
        not isinstance(observed_signature, str)
        or not isinstance(expected_signature, str)
        or len(observed_signature) != 64
        or len(expected_signature) != 64
        or not hmac.compare_digest(observed_signature, expected_signature)
    ):
        raise RelayError("owned session API server identity HMAC did not verify")
