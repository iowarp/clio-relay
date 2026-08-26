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

The raw identity-bound-stream wire mechanics (:mod:`clio_relay.
remote_connection_stream_io`) and the connections-by-cluster registry
(:mod:`clio_relay.remote_connection_registry`) are owned by their own
modules; both are re-exported here so every existing import of this module
keeps working unchanged.
"""

from __future__ import annotations

import http.client
import math
import secrets
import threading
from collections.abc import Iterator
from contextlib import suppress
from typing import Final, Literal, cast

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
from clio_relay.errors import ConfigurationError, RelayError

# Facade: the pooled-stream wire mechanics moved to remote_connection_stream_io.py
# (identity-bound stream open/request/read + the clio-relay#213 stale-stream
# classifier); the connections-by-cluster registry moved to
# remote_connection_registry.py. `_is_stale_stream_error`/
# `_open_identity_bound_stream`/`_request_json_on_stream` each still have a
# bare-name call site inside `RemoteConnection` below, so they are imported by
# name (this file's own globals resolve them, so a test monkeypatch on
# `clio_relay.remote_connection.<name>` still reaches those call sites).
# `MAX_SESSION_API_RESPONSE_BYTES`/`RemoteConnectionRegistry`/
# `connection_registry` have no reader left in this file's own body (only
# session_api.py and tests import them directly from here), so they use the
# `X as X` self-alias idiom ruff/pyflakes recognizes as an intentional
# re-export instead of a `from ... import` it would otherwise flag unused.
from clio_relay.remote_connection_lease import (
    SessionLeaseExpiredError as SessionLeaseExpiredError,
)
from clio_relay.remote_connection_registry import (
    RemoteConnectionRegistry as RemoteConnectionRegistry,
)
from clio_relay.remote_connection_registry import (
    connection_registry as connection_registry,
)
from clio_relay.remote_connection_registry import record_reconciliation_events, verify_bootstrap
from clio_relay.remote_connection_stream_io import (
    MAX_SESSION_API_RESPONSE_BYTES as MAX_SESSION_API_RESPONSE_BYTES,
)
from clio_relay.remote_connection_stream_io import LogStreamChunk as LogStreamChunk
from clio_relay.remote_connection_stream_io import (
    _is_stale_stream_error,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _open_identity_bound_stream,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _request_json_on_stream,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _stream_log_chunks_over_stream,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

MAX_RECORDED_CHANNEL_EVENTS: Final = 256
# Idle streams held over the one channel; more concurrent operations simply open
# more streams through the same forward, which costs no new transport.
MAX_POOLED_CHANNEL_STREAMS: Final = 8
DEFAULT_OWNED_SESSION_API_PORT: Final = 8765


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
    def state(self) -> Literal["connected", "authorization_required", "not_established"]:
        """Return this connection's typed lifecycle state (iowarp/clio-relay#276 B2).

        A read-only projection of the same transport reference/liveness this
        class already tracks -- it records no new state of its own.
        ``"authorization_required"`` means a channel was established at least
        once and has since dropped: the transport is not alive, but this
        connection still remembers holding a (now-dead) transport, so only an
        explicit, user-authorized :meth:`reconnect` -- never a background
        retry -- may replace it (the 2FA doctrine,
        docs/connection-model.md:141-157). ``"not_established"`` means no
        channel has ever been held on this connection object. ``session_attach.
        attach_owned_session`` branches on this exact literal to decide
        between resuming a live channel in place and performing the one
        authorized reconnect.

        The transport reference is read exactly ONCE, under :attr:`_lock`:
        reading it via two separate unlocked calls (``self.connected`` then
        ``self._transport``) could observe two different transport states
        across a concurrent ``close()``/``reconnect()`` racing this read,
        misreporting ``not_established`` for a connection that was actually
        ``authorization_required`` a moment earlier.
        """
        with self._lock:
            transport = self._transport
            if transport is not None and transport.is_alive():
                return "connected"
            if transport is not None:
                return "authorization_required"
            return "not_established"

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

    def stream_log_chunks(
        self,
        *,
        job_id: str,
        stream_name: str,
        offset: int = 0,
        poll_seconds: float | None = None,
    ) -> Iterator[LogStreamChunk]:
        """Follow one job log stream over the held channel, no extra ssh
        dial (clio-relay#221/#259). A mid-stream failure surfaces as
        :class:`~clio_relay.control_channel.ChannelDropped`, never retried
        here -- resuming from the last chunk's ``offset`` is the caller's own
        choice.
        """
        validate_channel_request(method="GET", path=f"/jobs/{job_id}/logs/{stream_name}/sse")
        stream = self._acquire_stream(reason="log_sse_opened")
        try:
            yield from _stream_log_chunks_over_stream(
                stream=stream,
                job_id=job_id,
                stream_name=stream_name,
                offset=offset,
                poll_seconds=poll_seconds,
                api_token=self._api_token,
                session_id=self._session_id,
                generation_id=self._generation_id,
            )
        finally:
            self._discard_stream(stream)

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

    def close(self, *, at_exit: bool = False) -> None:
        """Release the held channel and record the closure.

        ``transport.failure_detail()`` is read AFTER ``_release_locked``
        (which calls ``transport.close()``), not before: for the frp-based
        transports that is when a residual, secret-bearing config-cleanup
        failure would be folded in (``HeldFrpVisitor.config_cleanup_error``,
        #231 R5 opus review item R3) -- a normal close leaves it ``None`` and
        the event is unchanged, but a residual is never silently dropped from
        the ledger.

        ``at_exit=True`` (iowarp/clio-relay#285) records ``closed_at_exit``
        instead of ``closed`` -- the identical release, distinguishing the
        registry's atexit self-clean hook from an explicit caller-driven
        close in the acceptance report.
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
                    event="closed_at_exit" if at_exit else "closed",
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
            # #285/D5: the OTHER outcome (establish() itself raising, so
            # this line is never reached) is covered by the except-handler's
            # own call below -- never lost from the ledger either way.
            record_reconciliation_events(self, transport)
            endpoint = link.control_endpoint
            bootstrap = link.bootstrap
            verify_bootstrap(
                bootstrap,
                definition=self._definition,
                session_id=self._session_id,
                generation_id=self._generation_id,
                remote_api_port=self._remote_api_port,
            )
            stream = _open_identity_bound_stream(
                endpoint=endpoint,
                nonce=nonce,
                expected_identity=bootstrap.identity,
                timeout_seconds=self._timeout_seconds,
            )
        except BaseException as exc:
            if transport is not None:
                # D5: covers the case the try-block's own call above never
                # reaches -- establish() itself raising.
                record_reconciliation_events(self, transport)
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
