"""``RelayTransport`` implementations for modes (a) ``brokered_tcp`` and (b)
``udp_rendezvous`` (iowarp/clio-relay#231 R5, tracked by iowarp/clio-relay#188
items 1-3+5; the design is ``docs/design/relay-architecture-2026-08.md`` §8).

``control_channel.py``'s ``SshForwardTransport`` is the reference lifecycle for
mode (c): dial once, hold the process for the connection's lifetime, prove the
bring-up identity document out of band, refuse to redial internally. Both
classes here hold exactly one :class:`~clio_relay.frp_link.HeldFrpVisitor`
(the R4 substrate) instead -- a local ``frpc`` process holding an stcp/xtcp
visitor tunnel through the cluster's configured relay point -- and follow the
same discipline: ``establish`` is callable once (§8.5's R10 fix: a failed
attempt permanently consumes the instance, exactly like ``SshForwardTransport``
-- retrying means building a NEW transport), ``open_stream_channel`` refuses
(multiplexing onto the held link is not built for any mode yet), and ``close``
releases the held visitor.

Modes (a)/(b) have no ssh-authenticated act to carry the bring-up identity
document over the way mode (c)'s held SSH session does, so ``establish`` fetches
the exact same two facts (``session-identity``, ``session-status``) as plain
HTTP requests over the tunnel that is already held -- the same requests every
later owned-session operation makes (``remote_connection.py``'s
``_open_identity_bound_stream``/``session_status``). **Identity-first, always**
(§8.3's R1 security fix): the unauthenticated identity challenge is fetched and
verified against this connection's PINNED cluster/session/generation/nonce
BEFORE the bearer-authenticated status request is ever issued. A rogue process
squatting on the local loopback bind -- holding no secret of its own -- must
already know this connection's exact pinned identity to pass that check;
before this fix it received the real owner bearer token on request #0 for
free. This is not a cryptographic proof (the transport has no owner token to
check the response's HMAC against -- see ``OwnedSessionChannelBootstrap``'s
docstring); only the later re-proof in
``remote_connection.verify_session_identity`` closes that gap. The
``preshared_link_secret`` anchor (§8.3) does not cover the LOCAL bind end (the
loopback port) either -- see ``docs/connection-model.md``'s "Still deviating"
entry.

That anchor gap is why §8.3 requires a cluster to explicitly opt into
``preshared_link_secret`` before either mode may be used at all;
``control_channel.build_transport`` enforces that refusal before this module is
even imported (a lazy import there, to avoid a reverse circular import: this
module needs ``ChannelLink``/``ChannelEndpoint``/``OwnedSessionChannelBootstrap``
from ``control_channel.py``).

``udp_rendezvous``'s hole-punch failure is a typed :class:`TransportPunchFailed`
refusal in this slice, not the automatic in-mode fallback to ``brokered_tcp``'s
stcp visitor that ``docs/connection-model.md`` and §8.4 describe as the mode's
eventual sanctioned behavior -- see that class's docstring and
``docs/connection-model.md``'s "Still deviating" entry for the tracked
follow-up. This path must never render OR spawn an stcp visitor -- proven at
the render call itself, not only by inspecting rendered text
(``tests/test_frp_transport_dials.py``'s sabotage twin spies on
``frp_link.render_visitor_config``): ``transport_probe.py``'s probe-only
``allow_stcp_fallback`` has no equivalent here.

No real sockets are held open by anything in this module's own tests: no real
``frpc``, no real cluster, only a temp dir for rendered TOML and (for the one
health-timeout test that deliberately does not mock ``wait_for_channel_health``)
a real, immediately-refused loopback connection attempt against a port nothing
is listening on.
"""

from __future__ import annotations

import http.client
import json
import urllib.parse
from typing import ClassVar, Final, cast

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
from clio_relay.errors import RelayError
from clio_relay.frp_link import (
    FrpLinkConfig,
    FrpProcessFactory,
    FrpVisitorType,
    HeldFrpVisitor,
    assert_loopback_port_available,
    select_loopback_port,
    validate_channel_nonce,
)
from clio_relay.frp_proxy_naming import canonical_proxy_name
from clio_relay.frp_visitor_reconciliation import reconcile_stale_frp_visitors
from clio_relay.job_identity import OWNER_SESSION_ID_HEADER, SESSION_GENERATION_ID_HEADER
from clio_relay.session_api import SESSION_IDENTITY_SCHEMA

MAX_BRING_UP_FETCH_BYTES: Final = MAX_CHANNEL_BOOTSTRAP_BYTES

# The identity challenge's fields checked against this connection's pinned
# values before the authenticated status request is ever issued (§8.3/R1).
# ``hmac_sha256`` is deliberately excluded: the transport has no owner token
# to check it against, so it is not part of this pre-authentication gate --
# only the later re-proof (`remote_connection.verify_session_identity`)
# checks it.
_PINNED_IDENTITY_FIELDS: Final = (
    "cluster",
    "session_id",
    "session_generation_id",
    "nonce",
)


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
    fetches the bring-up document -- identity first -- over that SAME held
    link. Subclasses fix ``_mode``/``_visitor_type`` and may override
    :meth:`_translate_tunnel_failure` to turn a failed tunnel into a
    mode-specific typed error.
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
        # remote_connection_registry.verify_bootstrap, not here.
        self._remote_api_port = remote_api_port
        self._api_token = api_token
        self._identity_anchor: IdentityAnchor = identity_anchor
        self._frpc_bin = frpc_bin
        self._process_factory = process_factory
        self._local_bind_port = local_bind_port
        self._ready_timeout_seconds = ready_timeout_seconds
        self._proxy_name = canonical_proxy_name(definition, cluster=cluster)
        self._visitor: HeldFrpVisitor | None = None
        self._established = False
        self._failure_detail: str | None = None
        # iowarp/clio-relay#285: populated by the reconciliation pass at the
        # START of establish(), before this instance's own new visitor is
        # even rendered. RemoteConnection._establish reads
        # reaped_orphan_visitor_pids back (a plain optional attribute check --
        # SshForwardTransport carries no equivalent, since ssh_forward is
        # self-cleaning by construction) to fold each pid into its own
        # typed visitor_orphan_reaped channel event.
        self._reaped_orphan_visitor_pids: tuple[int, ...] = ()
        self._swept_config_dirs = 0
        self._reconciliation_skipped_reason: str | None = None

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

    @property
    def reaped_orphan_visitor_pids(self) -> tuple[int, ...]:
        """Return prior orphaned visitor pids reaped during this establish (#285)."""
        return self._reaped_orphan_visitor_pids

    @property
    def swept_stale_config_dirs(self) -> int:
        """Return the count of stale crash-orphaned visitor config dirs removed (#285)."""
        return self._swept_config_dirs

    @property
    def reconciliation_skipped_reason(self) -> str | None:
        """Return why THIS establish's reconciliation snapshot could not run (#285 D2)."""
        return self._reconciliation_skipped_reason

    def establish(self, *, nonce: str) -> ChannelLink:
        """Hold one frp visitor tunnel and fetch the bring-up document over it.

        ``_established`` is set right after the dial succeeds (matching
        ``SshForwardTransport``'s reference lifecycle, #231 R5 opus review
        item R10), BEFORE the health-wait/bring-up-fetch phase that can still
        fail: a failure from that point on permanently consumes this instance.
        ``establish`` is callable once, full stop -- a failed attempt is
        replaced by building a NEW transport, never retried in place
        (``RelayTransport``'s own Protocol docstring).

        iowarp/clio-relay#285: BEFORE any of that, one stale-visitor
        reconciliation pass runs for this exact ``frpc_bin`` -- re-verified,
        pid-reuse-safe reaping (D1) of any prior visitor whose owning CLI
        process is gone (a ``kill -9`` or crash the atexit hook in
        ``remote_connection_registry.py`` could never have caught), each
        reap also removing its own secret-bearing config dir (D3), plus a
        once-per-process sweep (D8) of aged crash-orphaned config dirs
        regardless of emptiness. This never blocks a legitimate concurrent
        CLI's own held visitor (a live parent is never touched) and never
        raises out of this method (``reconcile_stale_frp_visitors`` is
        itself best-effort; a snapshot it could not read at all surfaces as
        a typed ``reconciliation_skipped_reason``, D2, rather than silently
        finding nothing).
        """
        if self._established:
            raise RelayError(f"{self._mode} transport was already established")
        validate_channel_nonce(nonce)
        reconciliation = reconcile_stale_frp_visitors(frpc_bin=self._frpc_bin)
        self._reaped_orphan_visitor_pids = reconciliation.reaped_pids
        self._swept_config_dirs = reconciliation.swept_config_dirs
        self._reconciliation_skipped_reason = reconciliation.skipped_reason
        local_bind_port = self._local_bind_port or select_loopback_port(subject="held frp visitor")
        assert_loopback_port_available(local_bind_port, subject="frp visitor")
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
        visitor.establish()
        self._visitor = visitor
        self._established = True
        try:
            if not visitor.is_alive():
                raise RelayError(
                    _visitor_failure_message(
                        mode=self._mode,
                        visitor_type=self._visitor_type,
                        cluster=self._cluster,
                        visitor=visitor,
                        situation="exited immediately",
                    )
                )
            # Names the CONNECTION, not the process holding it (contrast
            # HeldFrpVisitor.wait_healthy's own "frp {type} visitor" default) --
            # #231 R4 opus review F4.
            visitor.wait_healthy(
                timeout_seconds=self._ready_timeout_seconds,
                subject=f"frp {self._visitor_type} link",
            )
        except BaseException as exc:
            visitor.close()
            self._failure_detail = _combined_failure_detail(visitor)
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
                cluster=self._cluster,
                session_id=self._session_id,
                session_generation_id=self._session_generation_id,
                nonce=nonce,
                timeout_seconds=self._ready_timeout_seconds,
            )
        except BaseException:
            visitor.close()
            self._failure_detail = _combined_failure_detail(visitor)
            self._visitor = None
            raise
        return ChannelLink(
            control_endpoint=endpoint,
            bootstrap=bootstrap,
            identity_anchor=self._identity_anchor,
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
            cleanup_error = visitor.config_cleanup_error
            if cleanup_error is not None:
                # Never lost: the rendered config carries a plaintext
                # token/secret, so a failed cleanup is a residual,
                # security-relevant fact even on an otherwise ordinary close
                # -- not routine stdout/stderr noise (#231 R5 opus review item
                # R3). RemoteConnection.close() reads this back via
                # failure_detail() to stamp a typed
                # reason="config_cleanup_error" on the "closed" event.
                self._failure_detail = cleanup_error


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


def _combined_failure_detail(visitor: HeldFrpVisitor) -> str | None:
    """Fold the visitor's stdout/stderr excerpt and any residual cleanup error.

    ``HeldFrpVisitor.close()`` never suppresses a failed config-directory
    cleanup (the config carries a plaintext token/secret); folding it in here
    makes sure a residual secret is visible wherever this transport's
    ``failure_detail()`` is read after a failed establish, not only inside
    ``HeldFrpVisitor``'s own tracking (#231 R5 opus review item R3).
    """
    parts = [part for part in (visitor.failure_detail(), visitor.config_cleanup_error) if part]
    return "; ".join(parts) if parts else None


def _visitor_failure_message(
    *,
    mode: TransportMode,
    visitor_type: FrpVisitorType,
    cluster: str,
    visitor: HeldFrpVisitor,
    situation: str,
) -> str:
    """Return a mode/visitor-type/cluster-prefixed message, detail appended.

    Mirrors ``transport_probe.py``'s ``_visitor_failure_message`` (#231 R4
    opus review F2, R5 review item R4): the prefix naming WHICH link this is
    is never replaced by the visitor's bounded stdout/stderr excerpt, only
    extended by it.
    """
    label = f"{mode} {visitor_type} visitor for cluster {cluster!r} {situation}"
    detail = _combined_failure_detail(visitor)
    return f"{label}: {detail}" if detail else label


def _verify_pinned_identity(
    identity: dict[str, object],
    *,
    cluster: str,
    session_id: str,
    session_generation_id: str,
    nonce: str,
) -> None:
    """Require the identity challenge to describe the exact pinned connection.

    §8.3/R1's security fix: this request carries no credential, so it is
    fetched and verified BEFORE the bearer-authenticated status request that
    follows -- a rogue process squatting on the local loopback bind cannot use
    this response alone to learn anything it did not already know, and the
    authenticated request is never issued until this passes.

    This is NOT a cryptographic proof: the transport has no owner token to
    check the response's ``hmac_sha256`` against
    (``OwnedSessionChannelBootstrap``'s own docstring says that token never
    leaves the cluster), so a rogue that already knows this connection's
    pinned cluster/session/generation/nonce could still pass this specific
    check -- only the later re-proof
    (``remote_connection.verify_session_identity``, against THIS document)
    closes that gap. See ``docs/connection-model.md``'s "Still deviating"
    entry: the preshared anchor does not cover the local bind end.
    """
    if identity.get("schema_version") != SESSION_IDENTITY_SCHEMA or any(
        identity.get(field) != expected
        for field, expected in zip(
            _PINNED_IDENTITY_FIELDS,
            (cluster, session_id, session_generation_id, nonce),
            strict=True,
        )
    ):
        raise ChannelBootstrapError(
            "owned session channel bring-up identity challenge did not describe the pinned "
            f"connection for {cluster}/{session_id}; refusing before authenticating"
        )


def _fetch_channel_bootstrap(
    *,
    endpoint: ChannelEndpoint,
    api_token: str,
    cluster: str,
    session_id: str,
    session_generation_id: str,
    nonce: str,
    timeout_seconds: float,
) -> OwnedSessionChannelBootstrap:
    """Fetch the bring-up document OVER the held link, identity first.

    Mode (c) composes this document from an ssh-authenticated cluster-local
    executor. Modes (a)/(b) have no such act, so this fetches the same two
    facts as plain HTTP over the tunnel that is already held -- but in a
    specific order (§8.3/R1): first the unauthenticated
    ``GET /session-identity?nonce=`` (the same identity challenge
    ``_open_identity_bound_stream`` proves before any credential is sent),
    verified against this connection's pinned identity, and ONLY THEN the
    authenticated ``GET /session-status`` (the same request
    ``RemoteConnection.session_status`` makes once the channel is up). A rogue
    loopback responder that cannot pass the first check never sees the bearer
    token the second one carries.
    """
    identity = _get_bounded_json(
        endpoint,
        path="/session-identity?" + urllib.parse.urlencode({"nonce": nonce}),
        timeout_seconds=timeout_seconds,
        headers={"Accept": "application/json"},
    )
    if not isinstance(identity, dict):
        raise ChannelBootstrapError(
            "owned session channel bring-up did not report a JSON identity document"
        )
    _verify_pinned_identity(
        cast("dict[str, object]", identity),
        cluster=cluster,
        session_id=session_id,
        session_generation_id=session_generation_id,
        nonce=nonce,
    )
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
    if not isinstance(status, dict):
        raise ChannelBootstrapError(
            "owned session channel bring-up did not report a JSON status document"
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
