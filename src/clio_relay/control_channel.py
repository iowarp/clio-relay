"""One held relay-to-relay channel per remote connection.

The relay transport design is a single persistent link between the local relay
process and one remote relay process.  The link is established once, at
connection bring-up, and every owned-session operation -- status, identity
challenge, job submission, ingest, artifact content, watch -- rides it as plain
HTTP against the mapped port.  Nothing in this module may reopen the underlying
transport for an individual operation.

The mode is a deployment-time configuration choice, made per connection, and
this layer never selects or switches one.  There is no probing, no "try TCP and
degrade to SSH": an operator configures the pathway a connection uses, a link
failure is reported as a typed failure, and a reconnect re-establishes the same
configured mode.

``brokered_tcp``
    TCP through an internet-accessible relay point.  Both relays dial out and a
    server-brokered handshake joins the two outbound connections.
``udp_rendezvous``
    The same rendezvous with a UDP hole-punching handshake.  When traversal
    fails its own handshake carries the link through the server instead; that
    is internal to this mode, not a switch to another one.
``ssh_forward``
    For infrastructure that permits nothing else: one SSH process holding one
    port forward for the lifetime of the connection.

``ssh_forward`` is implemented directly in this module (:class:`SshForwardTransport`,
the reference lifecycle).  ``brokered_tcp``/``udp_rendezvous`` are implemented in
:mod:`clio_relay.frp_transport`, built on the held-frp-visitor substrate in
:mod:`clio_relay.frp_link`; :func:`build_transport` dispatches to them but refuses
either mode with a typed error for a cluster that has not opted into the
``preshared_link_secret`` identity anchor their bring-up requires (§8.3), so a
missing opt-in is visible rather than silently served by a weaker anchor.
"""

from __future__ import annotations

import json
import queue
import shlex
import subprocess
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import IO, Any, Final, Literal, Protocol, cast

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.cluster_config import ClusterDefinition, IdentityAnchor
from clio_relay.config import TransportMode
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.frp_link import BoundedStderrBuffer, pump_stderr
from clio_relay.frp_link import select_loopback_port as _available_loopback_port
from clio_relay.frp_link import validate_channel_nonce as _validate_channel_nonce
from clio_relay.frp_link import wait_for_channel_health as _wait_for_channel_health
from clio_relay.remote_cli import remote_env
from clio_relay.remote_values import render_remote_shell_value
from clio_relay.session_wire_models import OwnedSessionIdentityChallengeRequest

CHANNEL_BOOTSTRAP_SCHEMA: Final = "clio-relay.channel-bootstrap.v1"
CHANNEL_EVENT_SCHEMA: Final = "clio-relay.control-channel-event.v1"

# The bring-up document is framed, not positional: a login shell's profile may
# print a banner first, and the cluster-local executors may pretty-print.
# They begin with a letter so no shell parses them as an option operand.
CHANNEL_BOOTSTRAP_BEGIN: Final = b"CLIO_RELAY_CHANNEL_BOOTSTRAP_BEGIN"
CHANNEL_BOOTSTRAP_END: Final = b"CLIO_RELAY_CHANNEL_BOOTSTRAP_END"

MAX_CHANNEL_BOOTSTRAP_BYTES: Final = 256 * 1024
# Deliberately pinned equal to frp_link.py's DEFAULT_STDERR_BUFFER_MAX_BYTES
# by tests/test_frp_link.py -- see that constant's comment.
MAX_CHANNEL_EVENT_DETAIL_CHARS: Final = 2_000
DEFAULT_CHANNEL_READY_TIMEOUT_SECONDS: Final = 30.0

# A user completing interactive two-factor authorization is the expected cost of
# bring-up in ``ssh_forward`` mode.  It is paid once per connection, so the
# bring-up deadline is generous while every later operation is not.
DEFAULT_CHANNEL_AUTHORIZATION_TIMEOUT_SECONDS: Final = 300.0

ChannelEventName = Literal[
    "authorization_required",
    "establishing",
    "established",
    "establish_failed",
    "dropped",
    "reestablishing",
    "reestablished",
    "stream_reproven",
    "closed",
    # iowarp/clio-relay#285 self-clean doctrine: "closed_at_exit" is the same
    # release as "closed", stamped by the registry's atexit hook instead of an
    # explicit caller (remote_connection_registry.py); "visitor_orphan_reaped"
    # is recorded once per prior-process orphan a NEW visitor's own spawn
    # reaped before dialing (frp_transport.py, via
    # frp_visitor_reconciliation.py), carrying that pid in `detail`.
    "closed_at_exit",
    "visitor_orphan_reaped",
    # D2 adversarial-review fix: a reconciliation snapshot the OS-native
    # inspection could not read at all (four typed reasons in
    # frp_visitor_reconciliation.SNAPSHOT_SKIP_*) is surfaced here instead of
    # being silently indistinguishable from "found no orphans".
    "visitor_reconciliation_skipped",
]


class TransportModeUnavailable(RelayError):
    """A declared transport mode has no implementation in this build.

    Every mode :data:`~clio_relay.config.TransportMode` currently declares has
    an implementation (#231 R5), so nothing raises this today; it stays
    reserved for a future mode added to that type before its own
    implementation lands. It is distinct from
    :class:`TransportIdentityAnchorRequired`, which is an *implemented* mode
    refusing a specific cluster's configuration, not a missing build.
    """


class TransportIdentityAnchorRequired(RelayError):
    """A frp-based mode is implemented but this cluster has not opted into it.

    ``brokered_tcp``/``udp_rendezvous`` have no ssh-authenticated act to carry the
    bring-up identity document over, so they require a cluster to explicitly accept
    the weaker ``preshared_link_secret`` anchor (§8.3) before either mode is used --
    a cluster that has not opted in does not fall through to it unannounced.
    """


class ChannelBootstrapError(RelayError):
    """Bring-up could not obtain the exact out-of-band bootstrap document."""


class ChannelDropped(RelayError):
    """The held channel is gone and only an explicit reconnect may replace it."""


class ChannelNotEstablished(RelayError):
    """An operation was attempted before the connection held a channel."""


@dataclass(frozen=True)
class ChannelEndpoint:
    """The local address at which the held channel reaches the remote relay."""

    host: str
    port: int

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("channel endpoint host must not be empty")
        if not 1 <= self.port <= 65_535:
            raise ValueError("channel endpoint port must be a valid TCP port")

    @property
    def base_url(self) -> str:
        """Return the loopback base URL for HTTP requests over this channel."""
        return f"http://{self.host}:{self.port}"


class OwnedSessionChannelBootstrap(BaseModel):
    """Facts the transport proves once, out of band, while establishing itself.

    The owner token that signs ``identity`` is minted cluster-side and never
    leaves the cluster, so the local relay cannot compute the expected identity
    document on its own.  The transport therefore carries the document over the
    same authenticated act that establishes the channel: in ``ssh_forward`` mode
    the single SSH process that holds the forward also runs the cluster-local
    status and challenge executors and reports their exact output.  That keeps
    the identity proof anchored out of band without spending a second dial.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.channel-bootstrap.v1"] = CHANNEL_BOOTSTRAP_SCHEMA
    status: dict[str, object] = Field(default_factory=dict[str, object])
    identity: dict[str, object] = Field(default_factory=dict[str, object])


class StreamChannelsUnavailable(RelayError):
    """This transport cannot yet carry multiplexed stream channels."""


@dataclass(frozen=True)
class ChannelLink:
    """One established relay-to-relay link.

    The link, not the owned-session API, is the unit the design holds open.
    Owned-session request/response rides ``control_endpoint`` today, but the
    same link is also what live application service streams must ride: a
    compute node reaches the cluster relay over cluster-internal connectivity,
    the cluster relay carries that traffic across this link, and the local relay
    serves it -- identically in every mode, because a compute node on a real
    HPC cluster has no route to the internet and cannot dial a relay host
    itself.

    ``stream_channels`` is therefore part of the link's shape from the start.
    No mode implements it yet; :meth:`RelayTransport.open_stream_channel`
    refuses with a typed error until one does, so adding it later extends this
    interface instead of breaking it.

    ``identity_anchor`` names what proves the bring-up identity document
    authentic (§8.3). ``None`` means the ssh-authenticated bootstrap act itself
    (``ssh_forward``); ``brokered_tcp``/``udp_rendezvous`` stamp
    ``"preshared_link_secret"`` here instead.
    """

    control_endpoint: ChannelEndpoint
    bootstrap: OwnedSessionChannelBootstrap
    stream_channels: bool = False
    identity_anchor: IdentityAnchor | None = None


class ChannelEvent(BaseModel):
    """One typed, visible transport lifecycle transition.

    Every establish, drop, and re-establish is recorded here.  A transport may
    never degrade, retry, or redial without producing one of these.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.control-channel-event.v1"] = CHANNEL_EVENT_SCHEMA
    cluster: str = Field(min_length=1)
    mode: TransportMode
    event: ChannelEventName
    attempt: int = Field(ge=0)
    occurred_at: str
    reason: str | None = None
    detail: str | None = None
    user_authorization_required: bool = False
    identity_anchor: IdentityAnchor | None = None


ChannelEventSink = Callable[[ChannelEvent], None]


def channel_event(
    *,
    cluster: str,
    mode: TransportMode,
    event: ChannelEventName,
    attempt: int,
    reason: str | None = None,
    detail: str | None = None,
    user_authorization_required: bool = False,
    identity_anchor: IdentityAnchor | None = None,
) -> ChannelEvent:
    """Build one bounded transport event with a machine-readable reason."""
    return ChannelEvent(
        cluster=cluster,
        mode=mode,
        event=event,
        attempt=attempt,
        occurred_at=datetime.now(UTC).isoformat(),
        reason=reason,
        detail=None if detail is None else detail[:MAX_CHANNEL_EVENT_DETAIL_CHARS],
        user_authorization_required=user_authorization_required,
        identity_anchor=identity_anchor,
    )


class ChannelProcess(Protocol):
    """The subset of :class:`subprocess.Popen` a held channel process needs."""

    stdin: IO[bytes] | None
    stdout: IO[bytes] | None
    stderr: IO[bytes] | None

    def poll(self) -> int | None:
        """Return the exit status, or None while the process still runs."""
        ...

    def terminate(self) -> None:
        """Ask the process to stop."""
        ...

    def kill(self) -> None:
        """Stop the process without asking."""
        ...

    def wait(self, timeout: float | None = None) -> int:
        """Wait for process termination."""
        ...


ChannelProcessFactory = Callable[..., ChannelProcess]
"""The injectable dial seam.

Exactly one call to a factory of this type is exactly one new SSH connection.
Tests assert the dial-count invariant by counting calls, so no code path may
reach :mod:`subprocess` for the control plane except through a factory.
"""


def spawn_channel_process(*args: Any, **kwargs: Any) -> ChannelProcess:
    """Spawn one real channel process (the production dial)."""
    return cast(ChannelProcess, subprocess.Popen(*args, **kwargs))


class RelayTransport(Protocol):
    """One establishable relay-to-relay link.

    Implementations own exactly one underlying connection for their lifetime.
    ``establish`` may be called once per transport object; a dropped channel is
    replaced by building a new transport, never by redialing inside one.
    """

    @property
    def mode(self) -> TransportMode:
        """Return the declared transport mode."""
        ...

    @property
    def requires_user_authorization(self) -> bool:
        """Return whether establishing may block on an interactive approval."""
        ...

    def establish(self, *, nonce: str) -> ChannelLink:
        """Bring the link up once and return it."""
        ...

    def open_stream_channel(self, *, name: str, remote_port: int) -> ChannelEndpoint:
        """Map one additional stream channel onto the SAME held link.

        This is how live application service traffic will ride the one link in
        a later slice. It must never open new transport: a mode that cannot
        multiplex additional channels onto its established link refuses with
        :class:`StreamChannelsUnavailable` rather than dialing.
        """
        ...

    def is_alive(self) -> bool:
        """Return whether the link is still held."""
        ...

    def failure_detail(self) -> str | None:
        """Return bounded diagnostic output captured from a failed link."""
        ...

    def close(self) -> None:
        """Release the link."""
        ...


class SshForwardTransport:
    """Mode (c): one SSH process holding one port forward for the connection.

    The same process that holds the forward runs the cluster-local bring-up
    command, so bring-up costs exactly one SSH connection and every subsequent
    owned-session operation costs none.  The process is kept alive by an open
    stdin pipe on the remote side; closing that pipe is how the channel is torn
    down.
    """

    def __init__(
        self,
        *,
        definition: ClusterDefinition,
        session_id: str,
        session_generation_id: str,
        remote_api_port: int,
        bootstrap_script: str,
        process_factory: ChannelProcessFactory | None = None,
        local_bind_port: int | None = None,
        ready_timeout_seconds: float = DEFAULT_CHANNEL_READY_TIMEOUT_SECONDS,
        authorization_timeout_seconds: float = (DEFAULT_CHANNEL_AUTHORIZATION_TIMEOUT_SECONDS),
        allow_interactive_authorization: bool = True,
    ) -> None:
        if remote_api_port <= 0 or remote_api_port > 65_535:
            raise ValueError("remote_api_port must be a valid TCP port")
        if ready_timeout_seconds <= 0:
            raise ValueError("ready_timeout_seconds must be positive")
        if authorization_timeout_seconds <= 0:
            raise ValueError("authorization_timeout_seconds must be positive")
        self._definition = definition
        self._session_id = session_id
        self._session_generation_id = session_generation_id
        self._remote_api_port = remote_api_port
        self._bootstrap_script = bootstrap_script
        self._process_factory = process_factory or spawn_channel_process
        self._local_bind_port = local_bind_port
        self._ready_timeout_seconds = ready_timeout_seconds
        self._authorization_timeout_seconds = authorization_timeout_seconds
        self._allow_interactive_authorization = allow_interactive_authorization
        self._process: ChannelProcess | None = None
        self._stderr_buffer: BoundedStderrBuffer | None = None
        self._established = False
        self._failure_detail: str | None = None

    @property
    def mode(self) -> TransportMode:
        """Return the declared transport mode."""
        return "ssh_forward"

    @property
    def requires_user_authorization(self) -> bool:
        """Interactive two-factor approval is expected once, at bring-up."""
        return self._allow_interactive_authorization

    def argv(self, *, local_port: int) -> list[str]:
        """Render the exact SSH argument vector for the held forward."""
        options = [
            "-o",
            "ExitOnForwardFailure=yes",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=4",
        ]
        if not self._allow_interactive_authorization:
            # Only a non-interactive deployment (an automated gate) may refuse
            # the authorization prompt.  The default keeps the user's single
            # bring-up approval possible.
            options = ["-o", "BatchMode=yes", *options]
        # ssh joins its trailing operands with single spaces into ONE command
        # string for the remote login shell; local argv boundaries are not
        # preserved. The remote command must therefore arrive pre-quoted, the
        # way remote_cli.py and session_lifecycle.py already do it. Passing the
        # script as a separate argv element would strip `set -euo pipefail` of
        # its effect and hand the body to whatever login shell the account has.
        remote_command = f"bash -lc {shlex.quote(self._bootstrap_script)}"
        return [
            "ssh",
            "-T",
            *options,
            "-L",
            f"127.0.0.1:{local_port}:127.0.0.1:{self._remote_api_port}",
            self._definition.ssh_host,
            remote_command,
        ]

    def establish(self, *, nonce: str) -> ChannelLink:
        """Dial once, hold the forward, and return the established link."""
        if self._established:
            raise RelayError("ssh forward transport was already established")
        _validate_channel_nonce(nonce)
        local_port = self._local_bind_port or _available_loopback_port()
        process = self._process_factory(
            self.argv(local_port=local_port),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._process = process
        self._established = True
        if process.stderr is not None:
            self._stderr_buffer = BoundedStderrBuffer()
            # Explicit name restores this thread's pre-promotion identity
            # (pump_stderr's own default is deliberately neutral, not
            # "frp"-branded -- this is ssh_forward, not frp; #231 R4 opus
            # review F5).
            pump_stderr(
                process.stderr, self._stderr_buffer, thread_name="clio-relay-channel-stderr"
            )
        try:
            bootstrap = self._read_bootstrap(process)
            endpoint = ChannelEndpoint(host="127.0.0.1", port=local_port)
            _wait_for_channel_health(
                process,
                base_url=endpoint.base_url,
                timeout_seconds=self._ready_timeout_seconds,
            )
        except BaseException:
            self._failure_detail = self._drain_stderr()
            self.close()
            raise
        return ChannelLink(control_endpoint=endpoint, bootstrap=bootstrap)

    def open_stream_channel(self, *, name: str, remote_port: int) -> ChannelEndpoint:
        """Refuse until this mode can multiplex channels onto the held forward.

        Adding a forward to an established SSH connection needs a control
        socket on that same connection. That is multiplexing the one held link,
        not per-operation dialing -- but it is not built here, and this must
        never fall back to a second SSH connection.
        """
        raise StreamChannelsUnavailable(
            f"ssh_forward cannot yet carry the {name!r} stream channel to remote port "
            f"{remote_port}; live service streams must ride the one held link, not a new one"
        )

    def is_alive(self) -> bool:
        """Return whether the held SSH process is still running."""
        process = self._process
        return process is not None and process.poll() is None

    def failure_detail(self) -> str | None:
        """Return bounded stderr captured when the channel failed."""
        return self._failure_detail

    def close(self) -> None:
        """Close stdin so the remote holder exits, then stop the SSH process."""
        process = self._process
        self._process = None
        if process is None:
            return
        stdin = process.stdin
        if stdin is not None:
            with suppress(OSError):
                stdin.close()
        if process.poll() is None:
            with suppress(subprocess.TimeoutExpired, OSError):
                process.wait(timeout=5)
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except (subprocess.TimeoutExpired, OSError):
                process.kill()
                with suppress(subprocess.TimeoutExpired, OSError):
                    process.wait(timeout=5)
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                with suppress(OSError):
                    stream.close()

    def _read_bootstrap(self, process: ChannelProcess) -> OwnedSessionChannelBootstrap:
        """Read the bring-up document the held process emits between its markers."""
        stdout = process.stdout
        if stdout is None:
            raise ChannelBootstrapError("channel bring-up process has no readable output")
        deadline_seconds = (
            self._authorization_timeout_seconds
            if self.requires_user_authorization
            else self._ready_timeout_seconds
        )
        payload = _read_delimited_document(
            stdout,
            timeout_seconds=deadline_seconds,
            maximum_bytes=MAX_CHANNEL_BOOTSTRAP_BYTES,
        )
        if payload is None:
            detail = self._drain_stderr() or "no bring-up document was produced"
            raise ChannelBootstrapError(
                f"owned session channel bring-up did not report its bootstrap: {detail}"
            )
        try:
            document = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            detail = self._drain_stderr() or payload[:512].decode("utf-8", errors="replace")
            raise ChannelBootstrapError(
                f"owned session channel bootstrap was not UTF-8 JSON: {detail}"
            ) from exc
        try:
            return OwnedSessionChannelBootstrap.model_validate(document)
        except ValueError as exc:
            raise ChannelBootstrapError(
                f"owned session channel bootstrap is not the exact contract: {exc}"
            ) from exc

    def _drain_stderr(self) -> str | None:
        """Return the most recent bounded diagnostics the channel process wrote.

        This reads the ring buffer the stderr pump fills, never the pipe.  A
        blocking read here would hang every bring-up failure path: the held
        process is still alive, so its stderr pipe has no EOF and a fixed-size
        read would wait for bytes that never come.
        """
        recorded = self._stderr_buffer
        if recorded is None:
            return None
        return recorded.text()


def owned_session_channel_bootstrap_script(
    *,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
    session_generation_id: str,
    nonce: str,
) -> str:
    """Render the one cluster-local command that reports bring-up and holds the link.

    The command reuses the exact cluster-local executors the per-operation SSH
    scripts used to call one at a time -- ``session recovery-status`` and
    ``session challenge-owned`` -- composes their already-valid JSON into one
    line, and then blocks on stdin so the SSH session (and therefore the port
    forward) stays up until the local relay closes the pipe.
    """
    relay_executable = render_remote_shell_value(
        definition.relay_executable,
        field="relay_executable",
    )
    challenge_request = OwnedSessionIdentityChallengeRequest(
        cluster=cluster,
        session_id=session_id,
        session_generation_id=session_generation_id,
        nonce=nonce,
    ).model_dump_json()
    begin = CHANNEL_BOOTSTRAP_BEGIN.decode("ascii")
    end = CHANNEL_BOOTSTRAP_END.decode("ascii")
    return (
        "set -euo pipefail\n"
        "umask 077\n"
        f"{remote_env(definition)}\n"
        f"__clio_status=$({relay_executable} session recovery-status "
        f"--cluster {shlex.quote(cluster)} --session-id {shlex.quote(session_id)})\n"
        f"__clio_identity=$(printf '%s' {shlex.quote(challenge_request)} | "
        f"{relay_executable} session challenge-owned)\n"
        # Frame the document so a login-shell banner, or an executor that
        # pretty-prints its JSON, cannot be mistaken for the bootstrap.
        f"printf '%s\\n' {shlex.quote(begin)}\n"
        'printf \'{"schema_version":"%s","status":%s,"identity":%s}\\n\' '
        f"{shlex.quote(CHANNEL_BOOTSTRAP_SCHEMA)} "
        '"$__clio_status" "$__clio_identity"\n'
        f"printf '%s\\n' {shlex.quote(end)}\n"
        # Hold the SSH session, and therefore the port forward, until the local
        # relay closes this pipe.
        "exec cat >/dev/null\n"
    )


def build_transport(
    *,
    mode: TransportMode,
    definition: ClusterDefinition,
    session_id: str,
    session_generation_id: str,
    remote_api_port: int,
    nonce: str,
    api_token: str | None = None,
    frpc_bin: str = "frpc",
    process_factory: ChannelProcessFactory | None = None,
    local_bind_port: int | None = None,
    ready_timeout_seconds: float = DEFAULT_CHANNEL_READY_TIMEOUT_SECONDS,
    allow_interactive_authorization: bool = True,
) -> RelayTransport:
    """Build the transport for the mode this connection is configured to use.

    ``brokered_tcp`` and ``udp_rendezvous`` slot in here as sibling
    implementations of ``ssh_forward``: this function never substitutes a
    different mode, so a connection configured for a server-brokered pathway
    can never quietly be served by SSH instead.
    """
    if mode == "ssh_forward":
        return SshForwardTransport(
            definition=definition,
            session_id=session_id,
            session_generation_id=session_generation_id,
            remote_api_port=remote_api_port,
            bootstrap_script=owned_session_channel_bootstrap_script(
                definition=definition,
                cluster=definition.name,
                session_id=session_id,
                session_generation_id=session_generation_id,
                nonce=nonce,
            ),
            process_factory=process_factory,
            local_bind_port=local_bind_port,
            ready_timeout_seconds=ready_timeout_seconds,
            allow_interactive_authorization=allow_interactive_authorization,
        )
    if mode in ("brokered_tcp", "udp_rendezvous"):
        # §8.3's ruling: refuse BEFORE spawning anything unless the cluster
        # definition explicitly opted into the weaker preshared-link anchor.
        # Not silent, not defaulted -- a cluster that never set this does not
        # fall through to using these modes unannounced.
        anchor = definition.frp_transport.identity_anchor
        if anchor != "preshared_link_secret":
            raise TransportIdentityAnchorRequired(
                f"cluster {definition.name!r} is configured for transport mode {mode!r} but "
                "has not opted into an identity anchor; set frp_transport.identity_anchor to "
                '"preshared_link_secret" in this cluster\'s definition before this mode can be '
                "used -- brokered_tcp/udp_rendezvous have no ssh-authenticated act to carry the "
                "bring-up identity document over (relay-architecture-2026-08.md §8.3)"
            )
        if not api_token:
            raise ConfigurationError(
                f"relay transport mode {mode!r} requires CLIO_RELAY_API_TOKEN to authenticate "
                "the owned session bring-up fetched over the held link"
            )
        # Local import: frp_transport.py needs ChannelLink/ChannelEndpoint/
        # OwnedSessionChannelBootstrap from this module, so the reverse import at
        # module top would cycle. Deferred here, both modules are fully loaded by
        # the time this function is ever called.
        from clio_relay.frp_transport import BrokeredTcpTransport, UdpRendezvousTransport

        transport_cls = BrokeredTcpTransport if mode == "brokered_tcp" else UdpRendezvousTransport
        return transport_cls(
            definition=definition,
            cluster=definition.name,
            session_id=session_id,
            session_generation_id=session_generation_id,
            remote_api_port=remote_api_port,
            api_token=api_token,
            identity_anchor=anchor,
            frpc_bin=frpc_bin,
            process_factory=process_factory,
            local_bind_port=local_bind_port,
            ready_timeout_seconds=ready_timeout_seconds,
        )
    raise ValueError(f"unknown relay transport mode: {mode!r}")


def _read_delimited_document(
    stream: IO[bytes],
    *,
    timeout_seconds: float,
    maximum_bytes: int,
) -> bytes | None:
    """Read the marker-delimited bring-up document within a bounded deadline.

    The document is framed by explicit markers rather than assumed to be the
    first line: the cluster-local executors are free to pretty-print their JSON,
    and ``bash -lc`` runs a login shell whose profile may write a banner to
    stdout before anything of ours appears.  Everything outside the markers is
    ignored.

    A pipe cannot be polled portably, so the blocking reads run on a helper
    thread while this function owns the deadline.  The thread is a daemon and
    ends at EOF, which the caller guarantees by tearing the process down.
    """
    lines: queue.Queue[bytes | None] = queue.Queue()

    def _pump() -> None:
        try:
            for line in stream:
                lines.put(line)
        except (OSError, ValueError):
            pass
        finally:
            lines.put(None)

    reader = threading.Thread(target=_pump, name="clio-relay-channel-bootstrap", daemon=True)
    reader.start()
    deadline = time.monotonic() + timeout_seconds
    collecting = False
    collected: list[bytes] = []
    collected_bytes = 0
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        try:
            line = lines.get(timeout=min(0.25, remaining))
        except queue.Empty:
            continue
        if line is None:
            return None
        marker = line.strip()
        if not collecting:
            collecting = marker == CHANNEL_BOOTSTRAP_BEGIN
            continue
        if marker == CHANNEL_BOOTSTRAP_END:
            return b"".join(collected)
        collected_bytes += len(line)
        if collected_bytes > maximum_bytes:
            raise ChannelBootstrapError("owned session channel bootstrap exceeded its byte limit")
        collected.append(line)
