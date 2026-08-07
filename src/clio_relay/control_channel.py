"""One held relay-to-relay channel per remote connection.

The relay transport design is a single persistent link between the local relay
process and one remote relay process.  The link is established once, at
connection bring-up, and every owned-session operation -- status, identity
challenge, job submission, ingest, artifact content, watch -- rides it as plain
HTTP against the mapped port.  Nothing in this module may reopen the underlying
transport for an individual operation.

Three transport modes are declared by the design:

``brokered_tcp``
    TCP through an internet-accessible relay point.  Both relays dial out and a
    server-brokered handshake joins the two outbound connections.
``udp_rendezvous``
    The same rendezvous with a UDP hole-punching handshake, falling back to the
    server-carried TCP path when traversal fails.
``ssh_forward``
    The fallback for infrastructure that permits nothing else: one SSH process
    holding one port forward for the lifetime of the connection.

Only ``ssh_forward`` is implemented here.  The other two modes are declared and
refused with a typed error so that a missing mode is visible rather than
silently degraded into per-operation SSH.
"""

from __future__ import annotations

import json
import queue
import shlex
import socket
import subprocess
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import IO, Any, Final, Literal, Protocol, cast

import httpx
from pydantic import BaseModel, ConfigDict, Field

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.remote_cli import remote_env
from clio_relay.remote_values import render_remote_shell_value
from clio_relay.session_lifecycle import OwnedSessionIdentityChallengeRequest

TransportMode = Literal["brokered_tcp", "udp_rendezvous", "ssh_forward"]

CHANNEL_BOOTSTRAP_SCHEMA: Final = "clio-relay.channel-bootstrap.v1"
CHANNEL_EVENT_SCHEMA: Final = "clio-relay.control-channel-event.v1"

MAX_CHANNEL_BOOTSTRAP_BYTES: Final = 256 * 1024
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
]


class TransportModeUnavailable(RelayError):
    """A declared transport mode has no implementation in this build."""


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

    def establish(self, *, nonce: str) -> tuple[ChannelEndpoint, OwnedSessionChannelBootstrap]:
        """Bring the link up once and return its endpoint and bootstrap facts."""
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
        return [
            "ssh",
            "-T",
            *options,
            "-L",
            f"127.0.0.1:{local_port}:127.0.0.1:{self._remote_api_port}",
            self._definition.ssh_host,
            "bash",
            "-lc",
            self._bootstrap_script,
        ]

    def establish(self, *, nonce: str) -> tuple[ChannelEndpoint, OwnedSessionChannelBootstrap]:
        """Dial once, hold the forward, and return the bring-up bootstrap."""
        if self._established:
            raise RelayError("ssh forward transport was already established")
        if len(nonce) != 64 or any(character not in "0123456789abcdef" for character in nonce):
            raise ValueError("channel bootstrap nonce must be a lowercase 256-bit hex value")
        local_port = self._local_bind_port or _available_loopback_port()
        process = self._process_factory(
            self.argv(local_port=local_port),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._process = process
        self._established = True
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
        return endpoint, bootstrap

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

    def _read_bootstrap(self, process: ChannelProcess) -> OwnedSessionChannelBootstrap:
        """Read the single bring-up document the held process prints first."""
        stdout = process.stdout
        if stdout is None:
            raise ChannelBootstrapError("channel bring-up process has no readable output")
        deadline_seconds = (
            self._authorization_timeout_seconds
            if self.requires_user_authorization
            else self._ready_timeout_seconds
        )
        line = _read_line_with_deadline(
            stdout,
            timeout_seconds=deadline_seconds,
            is_running=lambda: process.poll() is None,
        )
        if line is None:
            detail = self._drain_stderr() or "no bring-up document was produced"
            raise ChannelBootstrapError(
                f"owned session channel bring-up did not report its bootstrap: {detail}"
            )
        if len(line) > MAX_CHANNEL_BOOTSTRAP_BYTES:
            raise ChannelBootstrapError("owned session channel bootstrap exceeded its byte limit")
        try:
            document = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            detail = self._drain_stderr() or line[:512].decode("utf-8", errors="replace")
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
        """Return bounded stderr from the channel process without blocking forever."""
        process = self._process
        if process is None or process.stderr is None:
            return None
        try:
            payload = process.stderr.read(MAX_CHANNEL_EVENT_DETAIL_CHARS)
        except (OSError, ValueError):
            return None
        if not payload:
            return None
        return payload.decode("utf-8", errors="replace").strip() or None


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
    return (
        "set -euo pipefail\n"
        "umask 077\n"
        f"{remote_env(definition)}\n"
        f"__clio_status=$({relay_executable} session recovery-status "
        f"--cluster {shlex.quote(cluster)} --session-id {shlex.quote(session_id)})\n"
        f"__clio_identity=$(printf '%s' {shlex.quote(challenge_request)} | "
        f"{relay_executable} session challenge-owned)\n"
        'printf \'{"schema_version":"%s","status":%s,"identity":%s}\\n\' '
        f"{shlex.quote(CHANNEL_BOOTSTRAP_SCHEMA)} "
        '"$__clio_status" "$__clio_identity"\n'
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
    process_factory: ChannelProcessFactory | None = None,
    local_bind_port: int | None = None,
    ready_timeout_seconds: float = DEFAULT_CHANNEL_READY_TIMEOUT_SECONDS,
    allow_interactive_authorization: bool = True,
) -> RelayTransport:
    """Build the transport for one declared mode.

    ``brokered_tcp`` and ``udp_rendezvous`` are part of the design and slot in
    here as sibling implementations.  Until they exist this refuses with a typed
    error rather than falling back, so an unimplemented mode can never quietly
    become per-operation SSH.
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
        raise TransportModeUnavailable(
            f"relay transport mode {mode!r} is declared by the design but not implemented in "
            "this build; configure the ssh_forward fallback explicitly instead of falling back"
        )
    raise ValueError(f"unknown relay transport mode: {mode!r}")


def _available_loopback_port() -> int:
    """Select an unused loopback port for the local end of the forward."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    if not isinstance(port, int) or port <= 0:
        raise RelayError("could not select a loopback port for the owned session channel")
    return port


def _read_line_with_deadline(
    stream: IO[bytes],
    *,
    timeout_seconds: float,
    is_running: Callable[[], bool],
) -> bytes | None:
    """Read one line within a bounded deadline without blocking the caller forever.

    A pipe cannot be polled portably, so the blocking read runs on a helper
    thread while this function owns the deadline.  The thread is a daemon: if
    the deadline expires the caller tears the process down and the read fails.
    """
    results: queue.Queue[bytes | None] = queue.Queue(maxsize=1)

    def _read() -> None:
        try:
            results.put(stream.readline())
        except (OSError, ValueError):
            results.put(None)

    reader = threading.Thread(target=_read, name="clio-relay-channel-bootstrap", daemon=True)
    reader.start()
    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        try:
            line = results.get(timeout=min(0.25, remaining))
        except queue.Empty:
            if not is_running():
                # The process exited; give the reader one bounded chance to
                # surface whatever it had already buffered.
                try:
                    return results.get(timeout=0.5)
                except queue.Empty:
                    return None

            continue
        if line is None or not line.strip():
            return None
        return line.strip()


def _wait_for_channel_health(
    process: ChannelProcess,
    *,
    base_url: str,
    timeout_seconds: float,
) -> None:
    """Wait for the mapped port to answer without opening any new transport."""
    deadline = time.monotonic() + timeout_seconds
    last_error = "channel forward did not become ready"
    with httpx.Client(trust_env=False) as client:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RelayError(f"owned session channel exited during bring-up: {last_error}")
            try:
                response = client.get(base_url + "/healthz", timeout=min(0.5, timeout_seconds))
                if response.status_code == 200 and response.json().get("ok") is True:
                    return
                last_error = f"unexpected health response: HTTP {response.status_code}"
            except (httpx.HTTPError, TypeError, ValueError) as exc:
                last_error = str(exc)
            time.sleep(0.05)
    raise RelayError(f"owned session channel did not become ready: {last_error}")
