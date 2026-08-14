"""The frp process substrate shared by mode (c) and the modes (a)/(b) frp link.

``docs/design/relay-architecture-2026-08.md`` §8.2 (iowarp/clio-relay#231 R4,
tracked by iowarp/clio-relay#188 item 4) sizes this module from two duplicated
copies of "write frpc TOML, spawn ``frpc``, track it": the local-visitor half
of ``transport_probe.py``'s ``run_frp_http_probe``/
``_run_frp_http_probe_with_proxy_type`` (the remote-side SSH probe scripts
stay in that module -- only the LOCAL visitor lifecycle moves here) and the
mode-agnostic held-process primitives ``control_channel.py``'s
``SshForwardTransport`` already carries. ``service_runtime.py``'s much larger
third copy and ``transport_probe.py``'s remote-script generation are **not**
in scope here; that is issue #233's later, separate absorption.

This module owns two concerns:

``FrpLinkConfig`` / ``render_visitor_config``
    Resolve a cluster's frp link settings from ``ClusterDefinition.frp_transport``
    (``cluster_config.py``) and render the visitor TOML. The two secrets
    (frp-point token, STCP/XTCP pairing secret) are read from the cluster's
    *declared env bindings* -- ``FrpTransportConfig.token_env``
    (``CLIO_RELAY_FRP_TOKEN`` by default) and ``.stcp_secret_env``
    (``CLIO_RELAY_STCP_SECRET`` by default) -- never a literal default.
    Rendering itself delegates to ``relay_host.py``'s ``render_frpc_visitor_config``:
    that module stays the single owner of frp TOML rendering (§8.2); nothing
    here duplicates it.

``BoundedStderrBuffer`` / ``pump_stderr`` / ``wait_for_channel_health`` / ``HeldFrpVisitor``
    The held-process lifecycle primitives. ``BoundedStderrBuffer``,
    ``pump_stderr``, and ``wait_for_channel_health`` were promoted from
    ``control_channel.py`` -- they were already mode-agnostic there, so
    ``control_channel.py`` now imports them from here instead of keeping a
    second copy, and its own behavior is unchanged. ``HeldFrpVisitor`` is one
    spawned local ``frpc -c <toml>`` process holding an stcp/xtcp visitor
    tunnel, built with ``control_channel.SshForwardTransport``'s exact
    lifecycle discipline (the mode-(c) reference implementation): a bounded,
    continuously drained stderr buffer so the process never blocks writing
    diagnostics; ``poll()``-based liveness, never a blocking wait; ``close()``
    escalating terminate -> kill with timeouts; a 0600 rendered config file in
    its own temporary directory, removed on ``close()``; and a bounded stderr
    excerpt as the only failure detail exposed -- never a raw dump.

``frp_transport.py`` (R5) builds the ``brokered_tcp``/``udp_rendezvous``
``RelayTransport`` implementations on top of ``HeldFrpVisitor`` rather than
spawning ``frpc`` a fourth time. ``transport_probe.py``'s probe-only
``allow_stcp_fallback`` behavior (§8.2) is explicitly PROBE-ONLY and must
never be reachable from this module.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
import threading
import time
from collections import deque
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, Final, Literal, Protocol, cast

import httpx

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.relay_host import (
    FrpcVisitorConfig,
    FrpTransportProtocol,
    render_frpc_visitor_config,
)

DEFAULT_STDERR_BUFFER_MAX_BYTES: Final = 2_000
DEFAULT_FRP_VISITOR_HEALTH_TIMEOUT_SECONDS: Final = 30.0

FrpVisitorType = Literal["stcp", "xtcp"]


@dataclass(frozen=True)
class FrpLinkConfig:
    """One cluster's resolved frp link settings (never a literal secret).

    Build this either directly (callers that already resolved ``token`` and
    ``secret_key`` themselves, e.g. ``transport_probe.py``'s existing probe
    entry points) or via :meth:`from_cluster`, which reads both secrets from
    the cluster's declared environment bindings.
    """

    server_addr: str
    server_port: int
    protocol: FrpTransportProtocol
    token: str
    secret_key: str
    proxy_name: str

    @classmethod
    def from_cluster(
        cls,
        definition: ClusterDefinition,
        *,
        cluster: str,
        proxy_name: str,
        env: Mapping[str, str] | None = None,
    ) -> FrpLinkConfig:
        """Resolve server/protocol/token/secret from ``definition.frp_transport``.

        ``token``/``secret_key`` are read from ``env`` (``os.environ`` by
        default) at the names the cluster declares --
        ``frp_transport.token_env``/``frp_transport.stcp_secret_env`` -- and
        never default to a literal: a cluster whose declared env binding is
        unset fails loudly rather than resolving to an empty or hardcoded
        secret.

        Args:
            definition: The cluster's configuration.
            cluster: The cluster's logical name, used only in error text.
            proxy_name: The frp proxy/visitor name pair to use for this link.
            env: The environment mapping to resolve bindings from. Defaults
                to ``os.environ``.

        Returns:
            The resolved link configuration.

        Raises:
            ConfigurationError: The server address is blank, or either
                declared env binding is unset.
        """
        transport = definition.frp_transport
        environment = env if env is not None else os.environ
        server_addr = require_frp_server_addr(transport.server_addr, cluster)
        token = _require_env_binding(environment, transport.token_env, purpose="frp token")
        secret_key = _require_env_binding(
            environment,
            transport.stcp_secret_env,
            purpose="stcp/xtcp pairing secret",
        )
        return cls(
            server_addr=server_addr,
            server_port=transport.server_port,
            protocol=FrpTransportProtocol(transport.protocol),
            token=token,
            secret_key=secret_key,
            proxy_name=proxy_name,
        )


def require_frp_server_addr(server_addr: str, cluster: str) -> str:
    """Return the configured frp server address, or raise a typed refusal."""
    if server_addr.strip():
        return server_addr
    raise ConfigurationError(
        f"frp server address is not configured for cluster {cluster}; "
        "set it with `clio-relay cluster add --frp-server-addr ...`"
    )


def _require_env_binding(environment: Mapping[str, str], env_name: str, *, purpose: str) -> str:
    """Return the declared env binding's value, or raise a typed refusal.

    No silent fallback: an unset binding is a configuration error naming the
    exact env var the cluster declares, never a literal or an empty secret.
    """
    value = environment.get(env_name)
    if value:
        return value
    raise ConfigurationError(
        f"{purpose} is not set; the cluster declares it must come from the "
        f"{env_name} environment variable, never a literal default"
    )


def render_visitor_config(
    config: FrpLinkConfig,
    *,
    local_bind_port: int,
    visitor_type: FrpVisitorType = "stcp",
    keep_tunnel_open: bool = False,
) -> str:
    """Render the frpc visitor TOML for one held link.

    Delegates to ``relay_host.py``'s ``render_frpc_visitor_config`` -- that
    module stays the single owner of frp TOML rendering (§8.2); this is not a
    fourth copy of the logic.
    """
    if local_bind_port <= 0:
        raise ConfigurationError("local_bind_port must be positive")
    return render_frpc_visitor_config(
        FrpcVisitorConfig(
            server_addr=config.server_addr,
            server_port=config.server_port,
            token=config.token,
            transport_protocol=config.protocol,
            visitor_name=f"{config.proxy_name}-visitor",
            visitor_type=visitor_type,
            server_name=config.proxy_name,
            bind_port=local_bind_port,
            secret_key=config.secret_key,
            keep_tunnel_open=keep_tunnel_open,
        )
    )


class BoundedStderrBuffer:
    """A drained, bounded record of what a held process wrote to stderr.

    Promoted from ``control_channel.py`` (mode-agnostic there already): a
    long-held process's stderr pipe must be read continuously or it fills and
    the process blocks writing to it, with no error and no event. This is
    reached in ordinary operation -- ``ssh -L`` writes one line per refused
    forwarded connection, and ``frpc`` logs connection churn to stderr -- and
    the pipe buffer is only about 4 KiB on Windows.
    """

    def __init__(self, *, maximum_bytes: int = DEFAULT_STDERR_BUFFER_MAX_BYTES) -> None:
        if maximum_bytes <= 0:
            raise ValueError("stderr buffer maximum_bytes must be positive")
        self._maximum_bytes = maximum_bytes
        self._lock = threading.Lock()
        self._chunks: deque[bytes] = deque()
        self._size = 0

    def append(self, payload: bytes) -> None:
        """Record one chunk, discarding the oldest to stay inside the bound."""
        with self._lock:
            self._chunks.append(payload)
            self._size += len(payload)
            while self._size > self._maximum_bytes and len(self._chunks) > 1:
                self._size -= len(self._chunks.popleft())

    def text(self) -> str | None:
        """Return the retained diagnostics, or None when nothing was written."""
        with self._lock:
            joined = b"".join(self._chunks)
        detail = joined.decode("utf-8", errors="replace").strip()
        return detail[-self._maximum_bytes :] or None


def pump_stderr(stream: IO[bytes], buffer: BoundedStderrBuffer) -> threading.Thread:
    """Continuously drain one process's stderr into a bounded buffer."""

    def _pump() -> None:
        try:
            for line in stream:
                buffer.append(line)
        except (OSError, ValueError):
            pass

    thread = threading.Thread(target=_pump, name="clio-relay-frp-stderr", daemon=True)
    thread.start()
    return thread


class _HealthPollable(Protocol):
    """The minimum surface :func:`wait_for_channel_health` needs from a process."""

    def poll(self) -> int | None:
        """Return the exit status, or None while the process still runs."""
        ...


def wait_for_channel_health(
    process: _HealthPollable,
    *,
    base_url: str,
    timeout_seconds: float,
) -> None:
    """Wait for the mapped port to answer without opening any new transport.

    Mode-agnostic, promoted from ``control_channel.py``: the ``ssh_forward``
    control channel (``SshForwardTransport.establish``) and a held frp
    visitor (:class:`HeldFrpVisitor`, below) both use this identically to
    verify their held link is ready.
    """
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


class FrpProcess(Protocol):
    """The subset of :class:`subprocess.Popen` a held frp visitor process needs."""

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


FrpProcessFactory = Callable[..., FrpProcess]
"""The injectable dial seam for a held frp visitor process.

Mirrors ``control_channel.py``'s ``ChannelProcessFactory``: tests inject a
fake factory rather than monkeypatching :mod:`subprocess` directly.
"""


def spawn_frp_process(*args: Any, **kwargs: Any) -> FrpProcess:
    """Spawn one real ``frpc`` process (the production dial)."""
    return cast(FrpProcess, subprocess.Popen(*args, **kwargs))


class HeldFrpVisitor:
    """One spawned local ``frpc -c <toml>`` process holding a visitor tunnel.

    The substrate ``frp_transport.py`` (R5) builds the ``brokered_tcp``/
    ``udp_rendezvous`` :class:`~clio_relay.control_channel.RelayTransport`
    implementations on, and what ``transport_probe.py``'s local-visitor probe
    logic delegates to today. Lifecycle discipline mirrors
    ``SshForwardTransport`` (``control_channel.py``): a continuously drained,
    bounded stderr buffer so the process never blocks writing diagnostics;
    ``poll()``-based liveness, never a blocking wait; ``close()`` escalating
    terminate -> kill with timeouts; a 0600 rendered config file in its own
    temporary directory, removed on ``close()``; and a bounded stderr excerpt
    as the only failure detail exposed -- never a raw dump.
    """

    def __init__(
        self,
        *,
        frpc_bin: str,
        config: FrpLinkConfig,
        local_bind_port: int,
        visitor_type: FrpVisitorType = "stcp",
        keep_tunnel_open: bool = False,
        process_factory: FrpProcessFactory | None = None,
    ) -> None:
        if local_bind_port <= 0:
            raise ConfigurationError("local_bind_port must be positive")
        self._frpc_bin = frpc_bin
        self._config = config
        self._local_bind_port = local_bind_port
        self._visitor_type: FrpVisitorType = visitor_type
        self._keep_tunnel_open = keep_tunnel_open
        self._process_factory = process_factory or spawn_frp_process
        self._process: FrpProcess | None = None
        self._stderr_buffer: BoundedStderrBuffer | None = None
        self._stderr_thread: threading.Thread | None = None
        self._temp_dir: tempfile.TemporaryDirectory[str] | None = None
        self._config_path: Path | None = None
        self._established = False

    @property
    def base_url(self) -> str:
        """Return the loopback base URL the visitor maps traffic onto."""
        return f"http://127.0.0.1:{self._local_bind_port}"

    @property
    def config_path(self) -> Path | None:
        """Return the rendered visitor config path, once established."""
        return self._config_path

    def establish(self) -> None:
        """Write the visitor TOML to a 0600 temp file and spawn ``frpc -c <toml>``."""
        if self._established:
            raise RelayError("frp visitor was already established")
        temp_dir = tempfile.TemporaryDirectory(prefix="clio-relay-frp-visitor-")
        try:
            config_path = Path(temp_dir.name) / "frpc-visitor.toml"
            rendered = render_visitor_config(
                self._config,
                local_bind_port=self._local_bind_port,
                visitor_type=self._visitor_type,
                keep_tunnel_open=self._keep_tunnel_open,
            )
            config_path.write_text(rendered, encoding="utf-8")
            config_path.chmod(0o600)
            process = self._process_factory(
                [self._frpc_bin, "-c", str(config_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except BaseException:
            with suppress(OSError):
                temp_dir.cleanup()
            raise
        self._temp_dir = temp_dir
        self._config_path = config_path
        self._process = process
        self._established = True
        if process.stderr is not None:
            self._stderr_buffer = BoundedStderrBuffer()
            self._stderr_thread = pump_stderr(process.stderr, self._stderr_buffer)

    def wait_healthy(
        self,
        *,
        timeout_seconds: float = DEFAULT_FRP_VISITOR_HEALTH_TIMEOUT_SECONDS,
    ) -> None:
        """Wait for this visitor's mapped port to answer, using the shared health-wait."""
        process = self._process
        if process is None:
            raise RelayError("frp visitor has not been established")
        wait_for_channel_health(process, base_url=self.base_url, timeout_seconds=timeout_seconds)

    def is_alive(self) -> bool:
        """Return whether the held frpc visitor process is still running."""
        process = self._process
        return process is not None and process.poll() is None

    def failure_detail(self) -> str | None:
        """Return bounded stderr captured from the visitor -- never a raw dump.

        Joins the stderr-pump thread with a short bound first: once the
        process is confirmed dead its remaining stderr is finite and the pump
        drains it almost immediately, so this is safe without risking a hang
        on a still-live process (the join simply times out).
        """
        thread = self._stderr_thread
        if thread is not None:
            thread.join(timeout=2.0)
        buffer = self._stderr_buffer
        if buffer is None:
            return None
        return buffer.text()

    def close(self) -> None:
        """Stop the visitor process (terminate -> kill escalation) and its config.

        Deliberately does not drop the process reference: unlike
        ``SshForwardTransport`` (which never needs to inspect a closed
        channel again), callers here -- ``_finish_frp_probe_cleanup`` in
        particular -- verify teardown by calling :meth:`is_alive` again after
        ``close()``, so the process handle stays queryable.
        """
        process = self._process
        if process is not None:
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
        temp_dir = self._temp_dir
        self._temp_dir = None
        self._config_path = None
        if temp_dir is not None:
            with suppress(OSError):
                temp_dir.cleanup()
