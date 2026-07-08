"""End-to-end frp transport probes for relay HTTP surfaces."""

from __future__ import annotations

import os
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.relay_host import (
    FrpcConfig,
    FrpcVisitorConfig,
    FrpTransportProtocol,
    render_frpc_config,
    render_frpc_visitor_config,
)
from clio_relay.session_lifecycle import start_remote_session, teardown_remote_session


class ManagedProcess(Protocol):
    """Subset of subprocess.Popen used by the transport probe."""

    stdin: Any | None

    def poll(self) -> int | None:
        """Return process status."""
        ...

    def terminate(self) -> None:
        """Terminate the process."""
        ...

    def kill(self) -> None:
        """Kill the process."""
        ...

    def wait(self, timeout: float | None = None) -> int:
        """Wait for process termination."""
        ...


ProcessFactory = Callable[..., ManagedProcess]
HttpCheck = Callable[[str], list[str]]


def run_frp_http_probe(
    *,
    cluster: str,
    definition: ClusterDefinition,
    frpc_bin: str,
    token: str,
    secret_key: str,
    local_bind_port: int,
    remote_api_port: int = 8765,
    proxy_name: str = "relay-http",
    api_token: str | None = None,
    timeout_seconds: float = 30.0,
    process_factory: ProcessFactory | None = None,
    http_check: HttpCheck | None = None,
) -> list[str]:
    """Probe desktop-to-cluster HTTP reachability through frp STCP."""
    if local_bind_port <= 0:
        raise ConfigurationError("local_bind_port must be positive")
    if remote_api_port <= 0:
        raise ConfigurationError("remote_api_port must be positive")
    if timeout_seconds <= 0:
        raise ConfigurationError("timeout_seconds must be positive")
    _assert_local_bind_port_available(local_bind_port)
    factory = process_factory or _popen
    transport = definition.frp_transport
    server_addr = _require_frp_server_addr(transport.server_addr, cluster)
    protocol = FrpTransportProtocol(transport.protocol)
    with tempfile.TemporaryDirectory(prefix="clio-relay-transport-") as temp_dir:
        temp_path = Path(temp_dir)
        remote_frpc_config = render_frpc_config(
            FrpcConfig(
                server_addr=server_addr,
                server_port=transport.server_port,
                token=token,
                transport_protocol=protocol,
                proxy_name=proxy_name,
                local_port=remote_api_port,
                secret_key=secret_key,
            )
        )
        visitor_config_path = temp_path / "frpc-visitor.toml"
        visitor_config_path.write_text(
            render_frpc_visitor_config(
                FrpcVisitorConfig(
                    server_addr=server_addr,
                    server_port=transport.server_port,
                    token=token,
                    transport_protocol=protocol,
                    visitor_name=f"{proxy_name}-visitor",
                    server_name=proxy_name,
                    bind_port=local_bind_port,
                    secret_key=secret_key,
                )
            ),
            encoding="utf-8",
        )
        remote = factory(
            ["ssh", definition.ssh_host, "bash", "-s"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert remote.stdin is not None
        remote.stdin.write(
            _remote_probe_script(
                cluster=cluster,
                definition=definition,
                api_token=api_token,
                api_port=remote_api_port,
                frpc_config=remote_frpc_config,
            ).encode("utf-8")
        )
        remote.stdin.close()
        visitor = factory(
            [frpc_bin, "-c", str(visitor_config_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            time.sleep(1)
            if remote.poll() is not None:
                raise RelayError(_process_output_message(remote, "remote transport probe failed"))
            if visitor.poll() is not None:
                raise RelayError(_process_output_message(visitor, "local frpc visitor failed"))
            try:
                _wait_for_healthz(
                    f"http://127.0.0.1:{local_bind_port}/healthz",
                    timeout_seconds=timeout_seconds,
                )
            except RelayError as exc:
                _terminate(visitor)
                _terminate(remote)
                details = [
                    str(exc),
                    _process_output_message(remote, "remote transport probe still running"),
                    _process_output_message(visitor, "local frpc visitor still running"),
                ]
                raise RelayError("\n".join(details)) from exc
            if visitor.poll() is not None:
                raise RelayError(_process_output_message(visitor, "local frpc visitor failed"))
            lines = [
                f"transport.cluster={cluster}",
                f"transport.server={server_addr}:{transport.server_port}",
                f"transport.protocol={transport.protocol}",
                f"transport.local_url=http://127.0.0.1:{local_bind_port}",
                "transport.healthz=ok",
            ]
            if http_check is not None:
                lines.extend(http_check(f"http://127.0.0.1:{local_bind_port}"))
            return lines
        finally:
            _terminate(visitor)
            _terminate(remote)


def run_frp_direct_http_probe(
    *,
    cluster: str,
    definition: ClusterDefinition,
    frpc_bin: str,
    token: str,
    secret_key: str,
    local_bind_port: int,
    remote_api_port: int = 8765,
    proxy_name: str = "relay-http-direct",
    api_token: str | None = None,
    timeout_seconds: float = 30.0,
    process_factory: ProcessFactory | None = None,
    http_check: HttpCheck | None = None,
    allow_stcp_fallback: bool = True,
) -> list[str]:
    """Probe direct XTCP HTTP reachability, optionally falling back to STCP."""
    try:
        lines = _run_frp_http_probe_with_proxy_type(
            cluster=cluster,
            definition=definition,
            frpc_bin=frpc_bin,
            token=token,
            secret_key=secret_key,
            local_bind_port=local_bind_port,
            remote_api_port=remote_api_port,
            proxy_name=proxy_name,
            api_token=api_token,
            timeout_seconds=timeout_seconds,
            process_factory=process_factory,
            http_check=http_check,
            proxy_type="xtcp",
        )
    except RelayError as exc:
        if not allow_stcp_fallback:
            raise
        fallback_lines = run_frp_http_probe(
            cluster=cluster,
            definition=definition,
            frpc_bin=frpc_bin,
            token=token,
            secret_key=secret_key,
            local_bind_port=local_bind_port,
            remote_api_port=remote_api_port,
            proxy_name=f"{proxy_name}-fallback",
            api_token=api_token,
            timeout_seconds=timeout_seconds,
            process_factory=process_factory,
            http_check=http_check,
        )
        return [
            f"direct_transport.cluster={cluster}",
            "direct_transport.mode=xtcp",
            "direct_transport.result=frp_stcp",
            f"direct_transport.xtcp_error={str(exc).splitlines()[0]}",
            *fallback_lines,
        ]
    return [
        f"direct_transport.cluster={cluster}",
        "direct_transport.mode=xtcp",
        "direct_transport.result=xtcp",
        *lines,
    ]


def run_ssh_forward_http_probe(
    *,
    cluster: str,
    definition: ClusterDefinition,
    local_bind_port: int,
    remote_api_port: int = 8765,
    session_id: str = "relay-ssh-forward",
    api_token: str | None = None,
    timeout_seconds: float = 30.0,
    process_factory: ProcessFactory | None = None,
    http_check: HttpCheck | None = None,
    detach_remote: bool = False,
    replace_remote: bool = True,
) -> list[str]:
    """Probe desktop-to-cluster HTTP reachability through SSH port forwarding."""
    if local_bind_port <= 0:
        raise ConfigurationError("local_bind_port must be positive")
    if remote_api_port <= 0:
        raise ConfigurationError("remote_api_port must be positive")
    if timeout_seconds <= 0:
        raise ConfigurationError("timeout_seconds must be positive")
    _assert_local_bind_port_available(local_bind_port)
    start_lines = start_remote_session(
        cluster=cluster,
        definition=definition,
        session_id=session_id,
        remote_api_port=remote_api_port,
        api_token=api_token,
        replace=replace_remote,
    )
    factory = process_factory or _popen
    forward = factory(
        [
            "ssh",
            "-N",
            "-L",
            f"127.0.0.1:{local_bind_port}:127.0.0.1:{remote_api_port}",
            definition.ssh_host,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        time.sleep(1)
        if forward.poll() is not None:
            raise RelayError(_process_output_message(forward, "local ssh forward failed"))
        try:
            _wait_for_healthz(
                f"http://127.0.0.1:{local_bind_port}/healthz",
                timeout_seconds=timeout_seconds,
            )
        except RelayError as exc:
            _terminate(forward)
            details = [
                str(exc),
                _process_output_message(forward, "local ssh forward still running"),
            ]
            raise RelayError("\n".join(details)) from exc
        if forward.poll() is not None:
            raise RelayError(_process_output_message(forward, "local ssh forward failed"))
        lines = [
            f"transport.cluster={cluster}",
            "transport.protocol=ssh_forward",
            f"transport.ssh_host={definition.ssh_host}",
            f"transport.session_id={session_id}",
            f"transport.remote_api_port={remote_api_port}",
            f"transport.local_url=http://127.0.0.1:{local_bind_port}",
            "transport.healthz=ok",
            *start_lines,
        ]
        if http_check is not None:
            lines.extend(http_check(f"http://127.0.0.1:{local_bind_port}"))
        return lines
    finally:
        _terminate(forward)
        if not detach_remote:
            teardown_remote_session(definition=definition, session_id=session_id)


def _run_frp_http_probe_with_proxy_type(
    *,
    cluster: str,
    definition: ClusterDefinition,
    frpc_bin: str,
    token: str,
    secret_key: str,
    local_bind_port: int,
    remote_api_port: int,
    proxy_name: str,
    api_token: str | None,
    timeout_seconds: float,
    process_factory: ProcessFactory | None,
    http_check: HttpCheck | None,
    proxy_type: str,
) -> list[str]:
    if local_bind_port <= 0:
        raise ConfigurationError("local_bind_port must be positive")
    if remote_api_port <= 0:
        raise ConfigurationError("remote_api_port must be positive")
    if timeout_seconds <= 0:
        raise ConfigurationError("timeout_seconds must be positive")
    if proxy_type not in {"stcp", "xtcp"}:
        raise ConfigurationError(f"unsupported transport proxy type: {proxy_type}")
    _assert_local_bind_port_available(local_bind_port)
    factory = process_factory or _popen
    transport = definition.frp_transport
    server_addr = _require_frp_server_addr(transport.server_addr, cluster)
    protocol = FrpTransportProtocol(transport.protocol)
    with tempfile.TemporaryDirectory(prefix="clio-relay-transport-") as temp_dir:
        temp_path = Path(temp_dir)
        remote_frpc_config = render_frpc_config(
            FrpcConfig(
                server_addr=server_addr,
                server_port=transport.server_port,
                token=token,
                transport_protocol=protocol,
                proxy_name=proxy_name,
                proxy_type=proxy_type,
                local_port=remote_api_port,
                secret_key=secret_key,
            )
        )
        visitor_config_path = temp_path / "frpc-visitor.toml"
        visitor_config_path.write_text(
            render_frpc_visitor_config(
                FrpcVisitorConfig(
                    server_addr=server_addr,
                    server_port=transport.server_port,
                    token=token,
                    transport_protocol=protocol,
                    visitor_name=f"{proxy_name}-visitor",
                    visitor_type=proxy_type,
                    server_name=proxy_name,
                    bind_port=local_bind_port,
                    secret_key=secret_key,
                    keep_tunnel_open=proxy_type == "xtcp",
                )
            ),
            encoding="utf-8",
        )
        remote = factory(
            ["ssh", definition.ssh_host, "bash", "-s"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert remote.stdin is not None
        remote.stdin.write(
            _remote_probe_script(
                cluster=cluster,
                definition=definition,
                api_token=api_token,
                api_port=remote_api_port,
                frpc_config=remote_frpc_config,
            ).encode("utf-8")
        )
        remote.stdin.close()
        visitor = factory(
            [frpc_bin, "-c", str(visitor_config_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            time.sleep(1)
            if remote.poll() is not None:
                raise RelayError(_process_output_message(remote, "remote transport probe failed"))
            if visitor.poll() is not None:
                raise RelayError(_process_output_message(visitor, "local frpc visitor failed"))
            try:
                _wait_for_healthz(
                    f"http://127.0.0.1:{local_bind_port}/healthz",
                    timeout_seconds=timeout_seconds,
                )
            except RelayError as exc:
                _terminate(visitor)
                _terminate(remote)
                details = [
                    str(exc),
                    _process_output_message(remote, "remote transport probe still running"),
                    _process_output_message(visitor, "local frpc visitor still running"),
                ]
                raise RelayError("\n".join(details)) from exc
            if visitor.poll() is not None:
                raise RelayError(_process_output_message(visitor, "local frpc visitor failed"))
            lines = [
                f"transport.cluster={cluster}",
                f"transport.server={server_addr}:{transport.server_port}",
                f"transport.protocol={transport.protocol}",
                f"transport.proxy_type={proxy_type}",
                f"transport.local_url=http://127.0.0.1:{local_bind_port}",
                "transport.healthz=ok",
            ]
            if http_check is not None:
                lines.extend(http_check(f"http://127.0.0.1:{local_bind_port}"))
            return lines
        finally:
            _terminate(visitor)
            _terminate(remote)


def _remote_probe_script(
    *,
    cluster: str,
    definition: ClusterDefinition,
    api_token: str | None,
    api_port: int,
    frpc_config: str,
) -> str:
    token_export = ""
    require_token = ""
    if api_token is not None:
        token_export = f"export CLIO_RELAY_API_TOKEN={_shell_single_quote(api_token)}"
        require_token = " --require-token"
    jarvis_bin = definition.jarvis_bin or "$HOME/.local/bin/jarvis"
    frpc_bin = definition.frpc_bin or "$HOME/.local/bin/frpc"
    agent_bin = _cluster_agent_bin(definition)
    return f"""set -euo pipefail
export PATH="$HOME/.local/bin:$PATH"
export CLIO_RELAY_CORE_DIR={_shell_double_quote(definition.core_dir)}
export CLIO_RELAY_SPOOL_DIR={_shell_double_quote(definition.spool_dir)}
export CLIO_RELAY_JARVIS_BIN={_shell_double_quote(jarvis_bin)}
export CLIO_RELAY_FRPC_BIN={_shell_double_quote(frpc_bin)}
export CLIO_RELAY_AGENT_BIN={_shell_double_quote(agent_bin)}
export CLIO_RELAY_AGENT_ADAPTER={_shell_single_quote(definition.agent_adapter)}
{token_export}
tmp="$(mktemp -d)"
api_pid=""
frpc_pid=""
cleanup() {{
  if [ -n "$frpc_pid" ]; then kill "$frpc_pid" 2>/dev/null || true; fi
  if [ -n "$api_pid" ]; then kill "$api_pid" 2>/dev/null || true; fi
  wait 2>/dev/null || true
  rm -rf "$tmp"
}}
trap cleanup EXIT
cat > "$tmp/frpc.toml" <<'__CLIO_RELAY_FRPC_CONFIG__'
{frpc_config.rstrip()}
__CLIO_RELAY_FRPC_CONFIG__
echo "transport_probe_cluster={cluster}"
if python3 - {api_port} <<'__CLIO_RELAY_PORT_CHECK__'
import socket
import sys
port = int(sys.argv[1])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("127.0.0.1", port))
    except OSError:
        raise SystemExit(1)
__CLIO_RELAY_PORT_CHECK__
then
  :
else
  echo "remote API port is already occupied: {api_port}" >&2
  exit 1
fi
clio-relay api start --host 127.0.0.1 --port {api_port}{require_token} >"$tmp/api.log" 2>&1 &
api_pid="$!"
sleep 1
if ! kill -0 "$api_pid" 2>/dev/null; then
  cat "$tmp/api.log" >&2
  exit 1
fi
"$CLIO_RELAY_FRPC_BIN" -c "$tmp/frpc.toml" >"$tmp/frpc.log" 2>&1 &
frpc_pid="$!"
wait
"""


def _wait_for_healthz(url: str, *, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: str | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return
                last_error = f"status={response.status}"
        except (OSError, urllib.error.URLError) as exc:
            last_error = str(exc)
        time.sleep(0.5)
    raise RelayError(f"transport health check failed for {url}: {last_error}")


def _assert_local_bind_port_available(port: int) -> None:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("127.0.0.1", port))
    except OSError as exc:
        raise ConfigurationError(f"local visitor port is already occupied: {port}") from exc


def _require_frp_server_addr(server_addr: str, cluster: str) -> str:
    if server_addr.strip():
        return server_addr
    raise ConfigurationError(
        f"frp server address is not configured for cluster {cluster}; "
        "set it with `clio-relay cluster add --frp-server-addr ...`"
    )


def _terminate(process: ManagedProcess) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        process.terminate()
    else:
        process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _process_output_message(process: ManagedProcess, fallback: str) -> str:
    parts: list[str] = []
    for stream_name in ("stdout", "stderr"):
        stream = getattr(process, stream_name, None)
        if stream is None or not hasattr(stream, "read"):
            continue
        output = stream.read()
        if isinstance(output, bytes):
            text = output.decode("utf-8", errors="replace").strip()
        else:
            text = str(output).strip()
        if text:
            parts.append(text)
    return "\n".join(parts) if parts else fallback


def _shell_single_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def _shell_double_quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _cluster_agent_bin(definition: ClusterDefinition) -> str:
    if definition.agent_bin is not None:
        return definition.agent_bin
    if definition.agent_npm_bin is not None:
        return f"$HOME/.local/bin/{definition.agent_npm_bin}"
    return "agent"


def _popen(*args: Any, **kwargs: Any) -> ManagedProcess:
    return subprocess.Popen(*args, **kwargs)
