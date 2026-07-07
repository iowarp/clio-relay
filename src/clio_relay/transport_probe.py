"""End-to-end frp transport probes for relay HTTP surfaces."""

from __future__ import annotations

import os
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
    factory = process_factory or _popen
    transport = definition.frp_transport
    protocol = FrpTransportProtocol(transport.protocol)
    with tempfile.TemporaryDirectory(prefix="clio-relay-transport-") as temp_dir:
        temp_path = Path(temp_dir)
        remote_frpc_config = render_frpc_config(
            FrpcConfig(
                server_addr=transport.server_addr,
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
                    server_addr=transport.server_addr,
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
            _wait_for_healthz(
                f"http://127.0.0.1:{local_bind_port}/healthz",
                timeout_seconds=timeout_seconds,
            )
            lines = [
                f"transport.cluster={cluster}",
                f"transport.server={transport.server_addr}:{transport.server_port}",
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
export CLIO_RELAY_CORE_DIR="{definition.core_dir}"
export CLIO_RELAY_SPOOL_DIR="{definition.spool_dir}"
export CLIO_RELAY_JARVIS_BIN={_shell_double_quote(jarvis_bin)}
export CLIO_RELAY_FRPC_BIN={_shell_double_quote(frpc_bin)}
export CLIO_RELAY_AGENT_BIN={_shell_double_quote(agent_bin)}
export CLIO_RELAY_AGENT_ADAPTER={_shell_single_quote(definition.agent_adapter)}
{token_export}
tmp="$(mktemp -d)"
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$tmp"' EXIT
cat > "$tmp/frpc.toml" <<'__CLIO_RELAY_FRPC_CONFIG__'
{frpc_config.rstrip()}
__CLIO_RELAY_FRPC_CONFIG__
echo "transport_probe_cluster={cluster}"
clio-relay api start --host 127.0.0.1 --port {api_port}{require_token} &
"$CLIO_RELAY_FRPC_BIN" -c "$tmp/frpc.toml" &
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
