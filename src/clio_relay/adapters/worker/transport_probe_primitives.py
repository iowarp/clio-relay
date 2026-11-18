"""Low-level process/HTTP/shell primitives shared by the transport probes.

Split out of ``transport_probe.py`` (iowarp/clio-relay#231): the
``ManagedProcess`` protocol, the probe callback type aliases, and the small
process/health/shell helpers none of the probe orchestration functions treat
as a monkeypatch seam (contrast with ``_wait_for_healthz``'s sibling
``_cleanup_remote_probe`` in ``transport_probe_remote_cleanup.py`` -- both are
imported back into ``transport_probe.py`` by the same name so its still-
resident orchestration functions keep resolving them as bare names, but only
``_wait_for_healthz`` lives here because nothing patches it independently of
the module it is imported into).
"""

from __future__ import annotations

import os
import secrets
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any, Protocol

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.frp_link import HeldFrpVisitor


class ManagedProcess(Protocol):
    """Subset of subprocess.Popen used by the transport probe.

    ``stdin``/``stdout``/``stderr`` stay ``Any``: Protocol attributes are
    checked invariantly, and the test double in
    ``tests/test_transport_probe.py`` declares them with a narrower concrete
    type that only ``Any`` accepts both ways without editing that file.
    """

    stdin: Any | None
    stdout: Any | None
    stderr: Any | None

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
OwnedSessionHttpCheck = Callable[[str, str, str], list[str]]


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


def _require_api_token(api_token: str | None) -> str:
    if api_token is None or api_token == "":
        raise ConfigurationError(
            "transport probes require CLIO_RELAY_API_TOKEN for the owned remote API"
        )
    return api_token


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


def _visitor_failure_message(visitor: HeldFrpVisitor, label: str) -> str:
    """Return ``label``, plus the visitor's bounded stdout/stderr as detail.

    ``label`` is always a PREFIX, never a replacement (#231 R4 opus review
    F2): the pre-R4 code read both of the visitor's streams via
    ``_process_output_message`` and included them alongside a fixed label
    like "local frpc visitor failed"; delegating to
    ``HeldFrpVisitor.failure_detail()`` must not regress that to a bare label
    with no diagnostic content.
    """
    detail = visitor.failure_detail()
    return f"{label}: {detail}" if detail else label


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


def _cluster_agent_bin(definition: ClusterDefinition) -> str:
    if definition.agent_bin is not None:
        return definition.agent_bin
    if definition.agent_npm_bin is not None:
        return f"$HOME/.local/bin/{definition.agent_npm_bin}"
    return "agent"


def _popen(*args: Any, **kwargs: Any) -> ManagedProcess:
    return subprocess.Popen(*args, **kwargs)


def _probe_id(*, cluster: str, proxy_name: str) -> str:
    safe_cluster = "".join(item if item.isalnum() else "-" for item in cluster).strip("-")
    safe_proxy = "".join(item if item.isalnum() else "-" for item in proxy_name).strip("-")
    return f"{safe_cluster}-{safe_proxy}-{secrets.token_hex(8)}"
