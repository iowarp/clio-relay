"""The worker's site process: one owned API plus the frpc proxy that exposes it.

``clio-relay worker site-serve --state-dir D`` is what the ``worker`` adapter's
placement starts on the site, detached, after its one ssh pass. It reads what
placement wrote under ``D/run``, binds the owned-session API on loopback,
holds an frpc ``stcp`` proxy to the rendezvous server, and writes
``D/run/ready.json`` once both are up. It needs no systemd user manager, so it
runs on hosts that have none.

Files placement writes (all 0600):

- ``run/settings.json``: cluster, session and generation ids, the API token and
  the session owner token.
- ``run/registry.json``: a one-cluster registry, the process-bound authority.
- ``run/frp.env``: the rendezvous token and the stcp secret, ``NAME=value`` lines.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import socket
import subprocess
import threading
from pathlib import Path
from typing import Annotated, Any

import typer

SETTINGS_FILE = "settings.json"
REGISTRY_FILE = "registry.json"
FRP_ENV_FILE = "frp.env"
READY_FILE = "ready.json"
PID_FILE = "site.pid"
FRPC_TOML = "frpc.toml"
FRPC_LOG = "frpc.log"


def _write_private(path: Path, text: str) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(text)
    os.replace(tmp, path)


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        name, _, value = line.partition("=")
        values[name.strip()] = value
    return values


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


class _FrpcKeeper:
    """Keep one frpc proxy running for the life of the site process."""

    def __init__(self, argv: list[str], env: dict[str, str], log_path: Path) -> None:
        self._argv = argv
        self._env = env
        self._log_path = log_path
        self._stop = threading.Event()
        self._process: subprocess.Popen[bytes] | None = None
        self._thread = threading.Thread(target=self._loop, name="frpc-keeper", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _loop(self) -> None:
        delay = 1.0
        while not self._stop.is_set():
            with self._log_path.open("ab") as log:
                self._process = subprocess.Popen(  # noqa: S603
                    self._argv,
                    env=self._env,
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
            self._process.wait()
            if self._stop.wait(delay):
                return
            delay = min(delay * 2, 30.0)

    def stop(self) -> None:
        self._stop.set()
        process = self._process
        if process is not None and process.poll() is None:
            process.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=10)
            if process.poll() is None:
                process.kill()


def serve_site(state_dir: Path) -> None:
    """Run the site process until it is stopped."""
    import uvicorn

    from clio_relay.adapters.worker.frpc_unit import render_frpc_proxy_toml
    from clio_relay.cluster_config import (
        CLUSTER_REGISTRY_ENV,
        ClusterRegistry,
        cluster_route_revision,
    )

    run = state_dir / "run"
    settings: dict[str, Any] = json.loads((run / SETTINGS_FILE).read_text(encoding="utf-8"))
    registry_path = run / REGISTRY_FILE
    registry_bytes = registry_path.read_bytes()
    registry = ClusterRegistry.model_validate_json(registry_bytes)
    cluster = str(settings["cluster"])
    definition = registry.require(cluster)
    frp_secrets = _read_env_file(run / FRP_ENV_FILE)
    port = _free_loopback_port()

    os.environ.update(
        {
            "CLIO_RELAY_CORE_DIR": str(state_dir / "core"),
            "CLIO_RELAY_SPOOL_DIR": str(state_dir / "spool"),
            "CLIO_RELAY_OWNER_SESSION_ID": str(settings["session_id"]),
            "CLIO_RELAY_SESSION_GENERATION_ID": str(settings["generation_id"]),
            "CLIO_RELAY_OWNER_SESSION_CLUSTER": cluster,
            "CLIO_RELAY_OWNER_SESSION_API_PORT": str(port),
            "CLIO_RELAY_API_TOKEN": str(settings["api_token"]),
            "CLIO_RELAY_SESSION_OWNER_TOKEN": str(settings["session_owner_token"]),
            CLUSTER_REGISTRY_ENV: str(registry_path),
            "CLIO_RELAY_SESSION_REGISTRY_SHA256": hashlib.sha256(registry_bytes).hexdigest(),
            "CLIO_RELAY_SESSION_ROUTE_REVISION": cluster_route_revision(definition),
        }
    )
    # The app reads its gated environment once, at import.
    from clio_relay.adapters.worker.http_api import app as relay_http_app

    frpc_toml = run / FRPC_TOML
    _write_private(frpc_toml, render_frpc_proxy_toml(definition, cluster=cluster, local_port=port))
    frpc_bin = str(settings.get("frpc") or "frpc")
    keeper = _FrpcKeeper(
        [frpc_bin, "-c", str(frpc_toml)],
        {**os.environ, **frp_secrets},
        run / FRPC_LOG,
    )
    server = uvicorn.Server(
        uvicorn.Config(relay_http_app, host="127.0.0.1", port=port, access_log=False)
    )

    async def announce() -> None:
        while not server.started:
            if server.should_exit:
                return
            await asyncio.sleep(0.1)
        keeper.start()
        _write_private(
            run / READY_FILE,
            json.dumps({"port": port, "pid": os.getpid(), "session_id": settings["session_id"]}),
        )

    async def main() -> None:
        announcer = asyncio.create_task(announce())
        try:
            await server.serve()
        finally:
            announcer.cancel()
            keeper.stop()

    _write_private(run / PID_FILE, str(os.getpid()))
    asyncio.run(main())


def site_serve_command(
    state_dir: Annotated[
        Path, typer.Option("--state-dir", help="The worker's directory on this site.")
    ],
) -> None:
    """Serve this site's worker: the owned API and its frpc proxy."""
    serve_site(state_dir.expanduser().resolve())
