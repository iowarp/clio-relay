"""The worker's site process: its API plus the frpc proxy that exposes it.

``clio-relay worker site-serve --state-dir D`` is what the ``worker`` adapter's
placement starts on the site, detached, after its one ssh pass. It reads what
placement wrote under ``D/run``, binds the worker's API on loopback,
holds an frpc ``stcp`` proxy to the rendezvous server, and writes
``D/run/ready.json`` once both are up. It needs no systemd user manager, so it
runs on hosts that have none.

Files placement writes (all 0600):

- ``run/settings.json``: cluster, session and generation ids, the API token and
  the session owner token.
- ``run/registry.json``: a one-cluster registry, for the frp settings.
- ``run/frp.env``: the rendezvous token and the stcp secret, ``NAME=value`` lines.
"""

from __future__ import annotations

import asyncio
import contextlib
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
    from clio_relay.adapters.worker.site_app import SiteSettings, site_app
    from clio_relay.cluster_config import ClusterRegistry

    run = state_dir / "run"
    settings: dict[str, Any] = json.loads((run / SETTINGS_FILE).read_text(encoding="utf-8"))
    registry = ClusterRegistry.model_validate_json((run / REGISTRY_FILE).read_bytes())
    cluster = str(settings["cluster"])
    definition = registry.require(cluster)
    frp_secrets = _read_env_file(run / FRP_ENV_FILE)
    port = _free_loopback_port()
    app = site_app(
        SiteSettings(
            cluster=cluster,
            session_id=str(settings["session_id"]),
            generation_id=str(settings["generation_id"]),
            api_token=str(settings["api_token"]),
            session_owner_token=str(settings["session_owner_token"]),
            state_dir=state_dir,
            port=port,
        )
    )

    frpc_toml = run / FRPC_TOML
    _write_private(frpc_toml, render_frpc_proxy_toml(definition, cluster=cluster, local_port=port))
    frpc_bin = str(settings.get("frpc") or "frpc")
    keeper = _FrpcKeeper(
        [frpc_bin, "-c", str(frpc_toml)],
        {**os.environ, **frp_secrets},
        run / FRPC_LOG,
    )
    # Relay keeps idle streams for 60 s, so the site must keep them longer.
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, access_log=False, timeout_keep_alive=300)
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
    """Serve this site's worker: its API and its frpc proxy."""
    serve_site(state_dir.expanduser().resolve())
