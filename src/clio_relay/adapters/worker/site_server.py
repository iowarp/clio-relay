"""The worker's site process: its API plus the frpc proxy that exposes it.

``clio-relay worker site-serve --state-dir D`` is what the ``worker`` adapter's
placement starts on the site, detached, after its one ssh pass. It reads what
placement wrote under ``D/run``, binds the worker's API on loopback,
holds an frpc proxy to the rendezvous server (``stcp``, or ``xtcp`` when the
link mode is ``udp_rendezvous``), and writes
``D/run/ready.json`` once both are up. It needs no systemd user manager, so it
runs on hosts that have none.

Files placement writes (all 0600):

- ``run/settings.json``: cluster, session and generation ids, the API token,
  the session owner token and the link mode.
- ``run/registry.json``: a one-cluster registry, for the frp settings.
- ``run/frp.env``: the rendezvous token and the stcp secret, ``NAME=value`` lines.

It runs on a login node, so it stays light (:mod:`clio_relay.login_load`):
placement starts it through ``clio_light``. One runs per user and site: it
holds an ``flock`` on ``run/site.lock`` for its life and writes its pid to
``run/site.pid``. A second refuses with the holder's pid. It exits once no
request from relay has come for ``--idle-exit-s`` seconds and none is running,
and writes ``run/stopped.json`` with why. The next sign-in places it again.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Final, Literal

import typer

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Receive, Scope, Send

LinkMode = Literal["brokered_tcp", "udp_rendezvous"]
"""How relay reaches a placed worker: through the rendezvous, or by a UDP hole punch."""
PROXY_TYPE: Final[dict[str, Literal["stcp", "xtcp"]]] = {
    "brokered_tcp": "stcp",
    "udp_rendezvous": "xtcp",
}
"""The frp proxy type site-serve holds for each link mode."""

SETTINGS_FILE = "settings.json"
REGISTRY_FILE = "registry.json"
FRP_ENV_FILE = "frp.env"
READY_FILE = "ready.json"
PID_FILE = "site.pid"
LOCK_FILE = "site.lock"
STOPPED_FILE = "stopped.json"
FRPC_TOML = "frpc.toml"
FRPC_LOG = "frpc.log"
IDLE_EXIT_S = 1800
"""How long site-serve waits with no request from relay before it exits."""
_PR_SET_PDEATHSIG = 1


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


def _die_with_parent() -> None:
    """In the child: take a SIGTERM when site-serve dies, however it dies (Linux)."""
    with contextlib.suppress(Exception):
        import ctypes

        ctypes.CDLL(None, use_errno=True).prctl(_PR_SET_PDEATHSIG, signal.SIGTERM)


class _Activity:
    """Count relay's requests, so that site-serve knows how long it has been idle."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app
        self._running = 0
        self._last = time.monotonic()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        self._running += 1
        self._last = time.monotonic()
        try:
            await self._app(scope, receive, send)
        finally:
            self._running -= 1
            self._last = time.monotonic()

    def idle_s(self) -> float:
        return 0.0 if self._running else time.monotonic() - self._last


def _hold_lock(run: Path) -> int:
    """Hold ``run/site.lock`` for this process's life, or refuse when another holds it."""
    import fcntl

    fd = os.open(run / LOCK_FILE, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        holder = ""
        with contextlib.suppress(OSError):
            holder = (run / PID_FILE).read_text(encoding="utf-8").strip()
        raise SystemExit(
            f"another site-serve already runs for this site (pid {holder or 'unknown'})"
        ) from None
    os.set_inheritable(fd, False)
    return fd


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
                    preexec_fn=_die_with_parent,  # noqa: PLW1509 - the child only calls prctl
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


def serve_site(state_dir: Path, *, idle_exit_s: float = IDLE_EXIT_S) -> None:
    """Run the site process until it is stopped or has been idle ``idle_exit_s`` seconds.

    ``idle_exit_s`` of 0 keeps it running until it is stopped.
    """
    import uvicorn

    from clio_relay.adapters.worker.frpc_unit import render_frpc_proxy_toml
    from clio_relay.adapters.worker.site_app import SiteSettings, site_app
    from clio_relay.cluster_config import ClusterRegistry

    run = state_dir / "run"
    lock = _hold_lock(run)
    settings: dict[str, Any] = json.loads((run / SETTINGS_FILE).read_text(encoding="utf-8"))
    registry = ClusterRegistry.model_validate_json((run / REGISTRY_FILE).read_bytes())
    cluster = str(settings["cluster"])
    definition = registry.require(cluster)
    frp_secrets = _read_env_file(run / FRP_ENV_FILE)
    port = _free_loopback_port()
    mode = str(settings.get("link_mode") or "brokered_tcp")
    if mode not in PROXY_TYPE:
        raise SystemExit(f"unknown link mode {mode!r} in {run / SETTINGS_FILE}")
    stopped = {"reason": "stopped"}
    server: uvicorn.Server | None = None

    def request_shutdown() -> None:
        stopped["reason"] = "shutdown"
        if server is not None:
            server.should_exit = True

    activity = _Activity(
        site_app(
            SiteSettings(
                cluster=cluster,
                session_id=str(settings["session_id"]),
                generation_id=str(settings["generation_id"]),
                api_token=str(settings["api_token"]),
                session_owner_token=str(settings["session_owner_token"]),
                state_dir=state_dir,
                port=port,
                on_shutdown=request_shutdown,
            )
        )
    )

    frpc_toml = run / FRPC_TOML
    _write_private(
        frpc_toml,
        render_frpc_proxy_toml(
            definition, cluster=cluster, local_port=port, proxy_type=PROXY_TYPE[mode]
        ),
    )
    frpc_bin = str(settings.get("frpc") or "frpc")
    keeper = _FrpcKeeper(
        [frpc_bin, "-c", str(frpc_toml)],
        {**os.environ, **frp_secrets},
        run / FRPC_LOG,
    )
    # Relay keeps idle streams for 60 s, so the site must keep them longer.
    server = uvicorn.Server(
        uvicorn.Config(
            activity, host="127.0.0.1", port=port, access_log=False, timeout_keep_alive=300
        )
    )
    serving = server

    async def announce() -> None:
        while not serving.started:
            if serving.should_exit:
                return
            await asyncio.sleep(0.1)
        keeper.start()
        _write_private(
            run / READY_FILE,
            json.dumps({"port": port, "pid": os.getpid(), "session_id": settings["session_id"]}),
        )

    async def idle_watch() -> None:
        if idle_exit_s <= 0:
            return
        while not serving.should_exit:
            await asyncio.sleep(min(30.0, max(idle_exit_s / 10, 0.5)))
            if activity.idle_s() >= idle_exit_s:
                stopped["reason"] = "idle"
                serving.should_exit = True

    async def main() -> None:
        tasks = [asyncio.create_task(announce()), asyncio.create_task(idle_watch())]
        try:
            await serving.serve()
        finally:
            for task in tasks:
                task.cancel()
            keeper.stop()

    with contextlib.suppress(OSError):
        (run / STOPPED_FILE).unlink()
    _write_private(run / PID_FILE, str(os.getpid()))
    try:
        asyncio.run(main())
    finally:
        with contextlib.suppress(OSError):
            (run / READY_FILE).unlink()
        _write_private(
            run / STOPPED_FILE,
            json.dumps({"reason": stopped["reason"], "pid": os.getpid(), "at": time.time()}),
        )
        os.close(lock)


def site_serve_command(
    state_dir: Annotated[
        Path, typer.Option("--state-dir", help="The worker's directory on this site.")
    ],
    idle_exit_s: Annotated[
        float,
        typer.Option(
            "--idle-exit-s",
            min=0,
            help="Exit after this long with no request from relay. 0 never exits idle.",
        ),
    ] = IDLE_EXIT_S,
) -> None:
    """Serve this site's worker: its API and its frpc proxy."""
    serve_site(state_dir.expanduser().resolve(), idle_exit_s=idle_exit_s)
