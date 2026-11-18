"""The desktop end of a served port: a loopback bind that comes back (RL20, RL22, RL23).

A forward is an frpc visitor, or one ``ssh -L`` held by the system ``ssh``.
Relay watches its process and starts it again on the same port when it dies,
counting each restart so the person can see it. Whether the port is bound is
read by trying to bind it, never by connecting, because some services (a
``pvserver``) give themselves to the first client that connects.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shlex
import socket
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.serve import frp
from clio_relay.serve.plan import ServePlan

MAX_BACKOFF_S = 30.0
POLL_S = 1.0


def port_bound(port: int) -> bool:
    """Return whether something listens on the loopback ``port``, without connecting."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        if os.name != "nt":
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return True
        return False


def visitor_command(plan: ServePlan, directory: Path) -> tuple[list[str], dict[str, str]]:
    """Write the visitor configuration and return frpc's argv and environment."""
    rendezvous = plan.rendezvous
    if rendezvous is None:
        raise Refusal(RefusalReason.INVALID_CONFIG, "no rendezvous host is set")
    ca_file: str | None = None
    if rendezvous.protocol == "wss":
        ca_file = os.path.expanduser(rendezvous.ca_file) if rendezvous.ca_file else frp.default_ca_file()
    common = frp.desktop_common(rendezvous, ca_file)
    config = frp.render_visitor(
        common,
        [(port.name, port.local_port) for port in plan.ports],
        plan.operation_id,
        direct=rendezvous.direct,
    )
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "visitor.toml"
    path.write_text(config, encoding="utf-8")
    env = dict(os.environ)
    env[frp.ENV_TOKEN] = rendezvous.token()
    env[frp.ENV_SECRET_KEY] = plan.secret_key
    return [os.path.expanduser(rendezvous.frpc), "-c", str(path)], env


def ssh_command(
    plan: ServePlan, env: dict[str, str] | None = None
) -> tuple[list[str], dict[str, str]]:
    """Return the ``ssh -L`` that holds every port of an ``ssh`` site's service."""
    if not plan.ssh_argv:
        raise Refusal(RefusalReason.INVALID_CONFIG, "the adapter gave no ssh command")
    base, destination = plan.ssh_argv[:-1], plan.ssh_argv[-1]
    argv = [
        *base,
        "-N",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=3",
    ]
    for port in plan.ports:
        argv += ["-L", f"127.0.0.1:{port.local_port}:127.0.0.1:{port.remote_port}"]
    if plan.host and plan.login_host and plan.host != plan.login_host:
        jump = shlex.join([*base, "-W", "%h:%p", destination])
        argv += ["-o", f"ProxyCommand={jump}", plan.host]
    else:
        argv.append(destination)
    return argv, {**os.environ, **(env or {})}


def _is_ours(pid: int, marker: str) -> bool:
    if sys.platform.startswith("linux"):
        try:
            cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ")
        except OSError:
            return False
        return marker.encode() in cmdline
    if os.name == "nt":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            check=False,
        )
        return any(name in result.stdout.lower() for name in ("frpc", "ssh"))
    try:
        result = subprocess.run(
            ["ps", "-o", "command=", "-p", str(pid)], capture_output=True, text=True, check=False
        )
    except OSError:
        return False
    return marker in result.stdout


def kill_recorded(pid: int | None, marker: str) -> None:
    """Stop a forward a previous relay process left behind, if it is still ours."""
    if not pid or not _is_ours(pid, marker):
        return
    with contextlib.suppress(OSError):
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(pid), "/F"], capture_output=True, check=False)
        else:
            import signal

            os.kill(pid, signal.SIGTERM)


class Forward:
    """One held desktop forward, restarted on the same port while the service runs."""

    def __init__(
        self,
        plan: ServePlan,
        directory: Path,
        on_restart: Callable[[int, int | None], None],
    ) -> None:
        self.plan = plan
        self.directory = directory
        self.on_restart = on_restart
        self.process: asyncio.subprocess.Process | None = None
        self.task: asyncio.Task[None] | None = None
        self.restarts = 0
        self.closing = False
        self.ssh_env: dict[str, str] = {}
        self.marker = (
            str(directory)
            if plan.mode == "frp"
            else f"127.0.0.1:{plan.ports[0].local_port}:127.0.0.1"
        )

    @property
    def log_path(self) -> Path:
        """Return the forward's log."""
        return self.directory / "forward.log"

    def bound(self) -> bool:
        """Return whether every desktop port is bound."""
        return all(port_bound(port.local_port) for port in self.plan.ports)

    def last_error(self) -> str | None:
        """Return the last line the forward logged, for a status message."""
        try:
            lines = self.log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            return None
        return lines[-1][-300:] if lines else None

    def _command(self) -> tuple[list[str], dict[str, str]]:
        if self.plan.mode == "frp":
            return visitor_command(self.plan, self.directory)
        return ssh_command(self.plan, self.ssh_env)

    async def _spawn(self) -> None:
        argv, env = self._command()
        self.directory.mkdir(parents=True, exist_ok=True)
        log = self.log_path.open("ab")
        try:
            self.process = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=log,
                stderr=asyncio.subprocess.STDOUT,
                env=env,
                cwd=str(self.directory),
            )
        except OSError as exc:
            raise Refusal(
                RefusalReason.INVALID_CONFIG, f"cannot start {argv[0]}: {exc.strerror}"
            ) from exc
        finally:
            log.close()

    async def start(self) -> None:
        """Start the forward unless one already holds the ports."""
        if self.task is not None:
            return
        if not self.bound():
            await self._spawn()
            self.on_restart(self.restarts, self.process.pid if self.process else None)
        self.task = asyncio.create_task(self._watch())

    async def _watch(self) -> None:
        backoff = 1.0
        while not self.closing:
            await asyncio.sleep(POLL_S)
            if self.closing:
                return
            alive = self.process is not None and self.process.returncode is None
            if alive or (self.process is None and self.bound()):
                if alive and self.bound():
                    backoff = 1.0
                continue
            self.restarts += 1
            self._log(f"forward ended, starting it again on the same port (restart {self.restarts})")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_S)
            if self.closing:
                return
            try:
                await self._spawn()
            except Refusal as exc:
                self._log(str(exc))
                continue
            self.on_restart(self.restarts, self.process.pid if self.process else None)

    def _log(self, line: str) -> None:
        with contextlib.suppress(OSError), self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"relay: {line}\n")

    async def close(self) -> None:
        """Close the desktop ports at once."""
        self.closing = True
        if self.task is not None:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self.task
            self.task = None
        process = self.process
        if process is not None and process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                process.terminate()
            try:
                await asyncio.wait_for(process.wait(), 5)
            except TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
                await process.wait()
        elif self.bound():
            kill_recorded(self.plan.forward_pid, self.marker)
