"""The desktop end of a served port: a loopback bind that comes back (RL20, RL22, RL23).

A forward is an frpc visitor, or one ``ssh -L`` held by the system ``ssh``.
Relay watches its process and starts it again on the same port when it dies,
counting each restart so the person can see it. Whether the port is bound is
read by trying to bind it, never by connecting, because some services (a
``pvserver``) give themselves to the first client that connects.

The forward's process is started apart from relay's (see ``detach``): a host
may restart relay's MCP server at any time, and the desktop port stays up through
it. The next relay process finds the ports bound and adopts the forward rather
than starting a second one.
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

from clio_relay import programs
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.serve import detach, frp
from clio_relay.serve.plan import ServePlan

MAX_BACKOFF_S = 30.0
POLL_S = 1.0
CLOSED_MARKER = "closed"
"""In a forward's directory: ``clio-relay shutdown`` closed it, so no process restarts it."""


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
        ca_file = (
            os.path.expanduser(rendezvous.ca_file) if rendezvous.ca_file else frp.default_ca_file()
        )
    common = frp.desktop_common(rendezvous, ca_file)
    config = frp.render_visitor(
        common,
        [(port.name, port.local_port) for port in plan.ports],
        plan.operation_id,
        direct=rendezvous.direct,
        direct_wait_s=rendezvous.direct_wait_s,
    )
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "visitor.toml"
    path.write_text(config, encoding="utf-8")
    env = dict(os.environ)
    env[frp.ENV_TOKEN] = rendezvous.token()
    env[frp.ENV_SECRET_KEY] = plan.secret_key
    return [os.path.expanduser(rendezvous.frpc), "-c", str(path)], env


def ssh_command(
    plan: ServePlan, env: dict[str, str] | None = None, known_hosts: Path | None = None
) -> tuple[list[str], dict[str, str]]:
    """Return the ``ssh -L`` that holds every port of an ``ssh`` site's service.

    ``known_hosts`` holds the compute node's keys read on the login host; ssh checks
    it after the person's own known_hosts.
    """
    if not plan.ssh_argv:
        raise Refusal(RefusalReason.INVALID_CONFIG, "the adapter gave no ssh command")
    merged = {**os.environ, **(env or {})}
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
        if known_hosts is not None:
            argv += ["-o", f'GlobalKnownHostsFile="{known_hosts.as_posix()}"']
        argv += login_identity(base, destination, merged)
        argv += ["-o", f"ProxyCommand={_jump_command(base, destination, merged)}", plan.host]
    else:
        argv.append(destination)
    return argv, merged


IDENTITY_KEYS = ("user", "identityfile", "certificatefile", "identitiesonly", "identityagent")
"""What the login host's ssh settings say about logging in, carried to the node behind it."""


def login_identity(base: list[str], destination: str, env: dict[str, str]) -> list[str]:
    """Return the login destination's login settings as options for the node behind it.

    A person's ssh config names their user and key for the login host, which says
    nothing of the compute node relay reaches through it. ``ssh -G`` reads what the
    login host gets, so the node gets the same; empty when ssh cannot say.
    """
    program = programs.find(base[0], env) or base[0]
    try:
        done = subprocess.run(
            [program, *base[1:], "-G", destination],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
            env=env,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    if done.returncode != 0:
        return []
    options: list[str] = []
    for line in done.stdout.splitlines():
        key, _sep, value = line.strip().partition(" ")
        value = value.strip()
        if key.lower() in IDENTITY_KEYS and value and value.lower() != "none":
            options += ["-o", f'{key}="{value.replace(chr(92), "/")}"']
    return options


def _jump_command(base: list[str], destination: str, env: dict[str, str]) -> str:
    """Return the ``ssh -W`` hop through the login host, as ssh's ``ProxyCommand``.

    ssh starts a ``ProxyCommand`` itself. On Windows that is ``CreateProcess``, which
    looks in ssh's own directory first, so a bare ``ssh`` there would be System32's
    rather than the one relay found on the PATH (a host's wrapper, say). The hop
    therefore names the program relay found, quoted the way the platform reads it.
    """
    program = programs.find(base[0], env) or base[0]
    hop = [program, *base[1:], "-W", "%h:%p", destination]
    if os.name == "nt":
        return subprocess.list2cmdline([hop[0].replace("%", "%%"), *hop[1:]])
    return shlex.join([hop[0].replace("%", "%%"), *hop[1:]])


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


def forward_marker(plan: ServePlan, directory: Path) -> str:
    """Return what a forward's command line holds, to tell it from another process."""
    if plan.mode == "frp":
        return str(directory)
    return f"127.0.0.1:{plan.ports[0].local_port}:127.0.0.1"


def kill_recorded(pid: int | None, marker: str) -> bool:
    """Stop a forward a previous relay process left behind, if it is still ours.

    Returns whether it signalled one.
    """
    if not pid or not _is_ours(pid, marker):
        return False
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(pid), "/F"], capture_output=True, check=False)
        else:
            import signal

            os.kill(pid, signal.SIGTERM)
    except OSError:
        return False
    return True


class Forward:
    """One held desktop forward, restarted on the same port while the service runs."""

    def __init__(
        self,
        plan: ServePlan,
        directory: Path,
        on_restart: Callable[[int, int | None], None],
        *,
        detached: bool = True,
    ) -> None:
        self.plan = plan
        self.directory = directory
        self.on_restart = on_restart
        self.detached = detached
        self.process: detach.Process | None = None
        self.task: asyncio.Task[None] | None = None
        self.restarts = 0
        self.rebound = 0
        self.held = False
        self._respawned = False
        self.closing = False
        self.ssh_env: dict[str, str] = {}
        self.marker = forward_marker(plan, directory)

    def alive(self) -> bool:
        """Return whether this relay process started the forward and it still runs."""
        return self.process is not None and self.process.poll() is None

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

    def direct_note(self) -> str | None:
        """Return which path an XTCP forward is on, or None when it is not direct."""
        rendezvous = self.plan.rendezvous
        if self.plan.mode != "frp" or rendezvous is None or not rendezvous.direct:
            return None
        try:
            lines = self.log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            lines = []
        return frp.direct_note(lines, rendezvous.direct_wait_s)

    def _command(self) -> tuple[list[str], dict[str, str]]:
        if self.plan.mode == "frp":
            return visitor_command(self.plan, self.directory)
        known_hosts: Path | None = None
        if self.plan.host_keys:
            self.directory.mkdir(parents=True, exist_ok=True)
            known_hosts = self.directory / "known_hosts"
            known_hosts.write_text("\n".join(self.plan.host_keys) + "\n", encoding="utf-8")
        return ssh_command(self.plan, self.ssh_env, known_hosts)

    async def _spawn(self) -> None:
        argv, env = self._command()
        program = programs.resolved(argv, env)
        if program is None:
            raise Refusal(RefusalReason.UNREACHABLE, programs.missing(argv[0]), command=argv[0])
        self.directory.mkdir(parents=True, exist_ok=True)
        try:
            if self.detached:
                self.process = await asyncio.to_thread(
                    detach.spawn, program, env, self.directory, self.log_path
                )
                return
            with self.log_path.open("ab") as log:
                self.process = subprocess.Popen(
                    program,
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    env=env,
                    cwd=str(self.directory),
                )
        except OSError as exc:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                programs.start_failed(argv[0], exc),
                command=argv[0],
            ) from exc

    async def start(self) -> None:
        """Start the forward unless one already holds the ports, which it then adopts."""
        if self.task is not None:
            return
        if self.bound():
            self.held = True
            self._log(f"the ports are already bound; adopting that forward (pid {self.pid()})")
        else:
            await self._spawn()
            self.on_restart(self.rebound, self.process.pid if self.process else None)
        self.task = asyncio.create_task(self._watch())

    def pid(self) -> int | None:
        """Return the forward's process id: this process's child, else the one recorded."""
        if self.alive() and self.process is not None:
            return self.process.pid
        return self.plan.forward_pid

    async def _watch(self) -> None:
        backoff = 1.0
        while not self.closing:
            await asyncio.sleep(POLL_S)
            if self.closing:
                return
            alive = self.alive()
            bound = self.bound()
            if alive or bound:
                if not alive:
                    # Another relay process's forward holds the ports: adopt it.
                    self.process = None
                elif bound:
                    backoff = 1.0
                    if not self.held:
                        # The first hold is a bind, not a rebind, even after failed tries.
                        self.held = True
                        self._respawned = False
                    elif self._respawned:
                        # Count a rebind only once the new forward holds the ports.
                        self._respawned = False
                        self.rebound += 1
                        self.on_restart(self.rebound, self.process.pid if self.process else None)
                continue
            self.restarts += 1
            if (self.directory / CLOSED_MARKER).exists():
                self._log("shutdown closed this forward; not starting it again")
                return
            self._log(
                f"forward ended, starting it again on the same port (restart {self.restarts})"
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_S)
            if self.closing:
                return
            try:
                await self._spawn()
            except Refusal as exc:
                self._log(str(exc))
                continue
            self._respawned = True
            self.on_restart(self.rebound, self.process.pid if self.process else None)

    def _log(self, line: str) -> None:
        with contextlib.suppress(OSError), self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"relay: {line}\n")

    async def _stop_watching(self) -> None:
        self.closing = True
        if self.task is not None:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self.task
            self.task = None

    async def release(self) -> None:
        """Stop watching and leave the forward up, for the next relay process to adopt."""
        await self._stop_watching()
        if self.alive():
            self._log(f"relay is stopping; the forward stays up (pid {self.pid()})")

    async def close(self) -> None:
        """Close the desktop ports at once."""
        await self._stop_watching()
        process = self.process
        if process is not None and process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                process.terminate()
            try:
                await asyncio.to_thread(process.wait, 5)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
                await asyncio.to_thread(process.wait)
        elif self.bound():
            kill_recorded(self.plan.forward_pid, self.marker)
