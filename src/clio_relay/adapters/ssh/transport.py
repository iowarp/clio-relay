"""The system ``ssh`` and ``scp``, one process per call (RL10).

Relay embeds no SSH library. Each call is its own process with the person's
profile, so their agent, keys, jump hosts and ``~/.ssh/config`` apply, and
nothing is held open between calls. A site that asks for a code on every
connection is the one exception: with ``interactive_sign_in`` the person signs
in once through an ask, and later calls reuse that connection's master. The
master ends with the relay process that started it.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import os
import re
import shlex
import signal
import tempfile
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from clio_relay.adapters.ssh.config import SshConfig
from clio_relay.asks import askpass
from clio_relay.jobs.handles import STALL_S, ExecResult, StallReport
from clio_relay.jobs.refusals import Refusal, RefusalReason

logger = logging.getLogger(__name__)

POSIX = os.name == "posix"
MAX_STALL_STEP_S = 600.0

# ssh reports its own failures with exit code 255 and one of these on stderr.
_INTERACTIVE_MARKERS = ("keyboard-interactive", "password", "verification code")
_NETWORK_MARKERS = (
    "connection timed out",
    "connection refused",
    "could not resolve hostname",
    "no route to host",
    "network is unreachable",
    "connection closed",
    "connection reset",
    "kex_exchange_identification",
    "timeout, server",
    "broken pipe",
    "client_loop",
    "operation timed out",
    "lost connection",
    "control socket connect",
    "mux_client",
)

# ``ssh -N -f`` leaves the master in its own session, so it would outlive relay.
# This watcher, detached too, stops it once the relay process that started it ends.
_WATCH_MASTER = (
    'while kill -0 "$1" 2>/dev/null && kill -0 "$2" 2>/dev/null; do sleep 5; done; '
    'kill "$2" 2>/dev/null'
)


@dataclass
class Completed:
    """One finished process, with its output as bytes."""

    argv: list[str]
    exit_code: int | None
    stdout: bytes
    stderr: bytes
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    timed_out: bool = False
    duration_s: float = 0.0

    def text(self) -> str:
        """Return stdout as text."""
        return self.stdout.decode("utf-8", errors="replace")

    def error_text(self) -> str:
        """Return stderr as text."""
        return self.stderr.decode("utf-8", errors="replace")

    def to_exec_result(self) -> ExecResult:
        """Return the result in the ``exec`` verb's shape."""
        return ExecResult(
            argv=self.argv,
            exit_code=self.exit_code,
            stdout=self.text(),
            stderr=self.error_text(),
            stdout_truncated=self.stdout_truncated,
            stderr_truncated=self.stderr_truncated,
            timed_out=self.timed_out,
            duration_s=round(self.duration_s, 3),
        )


class _Sink:
    def __init__(self, limit: int | None) -> None:
        self.limit = limit
        self.data = bytearray()
        self.truncated = False
        self.last = time.monotonic()

    async def pump(self, stream: asyncio.StreamReader) -> None:
        while chunk := await stream.read(65536):
            self.last = time.monotonic()
            if self.limit is None:
                self.data += chunk
                continue
            room = self.limit - len(self.data)
            if room > 0:
                self.data += chunk[:room]
            if len(chunk) > room:
                self.truncated = True


def _kill(process: asyncio.subprocess.Process) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        if POSIX:
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()


async def run_process(
    argv: Sequence[str],
    *,
    timeout_s: float | None,
    max_output_bytes: int | None = None,
    stdin: bytes | None = None,
    env: Mapping[str, str] | None = None,
    hold_stdin: bool = False,
    on_stall: StallReport | None = None,
    stall_s: float = STALL_S,
) -> Completed:
    """Run ``argv`` with no shell. ``timeout_s=None`` waits as long as it takes.

    With ``hold_stdin``, standard input stays open until the process ends, so the
    other end sees it close only then. While the process is quiet for
    ``stall_s``, ``on_stall`` is called, then again after waits that double up
    to ``MAX_STALL_STEP_S``. Cancelling the caller kills the process group.
    """
    command = list(argv)
    started = time.monotonic()
    out = _Sink(max_output_bytes)
    err = _Sink(max_output_bytes)
    piped = stdin is not None or hold_stdin
    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.PIPE if piped else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=dict(env) if env is not None else None,
            start_new_session=POSIX,
        )
    except FileNotFoundError:
        return Completed(command, 127, b"", f"{command[0]}: command not found".encode())
    assert process.stdout is not None and process.stderr is not None

    async def _feed() -> None:
        if process.stdin is None:
            return
        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            if stdin:
                process.stdin.write(stdin)
                await process.stdin.drain()
        if hold_stdin:
            return
        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            process.stdin.close()

    work = asyncio.ensure_future(
        asyncio.gather(out.pump(process.stdout), err.pump(process.stderr), _feed(), process.wait())
    )
    timed_out = False
    deadline = None if timeout_s is None else started + timeout_s
    report_at = stall_s
    try:
        while True:
            now = time.monotonic()
            idle = now - max(out.last, err.last)
            if idle < stall_s:
                report_at = stall_s
            wait: float | None = None if on_stall is None else max(report_at - idle, 0.05)
            if deadline is not None:
                left = max(deadline - now, 0.0)
                wait = left if wait is None else min(wait, left)
            done, _ = await asyncio.wait({work}, timeout=wait)
            if done:
                break
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                timed_out = True
                _kill(process)
                # The whole process group is gone, so the pipes reach EOF and the pumps end.
                done, _ = await asyncio.wait({work}, timeout=10)
                if not done:
                    work.cancel()
                break
            idle = now - max(out.last, err.last)
            if on_stall is not None and idle >= report_at:
                report_at += min(report_at, MAX_STALL_STEP_S)
                await on_stall(idle, report_at - idle)
        with contextlib.suppress(asyncio.CancelledError):
            await work
    except asyncio.CancelledError:
        _kill(process)
        work.cancel()
        raise
    finally:
        if process.stdin is not None:
            with contextlib.suppress(BrokenPipeError, ConnectionResetError, RuntimeError):
                process.stdin.close()
        transport = getattr(process, "_transport", None)
        if transport is not None:
            transport.close()
    return Completed(
        argv=command,
        exit_code=None if timed_out else process.returncode,
        stdout=bytes(out.data),
        stderr=bytes(err.data),
        stdout_truncated=out.truncated,
        stderr_truncated=err.truncated,
        timed_out=timed_out,
        duration_s=time.monotonic() - started,
    )


def is_ssh_failure(result: Completed) -> bool:
    """Return whether ``ssh`` itself failed, as opposed to the remote command."""
    if result.exit_code != 255:
        return False
    text = result.error_text().lower()
    return (
        "permission denied" in text
        or "host key verification failed" in text
        or text.startswith("ssh:")
        or any(marker in text for marker in _NETWORK_MARKERS)
    )


def is_interactive_refusal(result: Completed) -> bool:
    """Return whether the site wanted a code or password that BatchMode could not give."""
    text = result.error_text().lower()
    return "permission denied" in text and any(m in text for m in _INTERACTIVE_MARKERS)


def is_network_failure(result: Completed) -> bool:
    """Return whether the connection failed or dropped, so a retry may work."""
    text = result.error_text().lower()
    return result.exit_code == 255 and any(marker in text for marker in _NETWORK_MARKERS)


def _short_dir(preferred: Path) -> Path:
    """Return a directory whose socket paths fit the 104-byte Unix limit."""
    if len(str(preferred)) + 42 < 100:
        return preferred
    uid = os.getuid() if hasattr(os, "getuid") else 0
    return Path(tempfile.gettempdir()) / f"clio-relay-ssh-{uid}"


class SshTransport:
    """Builds and runs ``ssh`` and ``scp`` for one site."""

    def __init__(self, config: SshConfig, *, site_id: str, state_dir: Path) -> None:
        self.config = config
        self.site_id = site_id
        self.state_dir = state_dir
        key = hashlib.sha256(f"{config.ssh_profile}|{state_dir}".encode()).hexdigest()[:16]
        self.control_path = _short_dir(state_dir / "cm") / f"{key}.sock"
        self._starting = asyncio.Lock()

    # argv

    def _common_options(self, *, batch: bool, master: bool = False) -> list[str]:
        options: list[str] = []
        if self.config.ssh_config:
            options += ["-F", os.path.expanduser(self.config.ssh_config)]
        options += [
            "-o",
            f"BatchMode={'yes' if batch else 'no'}",
            "-o",
            f"ConnectTimeout={self.config.connect_timeout_s}",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=4",
        ]
        if self.config.interactive_sign_in:
            options += [
                "-o",
                f"ControlPath={self.control_path}",
                "-o",
                f"ControlMaster={'yes' if master else 'no'}",
            ]
        for option in self.config.ssh_options:
            options += ["-o", option]
        return options

    def _use_askpass(self) -> bool:
        """Return whether ssh's prompts go to relay's askpass (the page, or a program)."""
        return self.config.interactive_sign_in

    def ssh_argv(self) -> list[str]:
        """Return the base ``ssh`` argv: program, options and host.

        Calls run with ``BatchMode=yes`` and reuse the signed-in master when there
        is one. ``serve`` adds its own flags (``-N -L``, ``-J``) before the host.
        """
        return [
            self.config.ssh_command,
            *self._common_options(batch=True),
            self.config.ssh_profile,
        ]

    def environment(self, operation_id: str | None = None) -> dict[str, str] | None:
        """Return the environment ssh and scp run with, or ``None`` for relay's own."""
        if not self._use_askpass():
            return None
        env = dict(os.environ)
        env.update(
            askpass.environment(
                site=self.site_id,
                operation_id=operation_id,
                title=f"Sign in to {self.site_id}",
            )
        )
        if self.config.askpass:
            env["SSH_ASKPASS"] = os.path.expanduser(self.config.askpass)
        return env

    # ssh

    async def ssh(
        self,
        script: str,
        *,
        data: bytes | None = None,
        timeout_s: float | None,
        max_output_bytes: int | None = None,
        what: str = "the site",
        check_timeout: bool = True,
        hold_stdin: bool = False,
        on_stall: StallReport | None = None,
    ) -> Completed:
        """Run ``script`` with ``sh`` on the site and refuse on an ssh failure.

        The script goes on standard input. With ``data`` or ``hold_stdin``, the
        script goes on the command line and ``data`` on standard input, which
        ``hold_stdin`` keeps open until the command ends.
        """
        if data is None and not hold_stdin:
            argv = [*self.ssh_argv(), "sh -s"]
            stdin = script.encode()
        else:
            argv = [*self.ssh_argv(), f"sh -c {shlex.quote(script)}"]
            stdin = data or b""
        result = await run_process(
            argv,
            timeout_s=timeout_s,
            max_output_bytes=max_output_bytes,
            stdin=stdin,
            env=self.environment(),
            hold_stdin=hold_stdin,
            on_stall=on_stall,
        )
        if result.timed_out and not check_timeout:
            return result
        self.raise_for_failure(result, what)
        return result

    def raise_for_failure(self, result: Completed, what: str) -> None:
        """Turn an ssh failure into a typed refusal."""
        if result.timed_out:
            raise Refusal(
                RefusalReason.TIMEOUT,
                f"{what} on {self.config.ssh_profile} did not answer; relay will try again",
                site=self.site_id,
            )
        local_programs = (self.config.ssh_command, self.config.scp_command)
        if (
            result.exit_code == 127
            and not result.stdout
            and b"command not found" in result.stderr
            and result.argv
            and result.argv[0] in local_programs
        ):
            raise Refusal(
                RefusalReason.ADAPTER_UNAVAILABLE,
                f"{result.argv[0]} is not installed on this machine",
                site=self.site_id,
            )
        if not is_ssh_failure(result):
            return
        detail = result.error_text().strip().splitlines()
        last = detail[-1] if detail else "ssh failed"
        if is_interactive_refusal(result):
            if self._use_askpass():
                raise Refusal(
                    RefusalReason.NEEDS_SIGN_IN,
                    f"{self.config.ssh_profile} needs you to sign in again: {last}",
                    site=self.site_id,
                )
            raise Refusal(
                RefusalReason.INTERACTIVE_AUTH_REQUIRED,
                (
                    f"{self.config.ssh_profile} asks for a code or password on every "
                    "connection, so the ssh adapter cannot use it unattended. Use the worker "
                    "adapter for this site: you sign in once and relay places a worker there. "
                    f"ssh said: {last}"
                ),
                site=self.site_id,
                adapter="worker",
            )
        raise Refusal(
            RefusalReason.UNREACHABLE,
            f"ssh could not reach {self.config.ssh_profile}: {last}",
            site=self.site_id,
        )

    # scp

    async def upload(self, sources: Sequence[Path], destination: str) -> None:
        """Copy local files into a directory on the site. A dropped transfer is retried."""
        if not sources:
            return
        argv = [
            self.config.scp_command,
            "-q",
            "-p",
            *self._common_options(batch=True),
            *[str(path) for path in sources],
            f"{self.config.ssh_profile}:{destination.rstrip('/')}/",
        ]
        attempt = 0
        while True:
            result = await run_process(
                argv, timeout_s=None, max_output_bytes=65536, env=self.environment()
            )
            if result.exit_code == 0:
                return
            if is_network_failure(result) and attempt < self.config.transfer_retries:
                attempt += 1
                logger.warning(
                    "copy to %s dropped (%s); retry %d of %d",
                    self.config.ssh_profile,
                    result.error_text().strip(),
                    attempt,
                    self.config.transfer_retries,
                )
                await asyncio.sleep(min(30, 2**attempt))
                continue
            if result.exit_code == 255 or is_ssh_failure(result):
                result.exit_code = 255
                self.raise_for_failure(result, "copying")
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"scp to {self.config.ssh_profile} failed: {result.error_text().strip()}",
                site=self.site_id,
            )

    # interactive sign-in

    async def master_up(self) -> bool:
        """Return whether a signed-in master connection is running."""
        return await self.master_pid() is not None

    async def master_pid(self) -> int | None:
        """Return the running master's pid (0 if ssh does not say), or ``None``."""
        if not self.config.interactive_sign_in or not self.control_path.exists():
            return None
        argv = [
            self.config.ssh_command,
            *self._common_options(batch=True),
            "-O",
            "check",
            self.config.ssh_profile,
        ]
        result = await run_process(argv, timeout_s=30, max_output_bytes=4096)
        if result.exit_code != 0:
            return None
        found = re.search(rb"pid=(\d+)", result.stderr + result.stdout)
        return int(found.group(1)) if found else 0

    def starter(self) -> Callable[[dict[str, str]], Awaitable[None]]:
        """Return the coroutine the sign-in page runs to bring the master up."""

        async def _start(env: dict[str, str]) -> None:
            async with self._starting:
                # A second master would find the socket taken and stay up as a
                # plain connection that nothing ever closes.
                if await self.master_up():
                    return
                # A master that was killed can leave its socket behind.
                with contextlib.suppress(OSError):
                    self.control_path.unlink()
                await self._start_master(env)
                await self._watch_master()

        return _start

    async def _start_master(self, env: dict[str, str]) -> None:
        self.control_path.parent.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(OSError):
            self.control_path.parent.chmod(0o700)
        argv = [
            self.config.ssh_command,
            *self._common_options(batch=False, master=True),
            "-o",
            "ControlPersist=no",
            # ControlMaster=yes makes this the master; adding -M would turn it into "ask".
            "-N",
            "-f",
            self.config.ssh_profile,
        ]
        merged = dict(os.environ)
        merged.update(env)
        if self.config.askpass:
            merged["SSH_ASKPASS"] = os.path.expanduser(self.config.askpass)
        # The backgrounded master keeps its stderr, so a pipe would never close.
        with tempfile.TemporaryFile() as log:
            process = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=log,
                env=merged,
                start_new_session=POSIX,
            )
            code = await process.wait()
            log.seek(0)
            detail = log.read(65536).decode("utf-8", errors="replace").strip()
        if code != 0:
            raise RuntimeError(f"ssh sign-in to {self.config.ssh_profile} failed: {detail}")

    async def _watch_master(self) -> None:
        """Stop the master when this relay process ends, however it ends."""
        pid = await self.master_pid()
        if not POSIX or not pid:
            return
        watcher = await asyncio.create_subprocess_exec(
            "sh",
            "-c",
            f"({_WATCH_MASTER}) &",
            "sh",
            str(os.getpid()),
            str(pid),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )
        await watcher.wait()

    def register_sign_in(self) -> None:
        """Offer the starter to relay's sign-in page."""
        askpass.register_sign_in(self.site_id, self.starter())

    async def stop_master(self) -> None:
        """Close the signed-in master connection, if one is up."""
        if not self.config.interactive_sign_in or not self.control_path.exists():
            return
        argv = [
            self.config.ssh_command,
            *self._common_options(batch=True),
            "-O",
            "exit",
            self.config.ssh_profile,
        ]
        await run_process(argv, timeout_s=30, max_output_bytes=4096)
