"""How relay reaches an ``ssh`` site: its own held connection, or the system ``ssh``.

Relay holds the connection itself (``held``), the same way on every OS. The
person's ``ssh`` still decides where and how to connect: ``ssh -G`` reads their
``~/.ssh/config`` (``profile``), and their agent and keys sign in. A site with
``interactive_sign_in`` signs in once through an ask, and later calls reuse that
connection.

A few profiles need the system ``ssh`` itself (``Profile.openssh_only``: a
PKCS#11 token, Kerberos, a security key with no agent, or a site's own ssh
program). Those calls run ``ssh`` and ``scp``, one process per call, and cannot
sign in with a code.

A call ends when its script exits on the site, not when the connection lets go:
the script reports its exit code on stderr behind a marker (``ends_with_marker``).
"""

from __future__ import annotations

import asyncio
import logging
import os
import shlex
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path

from clio_relay import programs
from clio_relay.adapters.ssh import errors, held, mux
from clio_relay.adapters.ssh.config import SshConfig
from clio_relay.adapters.ssh.process import (
    Completed,
    ends_with_marker,
    exit_marker,
    run_process,
)
from clio_relay.adapters.ssh.profile import OPENSSH_PROGRAMS
from clio_relay.asks import askpass
from clio_relay.jobs.handles import StallReport
from clio_relay.jobs.refusals import Refusal, RefusalReason

__all__ = [
    "Completed",
    "SshTransport",
    "ends_with_marker",
    "exit_marker",
    "run_process",
]

logger = logging.getLogger(__name__)

MAX_RETRY_WAIT_S = 30.0


class SshTransport:
    """Runs calls and copies on one site, over relay's held connection or the system ``ssh``."""

    def __init__(self, config: SshConfig, *, site_id: str, state_dir: Path) -> None:
        self.config = config
        self.site_id = site_id
        self.state_dir = state_dir
        self.held = held.Held(
            base=self._base(),
            destination=config.ssh_profile,
            signs_in=config.interactive_sign_in,
            channels=config.channels,
            timeout_s=config.connect_timeout_s,
            site=site_id,
            share=mux.path_for(state_dir, site_id) if config.interactive_sign_in else None,
        )

    # argv

    def _base(self) -> list[str]:
        """Return the ssh program and relay's options, without the host."""
        options: list[str] = [self.config.ssh_command]
        if self.config.ssh_config:
            options += ["-F", os.path.expanduser(self.config.ssh_config)]
        options += [
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={self.config.connect_timeout_s}",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=4",
        ]
        for option in self.config.ssh_options:
            options += ["-o", option]
        return options

    def ssh_argv(self) -> list[str]:
        """Return the system ``ssh`` argv for this site: program, options and host.

        ``ssh -G`` with these reads the profile, and a call on the fallback runs them.
        """
        return [*self._base(), self.config.ssh_profile]

    def environment(self, operation_id: str | None = None) -> dict[str, str] | None:
        """Return the environment signing in asks with, or ``None`` for relay's own."""
        if not self.config.interactive_sign_in:
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

    async def holds(self) -> bool:
        """Return whether calls go over relay's held connection, not the system ``ssh``.

        A site that signs in with a code needs the held connection, so a profile
        only the system ``ssh`` can serve refuses with its reason.
        """
        if Path(self.config.ssh_command).name.lower() not in OPENSSH_PROGRAMS:
            reason: str | None = f"the site names its own ssh program ({self.config.ssh_command})"
        else:
            reason = (await self.held.profile()).openssh_only
        if reason is None:
            return True
        if self.config.interactive_sign_in:
            raise Refusal(
                RefusalReason.INTERACTIVE_AUTH_REQUIRED,
                (
                    f"interactive_sign_in on {self.config.ssh_profile} needs relay to hold "
                    f"the signed-in connection, and {reason}, which only the system ssh "
                    "can use. Sign in with a key relay can read, or use the worker adapter"
                ),
                site=self.site_id,
                adapter="worker",
                cause="openssh_only",
            )
        return False

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
        retry: bool = True,
    ) -> Completed:
        """Run ``script`` with ``sh`` on the site and refuse on an ssh failure.

        The script goes on standard input. With ``data`` or ``hold_stdin``, the
        script goes on the command line and ``data`` on standard input, which
        ``hold_stdin`` keeps open until the command ends. A call whose connection
        failed or dropped runs again, up to ``retries`` times. Without ``retry``
        the script must not run twice, so it runs again only when the connection
        never opened. The call ends when the script does, even if something
        keeps the session up after it (see ``ends_with_marker``).
        """
        marker = exit_marker()
        wrapped = ends_with_marker(script, marker)
        if data is None and not hold_stdin:
            command = "sh -s"
            # sh reads a group whole before it runs any of it, so a stream cut
            # short by a dropped connection runs nothing rather than a part.
            stdin = ("{\n" + wrapped.rstrip("\n") + "\n}\n").encode()
        else:
            command = f"sh -c {shlex.quote(wrapped)}"
            stdin = data or b""
        use_held = await self.holds()

        async def once() -> Completed:
            if use_held:
                return await self.held.run(
                    command,
                    stdin=stdin,
                    hold_stdin=hold_stdin,
                    timeout_s=timeout_s,
                    max_output_bytes=max_output_bytes,
                    marker=marker,
                    on_stall=on_stall,
                )
            return await run_process(
                [*self.ssh_argv(), command],
                timeout_s=timeout_s,
                max_output_bytes=max_output_bytes,
                stdin=stdin,
                hold_stdin=hold_stdin,
                on_stall=on_stall,
                marker=marker,
            )

        result = await self.retrying(once, what, once=not retry)
        if result.timed_out and not check_timeout:
            return result
        self.raise_for_failure(result, what)
        if not result.remote_exit and not result.timed_out:
            raise errors.lost(
                result.error_text(),
                result.exit_code,
                profile=self.config.ssh_profile,
                site=self.site_id,
                what=what,
            )
        return result

    async def retrying(
        self,
        call: Callable[[], Awaitable[Completed]],
        what: str,
        *,
        scp: bool = False,
        once: bool = False,
    ) -> Completed:
        """Run ``call`` again while its connection fails or drops, up to ``retries`` times.

        With ``once``, only a connection that never opened is tried again.
        """
        attempt = 0
        while True:
            result = await call()
            code = result.exit_code
            if scp and code:
                code = 255  # scp and sftp exit 1 and pass on the connection's own message
            if (
                result.remote_exit
                or attempt >= self.config.retries
                or not errors.retryable(result.error_text(), code, once=once)
            ):
                return result
            attempt += 1
            wait = min(MAX_RETRY_WAIT_S, 2.0**attempt)
            detail = result.error_text().strip().splitlines()
            logger.warning(
                "%s on %s: the connection failed (%s); retry %d of %d in %.0f s",
                what,
                self.config.ssh_profile,
                detail[-1] if detail else "no detail",
                attempt,
                self.config.retries,
                wait,
            )
            await asyncio.sleep(wait)

    def raise_for_failure(self, result: Completed, what: str) -> None:
        """Turn an ssh failure into a typed refusal (see ``errors``)."""
        if result.timed_out:
            raise Refusal(
                RefusalReason.TIMEOUT,
                f"{what} on {self.config.ssh_profile} did not answer; relay will try again",
                site=self.site_id,
            )
        local_programs = (self.config.ssh_command, self.config.scp_command)
        # Only the spawn failure in ``run_process`` writes exactly this; a remote
        # shell's "bash: <program>: command not found" is the job's own failure.
        if (
            result.exit_code == 127
            and not result.stdout
            and result.argv
            and result.argv[0] in local_programs
            and result.stderr == f"{result.argv[0]}: command not found".encode()
        ):
            raise Refusal(
                RefusalReason.ADAPTER_UNAVAILABLE,
                programs.missing(result.argv[0]),
                site=self.site_id,
                command=result.argv[0],
            )
        refusal = errors.refusal(
            result.error_text(),
            result.exit_code,
            profile=self.config.ssh_profile,
            site=self.site_id,
            what=what,
            sign_in=self.config.interactive_sign_in,
        )
        if refusal is not None:
            raise refusal

    # copies

    async def upload(self, sources: Sequence[Path], destination: str) -> None:
        """Copy local files into a directory on the site. A dropped transfer is retried."""
        if not sources:
            return
        use_held = await self.holds()
        argv = [
            self.config.scp_command,
            "-q",
            "-p",
            *self._base()[1:],
            *[str(path) for path in sources],
            f"{self.config.ssh_profile}:{destination.rstrip('/')}/",
        ]

        async def once() -> Completed:
            if use_held:
                return await self.held.upload(sources, destination)
            return await run_process(argv, timeout_s=None, max_output_bytes=65536)

        result = await self.retrying(once, "copying", scp=True)
        if result.exit_code == 0:
            return
        result.exit_code = 255
        self.raise_for_failure(result, "copying")
        raise Refusal(
            RefusalReason.UNREACHABLE,
            f"copying to {self.config.ssh_profile} failed: {result.error_text().strip()}",
            site=self.site_id,
        )

    # interactive sign-in

    def signed_in(self) -> bool:
        """Return whether relay holds a signed-in connection to the site."""
        return self.held.signed_in()

    def forward_connection(self) -> held.Held | None:
        """Return the connection a served port rides, for a site that signs in with a code.

        Key sites forward from relay's detached forwarder instead, which outlives relay.
        """
        return self.held if self.config.interactive_sign_in else None

    def starter(self) -> Callable[[dict[str, str]], Awaitable[None]]:
        """Return the coroutine the sign-in page runs to sign in and hold the connection."""

        async def _start(env: dict[str, str]) -> None:
            await self.holds()
            merged = dict(os.environ)
            merged.update(env)
            if self.config.askpass:
                merged["SSH_ASKPASS"] = os.path.expanduser(self.config.askpass)
            try:
                await self.held.sign_in(merged)
            except held.SshFailure as failure:
                raise RuntimeError(
                    f"ssh sign-in to {self.config.ssh_profile} failed: {failure.text}"
                ) from None

        return _start

    def register_sign_in(self) -> None:
        """Offer the starter to relay's sign-in page."""
        askpass.register_sign_in(self.site_id, self.starter())

    async def close(self) -> int:
        """Close relay's connections to the site; return how many were up."""
        return await self.held.close()
