"""Release-check-scoped streaming glue (clio-relay#275).

``release_command_stream.py`` knows how to run and observe one process
tree; this module knows how a LOCAL RELEASE CHECK uses that primitive --
validating the checkout cwd before spawning anything in it, publishing
which check is currently running (:func:`active_check`) so its live-echoed
lines are prefixed with the check id, and assembling the default
:class:`CommandRunner` ``run_local_release_validation`` uses when the caller
injects none. Split out of ``release_validation.py`` (rather than grown
in place) to keep that file's own per-check orchestration under the
file-size ratchet -- the echo-prefix contract belongs to the stream seam,
not the check sequencing.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable, Generator
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Protocol

from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import WINDOWS_LEGACY_PATH_HEADROOM, logical_filesystem_path
from clio_relay.release_command_stream import run_streaming_command


class CommandRunner(Protocol):
    """Structural shape of an injectable release-check command runner."""

    def __call__(self, command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
        """Run one command in the release checkout."""
        ...


_active_check_id: ContextVar[str] = ContextVar("_active_check_id", default="")


@contextmanager
def active_check(check_id: str) -> Generator[None]:
    """Publish ``check_id`` for the block's duration (drives live-echo prefixing)."""
    token = _active_check_id.set(check_id)
    try:
        yield
    finally:
        _active_check_id.reset(token)


def validate_checkout_cwd(cwd: Path) -> Path:
    """Return the logical checkout cwd, refusing an unsafe or overlong Windows path."""
    logical_cwd = logical_filesystem_path(cwd)
    if os.name == "nt":
        absolute_cwd = os.path.abspath(logical_cwd)
        if absolute_cwd.startswith("\\\\"):
            raise ConfigurationError(
                "release subprocess checkout paths on Windows must not use UNC; "
                "run the gate from a local checkout path"
            )
        if len(absolute_cwd) >= WINDOWS_LEGACY_PATH_HEADROOM:
            raise ConfigurationError(
                "release subprocess checkout path exceeds the verified Windows path bound; "
                "run the gate from a shorter checkout path"
            )
    return logical_cwd


def run_checkout_command(
    command: list[str],
    *,
    cwd: Path,
    timeout_seconds: float | None = None,
    echo: Callable[[str], None] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run one release-gate command in a validated checkout, streaming output.

    ``timeout_seconds=None`` waits indefinitely (pre-#275 behavior); a
    timeout's typed reason -- and any other typed degradation -- is folded
    into stderr, never a bare nonzero exit or a silent difference.
    """
    result = run_streaming_command(
        command,
        cwd=validate_checkout_cwd(cwd),
        timeout_seconds=timeout_seconds,
        echo=echo,
    )
    return result.to_completed_process()


def default_command_runner(*, check_timeout_seconds: float | None) -> CommandRunner:
    """Streaming runner: check-id-prefixed live echo, bounded so a wedged
    check self-terminates with a typed reason (clio-relay#275)."""

    def runner(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
        check_id = _active_check_id.get()
        prefix = f"[{check_id}] " if check_id else ""

        def echo(line: str) -> None:
            print(f"{prefix}{line}", file=sys.stderr, flush=True)

        return run_checkout_command(
            command, cwd=cwd, timeout_seconds=check_timeout_seconds, echo=echo
        )

    return runner
