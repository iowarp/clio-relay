"""Bounded local-command execution substrate for owned-session SSH calls (#231 rework).

Extracted from ``session_lifecycle.py``: the typed bounded-process result, the
three remote-command outcome exceptions (deadline / rejected / ambiguous), and
the bounding wrapper around ``run_bounded_process``. Both the SSH script
transport (``session_remote_scripts.py``) and the start-query facade
(``session_start_query.py``) build on this, plus every still-resident
remote_session_* entry point in ``session_lifecycle.py`` itself.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from clio_relay.errors import BoundedCommandTimeout as _BoundedCommandTimeout
from clio_relay.errors import RelayError
from clio_relay.session_wire_models import OwnedSessionStartRejection


@dataclass(frozen=True)
class _BoundedCommandResult:
    """Bounded output captured from one local child command."""

    returncode: int
    stdout: bytes
    stderr: bytes


class _RemoteSessionCommandDeadline(RelayError):
    """The local transport deadline expired without proving remote completion."""


class _RemoteSessionCommandRejected(RelayError):
    """The authenticated remote command rejected this invocation."""

    def __init__(self, rejection: OwnedSessionStartRejection) -> None:
        super().__init__(rejection.error)
        self.rejection = rejection


class _RemoteSessionCommandAmbiguous(RelayError):
    """The SSH transport ended without proving whether the remote command completed."""


def _run_bounded_command(
    command: list[str],
    *,
    input_bytes: bytes = b"",
    timeout_seconds: float,
    stdout_limit: int,
    stderr_limit: int,
    environment: dict[str, str] | None = None,
) -> _BoundedCommandResult:
    """Run one isolated process tree while bounding both pipes before allocation."""
    from clio_relay.bounded_process import (
        BoundedProcessError,
        BoundedProcessOutputLimit,
        BoundedProcessTimeout,
        run_bounded_process,
    )

    try:
        result = run_bounded_process(
            command,
            environment=environment,
            input_bytes=input_bytes,
            timeout_seconds=timeout_seconds,
            stdout_maximum_bytes=stdout_limit,
            stderr_maximum_bytes=stderr_limit,
            require_enforceable=os.name == "nt",
        )
    except BoundedProcessTimeout as exc:
        raise _BoundedCommandTimeout(
            f"bounded command timed out after {timeout_seconds:g} seconds"
        ) from exc
    except BoundedProcessOutputLimit as exc:
        raise RelayError("bounded command output exceeded its byte limit") from exc
    except BoundedProcessError as exc:
        raise RelayError(f"bounded command process-tree cleanup failed: {exc}") from exc
    return _BoundedCommandResult(
        returncode=result.returncode,
        stdout=result.stdout.encode("utf-8"),
        stderr=result.stderr.encode("utf-8"),
    )
