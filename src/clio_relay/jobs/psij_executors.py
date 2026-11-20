"""Import PSI/J's batch executors on a host without POSIX signals.

Relay uses PSI/J's batch executors only to render scripts and the scheduler's
commands, which then run on the site. Importing any of them runs the
``psij.executors`` package, whose local executor installs a ``SIGCHLD``
handler when it loads on the main thread. Windows has no ``SIGCHLD``, so that
import fails and a relay on Windows could not submit, observe or cancel a
command job on a batch site. Relay never runs PSI/J's local executor, so on a
host without ``SIGCHLD`` it registers a placeholder for it before the import.
"""

from __future__ import annotations

import signal
import sys
import types
from typing import Any, NoReturn

LOCAL = "psij.executors.local"


class _NoLocalExecutor:
    """Stands in for PSI/J's local executor where it cannot load."""

    def __init__(self, *_args: Any, **_kwargs: Any) -> NoReturn:
        raise RuntimeError("PSI/J's local executor needs POSIX signals, which this host lacks")


def prepare() -> None:
    """Make ``psij.executors`` importable here. Call it before importing PSI/J's executors."""
    if hasattr(signal, "SIGCHLD") or LOCAL in sys.modules:
        return
    placeholder = types.ModuleType(LOCAL, "PSI/J's local executor, which this host cannot run")
    placeholder.LocalJobExecutor = _NoLocalExecutor  # pyright: ignore[reportAttributeAccessIssue]
    sys.modules[LOCAL] = placeholder
