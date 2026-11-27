"""Find the program relay starts, the way a shell would.

Relay starts its local tools (``ssh``, ``scp``, ``frpc``, ``git``) by argv, with
no shell. On Windows ``CreateProcess`` tries only ``<name>.exe`` for a bare
name, so it skips a ``.cmd`` or ``.bat`` that sits earlier on the PATH, such as
a host's wrapper for the tool, and starts a different program. Relay therefore
looks a bare name up with ``shutil.which`` against the PATH the child gets,
honouring ``PATHEXT``, and starts the path it found. A name with a directory in
it is the person's explicit choice and is used as given.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Mapping, Sequence


def explicit(command: str) -> bool:
    """Return whether ``command`` names a path rather than a program to look up."""
    return "/" in command or os.sep in command or bool(os.altsep and os.altsep in command)


def search_path(env: Mapping[str, str] | None = None) -> str | None:
    """Return the PATH a child started with ``env`` sees (relay's own when ``env`` is None)."""
    if env is None:
        return os.environ.get("PATH")
    if os.name == "nt":
        for key, value in env.items():
            if key.upper() == "PATH":
                return value
        return None
    return env.get("PATH")


def find(command: str, env: Mapping[str, str] | None = None) -> str | None:
    """Return the program to start for ``command``, or ``None`` when the PATH has none."""
    if not command:
        return None
    expanded = os.path.expanduser(command)
    if explicit(expanded):
        return expanded
    path = search_path(env)
    return shutil.which(expanded, path=path if path is not None else os.defpath)


def resolved(argv: Sequence[str], env: Mapping[str, str] | None = None) -> list[str] | None:
    """Return ``argv`` with its program found, or ``None`` when it is not on the PATH."""
    if not argv:
        return None
    program = find(argv[0], env)
    return None if program is None else [program, *argv[1:]]


def missing(command: str) -> str:
    """Say that ``command`` could not be found, for a refusal or an error."""
    return (
        f"relay cannot find {command} on this machine's PATH; install it or configure its full path"
    )


# Windows refuses to start, or deletes, a file its antivirus flags.
ANTIVIRUS_ERRORS = frozenset({225, 226})


def blocked(exc: OSError) -> bool:
    """Return whether starting a program failed because the host's antivirus blocked it."""
    return getattr(exc, "winerror", None) in ANTIVIRUS_ERRORS


def start_failed(command: str, exc: OSError) -> str:
    """Say why ``command`` would not start, naming an antivirus block plainly."""
    if blocked(exc):
        return (
            f"this machine's antivirus blocked {command} as unwanted software; allow that "
            "program in the antivirus settings, or configure the full path of a copy it permits"
        )
    return f"cannot start {command}: {exc.strerror or exc}"
