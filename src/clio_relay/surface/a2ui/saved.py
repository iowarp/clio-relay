"""The SSH hosts the person already has, for the add dialog's Saved tab.

Relay reads OpenSSH's own config: ``~/.ssh/config`` and every file it
``Include``s. A host application that keeps its own saved hosts in a file of
that format names it in :data:`SAVED_ENV`, and those hosts form their own
group. Patterns (``*``, ``?``, ``!``) are not hosts, so they are left out.
Nothing here writes or runs anything.
"""

from __future__ import annotations

import glob
import os
import shlex
from dataclasses import dataclass
from pathlib import Path

SAVED_ENV = "CLIO_RELAY_SAVED_SSH_CONFIG"
"""A file of OpenSSH config whose hosts the host application saved for the person."""

_MAX_FILES = 32


@dataclass(frozen=True)
class SshHost:
    """One ``Host`` alias and what it reaches."""

    alias: str
    hostname: str | None = None
    user: str | None = None


def _ssh_dir() -> Path:
    return Path.home() / ".ssh"


def _words(line: str) -> tuple[str, list[str]]:
    text = line.strip()
    if not text or text.startswith("#"):
        return "", []
    key, _, rest = text.replace("=", " ", 1).partition(" ")
    try:
        values = shlex.split(rest, comments=True, posix=True)
    except ValueError:
        values = rest.split()
    return key.lower(), values


def _include(pattern: str, base: Path) -> list[Path]:
    expanded = os.path.expanduser(pattern)
    if not os.path.isabs(expanded):
        expanded = str(base / expanded)
    return [Path(item) for item in sorted(glob.glob(expanded))]


def hosts(path: Path, seen: set[Path] | None = None) -> list[SshHost]:
    """Return the hosts ``path`` defines, following its ``Include`` lines, in file order."""
    visited: set[Path] = seen if seen is not None else set[Path]()
    try:
        resolved = path.resolve()
    except OSError:
        return []
    if resolved in visited or len(visited) >= _MAX_FILES:
        return []
    visited.add(resolved)
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    found: list[SshHost] = []
    current: list[str] = []
    facts: dict[str, str] = {}

    def close() -> None:
        found.extend(
            SshHost(alias=alias, hostname=facts.get("hostname"), user=facts.get("user"))
            for alias in current
        )

    for line in lines:
        key, values = _words(line)
        if key == "include":
            for item in values:
                for included in _include(item, _ssh_dir()):
                    found.extend(hosts(included, visited))
        elif key in ("host", "match"):
            close()
            facts = {}
            current = (
                [value for value in values if not any(mark in value for mark in "*?!")]
                if key == "host"
                else []
            )
        elif key in ("hostname", "user") and values and key not in facts:
            facts[key] = values[0]
    close()
    unique: dict[str, SshHost] = {}
    for host in found:
        unique.setdefault(host.alias, host)
    return list(unique.values())


def saved_file(environ: dict[str, str] | None = None) -> Path | None:
    """Return the host application's saved-hosts file, when it names one."""
    value = (environ if environ is not None else os.environ).get(SAVED_ENV, "").strip()
    return Path(os.path.expanduser(value)) if value else None


def known_hosts(environ: dict[str, str] | None = None) -> tuple[list[SshHost], list[SshHost]]:
    """Return the saved hosts and the other ``~/.ssh/config`` hosts, each alias once."""
    saved_path = saved_file(environ)
    saved = hosts(saved_path) if saved_path else []
    names = {host.alias for host in saved}
    others = [host for host in hosts(_ssh_dir() / "config") if host.alias not in names]
    return saved, others
