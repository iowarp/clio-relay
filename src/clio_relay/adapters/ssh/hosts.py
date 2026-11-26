"""The hosts an OpenSSH config defines, and where an ssh profile goes.

Relay reads the config and every file it ``Include``s; it runs nothing.
Patterns (``*``, ``?``, ``!``) are not hosts, so they are left out.
"""

from __future__ import annotations

import getpass
import glob
import os
import shlex
from dataclasses import dataclass
from pathlib import Path

_MAX_FILES = 32


@dataclass(frozen=True)
class SshHost:
    """One ``Host`` alias and what it reaches."""

    alias: str
    hostname: str | None = None
    user: str | None = None


def ssh_dir() -> Path:
    """Return the person's ``~/.ssh``."""
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
                for included in _include(item, ssh_dir()):
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


def destination(profile: str, config_file: str | None = None) -> tuple[str, str]:
    """Return the host and user ``ssh <profile>`` connects to, as the config names them.

    ``user@host`` names both; a ``Host`` alias takes its ``HostName`` and ``User``.
    What neither names is ssh's own default: the alias as the host, the local user.
    The host is lowercase, without a trailing dot.
    """
    user, _, alias = profile.strip().rpartition("@")
    path = Path(os.path.expanduser(config_file)) if config_file else ssh_dir() / "config"
    found = next((host for host in hosts(path) if host.alias == alias), None)
    hostname = (found.hostname if found and found.hostname else alias).strip().lower()
    return hostname.rstrip("."), user or (found.user if found else None) or getpass.getuser()
