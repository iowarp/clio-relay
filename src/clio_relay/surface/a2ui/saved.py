"""The SSH hosts the person already has, for the add dialog's Saved tab.

Relay reads OpenSSH's own config: ``~/.ssh/config`` and every file it
``Include``s (see :mod:`clio_relay.adapters.ssh.hosts`). A host application that
keeps its own saved hosts in a file of that format names it in
:data:`SAVED_ENV`, and those hosts form their own group. Nothing here writes or
runs anything.
"""

from __future__ import annotations

import os
from pathlib import Path

from clio_relay.adapters.ssh import hosts as ssh_hosts
from clio_relay.adapters.ssh.hosts import SshHost, hosts

__all__ = ["SAVED_ENV", "SshHost", "hosts", "known_hosts", "saved_file"]

SAVED_ENV = "CLIO_RELAY_SAVED_SSH_CONFIG"
"""A file of OpenSSH config whose hosts the host application saved for the person."""


def saved_file(environ: dict[str, str] | None = None) -> Path | None:
    """Return the host application's saved-hosts file, when it names one."""
    value = (environ if environ is not None else os.environ).get(SAVED_ENV, "").strip()
    return Path(os.path.expanduser(value)) if value else None


def known_hosts(environ: dict[str, str] | None = None) -> tuple[list[SshHost], list[SshHost]]:
    """Return the saved hosts and the other ``~/.ssh/config`` hosts, each alias once."""
    saved_path = saved_file(environ)
    saved = hosts(saved_path) if saved_path else []
    names = {host.alias for host in saved}
    others = [host for host in hosts(ssh_hosts.ssh_dir() / "config") if host.alias not in names]
    return saved, others
