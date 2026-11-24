"""Relay's own frp visitor: the desktop end of an frp path, in Python (RL20).

It replaces the ``frpc`` binary on the desktop. Some desktop antivirus quarantines
the upstream ``frpc.exe``, and most people can't, or shouldn't, add exclusions,
so relay speaks the protocol itself and ships no frp binary for the desktop.

The boundary is deliberately small:

- In: an frpc TOML file with the subset of keys relay renders (``config.py``
  says which), with ``{{ .Envs.NAME }}`` filled from the environment as frpc
  does. Anything else is refused with a plain message, never ignored.
- Out: loopback listeners, one per visitor, and log lines on stdout in frpc's
  format, so relay reads them the way it reads frpc's.
- Run as ``python -m clio_relay.frp_visitor -c visitor.toml``, a process of its
  own, so it outlives the relay process that started it, like frpc did.

It speaks frp 0.69's v1 wire protocol to ``frps``: token login, STCP visitor
connections, heartbeats, the yamux multiplexer, and the ``tcp`` (with or without
TLS), ``websocket`` and ``wss`` transports. XTCP (the direct path) is not built
yet: an XTCP visitor with a ``fallbackTo`` is served through its STCP fallback,
and the log says so. Nothing else in relay knows these details. Relay starts it
with :func:`command`, both for a served port (``serve.forward``) and for the
worker's link (``frp_link``).
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from pathlib import Path

MODULE = "clio_relay.frp_visitor"
"""What ``python -m`` runs; a process's argv names it, so relay can tell its own."""

PROTOCOL_VERSION = "0.69.1"
"""The frp release whose wire protocol this visitor speaks."""

START_MARKER = "start relay frp visitor"
"""Logged once when the process starts, so a reader can tell a restart."""

NO_DIRECT_MARKER = "has no direct path (XTCP) yet"
"""Logged when an XTCP visitor is served through its STCP fallback."""


def command(config_path: Path | str) -> list[str]:
    """Return the argv that runs the visitor on ``config_path`` with this interpreter.

    The one way relay starts a desktop frp visitor, for a served port and for
    the worker's link alike.
    """
    return [sys.executable, "-m", MODULE, "-c", str(config_path)]


def runs_visitor(argv: Sequence[str]) -> bool:
    """Return whether ``argv`` is a process started by :func:`command`."""
    return any(argv[index] == "-m" and argv[index + 1] == MODULE for index in range(len(argv) - 1))
