"""Lifecycle of the detached ``srun`` connector-launcher OS process.

Owns the bounded diagnostic read, graceful/forced termination, and the
background reaper thread that retains and reaps successful launchers
without persisting their PID anywhere durable.
"""

from __future__ import annotations

import subprocess
import threading
from typing import IO

from .constants import CONNECTOR_LAUNCHER_DIAGNOSTIC_BYTES

_CONNECTOR_LAUNCHER_REAPER_LOCK = threading.Lock()
_CONNECTOR_LAUNCHER_REAPER_WAKE = threading.Event()
_CONNECTOR_LAUNCHERS: set[subprocess.Popen[bytes]] = set()
_connector_launcher_reaper_thread: threading.Thread | None = None


def _read_connector_launcher_diagnostic(stream: IO[bytes]) -> str:
    """Return bounded private launcher diagnostics after the launcher has exited."""
    try:
        stream.seek(0)
        payload = stream.read(CONNECTOR_LAUNCHER_DIAGNOSTIC_BYTES + 1)
    except OSError:
        return ""
    bounded = payload[:CONNECTOR_LAUNCHER_DIAGNOSTIC_BYTES]
    return bounded.decode("utf-8", errors="replace").strip()


def _terminate_connector_launcher(process: subprocess.Popen[bytes]) -> None:
    """Boundedly terminate a detached launcher whose step identity was not proven."""
    if process.poll() is not None:
        return
    try:
        process.terminate()
        process.wait(timeout=2.0)
    except ProcessLookupError:
        return
    except subprocess.TimeoutExpired:
        process.kill()
        try:
            process.wait(timeout=2.0)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            return


def _register_connector_launcher_for_reaping(process: subprocess.Popen[bytes]) -> None:
    """Retain and reap one successful detached launcher without persisting its PID."""
    global _connector_launcher_reaper_thread
    with _CONNECTOR_LAUNCHER_REAPER_LOCK:
        _CONNECTOR_LAUNCHERS.add(process)
        if _connector_launcher_reaper_thread is None:
            _connector_launcher_reaper_thread = threading.Thread(
                target=_reap_connector_launchers,
                name="clio-relay-srun-reaper",
                daemon=True,
            )
            _connector_launcher_reaper_thread.start()
    _CONNECTOR_LAUNCHER_REAPER_WAKE.set()


def _reap_connector_launchers() -> None:
    """Bound one daemon reaper to all provider-detached launcher processes."""
    while True:
        _CONNECTOR_LAUNCHER_REAPER_WAKE.wait(timeout=0.5)
        _CONNECTOR_LAUNCHER_REAPER_WAKE.clear()
        with _CONNECTOR_LAUNCHER_REAPER_LOCK:
            launchers = tuple(_CONNECTOR_LAUNCHERS)
        for launcher in launchers:
            if launcher.poll() is None:
                continue
            with _CONNECTOR_LAUNCHER_REAPER_LOCK:
                _CONNECTOR_LAUNCHERS.discard(launcher)
