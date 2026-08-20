"""Leaf primitives for the packaged stdio child process: capture, environment, teardown.

Extracted from :mod:`clio_relay.mcp_stdio_validation` (file-size
decomposition; see ``scripts/check_file_size.py``). This module owns the
process-adjacent primitives that have no dependency on the deadline-driven
spawn/exchange orchestration itself: the thread-safe bounded pipe capture
buffer, the least-privilege packaged launch environment builder, explicit
target-only environment validation, the pipe-reader thread body, and
process-group teardown. ``mcp_stdio_validation_process.py`` (the
orchestration owner: ``_run_bounded_process``, the staged MCP handshake)
imports every name here as its own module-level binding -- not merely
calls through -- so that ``monkeypatch.setattr`` on that orchestration
module (where ``tests/test_mcp_stdio_validation.py`` patches ``_capture_
pipe``/``_terminate_bounded_process`` while exercising ``_run_bounded_
process`` directly) reaches the exact bare-global read the orchestrator's
own body performs, regardless of where each function's real definition
lives. None of these names are re-exported from the facade module --
``_packaged_launch_environment`` is the only one called directly on the
facade module object by a test, and that call-through works unchanged via
the facade's plain forwarding import.
"""

from __future__ import annotations

import os
import subprocess
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import BinaryIO, cast

from clio_relay.errors import RelayError
from clio_relay.mcp_stdio_validation_support import _SENSITIVE_ENVIRONMENT_NAME
from clio_relay.process_containment import owner_environment, terminate_owned_process

_STREAM_READ_BYTES = 64 * 1024
_PACKAGED_BASE_ENVIRONMENT_NAMES = frozenset(
    {
        "APPDATA",
        "COMSPEC",
        "DBUS_SESSION_BUS_ADDRESS",
        "HOME",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "LOCALAPPDATA",
        "LOGNAME",
        "PATH",
        "PATHEXT",
        "PROGRAMDATA",
        "SYSTEMDRIVE",
        "SYSTEMROOT",
        "TEMP",
        "TMP",
        "TMPDIR",
        "TZ",
        "USER",
        "USERPROFILE",
        "WINDIR",
        "XDG_RUNTIME_DIR",
        "CLIO_RELAY_CLI_MODE",
        "CLIO_RELAY_CLUSTER_REGISTRY",
        "CLIO_RELAY_CORE_DIR",
        "CLIO_RELAY_REMOTE_MCP_CACHE",
        "CLIO_RELAY_SPOOL_DIR",
        "CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_JOB",
        "CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_STREAM",
        "CLIO_RELAY_STORAGE_CORE_HIGH_WATER_BYTES",
        "CLIO_RELAY_STORAGE_JOB_CORE_ALLOWANCE_BYTES",
        "CLIO_RELAY_STORAGE_JOB_RESULT_ALLOWANCE_BYTES",
        "CLIO_RELAY_STORAGE_LOCK_TIMEOUT_SECONDS",
        "CLIO_RELAY_STORAGE_MAX_JOB_RESERVATION_BYTES",
        "CLIO_RELAY_STORAGE_MAX_LEDGER_BYTES",
        "CLIO_RELAY_STORAGE_MAX_RESERVATIONS",
        "CLIO_RELAY_STORAGE_MAX_SCAN_ACCOUNTED_BYTES",
        "CLIO_RELAY_STORAGE_MAX_SCAN_DEPTH",
        "CLIO_RELAY_STORAGE_MAX_SCAN_ENTRIES",
        "CLIO_RELAY_STORAGE_MINIMUM_FREE_BYTES",
        "CLIO_RELAY_STORAGE_RUNTIME_CHECK_INTERVAL_SECONDS",
        "CLIO_RELAY_STORAGE_SPOOL_HIGH_WATER_BYTES",
        "CLIO_RELAY_STORAGE_TOTAL_HIGH_WATER_BYTES",
    }
)


@dataclass
class _BoundedPipeCapture:
    """Thread-safe bounded bytes captured from one child pipe."""

    label: str
    maximum_bytes: int
    content: bytearray = field(default_factory=bytearray)
    overflow: bool = False
    error: BaseException | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)

    def append(self, chunk: bytes) -> bool:
        """Append within the cap and return whether the stream overflowed."""
        with self.lock:
            remaining = self.maximum_bytes - len(self.content)
            if remaining > 0:
                self.content.extend(chunk[:remaining])
            if len(chunk) > remaining:
                self.overflow = True
            return self.overflow

    def snapshot(self) -> tuple[bytes, bool, BaseException | None]:
        """Return an immutable capture snapshot."""
        with self.lock:
            return bytes(self.content), self.overflow, self.error


def _packaged_launch_environment() -> dict[str, str]:
    """Build a least-privilege broker environment without ambient credentials."""
    selected = {
        name: value
        for name, value in os.environ.items()
        if name.upper() in _PACKAGED_BASE_ENVIRONMENT_NAMES
        and _SENSITIVE_ENVIRONMENT_NAME.search(name) is None
    }
    return owner_environment(selected)


def _validated_extra_environment(
    extra_environment: Mapping[str, str] | None,
) -> dict[str, str]:
    """Validate explicit target-only values before one-shot broker transport."""
    environment: dict[str, str] = {}
    if extra_environment is None:
        return environment
    for name, value in extra_environment.items():
        if not name or "=" in name or "\x00" in name or "\x00" in value:
            raise RelayError("packaged MCP child environment contained an invalid entry")
        environment[name] = value
    return environment


def _capture_pipe(
    stream: BinaryIO,
    capture: _BoundedPipeCapture,
    activity: threading.Event,
) -> None:
    try:
        while True:
            chunk = os.read(stream.fileno(), _STREAM_READ_BYTES)
            if not chunk:
                return
            overflow = capture.append(chunk)
            activity.set()
            if overflow:
                return
    except OSError as exc:
        with capture.lock:
            capture.error = exc
        activity.set()


def _terminate_bounded_process(process: subprocess.Popen[bytes]) -> None:
    terminate_owned_process(cast(subprocess.Popen[str], cast(object, process)))
