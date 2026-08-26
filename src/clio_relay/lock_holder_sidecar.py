"""Lock-holder sidecar: names the process that holds a bounded relay lock.

iowarp/clio-relay#202: a bounded ``WorkerLifetimeLock`` acquisition that
timed out previously raised a bare "timed out acquiring worker lifetime
lock" with no way for the operator to learn WHO holds it -- forcing
``/proc/locks`` forensics to find, e.g., an owned-session ``api start``
process holding a persistent shared read lock across a bootstrap's exclusive
write request. This module is the one owner of the small side-channel
record that answers "who holds it":

- :func:`write_lock_holder_sidecar` is called by the lock owner immediately
  AFTER the real OS lock (``flock``/``LockFileEx``) is acquired. Writing the
  record is diagnostics, never synchronization: this call never raises. A
  write failure (permissions, disk full, an unwritable directory) is one
  typed log note, never an acquisition failure.
- :func:`remove_lock_holder_sidecar` is called by the lock owner on release,
  best-effort, for the same reason -- a removal failure just means a later
  reader sees a stale (dead-pid) record instead of no record, which is still
  a correct diagnosis, never a fatal condition.
- :func:`describe_lock_holder` is called ONLY on a timeout, to render one
  diagnostic line naming the holder (or explaining why none is known). It
  never raises, and it never breaks the lock -- diagnosis only. Breaking a
  lock automatically because a stale holder was found is an operator
  decision this module deliberately refuses to make.

**Sidecar file**: ``<lock_path>.holder.json`` next to the lock file itself,
written with a bounded, best-effort atomic replace (exclusive-create temp
file + ``os.replace``). Because ``flock``/``LockFileEx`` shared mode allows
multiple concurrent holders, the sidecar records only the MOST RECENT
acquirer -- enough to name a likely holder for an operator, not a complete
membership list of every shared reader.

**Liveness mechanism**: ``psutil`` is not a clio-relay dependency. POSIX
uses ``os.kill(pid, 0)``, the standard existence/permission probe (no signal
is actually delivered). Windows uses ``OpenProcess`` + ``GetExitCodeProcess``
via ``ctypes`` -- the same FFI style ``worker_lifetime_lock.py`` already
uses for ``LockFileEx``/``UnlockFileEx`` -- checked against ``STILL_ACTIVE``
(259) rather than treating a successful ``OpenProcess`` handle alone as
"alive", because a handle can still be obtained briefly for a pid that has
already exited. This was chosen over shelling out to ``tasklist`` (slower,
locale-dependent text parsing, one subprocess per liveness check) as the
single Windows liveness mechanism -- pick one, document it.

This module deliberately skips the codebase's stricter ACL-verification
atomic-write helper (``cluster_config_windows_paths.open_private_atomic_file``):
that helper raises on a permissive umask/ACL mismatch, which is the right
behavior for durable security-sensitive config but wrong here -- a
diagnostics-only sidecar holding no secrets (pid, a bounded argv summary, a
timestamp, a hostname) must degrade to "log and skip", never "raise",
regardless of the effective umask. The sidecar still inherits reasonable
privacy from the lock's own already-private (0700-owned) directory.
"""

from __future__ import annotations

import contextlib
import ctypes
import json
import logging
import os
import socket
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final, cast
from uuid import uuid4

from clio_relay.filesystem_paths import internal_filesystem_path

logger = logging.getLogger(__name__)

LOCK_HOLDER_SIDECAR_SCHEMA: Final = "clio-relay.lock-holder-sidecar.v1"
_SIDECAR_SUFFIX: Final = ".holder.json"
MAX_ARGV_SUMMARY_CHARS: Final = 512
MAX_SIDECAR_READ_BYTES: Final = 8192


def _sidecar_path(lock_path: Path) -> Path:
    """Return one lock file's sidecar path, normalized for raw filesystem I/O."""
    internal = internal_filesystem_path(lock_path, force_extended=True)
    return internal.with_name(internal.name + _SIDECAR_SUFFIX)


def _argv_summary() -> str:
    """Render one bounded, single-line summary of this process's invocation."""
    joined = (" ".join(sys.argv) or sys.executable).replace("\n", " ").replace("\r", " ")
    if len(joined) > MAX_ARGV_SUMMARY_CHARS:
        return joined[: MAX_ARGV_SUMMARY_CHARS - 1] + "…"
    return joined


@dataclass(frozen=True)
class LockHolderRecord:
    """One acquirer's identity, as recorded in (or parsed from) the sidecar file."""

    pid: int
    argv_summary: str
    acquired_at: str
    hostname: str


def _record_now() -> LockHolderRecord:
    """Capture this process's identity at the moment it acquired the lock."""
    return LockHolderRecord(
        pid=os.getpid(),
        argv_summary=_argv_summary(),
        acquired_at=datetime.now(UTC).isoformat(),
        hostname=socket.gethostname(),
    )


def _record_to_bytes(record: LockHolderRecord) -> bytes:
    payload = {
        "schema_version": LOCK_HOLDER_SIDECAR_SCHEMA,
        "pid": record.pid,
        "argv_summary": record.argv_summary,
        "acquired_at": record.acquired_at,
        "hostname": record.hostname,
    }
    return (json.dumps(payload, indent=2) + "\n").encode("utf-8")


def write_lock_holder_sidecar(lock_path: Path) -> None:
    """Best-effort record that this process just acquired ``lock_path``.

    Call ONLY after the real OS lock is held. Never raises: any failure is
    logged as one typed warning and swallowed -- a diagnostics side-write
    must never turn an already-successful acquisition into a failure
    (iowarp/clio-relay#202 design point 3).
    """
    sidecar_path = _sidecar_path(lock_path)
    temporary = sidecar_path.with_name(f".{sidecar_path.name}.{os.getpid()}.{uuid4().hex}.tmp")
    try:
        payload = _record_to_bytes(_record_now())
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0)
        descriptor = os.open(temporary, flags, 0o600)
        try:
            os.write(descriptor, payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, sidecar_path)
    except OSError as exc:
        logger.warning(
            "clio-relay: lock_holder_sidecar_write_failed lock=%s reason=%s",
            lock_path,
            exc,
        )
    finally:
        with contextlib.suppress(OSError):
            temporary.unlink(missing_ok=True)


def remove_lock_holder_sidecar(lock_path: Path) -> None:
    """Best-effort removal of the sidecar written by :func:`write_lock_holder_sidecar`.

    Never raises. A removal failure leaves a record that a later reader will
    correctly diagnose as a dead-pid "stale holder" once this process exits
    -- never a fatal condition for release.
    """
    sidecar_path = _sidecar_path(lock_path)
    try:
        sidecar_path.unlink(missing_ok=True)
    except OSError as exc:
        logger.warning(
            "clio-relay: lock_holder_sidecar_remove_failed lock=%s reason=%s",
            lock_path,
            exc,
        )


def _parse_record(raw: bytes) -> LockHolderRecord | None:
    """Parse a sidecar payload, returning ``None`` for any unreadable shape.

    Deliberately strict (a genuine JSON int/string per field, not a coerced
    numeric string) rather than the codebase's usual *loose* payload
    coercion (``runtime_metadata_coercion.py``): the only legitimate writer
    is :func:`write_lock_holder_sidecar`, which always emits these exact
    JSON types, so a mismatch is itself evidence of a foreign or corrupt
    file -- the right response is "holder record unreadable", never a
    best-effort coercion of untrusted shape.
    """
    try:
        decoded = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(decoded, dict):
        return None
    payload = cast(dict[str, Any], decoded)
    pid = payload.get("pid")
    argv_summary = payload.get("argv_summary")
    acquired_at = payload.get("acquired_at")
    hostname = payload.get("hostname")
    if (
        not isinstance(pid, int)
        or isinstance(pid, bool)
        or pid <= 0
        or not isinstance(argv_summary, str)
        or not isinstance(acquired_at, str)
        or not isinstance(hostname, str)
    ):
        return None
    return LockHolderRecord(
        pid=pid,
        argv_summary=argv_summary,
        acquired_at=acquired_at,
        hostname=hostname,
    )


def _posix_pid_alive(pid: int) -> bool | None:
    """Probe POSIX pid liveness with a signal-0 kill (no signal is delivered).

    ``ESRCH`` means the pid does not exist (dead). ``EPERM`` means it exists
    but is owned by another user (alive). Any other ``OSError`` is an
    unresolved result -- the caller must not treat it as a liveness answer.
    """
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return None
    return True


def _windows_pid_alive(pid: int) -> bool | None:
    """Probe Windows pid liveness via ``OpenProcess`` + ``GetExitCodeProcess``.

    See the module docstring for why this mechanism was chosen over
    ``tasklist`` or the (absent) ``psutil`` dependency.
    """
    from ctypes import wintypes

    process_query_limited_information = 0x1000
    still_active = 259
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    open_process = kernel32.OpenProcess
    open_process.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    open_process.restype = wintypes.HANDLE
    handle = open_process(process_query_limited_information, False, pid)
    if not handle:
        return False
    try:
        get_exit_code = kernel32.GetExitCodeProcess
        get_exit_code.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        get_exit_code.restype = wintypes.BOOL
        exit_code = wintypes.DWORD()
        if not get_exit_code(handle, ctypes.byref(exit_code)):
            return None
        return exit_code.value == still_active
    finally:
        kernel32.CloseHandle(handle)


def _pid_alive(pid: int) -> bool | None:
    """Return ``True``/``False`` for a resolved liveness answer, ``None`` if unknown."""
    if pid <= 0:
        return None
    if os.name == "nt":
        return _windows_pid_alive(pid)
    return _posix_pid_alive(pid)


def describe_lock_holder(lock_path: Path) -> str:
    """Render one diagnostic line naming (or explaining the absence of) a holder.

    Never raises -- this runs on the timeout path of a bounded lock
    acquisition and must not itself become a new failure mode. Three
    families of result (iowarp/clio-relay#202 pinned wording):

    - no sidecar file: "no holder record (...)"
    - sidecar present, recorded pid confirmed dead: "stale holder pid N (dead) ..."
    - sidecar present, recorded pid confirmed alive: "held by pid N (...) ..."

    A corrupt/foreign-shaped sidecar, or an unresolved liveness probe, folds
    into a clearly labeled fragment rather than crashing or fabricating a
    definite answer.
    """
    try:
        return _describe_lock_holder(lock_path)
    except Exception as exc:  # noqa: BLE001 - diagnostics must never crash the caller
        logger.warning(
            "clio-relay: lock_holder_diagnostic_failed lock=%s reason=%s",
            lock_path,
            exc,
        )
        return "holder record unreadable"


def _describe_lock_holder(lock_path: Path) -> str:
    sidecar_path = _sidecar_path(lock_path)
    try:
        raw = sidecar_path.read_bytes()
    except FileNotFoundError:
        return "no holder record (lock predates the sidecar or the holder crashed before recording)"
    except OSError as exc:
        logger.warning(
            "clio-relay: lock_holder_sidecar_read_failed lock=%s reason=%s",
            lock_path,
            exc,
        )
        return "holder record unreadable"

    if len(raw) > MAX_SIDECAR_READ_BYTES:
        return "holder record unreadable"
    record = _parse_record(raw)
    if record is None:
        return "holder record unreadable"

    liveness = _pid_alive(record.pid)
    if liveness is True:
        return (
            f"held by pid {record.pid} ({record.argv_summary}) "
            f"since {record.acquired_at} on host {record.hostname}"
        )
    if liveness is False:
        return (
            f"stale holder pid {record.pid} (dead) since {record.acquired_at} "
            "-- the sidecar was not cleaned up; the lock may be orphaned"
        )
    return (
        f"holder pid {record.pid} liveness could not be determined "
        f"({record.argv_summary}, since {record.acquired_at} on host {record.hostname})"
    )
