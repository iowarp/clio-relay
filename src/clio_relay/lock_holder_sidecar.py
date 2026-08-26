"""Lock-holder sidecar: names the process(es) that hold a bounded relay lock.

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
  write failure (permissions, disk full, an unwritable directory, a
  transient Windows sharing conflict that outlasts the bounded retry) is
  one typed log note, never an acquisition failure.
- :func:`remove_lock_holder_sidecar` is called by the lock owner on release,
  best-effort, for the same reason -- a removal failure just leaves this
  process's own entry to be correctly diagnosed as a dead-pid "stale
  holder" once it exits, never a fatal condition for release.
- :func:`describe_lock_holder` is called ONLY on a timeout, to render one
  diagnostic line naming the holder(s) (or explaining why none is known).
  It never raises, and it never breaks the lock -- diagnosis only. Breaking
  a lock automatically because a stale holder was found is an operator
  decision this module deliberately refuses to make.

**Per-holder directory (clio-relay#202 review D2)**: shared-mode locks
allow multiple CONCURRENT holders -- shared mode is this feature's home
ground (``endpoint.py``'s worker + ``storage_runtime.py``'s seal handoff;
#202's own incident was an ``api start`` shared holder blocking a
bootstrap exclusive request). A single shared slot is INCOHERENT there:
whichever holder wrote last "owns" it, so an earlier releaser deletes a
still-live co-holder's record ("no holder record" -- false), and a
crashed co-holder's leftover record renders "stale... may be orphaned"
even while another holder legitimately holds the lock. The fix is a
directory, ``<lock_path>.holders/``, one file per pid: each acquirer
writes and later removes ONLY its own file. :func:`describe_lock_holder`
renders every live entry (an exclusive lock is simply the one-entry case,
no special casing needed); a dead entry is named individually as "stale",
but "may be orphaned" is asserted only when EVERY readable entry is
confirmed dead -- never while a live holder remains.

**Liveness mechanism**: ``psutil`` is not a clio-relay dependency. POSIX
uses ``os.kill(pid, 0)`` (no signal delivered): ``ESRCH`` = dead, ``EPERM``
= alive under another user. Windows uses ``OpenProcess`` +
``GetExitCodeProcess`` via ``ctypes`` -- the same FFI style
``worker_lifetime_lock.py`` uses for ``LockFileEx``/``UnlockFileEx`` --
checked against ``STILL_ACTIVE`` (259) rather than treating a successful
``OpenProcess`` handle alone as "alive" (a handle can be obtained briefly
after exit). Chosen over shelling out to ``tasklist`` (slower,
locale-dependent parsing, one subprocess per check). Two imprecisions,
both inherent to Win32 rather than this module's choices: (1)
``OpenProcess`` failing is not simply "dead" -- ``ERROR_ACCESS_DENIED``
(5) means a real, inspection-restricted process (mirrors POSIX ``EPERM``,
alive); only ``ERROR_INVALID_PARAMETER`` (87) means dead; any other error
is unresolved (clio-relay#202 D1). (2) a process exiting with status 259
would misread as ``STILL_ACTIVE`` -- unusual, and a standing Win32
property, not new here.

This module skips the codebase's stricter ACL-verification atomic-write
helper (``cluster_config_windows_paths.open_private_atomic_file``, which
raises on a permissive umask/ACL mismatch -- right for durable
security-sensitive config, wrong for a diagnostics-only record holding no
secrets, which must "log and skip", never "raise"), and skips ``fsync`` on
every write (best-effort diagnostic, not durable state; the syscall cost
is unwarranted on the hot acquisition path). The holders directory still
inherits reasonable privacy from the lock's own already-private
(0700-owned) parent directory.
"""

from __future__ import annotations

import contextlib
import ctypes
import json
import logging
import os
import socket
import stat
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final, cast
from uuid import uuid4

from clio_relay.filesystem_paths import internal_filesystem_path

logger = logging.getLogger(__name__)

LOCK_HOLDER_SIDECAR_SCHEMA: Final = "clio-relay.lock-holder-sidecar.v1"
_HOLDERS_DIR_SUFFIX: Final = ".holders"
MAX_ARGV_SUMMARY_CHARS: Final = 512
MAX_SIDECAR_READ_BYTES: Final = 8192
#: Defense-in-depth bound on how many holder files one diagnostic scan will
#: read (clio-relay#202 D7 applied the same "never unbounded" discipline to
#: entry COUNT, not just per-file size). Ordinary use is 1-3 concurrent
#: holders; this only guards a pathological/corrupted directory.
MAX_HOLDER_ENTRIES: Final = 64
_RETRY_ATTEMPTS: Final = 3
_RETRY_DELAY_SECONDS: Final = 0.01
_STALE_TEMP_FILE_AGE_SECONDS: Final = 3600
_NO_HOLDER_RECORD_MESSAGE: Final = (
    "no holder record (lock predates the sidecar or the holder crashed before recording)"
)


def _holders_dir(lock_path: Path) -> Path:
    """Return one lock file's per-holder directory, normalized for raw filesystem I/O."""
    internal = internal_filesystem_path(lock_path, force_extended=True)
    return internal.with_name(internal.name + _HOLDERS_DIR_SUFFIX)


def _argv_summary() -> str:
    """Render one bounded, single-line summary of this process's invocation."""
    joined = (" ".join(sys.argv) or sys.executable).replace("\n", " ").replace("\r", " ")
    if len(joined) > MAX_ARGV_SUMMARY_CHARS:
        return joined[: MAX_ARGV_SUMMARY_CHARS - 1] + "…"
    return joined


@dataclass(frozen=True)
class LockHolderRecord:
    """One acquirer's identity, as recorded in (or parsed from) its holder file."""

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


def _create_exclusive(path: Path, payload: bytes) -> None:
    """Create one new file containing ``payload``, refusing to clobber an existing one."""
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        os.write(descriptor, payload)
    finally:
        os.close(descriptor)


def _replace_with_retry(source: Path, target: Path) -> None:
    """Atomically replace ``target`` with ``source``, retrying a transient Windows conflict.

    A concurrent :func:`describe_lock_holder` reader can hold a brief open
    handle on ``target`` on Windows, where ``os.replace`` (``MoveFileExW``)
    can fail with a transient sharing violation if that handle predates
    ``FILE_SHARE_DELETE`` semantics (clio-relay#202 D4, reproduced live:
    WinError 32 on replace, WinError 5 on unlink). A short bounded retry
    clears the ordinary case; a handle still held after ``_RETRY_ATTEMPTS``
    propagates to the caller's typed log note, never an acquisition failure.
    """
    for attempt in range(_RETRY_ATTEMPTS):
        try:
            os.replace(source, target)
            return
        except OSError:
            if attempt + 1 >= _RETRY_ATTEMPTS:
                raise
            time.sleep(_RETRY_DELAY_SECONDS)


def _unlink_with_retry(path: Path) -> None:
    """Remove ``path`` if present, retrying the same transient Windows conflict."""
    for attempt in range(_RETRY_ATTEMPTS):
        try:
            path.unlink(missing_ok=True)
            return
        except OSError:
            if attempt + 1 >= _RETRY_ATTEMPTS:
                raise
            time.sleep(_RETRY_DELAY_SECONDS)


def _sweep_stale_temp_files(holders_dir: Path) -> None:
    """Best-effort cleanup of leftover temp files from a prior crashed write.

    Bounded to this module's own temp-file naming pattern
    (``.<pid>.<uuid>.tmp``), and only once they are old enough
    (``_STALE_TEMP_FILE_AGE_SECONDS``) to be confident they are not a
    concurrent holder's in-flight write. Any individual failure is
    swallowed -- this is housekeeping, not correctness, and must never be
    why an acquisition fails.
    """
    try:
        entries = list(holders_dir.iterdir())
    except OSError:
        return
    cutoff = time.time() - _STALE_TEMP_FILE_AGE_SECONDS
    for entry in entries:
        if not (entry.name.startswith(".") and entry.name.endswith(".tmp")):
            continue
        try:
            if entry.stat().st_mtime < cutoff:
                entry.unlink(missing_ok=True)
        except OSError:
            continue


def write_lock_holder_sidecar(lock_path: Path) -> None:
    """Best-effort record that this process holds ``lock_path`` right now.

    Writes THIS process's own entry, ``<lock_path>.holders/<pid>.json`` --
    never touches a co-holder's file (clio-relay#202 D2). Call ONLY after
    the real OS lock is held. Never raises: any failure is logged as one
    typed warning and swallowed -- a diagnostics side-write must never turn
    an already-successful acquisition into a failure.
    """
    pid = os.getpid()
    try:
        holders_dir = _holders_dir(lock_path)
        # 0o700 matches the private core-dir discipline (house standard; the
        # default umask mode was flagged in review residual 4). No-op on NT.
        holders_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        target = holders_dir / f"{pid}.json"
        temporary = holders_dir / f".{pid}.{uuid4().hex}.tmp"
        payload = _record_to_bytes(_record_now())
        _create_exclusive(temporary, payload)
        try:
            _replace_with_retry(temporary, target)
        finally:
            with contextlib.suppress(OSError):
                temporary.unlink(missing_ok=True)
        _sweep_stale_temp_files(holders_dir)
    except (OSError, ValueError) as exc:
        logger.warning(
            "clio-relay: lock_holder_sidecar_write_failed lock=%s reason=%s",
            lock_path,
            exc,
        )


def remove_lock_holder_sidecar(lock_path: Path) -> None:
    """Best-effort removal of THIS process's own holder entry, on release.

    Never touches a co-holder's file, so one holder releasing first can
    never blind the diagnostic to another holder that still legitimately
    holds the lock (clio-relay#202 D2). Never raises.
    """
    pid = os.getpid()
    try:
        target = _holders_dir(lock_path) / f"{pid}.json"
        _unlink_with_retry(target)
    except (OSError, ValueError) as exc:
        logger.warning(
            "clio-relay: lock_holder_sidecar_remove_failed lock=%s reason=%s",
            lock_path,
            exc,
        )


def _parse_record(raw: bytes) -> LockHolderRecord | None:
    """Parse one holder file's payload, returning ``None`` for any unreadable shape.

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


def _os_read(descriptor: int, size: int) -> bytes:
    """``os.read`` seam, isolated so a test can assert the bounded read size directly."""
    return os.read(descriptor, size)


def _read_bounded_no_follow(path: Path) -> bytes | None:
    """Read one holder file, bounded and symlink-refusing (clio-relay#202 D7).

    A plain ``Path.read_bytes()`` follows symlinks and has no size bound --
    a symlinked FIFO would hang a read forever, and this runs on the
    timeout path of an ALREADY bounded lock acquisition. ``O_NOFOLLOW``
    refuses a symlink outright (surfaces as ``OSError``, folded into
    "unreadable" by the caller); the regular-file check refuses a
    FIFO/device/socket even on a platform without ``O_NOFOLLOW``; the
    single, capped :func:`_os_read` call never pulls more than
    ``MAX_SIDECAR_READ_BYTES`` + 1 bytes into memory regardless of the
    file's real size. Returns ``None`` for a non-regular-file target
    (skip, not an error) or a payload over the cap (rejected as
    unreadable).
    """
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(path, flags)
    try:
        file_stat = os.fstat(descriptor)
        if not stat.S_ISREG(file_stat.st_mode):
            return None
        raw = _os_read(descriptor, MAX_SIDECAR_READ_BYTES + 1)
    finally:
        os.close(descriptor)
    if len(raw) > MAX_SIDECAR_READ_BYTES:
        return None
    return raw


def _read_holder_record(path: Path) -> tuple[LockHolderRecord | None, str]:
    """Read and parse one holder file, tolerating disappearance and corruption.

    Returns ``(record, "ok")`` for a clean record, ``(None, "vanished")`` when
    the file disappeared between listing and read (the holder released
    mid-scan -- counted toward "no holder record", NOT "unreadable"), and
    ``(None, "unreadable")`` for a corrupt/oversize/unreadable file
    (clio-relay#202 review residual 2: the two absences mean different
    things to an operator).
    """
    try:
        raw = _read_bounded_no_follow(path)
    except FileNotFoundError:
        return None, "vanished"
    except OSError as exc:
        logger.warning(
            "clio-relay: lock_holder_sidecar_read_failed lock=%s reason=%s",
            path,
            exc,
        )
        return None, "unreadable"
    if raw is None:
        return None, "unreadable"
    record = _parse_record(raw)
    if record is None:
        return None, "unreadable"
    return record, "ok"


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


def _win32_open_process(pid: int) -> tuple[int, int]:
    """``OpenProcess`` syscall seam: returns ``(handle, last_error)``.

    Isolated as its own function (rather than inlined in
    :func:`_windows_pid_alive`) so a test can monkeypatch exactly this
    Win32 boundary -- e.g. to simulate ``ERROR_ACCESS_DENIED`` (clio-relay
    #202 D1) without a real protected-process pid, which is not reliably
    obtainable in a test environment. ``GetLastError()`` is read HERE,
    immediately after the call that set it: ctypes' ``use_last_error``
    error slot reflects only the most recently made such call, so reading
    it later risks reading a different function's stored error.
    """
    from ctypes import wintypes

    process_query_limited_information = 0x1000
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    open_process = kernel32.OpenProcess
    open_process.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    open_process.restype = wintypes.HANDLE
    handle = open_process(process_query_limited_information, False, pid)
    return handle, ctypes.get_last_error()


def _win32_get_exit_code(handle: int) -> int | None:
    """``GetExitCodeProcess`` syscall seam; ``None`` when the call itself fails."""
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_exit_code = kernel32.GetExitCodeProcess
    get_exit_code.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    get_exit_code.restype = wintypes.BOOL
    exit_code = wintypes.DWORD()
    if not get_exit_code(handle, ctypes.byref(exit_code)):
        return None
    return exit_code.value


def _win32_close_handle(handle: int) -> None:
    """``CloseHandle`` syscall seam."""
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CloseHandle(handle)


def _windows_pid_alive(pid: int) -> bool | None:
    """Probe Windows pid liveness via ``OpenProcess`` + ``GetExitCodeProcess``.

    See the module docstring for why this mechanism was chosen and for the
    ``STILL_ACTIVE`` ambiguity it inherits from the Win32 API itself.

    ``OpenProcess`` failing is not simply "dead": ``ERROR_ACCESS_DENIED``
    (5) means the pid names a real, running, inspection-restricted process
    (mirrors the POSIX ``EPERM`` case above) -- alive, not dead.
    ``ERROR_INVALID_PARAMETER`` (87) means no such pid exists -- dead. Any
    other error is unresolved (clio-relay#202 D1: the prior version treated
    every ``OpenProcess`` failure as "dead", misdiagnosing an
    access-denied protected pid as exited).
    """
    error_access_denied = 5
    error_invalid_parameter = 87
    still_active = 259

    handle, error = _win32_open_process(pid)
    if not handle:
        if error == error_access_denied:
            return True
        if error == error_invalid_parameter:
            return False
        return None
    try:
        exit_code = _win32_get_exit_code(handle)
        if exit_code is None:
            return None
        return exit_code == still_active
    finally:
        _win32_close_handle(handle)


def _pid_alive(pid: int) -> bool | None:
    """Return ``True``/``False`` for a resolved liveness answer, ``None`` if unknown."""
    if pid <= 0:
        return None
    if os.name == "nt":
        return _windows_pid_alive(pid)
    return _posix_pid_alive(pid)


def _live_fragment(record: LockHolderRecord) -> str:
    return (
        f"held by pid {record.pid} ({record.argv_summary}) "
        f"since {record.acquired_at} on host {record.hostname}"
    )


def _dead_fragment(record: LockHolderRecord) -> str:
    return f"stale holder pid {record.pid} (dead) since {record.acquired_at}"


def _unresolved_fragment(record: LockHolderRecord) -> str:
    return (
        f"holder pid {record.pid} liveness could not be determined "
        f"({record.argv_summary}, since {record.acquired_at} on host {record.hostname})"
    )


def describe_lock_holder(lock_path: Path) -> str:
    """Render one diagnostic line naming (or explaining the absence of) the holder(s).

    Never raises -- this runs on the timeout path of a bounded lock
    acquisition and must not itself become a new failure mode. Four
    families of result (clio-relay#202 pinned wording, extended by review
    D2 for the multi-holder case):

    - no holders directory, or one with no readable entries: "no holder
      record (...)"
    - every readable entry confirmed dead: the stale fragment(s), joined,
      with the "may be orphaned" verdict appended ONCE
    - at least one entry confirmed alive (or unresolved): that entry's
      "held by pid N (...) ..." line -- the ordinary exclusive-lock case is
      simply the one-entry rendering of this, unchanged from before. Any
      co-occurring dead entry is still named, individually, but WITHOUT the
      orphaned verdict -- a live holder means the lock is not orphaned.
    - a directory that cannot even be listed, or every entry present but
      unparseable: "holder record unreadable"

    A corrupt/foreign-shaped entry, or an unresolved liveness probe, folds
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
    holders_dir = _holders_dir(lock_path)
    try:
        entries = sorted(holders_dir.iterdir())
    except FileNotFoundError:
        return _NO_HOLDER_RECORD_MESSAGE
    except OSError as exc:
        logger.warning(
            "clio-relay: lock_holder_sidecar_listing_failed lock=%s reason=%s",
            lock_path,
            exc,
        )
        return "holder record unreadable"

    candidate_files = [
        entry for entry in entries if entry.suffix == ".json" and not entry.name.startswith(".")
    ]
    if not candidate_files:
        return _NO_HOLDER_RECORD_MESSAGE
    truncated = len(candidate_files) > MAX_HOLDER_ENTRIES
    candidate_files = candidate_files[:MAX_HOLDER_ENTRIES]

    live_fragments: list[str] = []
    dead_fragments: list[str] = []
    unresolved_fragments: list[str] = []
    readable_count = 0
    unreadable_count = 0
    for holder_file in candidate_files:
        record, outcome = _read_holder_record(holder_file)
        if record is None:
            if outcome == "unreadable":
                unreadable_count += 1
            # "vanished" = the holder released mid-scan; counts toward the
            # no-record verdict below, never toward "unreadable".
            continue
        readable_count += 1
        liveness = _pid_alive(record.pid)
        if liveness is True:
            live_fragments.append(_live_fragment(record))
        elif liveness is False:
            dead_fragments.append(_dead_fragment(record))
        else:
            unresolved_fragments.append(_unresolved_fragment(record))

    if readable_count == 0:
        if unreadable_count == 0:
            return _NO_HOLDER_RECORD_MESSAGE
        return "holder record unreadable"

    truncation_note = " (+more not shown)" if truncated else ""
    not_confirmed_dead = live_fragments + unresolved_fragments
    if not_confirmed_dead:
        return "; ".join(not_confirmed_dead + dead_fragments) + truncation_note
    return (
        "; ".join(dead_fragments)
        + " -- the sidecar was not cleaned up; the lock may be orphaned"
        + truncation_note
    )
