"""Stale frpc visitor reconciliation: reap prior-process orphans, sweep crumb dirs.

iowarp/clio-relay#285: a ``brokered_tcp``/``udp_rendezvous`` connection's held
frpc visitor (:class:`~clio_relay.frp_link.HeldFrpVisitor`) has no
self-tearing-down tether the way ``ssh_forward``'s held stdin pipe does
(``control_channel.py``'s own module docstring) -- nothing closes it when the
owning CLI process exits. The atexit hook
(:mod:`clio_relay.remote_connection_registry`) covers the ordinary-exit case;
this module covers what an atexit hook structurally cannot -- a ``kill -9``'d
or crashed CLI leaves its visitor process (and, on the crash path, its
emptied-out rendered-config temp dir shell) with no owner, and no atexit hook
ever runs for a process that was signaled, not asked to exit.

:func:`reconcile_stale_frp_visitors` is the one entry point production code
calls, at the START of every new visitor spawn
(``frp_transport._FrpChannelTransport.establish``, before the new visitor is
even rendered). It takes ONE process-table snapshot
(:func:`default_process_snapshot`, cross-platform, the injectable seam every
test in this module's own suite replaces with fake
:class:`ProcessRecord` rows) and:

* :func:`reap_stale_frp_visitors` reaps every candidate row whose cmdline
  carries both this run's configured ``frpc_bin`` and the
  ``HeldFrpVisitor.establish``-rendered config's directory-naming convention
  (:data:`~clio_relay.frp_link.VISITOR_CONFIG_DIR_PREFIX` in its ``-c`` path),
  AND whose parent pid is absent from that SAME snapshot -- a live parent
  means a concurrent CLI still legitimately holds that visitor, and it is
  never touched (structural fact, not a heuristic: reaping never depends on
  anything but the one snapshot's own pid/parent-pid/cmdline columns).
* :func:`sweep_stale_visitor_config_dirs` removes empty
  ``clio-relay-frp-visitor-*`` directories older than a bound from the OS
  temp root -- the crash-path residual ``HeldFrpVisitor.close`` never got a
  chance to run for (the config *file* inside it, which carries the
  plaintext frp token/stcp secret, is gone either way by the time this
  fires: either a normal close removed the whole directory already, or this
  module's own reap above removed the file when it killed the process but
  left the now-empty directory for THIS sweep to remove next).

Both cross-platform process-inspection primitives here (POSIX ``/proc``,
Windows ``Get-CimInstance Win32_Process``) mirror the established pattern
``service_runtime_connector_identity.py`` already uses for the identical "is
this connector's process tree still owned" question -- not ``psutil`` (not a
clio-relay dependency; see ``pyproject.toml``) and not a hand-rolled WMI
query, but the SAME ``Get-CimInstance Win32_Process | Select-Object
ProcessId,ParentProcessId,CommandLine`` PowerShell subprocess that module's
``_windows_connector_descendants`` already runs, bounded by the same kind of
wall-clock timeout.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import tempfile
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Final, cast

from clio_relay.frp_link import VISITOR_CONFIG_DIR_PREFIX

logger = logging.getLogger(__name__)

# One-shot dial/enumeration/termination bound -- the same "must not hang a
# spawn behind a wedged OS call" discipline
# ``service_runtime_primitives._LOCAL_CLEANUP_COMMAND_TIMEOUT_SECONDS`` and
# ``process_containment_types.TERMINATION_TIMEOUT_SECONDS`` already apply to
# this exact family of process-table/kill calls, just not shared cross-module
# (each subsystem keeps its own copy; see those modules' own history).
DEFAULT_RECONCILE_TIMEOUT_SECONDS: Final = 5.0
# The Windows process enumeration below is one PowerShell subprocess for the
# WHOLE system's process table, not a single-pid probe -- give it more room
# than the per-pid kill bound above.
DEFAULT_ENUMERATION_TIMEOUT_SECONDS: Final = 20.0
# "say 1 hour" (iowarp/clio-relay#285's own fix direction): a crash-path
# empty config-dir shell younger than this might still belong to a visitor
# whose process this exact reconciliation pass has not reached the reap-check
# for yet (this pass's own reap runs first, but a DIFFERENT connection's
# establish could be racing this one) -- the age bound is what makes sweeping
# it safe regardless.
DEFAULT_STALE_CONFIG_DIR_AGE_SECONDS: Final = 60.0 * 60.0


@dataclass(frozen=True)
class ProcessRecord:
    """One process-table row: enough to classify a visitor candidate and its parent.

    ``parent_pid`` is ``None`` when the OS-native inspection could not
    determine it (never fabricated as ``0`` or ``-1``, which some platforms
    use as sentinels for "no parent" but which would otherwise collide with a
    prior orphan-reap pass's own residual bookkeeping) -- such a record is
    still eligible to be a REAPED candidate, but can never itself stand in as
    another record's "live parent" (see :func:`reap_stale_frp_visitors`).
    """

    pid: int
    parent_pid: int | None
    cmdline: str


ProcessSnapshotProvider = Callable[[], Sequence[ProcessRecord]]
"""The injectable process-inspection seam every test in this module's own suite replaces."""


@dataclass(frozen=True)
class ReconciliationResult:
    """What one reconciliation pass did, for the caller's own typed event(s).

    ``reaped_pids`` is what ``RemoteConnection._establish`` (#285) folds into
    one ``visitor_orphan_reaped`` :class:`~clio_relay.control_channel.ChannelEvent`
    per pid on the connection's own ledger; ``swept_config_dirs`` is logged
    here (a whole-temp-root sweep is not one connection's own event).
    """

    reaped_pids: tuple[int, ...]
    swept_config_dirs: int


def reconcile_stale_frp_visitors(
    *,
    frpc_bin: str,
    process_snapshot: ProcessSnapshotProvider | None = None,
    terminate_process: Callable[[int], None] | None = None,
    temp_root: Path | None = None,
    max_config_dir_age_seconds: float = DEFAULT_STALE_CONFIG_DIR_AGE_SECONDS,
) -> ReconciliationResult:
    """Reap orphaned frpc visitors for this binary AND sweep stale crumb dirs.

    The one entry point ``frp_transport._FrpChannelTransport.establish``
    calls at the start of every visitor spawn -- production code never calls
    :func:`reap_stale_frp_visitors`/:func:`sweep_stale_visitor_config_dirs`
    directly; this module's own test suite does, to keep each concern's unit
    tests independent.

    Best-effort and never allowed to block a new visitor's own spawn: any
    unexpected failure here is caught, logged as a typed, structured fact
    (never a bare ``except: pass`` -- the no-silent-fallback house rule
    applies to best-effort cleanup too), and reported as an empty result
    rather than raised. The lower-level functions this composes are already
    individually defensive (a snapshot failure returns no candidates, a
    per-pid kill failure is swallowed by :func:`_terminate_pid`), so this is
    a second, belt-and-suspenders backstop, not the primary guard.
    """
    try:
        reaped = reap_stale_frp_visitors(
            frpc_bin=frpc_bin,
            process_snapshot=process_snapshot,
            terminate_process=terminate_process,
        )
        swept = sweep_stale_visitor_config_dirs(
            temp_root=temp_root,
            max_age_seconds=max_config_dir_age_seconds,
        )
    except Exception as exc:  # noqa: BLE001 -- best-effort cleanup must never block a spawn
        logger.warning("clio-relay: visitor_reconciliation_failed reason=%s", exc)
        return ReconciliationResult(reaped_pids=(), swept_config_dirs=0)
    if swept:
        logger.info("clio-relay: visitor_config_dirs_swept count=%s", swept)
    return ReconciliationResult(reaped_pids=reaped, swept_config_dirs=swept)


def reap_stale_frp_visitors(
    *,
    frpc_bin: str,
    process_snapshot: ProcessSnapshotProvider | None = None,
    terminate_process: Callable[[int], None] | None = None,
) -> tuple[int, ...]:
    """Reap prior frpc visitors for this binary whose owning CLI is gone.

    Takes exactly ONE process-table snapshot (``process_snapshot``, the real
    cross-platform :func:`default_process_snapshot` by default) and reaps
    every candidate row whose cmdline carries both this run's ``frpc_bin``
    and the rendered-visitor-config directory-naming convention
    (:func:`_is_stale_visitor_candidate`), AND whose ``parent_pid`` is not
    any ``pid`` present in that SAME snapshot. A live parent is never
    touched -- that is a concurrent CLI's own held visitor, not an orphan
    (the acceptance bar iowarp/clio-relay#285 states explicitly: "Reaping
    must never kill a process whose parent is still alive").
    """
    snapshot = (process_snapshot or default_process_snapshot)()
    live_pids = {record.pid for record in snapshot}
    reap = terminate_process or _terminate_pid
    reaped: list[int] = []
    for record in snapshot:
        if not _is_stale_visitor_candidate(record, frpc_bin=frpc_bin):
            continue
        if record.parent_pid is not None and record.parent_pid in live_pids:
            continue
        reap(record.pid)
        reaped.append(record.pid)
        logger.info(
            "clio-relay: visitor_orphan_reaped pid=%s frpc_bin=%s",
            record.pid,
            frpc_bin,
        )
    return tuple(reaped)


def sweep_stale_visitor_config_dirs(
    *,
    temp_root: Path | None = None,
    max_age_seconds: float = DEFAULT_STALE_CONFIG_DIR_AGE_SECONDS,
    now: float | None = None,
) -> int:
    """Remove empty crash-orphaned visitor config dirs older than the bound.

    Only ever removes a directory that is (a) named with the exact
    :data:`~clio_relay.frp_link.VISITOR_CONFIG_DIR_PREFIX` and (b) EMPTY -- a
    config file still inside it means either a live visitor still owns it
    (this function never even looks at the process table, so it cannot tell
    that case from a race with a reap that has not deleted the file yet --
    either way, only the crash-path residual EMPTY shell
    ``HeldFrpVisitor.close`` never got to run for is this function's job,
    never a populated, still-secret-bearing directory) or exactly that race.
    """
    root = temp_root if temp_root is not None else Path(tempfile.gettempdir())
    deadline = (now if now is not None else time.time()) - max_age_seconds
    swept = 0
    try:
        entries = list(root.iterdir())
    except OSError:
        return 0
    for entry in entries:
        if not entry.name.startswith(VISITOR_CONFIG_DIR_PREFIX):
            continue
        try:
            if not entry.is_dir():
                continue
            if entry.stat().st_mtime > deadline:
                continue
            if any(entry.iterdir()):
                continue
            entry.rmdir()
        except OSError:
            continue
        swept += 1
    return swept


def _is_stale_visitor_candidate(record: ProcessRecord, *, frpc_bin: str) -> bool:
    """Return whether ``record`` looks like one of OUR rendered visitors."""
    command = record.cmdline.casefold()
    if VISITOR_CONFIG_DIR_PREFIX.casefold() not in command:
        return False
    return _command_names_binary(command, frpc_bin)


def _command_names_binary(command: str, frpc_bin: str) -> bool:
    """Return whether ``command``'s OWN leading token names this configured binary.

    Deliberately checks only the leading token (``argv[0]``), never a
    substring search across the whole command: every candidate's cmdline
    also carries its OWN rendered ``frpc-visitor.toml`` filename (see
    :data:`~clio_relay.frp_link.VISITOR_CONFIG_DIR_PREFIX`), which itself
    contains the literal substring ``"frpc"`` -- a plain ``"frpc" in
    command`` search would therefore match every candidate regardless of
    which binary actually launched it, defeating the whole "same binary"
    scoping this exists for.

    A QUALIFIED ``frpc_bin`` (a path -- contains a separator) requires an
    EXACT normalized-path match: two ``frpc`` copies that merely share a
    basename but live in different directories are different deployments'
    binaries, never treated as the same one. An UNQUALIFIED ``frpc_bin``
    (bare, ``PATH``-resolved at spawn time -- ``subprocess.Popen`` never
    rewrites ``argv[0]`` to the resolved absolute path) instead compares
    basenames with any file extension stripped, so a Windows cluster
    configured with the bare ``"frpc"`` still matches an observed
    ``"...\\frpc.exe"`` cmdline.
    """
    argv0 = _leading_command_token(command)
    if not argv0:
        return False
    normalized_argv0 = argv0.replace("\\", "/").casefold()
    normalized_configured = frpc_bin.strip().replace("\\", "/").casefold()
    if not normalized_configured:
        return False
    if "/" in normalized_configured:
        return normalized_argv0 == normalized_configured
    return Path(normalized_argv0).stem == Path(normalized_configured).stem


def _leading_command_token(command: str) -> str:
    """Return ``command``'s first argv-shaped token (a quoted path, or up to the first space)."""
    stripped = command.strip()
    if not stripped:
        return ""
    if stripped[0] == '"':
        closing = stripped.find('"', 1)
        if closing != -1:
            return stripped[1:closing]
        return stripped[1:]
    return stripped.split(None, 1)[0]


def _terminate_pid(pid: int) -> None:
    """Terminate one orphaned visitor process (escalates to a hard kill).

    Injectable via :func:`reap_stale_frp_visitors`'s ``terminate_process`` --
    this, the real implementation, is deliberately the ONLY call site here
    that ever reaches the OS to kill anything. POSIX: SIGTERM then SIGKILL,
    matching ``HeldFrpVisitor.close``'s own escalation. Windows: ``taskkill
    /PID <pid> /T /F``, matching
    ``process_containment_windows._terminate_windows_tree``'s own primitive.
    Either way this is a best-effort bound, not a verified teardown -- a
    process that survives is left for the NEXT reconciliation pass, which
    reaps it again (idempotent: a genuinely-dead pid is simply absent from
    the next snapshot's candidate rows).
    """
    if os.name == "nt":
        with suppress(OSError, subprocess.TimeoutExpired):
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                check=False,
                capture_output=True,
                timeout=DEFAULT_RECONCILE_TIMEOUT_SECONDS,
            )
        return
    with suppress(ProcessLookupError):
        os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + DEFAULT_RECONCILE_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    with suppress(ProcessLookupError):
        os.kill(pid, signal.SIGKILL)


def default_process_snapshot() -> tuple[ProcessRecord, ...]:
    """Return the live process table (pid/parent/cmdline), cross-platform.

    POSIX reads ``/proc`` directly (this repository's existing pattern --
    ``service_runtime_connector_identity._local_process_ids``); Windows runs
    the SAME ``Get-CimInstance Win32_Process`` PowerShell subprocess that
    module's ``_windows_connector_descendants`` already uses for the
    identical "is this process tree still owned" question. Every record this
    returns describes ONE point in time -- callers must not mix records taken
    from two separate calls when deciding whether a parent is alive, since a
    pid can be reused between them (:func:`reap_stale_frp_visitors` takes
    exactly one snapshot for this reason).

    Never raises: either platform's inspection failing (permission denied,
    ``powershell`` unavailable, a malformed/short-lived process disappearing
    mid-read) returns an empty snapshot rather than aborting the caller's
    reap -- an empty snapshot simply reaps nothing this pass, which the next
    visitor spawn's own pass tries again.
    """
    if os.name == "nt":
        return _windows_process_snapshot()
    return _posix_process_snapshot()


def _posix_process_snapshot(proc_root: Path | None = None) -> tuple[ProcessRecord, ...]:
    """Parse a ``/proc``-shaped tree into process records.

    ``proc_root`` defaults to the real ``/proc``; this module's own test
    suite passes a fake tree (a real temp directory shaped like ``/proc`` --
    numbered subdirectories each with a ``stat``/``cmdline`` file) so the
    parsing logic is exercised on any host OS, not only Linux.
    """
    root = proc_root if proc_root is not None else Path("/proc")
    records: list[ProcessRecord] = []
    try:
        entries = list(root.iterdir())
    except OSError:
        return ()
    for proc in entries:
        if not proc.name.isdigit():
            continue
        pid = int(proc.name)
        try:
            stat_text = (proc / "stat").read_text(encoding="utf-8")
            closing = stat_text.rfind(")")
            fields = stat_text[closing + 2 :].split() if closing >= 0 else []
            parent_pid = int(fields[1]) if len(fields) > 1 else None
            cmdline = (
                (proc / "cmdline")
                .read_bytes()
                .replace(b"\x00", b" ")
                .decode("utf-8", errors="replace")
            )
        except (FileNotFoundError, ProcessLookupError, OSError, IndexError, ValueError):
            continue
        records.append(ProcessRecord(pid=pid, parent_pid=parent_pid, cmdline=cmdline))
    return tuple(records)


def _windows_process_snapshot() -> tuple[ProcessRecord, ...]:
    command = (
        "@(Get-CimInstance Win32_Process | "
        "Select-Object ProcessId,ParentProcessId,CommandLine) | ConvertTo-Json -Compress"
    )
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", command],
            check=False,
            capture_output=True,
            text=True,
            timeout=DEFAULT_ENUMERATION_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ()
    if result.returncode != 0 or not result.stdout.strip():
        return ()
    try:
        loaded = cast(object, json.loads(result.stdout))
    except json.JSONDecodeError:
        return ()
    raw_items = cast("list[object]", loaded) if isinstance(loaded, list) else [loaded]
    records: list[ProcessRecord] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        record = cast("dict[str, object]", item)
        raw_pid = record.get("ProcessId")
        raw_parent_pid = record.get("ParentProcessId")
        raw_cmdline = record.get("CommandLine")
        if not isinstance(raw_pid, int) or isinstance(raw_pid, bool) or raw_pid <= 0:
            continue
        parent_pid = (
            raw_parent_pid
            if isinstance(raw_parent_pid, int) and not isinstance(raw_parent_pid, bool)
            else None
        )
        cmdline = raw_cmdline if isinstance(raw_cmdline, str) else ""
        records.append(ProcessRecord(pid=raw_pid, parent_pid=parent_pid, cmdline=cmdline))
    return tuple(records)
