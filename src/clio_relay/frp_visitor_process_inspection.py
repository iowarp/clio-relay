"""Cross-platform process-table inspection primitives (#285).

Owner module for :mod:`clio_relay.frp_visitor_reconciliation`'s process-table
reads -- split out purely for file size (the reconciliation POLICY --
classification, reap, sweep, config-dir removal -- lives there; this module
knows nothing about the visitor-config-dir naming convention or any of that
policy, only "what does the OS process table say right now"). One-directional
dependency: this module has zero imports from
:mod:`clio_relay.frp_visitor_reconciliation`, which imports every public name
here.

POSIX reads ``/proc`` directly (this repository's existing pattern --
``service_runtime_connector_identity._local_process_ids``); Windows runs the
SAME ``Get-CimInstance Win32_Process`` PowerShell subprocess that module's
``_windows_connector_descendants`` already uses for the identical "is this
process tree still owned" question -- not ``psutil`` (not a clio-relay
dependency; see ``pyproject.toml``) and not a hand-rolled WMI query.

Two distinct read shapes:

* :func:`default_process_snapshot` reads the WHOLE process table once. Every
  failure mode (D2 adversarial-review fix, 2026-08-26) -- the OS-native
  inspection erroring, timing out, exiting non-zero, or returning malformed
  output -- returns a typed :data:`SNAPSHOT_SKIP_OSERROR`/:data:`SNAPSHOT_SKIP_TIMEOUT`/
  :data:`SNAPSHOT_SKIP_EXIT_STATUS`/:data:`SNAPSHOT_SKIP_MALFORMED_OUTPUT`
  reason on :class:`ProcessSnapshot` rather than an indistinguishable empty
  result -- a snapshot the caller could never read at all must never look
  like "genuinely found no orphans" (that silence, on a
  PowerShell-constrained host, is exactly how a leak persists forever with
  zero visible signal).
* :func:`default_single_process_lookup` re-reads ONE pid fresh (D1
  adversarial-review fix): a whole-table snapshot is stale the moment it is
  taken, and a pid is a reusable OS resource -- the reconciliation policy
  module re-verifies a candidate's exact pid immediately before killing it,
  never trusting the earlier snapshot row alone for that decision.
"""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Final, cast

# Per-pid query bound (single-process lookup, D1) -- tighter than the
# whole-table enumeration bound below since it is one targeted WMI filter,
# not a full process-table dump.
DEFAULT_RECONCILE_TIMEOUT_SECONDS: Final = 5.0
# D8 (adversarial-review perf fix): was 20.0 -- a worst-case 20s block on
# EVERY visitor spawn's bring-up path for one wedged PowerShell call is not
# "bounded", it is a functional hang budget. A host this slow to answer one
# WMI query is treated the same as "unavailable" (SNAPSHOT_SKIP_TIMEOUT),
# not waited out longer.
DEFAULT_ENUMERATION_TIMEOUT_SECONDS: Final = 5.0

# Typed snapshot-skip reasons (D2): every failure mode used to return an
# empty snapshot with ZERO log output -- indistinguishable from "genuinely
# found no orphans". Every one of these is now a distinct, logged,
# structured fact surfaced on the reconciliation policy module's own result
# types.
SNAPSHOT_SKIP_OSERROR: Final = "snapshot_unavailable:oserror"
SNAPSHOT_SKIP_TIMEOUT: Final = "snapshot_unavailable:timeout"
SNAPSHOT_SKIP_EXIT_STATUS: Final = "snapshot_unavailable:exit_status"
SNAPSHOT_SKIP_MALFORMED_OUTPUT: Final = "snapshot_unavailable:malformed_output"


@dataclass(frozen=True)
class ProcessRecord:
    """One process-table row: enough to classify a visitor candidate and its parent.

    ``parent_pid`` is ``None`` when the OS-native inspection could not
    determine it (never fabricated as ``0`` or ``-1``, which some platforms
    use as sentinels for "no parent" but which would otherwise collide with a
    prior orphan-reap pass's own residual bookkeeping) -- such a record is
    still eligible to be a REAPED candidate, but can never itself stand in as
    another record's "live parent" (the reconciliation policy module's own
    concern).
    """

    pid: int
    parent_pid: int | None
    cmdline: str


@dataclass(frozen=True)
class ProcessSnapshot:
    """One process-table read: the records found, or why none could be read.

    ``skipped_reason`` is ``None`` on a genuine, successful read (regardless
    of whether it found zero matching processes) -- set only when the
    inspection ITSELF failed (D2), so a caller can tell "no orphans exist"
    from "the OS-native inspection did not run".
    """

    records: tuple[ProcessRecord, ...]
    skipped_reason: str | None = None


ProcessSnapshotProvider = Callable[[], ProcessSnapshot]
"""The injectable process-inspection seam every test suite replaces."""

SingleProcessLookup = Callable[[int], "ProcessRecord | None"]
"""Re-read ONE pid's CURRENT record immediately before a kill (D1).

Returns ``None`` when the pid no longer exists (nothing to kill -- it
already exited on its own). Distinct from :data:`ProcessSnapshotProvider`:
this is a fresh, single-pid read taken as close as possible to the kill
decision, never reused across multiple candidates and never satisfied from
the earlier whole-table snapshot, which is exactly the stale data D1 exists
to stop trusting.
"""


def default_process_snapshot() -> ProcessSnapshot:
    """Return the live process table (pid/parent/cmdline), cross-platform.

    Every record this returns describes ONE point in time -- callers must
    not mix records taken from two separate calls when deciding whether a
    parent is alive, since a pid can be reused between them (the
    reconciliation policy module takes exactly one whole-table snapshot for
    this reason, and separately re-verifies each candidate's own pid fresh
    before killing it via :func:`default_single_process_lookup`, D1).

    Never raises: either platform's inspection failing returns a
    :class:`ProcessSnapshot` with a typed ``skipped_reason`` (D2) rather than
    aborting the caller's reap.
    """
    if os.name == "nt":
        return _windows_process_snapshot()
    return _posix_process_snapshot()


def _posix_process_snapshot(proc_root: Path | None = None) -> ProcessSnapshot:
    """Parse a ``/proc``-shaped tree into process records.

    ``proc_root`` defaults to the real ``/proc``; the test suite passes a
    fake tree (a real temp directory shaped like ``/proc`` -- numbered
    subdirectories each with a ``stat``/``cmdline`` file) so the parsing
    logic is exercised on any host OS, not only Linux. A single
    malformed/vanished ENTRY is skipped individually (a process that exited
    mid-read is not a whole-snapshot failure); only the top-level directory
    walk itself failing is (D2).
    """
    root = proc_root if proc_root is not None else Path("/proc")
    records: list[ProcessRecord] = []
    try:
        entries = list(root.iterdir())
    except OSError:
        return ProcessSnapshot(records=(), skipped_reason=SNAPSHOT_SKIP_OSERROR)
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
    return ProcessSnapshot(records=tuple(records))


def _windows_process_snapshot() -> ProcessSnapshot:
    """Enumerate the whole process table via one bounded PowerShell subprocess.

    Every one of the four ways this can fail -- the subprocess itself
    erroring (OSError, e.g. ``powershell`` missing), timing out, exiting
    non-zero, or returning output that is not the well-formed JSON expected
    -- returns its OWN distinct :data:`SNAPSHOT_SKIP_*` reason (D2) rather
    than collapsing into one indistinguishable empty result.
    """
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
    except subprocess.TimeoutExpired:
        return ProcessSnapshot(records=(), skipped_reason=SNAPSHOT_SKIP_TIMEOUT)
    except OSError:
        return ProcessSnapshot(records=(), skipped_reason=SNAPSHOT_SKIP_OSERROR)
    if result.returncode != 0:
        return ProcessSnapshot(records=(), skipped_reason=SNAPSHOT_SKIP_EXIT_STATUS)
    if not result.stdout.strip():
        return ProcessSnapshot(records=(), skipped_reason=SNAPSHOT_SKIP_MALFORMED_OUTPUT)
    try:
        loaded = cast(object, json.loads(result.stdout))
    except json.JSONDecodeError:
        return ProcessSnapshot(records=(), skipped_reason=SNAPSHOT_SKIP_MALFORMED_OUTPUT)
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
    return ProcessSnapshot(records=tuple(records))


def default_single_process_lookup(pid: int) -> ProcessRecord | None:
    """Re-read ONE pid's CURRENT record, cross-platform (D1).

    Returns ``None`` when the pid no longer exists. Deliberately a
    single-pid query, not a slice of a whole-table snapshot: the point is a
    read taken as close as possible to the kill decision, not a cached one.
    """
    if os.name == "nt":
        return _windows_single_process_record(pid)
    return _posix_single_process_record(pid)


def _posix_single_process_record(pid: int, proc_root: Path | None = None) -> ProcessRecord | None:
    """Read one ``/proc/<pid>`` entry fresh; ``None`` if it is gone.

    ``proc_root`` defaults to the real ``/proc``, mirroring
    :func:`_posix_process_snapshot`'s own injectable-root shape for the same
    "exercise the parser on any host OS" testability reason.
    """
    root = proc_root if proc_root is not None else Path("/proc")
    proc = root / str(pid)
    try:
        stat_text = (proc / "stat").read_text(encoding="utf-8")
        closing = stat_text.rfind(")")
        fields = stat_text[closing + 2 :].split() if closing >= 0 else []
        parent_pid = int(fields[1]) if len(fields) > 1 else None
        cmdline = (
            (proc / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", errors="replace")
        )
    except (FileNotFoundError, ProcessLookupError, OSError, IndexError, ValueError):
        return None
    return ProcessRecord(pid=pid, parent_pid=parent_pid, cmdline=cmdline)


def _windows_single_process_record(pid: int) -> ProcessRecord | None:
    """Query one Windows process fresh via a single-pid WMI filter; ``None`` if gone.

    Mirrors ``service_runtime_connector_identity._observe_windows_process``'s
    single-pid ``Get-CimInstance -Filter`` pattern -- a targeted query, not a
    slice of the whole-table enumeration :func:`_windows_process_snapshot`
    already ran (D1: the whole point is a FRESH read, not a cached one).
    """
    command = (
        f"$p = Get-CimInstance Win32_Process -Filter 'ProcessId = {pid}'; "
        "if ($null -eq $p) { exit 3 }; "
        "[pscustomobject]@{ParentProcessId=$p.ParentProcessId; CommandLine=$p.CommandLine} "
        "| ConvertTo-Json -Compress"
    )
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", command],
            check=False,
            capture_output=True,
            text=True,
            timeout=DEFAULT_RECONCILE_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode == 3:
        return None
    if result.returncode != 0 or not result.stdout.strip():
        return None
    try:
        loaded = cast(object, json.loads(result.stdout))
    except json.JSONDecodeError:
        return None
    if not isinstance(loaded, dict):
        return None
    payload = cast("dict[str, object]", loaded)
    raw_parent_pid = payload.get("ParentProcessId")
    raw_cmdline = payload.get("CommandLine")
    parent_pid = (
        raw_parent_pid
        if isinstance(raw_parent_pid, int) and not isinstance(raw_parent_pid, bool)
        else None
    )
    cmdline = raw_cmdline if isinstance(raw_cmdline, str) else ""
    return ProcessRecord(pid=pid, parent_pid=parent_pid, cmdline=cmdline)
