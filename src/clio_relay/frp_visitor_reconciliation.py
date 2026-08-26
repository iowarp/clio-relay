"""Stale frpc visitor reconciliation: reap prior-process orphans, sweep crumb dirs.

iowarp/clio-relay#285: a ``brokered_tcp``/``udp_rendezvous`` connection's held
frpc visitor (:class:`~clio_relay.frp_link.HeldFrpVisitor`) has no
self-tearing-down tether the way ``ssh_forward``'s held stdin pipe does
(``control_channel.py``'s own module docstring) -- nothing closes it when the
owning CLI process exits. The atexit hook
(:mod:`clio_relay.remote_connection_registry`) covers the ordinary-exit case;
this module covers what an atexit hook structurally cannot -- a ``kill -9``'d
or crashed CLI leaves its visitor process (and its rendered-config temp dir,
secret included) with no owner, and no atexit hook ever runs for a process
that was signaled, not asked to exit.

The cross-platform process-table READ primitives (POSIX ``/proc``, Windows
``Get-CimInstance Win32_Process``) live in the owner module
:mod:`clio_relay.frp_visitor_process_inspection` -- split out purely for file
size; this module owns the POLICY built on top of them (what counts as one
of OUR visitors, when to reap one, when to sweep a config dir).

:func:`reconcile_stale_frp_visitors` is the one entry point production code
calls, at the START of every new visitor spawn
(``frp_transport._FrpChannelTransport.establish``, before the new visitor is
even rendered). It takes ONE process-table snapshot
(:func:`~clio_relay.frp_visitor_process_inspection.default_process_snapshot`,
the injectable seam every test in this module's own suite replaces with
fake :class:`~clio_relay.frp_visitor_process_inspection.ProcessRecord` rows)
and:

* :func:`reap_stale_frp_visitors` reaps every candidate row whose cmdline
  carries both this run's configured ``frpc_bin`` and the
  ``HeldFrpVisitor.establish``-rendered config's directory-naming convention
  (:data:`~clio_relay.frp_link.VISITOR_CONFIG_DIR_PREFIX` in its ``-c`` path),
  AND whose parent pid is absent from that SAME snapshot -- a live parent
  means a concurrent CLI still legitimately holds that visitor, and it is
  never touched. Adversarial-review fix D1 (2026-08-26): the ORIGINAL
  snapshot row is not itself trusted at kill time -- a pid is a reusable OS
  resource, and the window between "took the snapshot" and "sent the kill"
  is exactly where a reused pid could now name an unrelated, innocent
  process. Immediately before killing, ``default_single_process_lookup``
  re-reads that ONE pid fresh and re-runs the SAME candidate classification
  against the fresh record; the kill proceeds only if it still matches.
  Every currently-implemented ``terminate_process`` also removes the reaped
  visitor's own rendered config directory (D3) -- it carries a plaintext frp
  token/stcp secret, and this is the only path that removes it immediately
  rather than waiting for the next bounded sweep.
* :func:`sweep_stale_visitor_config_dirs` removes ``clio-relay-frp-visitor-*``
  directories older than a bound from the OS temp root, REGARDLESS of
  whether they are empty (D3): such a directory only ever contains one
  rendered ``frpc-visitor.toml``, itself carrying that same plaintext
  secret, so an aged, still-populated one is exactly the crash path (a
  ``kill -9``'d or already-exited CLI whose visitor's pid reconciliation
  never got a chance to reap -- gone before the process table was even
  read) that nothing else was ever going to clean up. Removing a
  secret-at-rest is strictly more urgent than preserving a directory a
  crash left behind.

**Known limitation (D7, documented rather than fixed):** on Windows, a
``parent_pid`` this module observes as "alive" is only ever cross-checked
against the CURRENT snapshot's own pid column -- never against a start-time
identity the way ``process_containment_recorded.process_start_identity``
does for OWNED processes elsewhere in this codebase. If the ORIGINAL parent
CLI process exits and the OS reuses its exact pid for an unrelated process
before this module's next reconciliation pass runs, a genuinely orphaned
visitor is misclassified as "parent still alive" and is never reaped by
THIS mechanism -- a false negative (the leak persists longer, bounded only
by the next reconciliation pass to observe a DIFFERENT, non-reused parent
pid state) rather than a false positive (nothing here ever kills a process
whose CURRENT record still matches an alive parent). A creation-time
plausibility check (parent must PREDATE the child) was evaluated and
deliberately not implemented: ``Get-CimInstance``'s ``CreationDate`` is a
``[datetime]`` whose ``ConvertTo-Json`` serialization format is not pinned
across PowerShell versions (WMI's ``/Date(ms)/`` wrapper on Desktop
PowerShell vs. a plain ISO-8601 string on PowerShell 7+), so parsing it
without a verified target runtime risks silently MISreading a timestamp --
which cuts the wrong way for a safety mechanism: a parsing bug here could
turn a false negative (leak persists) into a false positive (kills a
live-parented process), the exact failure D1 above exists to prevent. Not
attempted without a pinned, verified serialization format to parse against.
"""

from __future__ import annotations

import logging
import os
import shutil
import signal
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from clio_relay.frp_link import VISITOR_CONFIG_DIR_PREFIX
from clio_relay.frp_visitor_process_inspection import (
    DEFAULT_RECONCILE_TIMEOUT_SECONDS,
    ProcessSnapshotProvider,
    default_process_snapshot,
    default_single_process_lookup,
)
from clio_relay.frp_visitor_process_inspection import (
    ProcessRecord as ProcessRecord,
)
from clio_relay.frp_visitor_process_inspection import (
    ProcessSnapshot as ProcessSnapshot,
)
from clio_relay.frp_visitor_process_inspection import (
    SingleProcessLookup as SingleProcessLookup,
)

logger = logging.getLogger(__name__)

# "say 1 hour" (iowarp/clio-relay#285's own fix direction): a crash-path
# residual younger than this might still belong to a visitor this exact
# reconciliation pass has not reached the reap-check for yet (this pass's
# own reap runs first, but a DIFFERENT connection's establish could be
# racing this one) -- the age bound is what makes sweeping it safe
# regardless of emptiness (D3).
DEFAULT_STALE_CONFIG_DIR_AGE_SECONDS: Final = 60.0 * 60.0


@dataclass(frozen=True)
class ReapOutcome:
    """What one reap pass did: which pids were reaped, or why it was skipped."""

    reaped_pids: tuple[int, ...]
    skipped_reason: str | None = None


@dataclass(frozen=True)
class ReconciliationResult:
    """What one reconciliation pass did, for the caller's own typed event(s).

    ``reaped_pids`` is what ``RemoteConnection._establish`` (#285) folds into
    one ``visitor_orphan_reaped`` :class:`~clio_relay.control_channel.ChannelEvent`
    per pid on the connection's own ledger; ``swept_config_dirs`` is logged
    here (a whole-temp-root sweep is not one connection's own event).
    ``skipped_reason`` (D2) is folded into a typed
    ``visitor_reconciliation_skipped`` channel event when set, so a snapshot
    that could not run at all is never silently indistinguishable from one
    that legitimately found nothing.
    """

    reaped_pids: tuple[int, ...]
    swept_config_dirs: int
    skipped_reason: str | None = None


# D8: the config-dir sweep is a whole-temp-root directory walk (~100ms) --
# cheap once per process lifetime, not cheap on EVERY visitor spawn. Guarded
# by a lock since concurrent connections may establish from different
# threads; tests reset this via `monkeypatch.setattr(reconciliation,
# "_sweep_has_run", False)` to exercise the composed sweep more than once.
_SWEEP_LOCK = threading.Lock()
_sweep_has_run = False


def reconcile_stale_frp_visitors(
    *,
    frpc_bin: str,
    process_snapshot: ProcessSnapshotProvider | None = None,
    terminate_process: Callable[[int], None] | None = None,
    process_lookup: SingleProcessLookup | None = None,
    temp_root: Path | None = None,
    max_config_dir_age_seconds: float = DEFAULT_STALE_CONFIG_DIR_AGE_SECONDS,
) -> ReconciliationResult:
    """Reap orphaned frpc visitors for this binary AND sweep stale crumb dirs.

    The one entry point ``frp_transport._FrpChannelTransport.establish``
    calls at the start of every visitor spawn -- production code never calls
    :func:`reap_stale_frp_visitors`/:func:`sweep_stale_visitor_config_dirs`
    directly; this module's own test suite does, to keep each concern's unit
    tests independent. The sweep itself runs at most once per process (D8,
    see :data:`_sweep_has_run`); the reap runs on every call, matching "at
    visitor spawn" (iowarp/clio-relay#285's own fix direction).

    Best-effort and never allowed to block a new visitor's own spawn: any
    unexpected failure here is caught, logged as a typed, structured fact
    (never a bare ``except: pass`` -- the no-silent-fallback house rule
    applies to best-effort cleanup too), and reported as an empty,
    typed-skipped result rather than raised. The lower-level functions this
    composes are already individually defensive (a snapshot failure reports
    a typed ``skipped_reason`` rather than raising, a per-pid kill failure is
    swallowed by :func:`_terminate_pid`), so this is a second,
    belt-and-suspenders backstop, not the primary guard.
    """
    try:
        reap_outcome = reap_stale_frp_visitors(
            frpc_bin=frpc_bin,
            process_snapshot=process_snapshot,
            terminate_process=terminate_process,
            process_lookup=process_lookup,
        )
        swept = _sweep_once(temp_root=temp_root, max_age_seconds=max_config_dir_age_seconds)
    except Exception as exc:  # noqa: BLE001 -- best-effort cleanup must never block a spawn
        logger.warning("clio-relay: visitor_reconciliation_failed reason=%s", exc)
        return ReconciliationResult(
            reaped_pids=(),
            swept_config_dirs=0,
            skipped_reason="reconciliation_failed:exception",
        )
    if swept:
        logger.info("clio-relay: visitor_config_dirs_swept count=%s", swept)
    if reap_outcome.skipped_reason is not None:
        logger.warning(
            "clio-relay: visitor_snapshot_skipped reason=%s frpc_bin=%s",
            reap_outcome.skipped_reason,
            frpc_bin,
        )
    return ReconciliationResult(
        reaped_pids=reap_outcome.reaped_pids,
        swept_config_dirs=swept,
        skipped_reason=reap_outcome.skipped_reason,
    )


def reap_stale_frp_visitors(
    *,
    frpc_bin: str,
    process_snapshot: ProcessSnapshotProvider | None = None,
    terminate_process: Callable[[int], None] | None = None,
    process_lookup: SingleProcessLookup | None = None,
) -> ReapOutcome:
    """Reap prior frpc visitors for this binary whose owning CLI is gone.

    Takes exactly ONE process-table snapshot (``process_snapshot``, the real
    cross-platform ``default_process_snapshot`` by default) and, for every
    candidate row whose cmdline carries both this run's ``frpc_bin`` and the
    rendered-visitor-config directory-naming convention
    (:func:`_is_stale_visitor_candidate`) with a parent pid absent from that
    SAME snapshot (a live parent means a concurrent CLI still legitimately
    holds it -- never touched): re-reads that ONE pid fresh
    (``process_lookup``, D1) immediately before killing and re-classifies
    the FRESH record. Only a still-matching fresh record is actually killed
    -- a pid the OS reused for an unrelated process between the snapshot and
    now is left alone, full stop (the acceptance bar iowarp/clio-relay#285
    states explicitly: "Reaping must never kill a process whose parent is
    still alive"; D1 extends the same discipline to the candidate's own pid).
    Each actual kill also removes that visitor's own rendered config
    directory (D3) -- the plaintext frp token/stcp secret inside it should
    not wait for the next bounded sweep when this pass already proved the
    process is gone.

    A snapshot the underlying OS-native inspection could not read at all
    (D2) is reported via ``ReapOutcome.skipped_reason`` rather than treated
    as "found nothing" -- see ``ProcessSnapshot``.
    """
    snapshot = (process_snapshot or default_process_snapshot)()
    if snapshot.skipped_reason is not None:
        return ReapOutcome(reaped_pids=(), skipped_reason=snapshot.skipped_reason)
    live_pids = {record.pid for record in snapshot.records}
    reap = terminate_process or _terminate_pid
    lookup = process_lookup or default_single_process_lookup
    reaped: list[int] = []
    for record in snapshot.records:
        if not _is_stale_visitor_candidate(record, frpc_bin=frpc_bin):
            continue
        if record.parent_pid is not None and record.parent_pid in live_pids:
            continue
        # D1: the snapshot row above is stale by construction -- re-read this
        # ONE pid fresh, immediately before killing, and re-classify it. A
        # pid the OS has since reused for an unrelated process fails this
        # re-check and is never touched.
        fresh = lookup(record.pid)
        if fresh is None:
            continue  # already gone on its own; nothing to kill
        if not _is_stale_visitor_candidate(fresh, frpc_bin=frpc_bin):
            continue  # pid reused by a non-matching process -- refuse
        reap(record.pid)
        reaped.append(record.pid)
        logger.info(
            "clio-relay: visitor_orphan_reaped pid=%s frpc_bin=%s",
            record.pid,
            frpc_bin,
        )
        _remove_visitor_config_dir(fresh.cmdline)
    return ReapOutcome(reaped_pids=tuple(reaped))


def sweep_stale_visitor_config_dirs(
    *,
    temp_root: Path | None = None,
    max_age_seconds: float = DEFAULT_STALE_CONFIG_DIR_AGE_SECONDS,
    now: float | None = None,
) -> int:
    """Remove crash-orphaned visitor config dirs older than the bound.

    Removes the ENTIRE directory tree, not only an empty shell (D3
    adversarial-review fix -- an earlier revision of this function refused
    to touch a non-empty directory and incorrectly claimed
    ``reap_stale_frp_visitors`` already emptied it first; it did not, until
    THIS same fix round also taught the reap step to remove its own reaped
    visitor's config dir directly, see :func:`_remove_visitor_config_dir`).
    A ``clio-relay-frp-visitor-*`` directory only ever contains one rendered
    ``frpc-visitor.toml``, which carries a PLAINTEXT frp token/stcp secret --
    a directory this sweep finds non-empty and aged past the bound is
    exactly the crash path (``kill -9``, or a visitor process that already
    exited on its own before any reap pass ever ran for it) where nothing
    else was ever going to remove that secret. Removing a secret-at-rest is
    strictly more urgent than preserving a stray directory a crash left
    behind, so this never special-cases "only if empty".
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
            shutil.rmtree(entry)
        except OSError:
            continue
        swept += 1
    return swept


def _sweep_once(*, temp_root: Path | None, max_age_seconds: float) -> int:
    """Run the config-dir sweep at most once per process (D8).

    A whole-temp-root directory walk on EVERY visitor spawn is unnecessary
    cost on the bring-up hot path; once per process lifetime is enough to
    bound how long a crash-path secret can linger, without paying the walk
    on every reconnect/new-cluster-connect too.
    """
    global _sweep_has_run  # noqa: PLW0603 -- the documented once-per-process gate itself
    with _SWEEP_LOCK:
        if _sweep_has_run:
            return 0
        _sweep_has_run = True
    return sweep_stale_visitor_config_dirs(temp_root=temp_root, max_age_seconds=max_age_seconds)


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


def _tokenize_command(command: str) -> list[str]:
    """A minimal, good-enough argv tokenizer: a quoted run stays one token.

    Only used to recover the ``-c <config_path>`` argument
    (:func:`_extract_config_path`, D3) from a cmdline already CONFIRMED to
    match :func:`_is_stale_visitor_candidate` -- not a general shell parser.
    """
    tokens: list[str] = []
    remaining = command.strip()
    while remaining:
        if remaining[0] == '"':
            closing = remaining.find('"', 1)
            if closing == -1:
                tokens.append(remaining[1:])
                break
            tokens.append(remaining[1:closing])
            remaining = remaining[closing + 1 :].lstrip()
            continue
        split = remaining.split(None, 1)
        tokens.append(split[0])
        remaining = split[1] if len(split) > 1 else ""
    return tokens


def _extract_config_path(command: str) -> str | None:
    """Return the ``-c <path>`` argument of a visitor cmdline, or ``None``.

    ``HeldFrpVisitor.establish``'s spawn convention is exactly
    ``[frpc_bin, "-c", str(config_path)]`` -- three tokens, no other flags --
    so the path is whatever token immediately follows ``-c``.
    """
    tokens = _tokenize_command(command)
    for index, token in enumerate(tokens):
        if token == "-c" and index + 1 < len(tokens):
            return tokens[index + 1]
    return None


def _remove_visitor_config_dir(cmdline: str) -> None:
    """Remove a just-reaped visitor's own rendered (secret-bearing) config dir.

    D3 adversarial-review fix: called ONLY right after a real kill
    succeeded, using the FRESH (D1-reverified) cmdline that already matched
    :func:`_is_stale_visitor_candidate`. Best-effort and narrowly guarded --
    extraction failing, or the resolved directory not actually carrying the
    ``clio-relay-frp-visitor-`` naming convention, refuses to touch anything
    rather than ever ``rm -rf`` an unrelated path; either failure just
    leaves the directory for the next :func:`sweep_stale_visitor_config_dirs`
    pass to remove once it ages past the bound instead of raising and
    losing the process-kill this accompanies.
    """
    config_path_str = _extract_config_path(cmdline)
    if config_path_str is None:
        return
    try:
        config_dir = Path(config_path_str).resolve().parent
    except OSError:
        return
    if not config_dir.name.startswith(VISITOR_CONFIG_DIR_PREFIX):
        return
    shutil.rmtree(config_dir, ignore_errors=True)


def _terminate_pid(pid: int) -> None:
    """Terminate one orphaned visitor process (escalates to a hard kill).

    Injectable via :func:`reap_stale_frp_visitors`'s ``terminate_process`` --
    this, the real implementation, is deliberately the ONLY call site here
    that ever reaches the OS to kill anything. POSIX: SIGTERM then SIGKILL,
    matching ``HeldFrpVisitor.close``'s own escalation. Windows: ``taskkill
    /PID <pid> /F`` -- deliberately WITHOUT ``/T`` (D1 adversarial-review
    fix): ``frpc`` spawns no children of its own, so descending the process
    tree only ever adds blast radius; the reviewer's own repro fed a stale
    row and watched ``/T`` amplify the (already-wrong, now separately fixed
    by the D1 re-verify above) kill into an unrelated child process too.
    Either way this is a best-effort bound, not a verified teardown -- a
    process that survives is left for the NEXT reconciliation pass, which
    reaps it again (idempotent: a genuinely-dead pid is simply absent from
    the next snapshot's candidate rows).
    """
    if os.name == "nt":
        with suppress(OSError, subprocess.TimeoutExpired):
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/F"],
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
