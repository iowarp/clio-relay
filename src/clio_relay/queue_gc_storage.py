"""GC quarantine-tree filesystem storage: move, purge, and candidate removal.

Owns every pure filesystem primitive the terminal-job GC orchestration
(``queue_job_gc.py``) uses to quarantine and later purge durable records: the
crash-safe cross-directory move (``move_gc_path``), the bounded, symlink/
reparse-refusing purge walk (``purge_tree_batch``/``purge_quarantined_tree_
batch``), and the platform-specific candidate removal (Windows re-stats and
POSIX ``dir_fd``-anchored unlink/rmdir, both refusing a trash root or
ancestry that changed since it was last observed). None of these functions
touch a canonical queue record -- they operate purely on paths already
staged under a job's ``gc_trash`` tree.

Sabotage seam (design row: "then ``queue_job_gc.queue_gc_storage.move_gc_
path``"): ``move_gc_path`` is a bare module-level function -- exactly the
``write_job``/``sync_operational_indexes`` module-twin idiom -- so ``queue_
job_gc.py`` calls it module-qualified (``queue_gc_storage.move_gc_path(...)``)
and a test can intercept it via ``monkeypatch.setattr(queue_job_gc,
"queue_gc_storage", isolated_namespace)``.

``after_gc_checkpoint`` (design §4's own already-recorded post-split lookup
for the fault-injection seam formerly named ``ClioCoreQueue._after_gc_
checkpoint``) also lives here: every phase-checkpoint call in ``queue_job_
gc.py`` resolves it module-qualified, matching the design doc's own
prescription rather than staying a facade-resident instance method.
"""

from __future__ import annotations

import logging
import os
import stat
from pathlib import Path

from clio_relay import queue_layout, queue_store_read
from clio_relay.errors import QueueConflictError, queue_conflict_from_cause
from clio_relay.models import JobGcPhase

logger = logging.getLogger(__name__)


def after_gc_checkpoint(_phase: JobGcPhase) -> None:
    """Fault-injection seam invoked only after a durable GC phase checkpoint."""


def _path_lstat(path: Path) -> os.stat_result | None:
    return queue_store_read.path_lstat(path)


def _ensure_gc_parent(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    current = path
    while True:
        current_stat = os.lstat(current)
        if not stat.S_ISDIR(current_stat.st_mode) or queue_layout.record_is_reparse(current_stat):
            raise QueueConflictError(f"GC destination contains an unsafe directory: {current}")
        if current.parent == current:
            return
        current = current.parent


def move_gc_path(source: Path, destination: Path) -> bool:
    """Crash-safely quarantine one path by atomic rename, refusing unsafe sources."""
    source_stat = _path_lstat(source)
    destination_stat = _path_lstat(destination)
    if source_stat is None:
        if destination_stat is not None:
            return False
        return False
    if destination_stat is not None:
        raise QueueConflictError(f"GC source and destination both exist: {source}")
    if stat.S_ISLNK(source_stat.st_mode) or queue_layout.record_is_reparse(source_stat):
        raise QueueConflictError(f"GC refuses a symlink or reparse-point source: {source}")
    if not stat.S_ISREG(source_stat.st_mode) and not stat.S_ISDIR(source_stat.st_mode):
        raise QueueConflictError(f"GC refuses a non-file source: {source}")
    _ensure_gc_parent(destination.parent)
    if os.stat(source.parent).st_dev != os.stat(destination.parent).st_dev:
        raise QueueConflictError(f"GC move would cross filesystems: {source}")
    try:
        source.replace(destination)
    except OSError as exc:
        raise queue_conflict_from_cause(
            f"GC could not quarantine {source}",
            cause=exc,
            logger=logger,
        ) from exc
    return True


def purge_quarantined_tree_batch(root: Path, *, limit: int) -> tuple[int, bool]:
    """Remove at most ``limit`` entries from one quarantined owned tree."""
    return purge_tree_batch(root, limit=limit)


def purge_tree_batch(root: Path, *, limit: int) -> tuple[int, bool]:
    if limit < 0 or limit > 100:
        raise ValueError("GC purge limit must be between 0 and 100")
    if limit == 0:
        return 0, _path_lstat(root) is None
    removed = 0
    while removed < limit:
        deleted = _purge_one_gc_entry(root, root=root)
        if not deleted:
            break
        removed += 1
    return removed, _path_lstat(root) is None


def _purge_one_gc_entry(path: Path, *, root: Path) -> bool:
    root_stat = _path_lstat(root)
    if root_stat is None:
        return False
    if not stat.S_ISDIR(root_stat.st_mode) or queue_layout.record_is_reparse(root_stat):
        raise QueueConflictError(f"GC trash root is not a regular directory: {root}")
    candidate = path
    depth = 0
    inspected = 0
    while True:
        inspected += 1
        if inspected > queue_layout.MAX_GC_PURGE_SCAN_ENTRIES:
            raise QueueConflictError(f"GC trash traversal exceeded its entry bound: {root}")
        candidate_stat = _path_lstat(candidate)
        if candidate_stat is None:
            return False
        is_directory = stat.S_ISDIR(candidate_stat.st_mode)
        if (
            stat.S_ISLNK(candidate_stat.st_mode)
            or queue_layout.record_is_reparse(candidate_stat)
            or not is_directory
        ):
            if candidate == root:
                raise QueueConflictError(f"GC trash root is not a regular directory: {root}")
            _remove_gc_candidate(
                root,
                candidate,
                root_stat=root_stat,
                candidate_stat=candidate_stat,
            )
            return True
        try:
            with os.scandir(candidate) as entries:
                entry = next(entries, None)
        except OSError as exc:
            raise queue_conflict_from_cause(
                f"GC could not scan quarantined directory {candidate}",
                cause=exc,
                logger=logger,
            ) from exc
        after_scan = _path_lstat(candidate)
        if after_scan is None or not os.path.samestat(candidate_stat, after_scan):
            raise QueueConflictError(f"GC trash changed during traversal: {candidate}")
        if entry is None:
            _remove_gc_candidate(
                root,
                candidate,
                root_stat=root_stat,
                candidate_stat=candidate_stat,
            )
            return True
        depth += 1
        if depth > queue_layout.MAX_GC_PURGE_DEPTH:
            raise QueueConflictError(f"GC trash traversal exceeded its depth bound: {root}")
        candidate = Path(entry.path)


def _remove_gc_candidate(
    root: Path,
    candidate: Path,
    *,
    root_stat: os.stat_result,
    candidate_stat: os.stat_result,
) -> None:
    if os.name != "nt":
        _remove_gc_candidate_posix(root, candidate, candidate_stat=candidate_stat)
        return
    current_root = _path_lstat(root)
    current_candidate = _path_lstat(candidate)
    if (
        current_root is None
        or current_candidate is None
        or not os.path.samestat(root_stat, current_root)
        or not os.path.samestat(candidate_stat, current_candidate)
    ):
        raise QueueConflictError(f"GC trash changed before deletion: {candidate}")
    _validate_gc_candidate_ancestry(root, candidate)
    try:
        if stat.S_ISDIR(candidate_stat.st_mode):
            os.rmdir(candidate)
        else:
            candidate.unlink()
    except OSError as exc:
        raise queue_conflict_from_cause(
            f"GC could not remove quarantined path {candidate}",
            cause=exc,
            logger=logger,
        ) from exc


def _remove_gc_candidate_posix(
    root: Path,
    candidate: Path,
    *,
    candidate_stat: os.stat_result,
) -> None:
    anchor = root if candidate != root else root.parent
    relative = candidate.relative_to(anchor)
    parts = relative.parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise QueueConflictError(f"GC candidate escaped its trash root: {candidate}")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptors: list[int] = []
    try:
        descriptor = os.open(anchor, flags)
        descriptors.append(descriptor)
        for part in parts[:-1]:
            descriptor = os.open(part, flags, dir_fd=descriptor)
            descriptors.append(descriptor)
        name = parts[-1]
        current = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
        if not os.path.samestat(candidate_stat, current):
            raise QueueConflictError(f"GC trash changed before deletion: {candidate}")
        if stat.S_ISDIR(current.st_mode):
            os.rmdir(name, dir_fd=descriptor)
        else:
            os.unlink(name, dir_fd=descriptor)
    except QueueConflictError:
        raise
    except OSError as exc:
        raise queue_conflict_from_cause(
            f"GC could not remove quarantined path {candidate}",
            cause=exc,
            logger=logger,
        ) from exc
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _validate_gc_candidate_ancestry(root: Path, candidate: Path) -> None:
    relative = candidate.relative_to(root) if candidate != root else Path()
    current = root
    for part in relative.parts[:-1]:
        current /= part
        current_stat = os.lstat(current)
        if not stat.S_ISDIR(current_stat.st_mode) or queue_layout.record_is_reparse(current_stat):
            raise QueueConflictError(f"GC candidate has unsafe ancestry: {candidate}")
