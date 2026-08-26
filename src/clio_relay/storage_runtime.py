"""Production queue admission and running-job storage coordination.

The filesystem policy is intentionally queue-agnostic.  This module supplies the
relay-specific pieces: deterministic per-job sizing, bounded startup adoption from
the authoritative active-job index, and a cheap running-child guard.

This module is a thin facade over its owner modules: the storage-decision
error hierarchy lives in ``storage_runtime_errors.py``, the target-agnostic
:class:`~clio_relay.storage_runtime_core.StorageRuntime` in
``storage_runtime_core.py``, and ``StorageManagedQueue``'s admission/leasing
method groups in ``storage_managed_queue_admission.py`` /
``storage_managed_queue_leasing.py`` (composed in ``storage_managed_queue.py``).
Every name below is re-exported because it has a real consumer outside this
file -- production code, or a test that imports it directly from
``clio_relay.storage_runtime``.

What stays resident here -- the production queue factory, the shared-writer
seal handoff, and the bounded index-migration driver -- does so because
``tests/test_worker_lifetime_lock.py`` patches their bare module globals
(``QUEUE_SEAL_LIFETIME_TIMEOUT_SECONDS``, ``exclusive_migration_lifetime``)
via ``monkeypatch.setattr(storage_runtime_module, ...)``, expecting those
names to be looked up in *this* module's namespace at call time. Moving the
functions to another owner module would silently stop the patches from
affecting behavior (the moved function's own bare-global lookups would
resolve against its new module's namespace instead), which is exactly the
kind of un-flagged regression a pure code move must not introduce.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from clio_relay.core_queue import ClioCoreQueue, QueueSealRequiresExclusive
from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.storage_managed_queue import StorageManagedQueue as StorageManagedQueue
from clio_relay.storage_policy import StorageReason
from clio_relay.storage_runtime_core import (
    STORAGE_RUNTIME_STATUS_SCHEMA as STORAGE_RUNTIME_STATUS_SCHEMA,
)
from clio_relay.storage_runtime_core import StorageRuntime as StorageRuntime
from clio_relay.storage_runtime_core import StorageRuntimeConfig as StorageRuntimeConfig
from clio_relay.storage_runtime_core import (
    storage_runtime_from_settings as storage_runtime_from_settings,
)
from clio_relay.storage_runtime_errors import StorageAdmissionError as StorageAdmissionError
from clio_relay.storage_runtime_errors import StorageRuntimeError as StorageRuntimeError
from clio_relay.storage_runtime_errors import StorageRuntimeViolation as StorageRuntimeViolation
from clio_relay.storage_runtime_errors import _denied_decision
from clio_relay.worker_lifetime_lock import (
    WorkerLifetimeLock,
    WorkerLifetimeLockUnavailable,
    exclusive_migration_lifetime,
)

if TYPE_CHECKING:
    from clio_relay.config import RelaySettings

QUEUE_SEAL_LIFETIME_TIMEOUT_SECONDS = 30.0
_MIGRATION_BATCH_SIZE = 10_000
_MIGRATION_FAMILY_BOUND = 20
_MIGRATION_FIXED_BATCHES = 32


def storage_managed_queue(
    settings: RelaySettings,
    *,
    migrate_legacy_output: bool = False,
    writer_lifetime_lock: WorkerLifetimeLock | None = None,
) -> StorageManagedQueue:
    """Create a production queue under shared writer or exclusive migration ownership."""
    if migrate_legacy_output:
        if writer_lifetime_lock is not None:
            raise ValueError("migration cannot reuse shared writer lifetime ownership")
        with exclusive_migration_lifetime(settings.core_dir) as locked_core:
            pinned_settings = settings.model_copy(update={"core_dir": locked_core.root})
            runtime = storage_runtime_from_settings(pinned_settings)
            queue = StorageManagedQueue(locked_core.root, storage_runtime=runtime)
            queue.initialize(
                migrate_legacy_output=True,
                locked_core=locked_core,
            )
            _complete_bounded_index_migration(queue, runtime)
            runtime.reconcile_startup(queue)
            queue.close()
            return queue
    owned_lifetime_lock: WorkerLifetimeLock | None = None
    lifetime_lock = writer_lifetime_lock
    if lifetime_lock is None:
        owned_lifetime_lock = WorkerLifetimeLock(
            settings.core_dir,
            mode="shared",
            timeout_seconds=QUEUE_SEAL_LIFETIME_TIMEOUT_SECONDS,
        ).acquire()
        lifetime_lock = owned_lifetime_lock
    if not lifetime_lock.acquired or lifetime_lock.mode != "shared":
        raise ValueError("production queue requires acquired shared writer lifetime ownership")
    try:
        initialize_queue_with_shared_writer_fencing(lifetime_lock)
        pinned_settings = settings.model_copy(update={"core_dir": lifetime_lock.core_dir})
        # Audit and initialize before StorageRuntime or StoragePolicy can create
        # `.storage`. A normal startup that encounters legacy output remains a
        # read-only refusal with respect to storage accounting and spool state.
        runtime = storage_runtime_from_settings(pinned_settings)
        queue = StorageManagedQueue(
            lifetime_lock.core_dir,
            storage_runtime=runtime,
            writer_lifetime_lock=lifetime_lock,
            owns_writer_lifetime_lock=owned_lifetime_lock is not None,
        )
        queue.initialize()
        _complete_bounded_index_migration(queue, runtime)
        runtime.reconcile_startup(queue)
    except BaseException:
        if owned_lifetime_lock is not None:
            owned_lifetime_lock.release()
        raise
    return queue


def initialize_queue_with_shared_writer_fencing(lifetime_lock: WorkerLifetimeLock) -> None:
    """Create a missing queue seal under exclusive ownership, then restore shared ownership."""
    core_dir = lifetime_lock.core_dir
    internal_core_dir = internal_filesystem_path(core_dir, force_extended=True)
    try:
        ClioCoreQueue(core_dir).initialize(allow_exclusive_seal=False)
        return
    except QueueSealRequiresExclusive:
        try:
            original_stat = os.stat(internal_core_dir)
        except OSError as exc:
            raise ConfigurationError(
                f"queue root identity cannot be captured before seal fencing: {exc}"
            ) from exc
        lifetime_lock.release()

    try:
        with exclusive_migration_lifetime(
            core_dir,
            timeout_seconds=QUEUE_SEAL_LIFETIME_TIMEOUT_SECONDS,
        ) as locked_core:
            if (locked_core.device, locked_core.inode) != (
                original_stat.st_dev,
                original_stat.st_ino,
            ):
                raise ConfigurationError(
                    "queue root changed before establishing its indexed-era seal"
                )
            ClioCoreQueue(locked_core.root).initialize(locked_core=locked_core)
    finally:
        if not lifetime_lock.acquired:
            try:
                lifetime_lock.acquire(
                    timeout_seconds=QUEUE_SEAL_LIFETIME_TIMEOUT_SECONDS,
                )
            except WorkerLifetimeLockUnavailable as exc:
                raise WorkerLifetimeLockUnavailable(
                    "timed out restoring shared writer ownership after queue seal handoff",
                    holder_diagnostic=exc.holder_diagnostic,
                ) from exc

    try:
        reacquired_stat = os.stat(
            internal_filesystem_path(
                lifetime_lock.core_dir,
                force_extended=True,
            )
        )
    except OSError as exc:
        raise ConfigurationError(
            f"queue root identity cannot be verified after seal fencing: {exc}"
        ) from exc
    if (original_stat.st_dev, original_stat.st_ino) != (
        reacquired_stat.st_dev,
        reacquired_stat.st_ino,
    ):
        raise ConfigurationError("queue root changed while establishing its indexed-era seal")
    ClioCoreQueue(lifetime_lock.core_dir).initialize(allow_exclusive_seal=False)


def _complete_bounded_index_migration(
    queue: ClioCoreQueue,
    runtime: StorageRuntime,
) -> None:
    max_family_batches = (
        runtime.config.limits.max_scan_entries + _MIGRATION_BATCH_SIZE - 1
    ) // _MIGRATION_BATCH_SIZE
    max_batches = _MIGRATION_FIXED_BATCHES + _MIGRATION_FAMILY_BOUND * max_family_batches
    status = queue.index_migration_status()
    for _batch in range(max_batches):
        if status.get("complete") is True:
            return
        status = queue.migrate_indexes_batch(batch_size=_MIGRATION_BATCH_SIZE)
    if status.get("complete") is True:
        return
    raise StorageRuntimeError(
        _denied_decision(
            StorageReason.SCAN_ENTRY_LIMIT,
            "queue index migration exceeded its bounded startup work limit",
            details={
                "batch_size": _MIGRATION_BATCH_SIZE,
                "max_batches": max_batches,
                "max_scan_entries_per_family": runtime.config.limits.max_scan_entries,
            },
        )
    )
