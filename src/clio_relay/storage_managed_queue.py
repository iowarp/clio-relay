"""The storage-managed queue: writer lifetime plus storage-enforcing composition.

Owns :class:`StorageManagedQueue` itself -- construction, the closed-queue
guard, and shared-writer-lifetime release -- composed from
:class:`~clio_relay.storage_managed_queue_admission.StorageManagedQueueAdmissionMixin`
(reserve-before-admit submission and the input-ingest retry lifecycle) and
:class:`~clio_relay.storage_managed_queue_leasing.StorageManagedQueueLeasingMixin`
(terminal-release wrapping for transitions, recovery, and leasing).
"""

from __future__ import annotations

import os
from contextlib import suppress
from pathlib import Path

from clio_relay.core_queue import DEFAULT_CORE_LOCK_TIMEOUT_SECONDS
from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import internal_filesystem_path, logical_filesystem_path
from clio_relay.storage_managed_queue_admission import StorageManagedQueueAdmissionMixin
from clio_relay.storage_managed_queue_leasing import StorageManagedQueueLeasingMixin
from clio_relay.storage_runtime_core import StorageRuntime
from clio_relay.worker_lifetime_lock import LockedCoreIdentity, WorkerLifetimeLock


class StorageManagedQueue(StorageManagedQueueAdmissionMixin, StorageManagedQueueLeasingMixin):
    """Clio-core facade with durable reserve-before-admit and terminal release."""

    def __init__(
        self,
        root: Path,
        *,
        storage_runtime: StorageRuntime,
        writer_lifetime_lock: WorkerLifetimeLock | None = None,
        owns_writer_lifetime_lock: bool = False,
        lock_timeout_seconds: float = DEFAULT_CORE_LOCK_TIMEOUT_SECONDS,
    ) -> None:
        self._closed = False
        if logical_filesystem_path(Path(root).absolute()) != logical_filesystem_path(
            storage_runtime.config.core_root.absolute()
        ):
            raise ValueError("managed queue root must match the storage runtime core root")
        if owns_writer_lifetime_lock and writer_lifetime_lock is None:
            raise ValueError("an owned writer lifetime lock must be provided")
        if writer_lifetime_lock is not None:
            if not writer_lifetime_lock.acquired or writer_lifetime_lock.mode != "shared":
                raise ValueError("managed queue writer lifetime lock must hold shared ownership")
            root_stat = os.stat(internal_filesystem_path(Path(root), force_extended=True))
            lock_stat = os.stat(
                internal_filesystem_path(
                    writer_lifetime_lock.core_dir,
                    force_extended=True,
                )
            )
            if (root_stat.st_dev, root_stat.st_ino) != (lock_stat.st_dev, lock_stat.st_ino):
                raise ValueError("managed queue root must match its writer lifetime lock")
        super().__init__(root, lock_timeout_seconds=lock_timeout_seconds)
        self.storage_runtime = storage_runtime
        self._writer_lifetime_lock = writer_lifetime_lock
        self._owns_writer_lifetime_lock = owns_writer_lifetime_lock

    def __getattribute__(self, name: str) -> object:
        """Reject every public queue operation after lifetime ownership ends."""
        value = super().__getattribute__(name)
        if name == "close" or name.startswith("_") or not callable(value):
            return value
        try:
            closed = super().__getattribute__("_closed")
        except AttributeError:
            return value
        if closed:
            raise ConfigurationError("managed queue is closed and cannot perform operations")
        return value

    @property
    def closed(self) -> bool:
        """Return whether this queue's writer lifetime has ended."""
        return self._closed

    def initialize(
        self,
        *,
        migrate_legacy_output: bool = False,
        locked_core: LockedCoreIdentity | None = None,
        allow_exclusive_seal: bool = False,
    ) -> None:
        """Initialize only while this managed queue retains writer ownership."""
        del allow_exclusive_seal
        if self._closed:
            raise ConfigurationError("managed queue is closed and cannot perform operations")
        super().initialize(
            migrate_legacy_output=migrate_legacy_output,
            locked_core=locked_core,
            allow_exclusive_seal=False,
        )

    def close(self) -> None:
        """Release queue-owned core writer lifetime ownership."""
        self._closed = True
        if not self._owns_writer_lifetime_lock:
            return
        self._owns_writer_lifetime_lock = False
        lifetime_lock = self._writer_lifetime_lock
        self._writer_lifetime_lock = None
        if lifetime_lock is not None:
            lifetime_lock.release()

    def __del__(self) -> None:
        with suppress(Exception):
            self.close()
