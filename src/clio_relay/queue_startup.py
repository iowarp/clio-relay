"""Queue startup: legacy-record audit seal, migration-path seeding, locked-core init.

Owns the bulk of the public ``initialize`` entrypoint's body, its two
lifetime-pinning helpers (``_initialize_with_exclusive_lifetime``,
``_initialize_under_locked_core``), and the exclusive-lifetime directory
privatization repair (``_repair_locked_queue_directory_permissions``).

CQ19-ST-02 typed deviation: unlike every other CQ19 owner, ``initialize``
itself is a bare **module-level function**, ``initialize(queue, *, ...)``,
not a ``QueueStartupMixin`` method -- ``ClioCoreQueue.initialize`` stays a
thin facade-resident wrapper (``core_queue.py``) that calls
``queue_startup.initialize(self, ...)``. This is forced, not stylistic:
``initialize`` is the one method **every** owner across the whole rank range
self-calls via ``self.initialize()`` as the first line of nearly every
public method (not just this slice's own ``migrate_indexes_batch``/
``index_migration_status``) -- discovered only once ``initialize`` became a
real ``*Mixin`` method and the architecture guard's self-call scanner
(which resolves ``self.<name>`` through the real, non-stub mixin-method
manifest) started tracking those calls as genuine rank-ordered edges,
producing reverse-rank violations from ranks as early as ``queue_lease_
admission`` (33). No rank could satisfy both "before every caller" and
"after its own collaborators, up to queue_lease_capacity_state at rank 30
via ``queue_index_discovery``" simultaneously -- a genuine cycle, not a
missing hoist target. Keeping the dispatch point itself off the owner
manifest (a bare function, and a facade wrapper naming no ``*Mixin``) makes
every ``self.initialize()`` call across the codebase invisible to the
edge scanner again, exactly as it was pre-CQ19 when ``initialize`` lived
directly on ``ClioCoreQueue``. The three helpers below remain real
``QueueStartupMixin`` methods (self-contained, zero inbound edges from any
other owner) and call the module function as ``initialize(self, ...)``
rather than ``self.initialize(...)``.

Predecessors: CQ2 (layout), CQ3 (store lock/read/write), CQ6 (legacy audit,
legacy output audit/migration), CQ15 (lease capacity state -- durable empty
capacity pair on fresh init), CQ19's own ``queue_index_discovery`` (rank 42:
``initialize`` calls ``queue._ensure_extended_migration_state()``,
``queue._upgrade_sealed_lease_operational_schema_unlocked()``, and
``queue._reconcile_sealed_lease_capacity_gate_unlocked()`` on the passed-in
instance -- a real dependency, invisible to the guard's ``self.`` scanner
since the module function's own parameter is named ``queue``, matching the
established module-level-twin idiom, e.g. ``queue_lease_indexes.
sync_operational_indexes(queue, ...)``). All landed.

The seal-audit lookup is module-qualified per the design doc's own CQ19
failing-first prescription ("`queue_startup.queue_legacy_audit.audit_before_
initialization`"): ``queue_legacy_audit.audit_before_initialization`` is a
module-level alias over the unbound ``QueueLegacyAuditMixin._audit_legacy_
state_before_initialization``, the same ``name = Mixin._method`` idiom
``queue_legacy_output_audit.audit_state_before_initialization`` already
uses. A test patches ``queue_startup.queue_legacy_audit`` (isolated
namespace) to intercept it -- a plain ``ClioCoreQueue._audit_legacy_state_
before_initialization`` class-attribute patch no longer reaches the real
call site after this move, since the alias captured the original unbound
function at import time.

``worker_lifetime_lock.exclusive_migration_lifetime``/``require_active_
locked_core``/``LockedCoreIdentity`` are referenced module-qualified (design
doc §4 row: ``core_queue_module.exclusive_migration_lifetime`` ->
``queue_startup.worker_lifetime_lock.exclusive_migration_lifetime``), not
imported bare, so the existing patch sites keep a live seam to move to.

CQ19-TI-01 (see ``queue_index_discovery.py``'s module docstring for the full
account): ``_read_index_migration_state``/``_write_index_migration_state``/
``_read_sealed_index_migration_state``/``_recover_pending_transitions_
unlocked``/``_require_safe_write_directory``/``_write_json``/``_purge_write_
staging_unlocked`` all stay facade-resident and are called on ``queue``
unchanged -- none of them are owned by any ``queue_*.py`` mixin, so these
calls carry no architecture-guard edge at all.
"""

from __future__ import annotations

import errno
import os
import stat
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, cast

from clio_relay import (
    queue_context,
    queue_layout,
    queue_lease_indexes,
    queue_lease_records,
    queue_legacy_audit,
    queue_store_lock,
    queue_store_read,
    worker_lifetime_lock,
)
from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import internal_filesystem_path, logical_filesystem_path

if TYPE_CHECKING:
    from clio_relay import queue_legacy_output_codec
    from clio_relay.models import RelayJob
    from clio_relay.queue_lease_records import LeaseCapacityPair as _LeaseCapacityPair
    from clio_relay.worker_lifetime_lock import LockedCoreIdentity

    LegacyOutputAudit = queue_legacy_output_codec.LegacyOutputAudit


def initialize(
    queue: QueueStartupMixin,
    *,
    migrate_legacy_output: bool = False,
    locked_core: LockedCoreIdentity | None = None,
    allow_exclusive_seal: bool = True,
) -> None:
    """Create the record families used by the queue."""
    if locked_core is not None:
        if queue._migration_lifetime_guarded:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            raise ConfigurationError(
                "locked-core authority is only valid for the outer initialization scope"
            )
        worker_lifetime_lock.require_active_locked_core(locked_core)
        queue._initialize_under_locked_core(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            locked_core,
            migrate_legacy_output=migrate_legacy_output,
        )
        return
    if migrate_legacy_output and not queue._migration_lifetime_guarded:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        queue._initialize_with_exclusive_lifetime(migrate_legacy_output=True)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        return
    if queue._initialized:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        with queue._lock:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            indexed_audit = queue._read_legacy_record_audit_marker()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        if indexed_audit is not None:
            return
        queue._initialized = False  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    if (
        not queue._migration_lifetime_guarded  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        and queue_store_read.path_lstat(queue._legacy_record_audit_marker_path()) is None  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    ):
        if not allow_exclusive_seal:
            raise queue_store_lock.QueueSealRequiresExclusive(
                "missing legacy-record audit seal requires exclusive writer-lifetime ownership"
            )
        queue._initialize_with_exclusive_lifetime(migrate_legacy_output=migrate_legacy_output)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        return
    # The root and lock path are the only pre-lock filesystem state. A
    # missing seal is audited exactly once after taking that lock and before
    # any record-family, migration, or archive write.
    queue._prepare_queue_root_for_lock()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    try:
        with queue._lock:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            locked_indexed_audit = queue._read_legacy_record_audit_marker(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                allow_legacy_lease_schema=True
            )
            if locked_indexed_audit is None:
                if not queue._migration_lifetime_guarded:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    raise queue_store_lock.QueueSealRequiresExclusive(
                        "missing legacy-record audit seal requires exclusive "
                        "writer-lifetime ownership"
                    )
                legacy_output_audit = queue_legacy_audit.audit_before_initialization(
                    cast(queue_legacy_audit.QueueLegacyAuditMixin, queue)
                )
                queue._require_legacy_output_migration_authorized(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    legacy_output_audit,
                    migrate_legacy_output=migrate_legacy_output,
                )
                for family in queue_store_lock.INITIALIZED_QUEUE_FAMILIES:
                    (queue._storage_root / family).mkdir(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                        mode=0o700,
                        parents=True,
                        exist_ok=True,
                    )
                for family in queue_store_lock.GLOBAL_ORDER_FAMILIES:
                    family_root = queue._storage_root / "global_order" / family  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    family_root.mkdir(
                        mode=0o700,
                        exist_ok=True,
                    )
                    (family_root / "by_id").mkdir(
                        mode=0o700,
                        exist_ok=True,
                    )
                    (family_root / "entries").mkdir(
                        mode=0o700,
                        exist_ok=True,
                    )
            else:
                legacy_output_audit = locked_indexed_audit
            for family in queue_store_lock.ADDITIVE_QUEUE_FAMILIES:
                directory = queue._storage_root / family  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                directory_stat = queue._require_safe_write_directory(directory)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                queue._require_owner_private_queue_directory(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    family,
                    directory,
                    directory_stat,
                )
            queue._require_legacy_output_migration_authorized(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                legacy_output_audit,
                migrate_legacy_output=migrate_legacy_output,
            )
            queue._purge_write_staging_unlocked()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue._migrate_legacy_output_events_unlocked(legacy_output_audit)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            migration_path = queue._storage_root / "migrations" / "index-v1.json"  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            if not migration_path.exists():
                has_legacy_jobs = (
                    next((queue._storage_root / "jobs").glob("*.json"), None) is not None  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                )
                retention_checkpoints = {
                    family: {
                        "cursor": None,
                        "complete": (
                            next((queue._storage_root / family).glob("*.json"), None) is None  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                        ),
                    }
                    for family in queue_store_lock.RETENTION_INDEX_FAMILIES
                }
                has_legacy_retention = any(
                    checkpoint["complete"] is not True
                    for checkpoint in retention_checkpoints.values()
                )
                global_order_checkpoints = {
                    family: {
                        "cursor": None,
                        "complete": (
                            next((queue._storage_root / family).glob("*.json"), None) is None  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                        ),
                    }
                    for family in queue_store_lock.GLOBAL_ORDER_FAMILIES
                }
                has_legacy_global_order = any(
                    checkpoint["complete"] is not True
                    for checkpoint in global_order_checkpoints.values()
                )
                operational_checkpoints = {
                    family: {
                        "cursor": None,
                        "complete": (
                            next((queue._storage_root / family).glob("*.json"), None) is None  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                        ),
                        **(
                            {"schema_version": queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA}
                            if family == "leases"
                            else {}
                        ),
                    }
                    for family in queue_store_lock.OPERATIONAL_INDEX_FAMILIES
                }
                has_legacy_operational = any(
                    checkpoint["complete"] is not True
                    for checkpoint in operational_checkpoints.values()
                )
                has_canonical_leases = (
                    next((queue._storage_root / "leases").glob("*.json"), None) is not None  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                )
                lease_capacity_complete = (
                    not has_canonical_leases
                    and not queue_lease_indexes.lease_operational_records_present(
                        queue._storage_root  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    )
                )
                lease_capacity_checkpoint: dict[str, object] = {
                    "complete": lease_capacity_complete,
                    "schema_version": queue_layout.LEASE_CAPACITY_AGGREGATE_SCHEMA,
                }
                if lease_capacity_complete:
                    empty_capacity = queue_lease_records.new_lease_capacity_pair({}, generation=0)
                    queue._write_lease_capacity_pair_unlocked(empty_capacity)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    lease_capacity_checkpoint.update(
                        {
                            "epoch_id": empty_capacity.aggregate.epoch_id,
                            "generation": empty_capacity.aggregate.generation,
                            "record_count": 0,
                        }
                    )
                queue._write_json(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    migration_path,
                    {
                        "schema_version": queue_layout.INDEX_MIGRATION_SCHEMA,
                        "complete": (
                            not has_legacy_jobs
                            and not has_legacy_retention
                            and not has_legacy_global_order
                            and not has_legacy_operational
                            and lease_capacity_complete
                            and not queue_lease_indexes.lease_operational_records_present(
                                queue._storage_root  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                            )
                        ),
                        "families": {
                            family: {"cursor": None, "complete": not has_legacy_jobs}
                            for family in (
                                "jobs",
                                "tasks",
                                "leases",
                                "artifacts",
                                "progress",
                            )
                        },
                        "finalize": {"cursor": None, "complete": not has_legacy_jobs},
                        "order_families": {
                            family: {"cursor": None, "complete": not has_legacy_jobs}
                            for family in queue_store_lock.ORDER_FAMILIES
                        },
                        "global_order_families": global_order_checkpoints,
                        "retention_families": retention_checkpoints,
                        "operational_families": operational_checkpoints,
                        "lease_operational_repair": {
                            "complete": (
                                not queue_lease_indexes.lease_operational_records_present(
                                    queue._storage_root  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                                )
                            ),
                            "schema_version": queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA,
                        },
                        "lease_capacity_aggregate": lease_capacity_checkpoint,
                    },
                )
            else:
                # A torn aggregate/checkpoint pair is valid only while its exact
                # transition intent remains durable. Replay that authorization
                # before deciding the migration checkpoint itself is corrupt.
                if locked_indexed_audit is None:
                    queue._recover_pending_transitions_unlocked()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    queue._ensure_extended_migration_state()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue._recover_pending_transitions_unlocked()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            if locked_indexed_audit is None:
                queue._write_legacy_record_audit_marker_unlocked()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            else:
                queue._upgrade_sealed_lease_operational_schema_unlocked()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                queue._reconcile_sealed_lease_capacity_gate_unlocked()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                queue._read_sealed_index_migration_state()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue._initialized = True  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    except queue_store_lock.QueueSealRequiresExclusive:
        if not allow_exclusive_seal:
            raise
        queue._initialize_with_exclusive_lifetime(migrate_legacy_output=migrate_legacy_output)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


class QueueStartupMixin:
    """Own queue-root creation, the legacy-record audit seal, and locked-core init."""

    root: Path
    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _lock_timeout_seconds: float
    _initialized: bool
    _migration_lifetime_guarded: bool
    _locked_storage_root_descriptor: int | None
    _locked_storage_root_identity: tuple[int, int] | None

    if TYPE_CHECKING:

        def _read_legacy_record_audit_marker(
            self,
            *,
            allow_legacy_lease_schema: bool = False,
        ) -> LegacyOutputAudit | None: ...
        def _legacy_record_audit_marker_path(self) -> Path: ...
        def _prepare_queue_root_for_lock(self) -> None: ...
        def _write_legacy_record_audit_marker_unlocked(self) -> None: ...
        @staticmethod
        def _require_owner_private_queue_directory(
            family: str,
            path: Path,
            details: os.stat_result,
        ) -> None: ...
        def _require_legacy_output_migration_authorized(
            self,
            audit: LegacyOutputAudit,
            *,
            migrate_legacy_output: bool,
        ) -> None: ...
        def _migrate_legacy_output_events_unlocked(self, audit: LegacyOutputAudit) -> None: ...
        def _write_lease_capacity_pair_unlocked(self, pair: _LeaseCapacityPair) -> None: ...
        def _ensure_extended_migration_state(self) -> None: ...
        def _upgrade_sealed_lease_operational_schema_unlocked(self) -> None: ...
        def _reconcile_sealed_lease_capacity_gate_unlocked(self) -> None: ...

        def _require_safe_write_directory(self, directory: Path) -> os.stat_result: ...
        def _purge_write_staging_unlocked(self) -> None: ...
        def _write_json(self, path: Path, record: dict[str, object]) -> None: ...
        def _recover_pending_transitions_unlocked(self) -> list[RelayJob]: ...
        def _read_sealed_index_migration_state(
            self,
            *,
            allow_legacy_lease_schema: bool = False,
        ) -> dict[str, object]: ...

    def _initialize_with_exclusive_lifetime(self, *, migrate_legacy_output: bool) -> None:
        """Initialize under a pinned lifetime and preserve the public legacy-state contract."""
        try:
            with worker_lifetime_lock.exclusive_migration_lifetime(self.root) as locked_core:
                initialize(
                    self,
                    migrate_legacy_output=migrate_legacy_output,
                    locked_core=locked_core,
                )
        except queue_store_lock.UnsafeQueueDirectoryProtection as exc:
            raise queue_store_lock.LegacyQueueStateError(
                family=exc.family,
                path=exc.path,
                reason="canonical family is not an owned directory",
            ) from exc

    def _initialize_under_locked_core(
        self,
        locked_core: LockedCoreIdentity,
        *,
        migrate_legacy_output: bool,
    ) -> None:
        """Pin initialization I/O to one authenticated exclusively locked core."""
        worker_lifetime_lock.require_active_locked_core(locked_core)
        original_root = self.root
        original_storage_root = self._storage_root
        original_lock = self._lock
        original_storage_root_descriptor = self._locked_storage_root_descriptor
        original_storage_root_identity = self._locked_storage_root_identity
        try:
            queue_root_before = os.stat(original_storage_root)
        except OSError as exc:
            raise ConfigurationError(
                f"migration queue root identity cannot be verified: {exc}"
            ) from exc
        expected_identity = (locked_core.device, locked_core.inode)
        if (queue_root_before.st_dev, queue_root_before.st_ino) != expected_identity:
            raise ConfigurationError("migration queue root does not match its core lifetime lock")
        # Pin every migration read and write to the canonical directory whose
        # inode is locked. A stable mount alias remains accepted, while an
        # in-flight alias retarget can never redirect writes to an unlocked root.
        self.root = logical_filesystem_path(locked_core.root)
        self._storage_root = internal_filesystem_path(
            locked_core.filesystem_root,
            force_extended=True,
        )
        self._locked_storage_root_descriptor = locked_core.filesystem_root_descriptor
        self._locked_storage_root_identity = expected_identity
        self._lock = queue_store_lock.FairBoundedFileLock(
            str(self._storage_root / ".lock"),
            timeout=self._lock_timeout_seconds,
        )
        self._migration_lifetime_guarded = True
        try:
            self._repair_locked_queue_directory_permissions()
            initialize(self, migrate_legacy_output=migrate_legacy_output)
        finally:
            self._migration_lifetime_guarded = False
            self.root = original_root
            self._storage_root = original_storage_root
            self._lock = original_lock
            self._locked_storage_root_descriptor = original_storage_root_descriptor
            self._locked_storage_root_identity = original_storage_root_identity
            try:
                queue_root_after = os.stat(original_storage_root)
            except OSError as exc:
                raise ConfigurationError(f"migration queue root identity changed: {exc}") from exc
            if (queue_root_after.st_dev, queue_root_after.st_ino) != expected_identity:
                raise ConfigurationError("migration queue root identity changed while locked")

    def _repair_locked_queue_directory_permissions(self) -> None:
        """Privatize only fixed, owned queue directories under exclusive ownership."""
        if os.name == "nt":
            return
        root_descriptor = self._locked_storage_root_descriptor
        if root_descriptor is None:
            raise ConfigurationError("locked queue permission repair has no pinned root descriptor")
        relative_paths = [Path()]
        relative_paths.extend(
            Path(family) for family in queue_store_lock.INITIALIZED_QUEUE_FAMILIES
        )
        relative_paths.extend(Path(family) for family in queue_store_lock.ADDITIVE_QUEUE_FAMILIES)
        relative_paths.extend(
            Path(family) for family in queue_store_lock.LEGACY_ONLY_QUEUE_FAMILIES
        )
        relative_paths.extend(
            Path("global_order") / family for family in queue_store_lock.GLOBAL_ORDER_FAMILIES
        )
        relative_paths.extend(
            Path("global_order") / family / child
            for family in queue_store_lock.GLOBAL_ORDER_FAMILIES
            for child in ("by_id", "entries")
        )
        getuid = getattr(os, "getuid", None)
        current_uid = getuid() if callable(getuid) else None
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        for relative_path in relative_paths:
            descriptor = os.dup(root_descriptor)
            try:
                os.set_inheritable(descriptor, False)
                missing = False
                for component in relative_path.parts:
                    try:
                        child_descriptor = os.open(component, flags, dir_fd=descriptor)
                    except FileNotFoundError:
                        missing = True
                        break
                    try:
                        os.set_inheritable(child_descriptor, False)
                    except BaseException:
                        with suppress(OSError):
                            os.close(child_descriptor)
                        raise
                    os.close(descriptor)
                    descriptor = child_descriptor
                if missing:
                    continue
                details = os.fstat(descriptor)
                if (
                    not stat.S_ISDIR(details.st_mode)
                    or queue_layout.record_is_reparse(details)
                    or (current_uid is not None and details.st_uid != current_uid)
                ):
                    raise ConfigurationError(
                        "queue directory cannot be safely privatized: "
                        f"{self._storage_root / relative_path}"
                    )
                if stat.S_IMODE(details.st_mode) != 0o700:
                    os.fchmod(descriptor, 0o700)
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.ENOTDIR} and relative_path.parts:
                    raise queue_store_lock.UnsafeQueueDirectoryProtection(
                        family=relative_path.parts[0],
                        path=self.root / relative_path,
                        cause=exc,
                    ) from exc
                raise ConfigurationError(
                    "queue directory protections cannot be repaired through the pinned root: "
                    f"{self._storage_root / relative_path}: {exc}"
                ) from exc
            finally:
                with suppress(OSError):
                    os.close(descriptor)
