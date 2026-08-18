import logging
import os
import stat
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from clio_relay import (
    queue_layout,
    queue_legacy_output_codec,
    queue_store_lock,
    queue_store_read,
    queue_store_write,
)
from clio_relay.errors import QueueConflictError, queue_conflict_from_cause
from clio_relay.models import JobTombstone

LegacyOutputAudit = queue_legacy_output_codec.LegacyOutputAudit
LegacyOutputRecord = queue_legacy_output_codec.LegacyOutputRecord
logger = logging.getLogger(__name__)


def after_migration_phase(_phase: str, _path: Path) -> None:
    """Fault-injection seam after each durable legacy-output phase."""


class QueueLegacyOutputMigrationMixin:
    """Own authorized legacy-output writes on the composed queue facade."""

    _storage_root: Path

    if TYPE_CHECKING:
        # F12 (block-2 review): declared so these resolve through normal
        # self.method() lookup -- and therefore stay valid under a
        # ClioCoreQueue/instance patch -- instead of the prior
        # _AuditMixin.method(cast(_AuditMixin, self), ...) unbound-call
        # style, which bypassed MRO entirely.
        def _audit_one_legacy_output_event(
            self,
            path: Path,
            *,
            job_id: str,
            seq: int,
        ) -> LegacyOutputRecord | None: ...

        def _audit_legacy_output_auxiliary_state(self) -> None: ...

        def _iter_legacy_output_auxiliary_paths(
            self,
            family: Literal[
                "legacy_output_archives",
                "legacy_output_receipts",
                "legacy_output_retired",
            ],
        ) -> Iterable[tuple[Path, str, int]]: ...

    def _write_legacy_output_archive(self, path: Path, record: LegacyOutputRecord) -> None:
        if queue_store_read.path_lstat(path) is not None:
            queue_legacy_output_codec.validate_legacy_output_archive(path, record)
            return
        queue_store_write.write_bytes(
            self._storage_root,
            path,
            record.original_bytes,
            max_bytes=queue_layout.MAX_LEGACY_OUTPUT_RECORD_BYTES,
        )
        queue_legacy_output_codec.validate_legacy_output_archive(path, record)

    def _write_legacy_output_receipt(self, path: Path, record: LegacyOutputRecord) -> None:
        if queue_store_read.path_lstat(path) is not None:
            queue_legacy_output_codec.validate_legacy_output_receipt(path, record)
            return
        receipt = queue_legacy_output_codec.legacy_output_receipt(
            record,
            receipt_schema=queue_layout.LEGACY_OUTPUT_RECEIPT_SCHEMA,
        )
        queue_store_write.write_json(self._storage_root, path, receipt)
        queue_legacy_output_codec.validate_legacy_output_receipt(path, record)

    def _migrate_legacy_output_events_unlocked(self, audit: LegacyOutputAudit) -> None:
        if audit.marker_complete:
            return
        if len(audit.migration_keys) != audit.migration_records:
            raise QueueConflictError("legacy output migration plan is incomplete")
        migration_records = archive_bytes = 0
        for job_id, seq in audit.migration_keys:
            path = self._storage_root / "events" / job_id / f"{seq:020d}.json"
            if queue_store_read.path_lstat(path) is None:
                raise QueueConflictError(
                    f"legacy output event disappeared after its complete audit: {path}"
                )
            was_oversized = path.stat().st_size > queue_layout.RECORD_FAMILY_MAX_BYTES["events"]
            record = self._audit_one_legacy_output_event(path, job_id=job_id, seq=seq)
            if record is None:
                continue
            migration_records += 1
            archive_bytes += len(record.original_bytes)
            archive_path = queue_legacy_output_codec.archive_path(self._storage_root, job_id, seq)
            receipt_path = queue_legacy_output_codec.receipt_path(self._storage_root, job_id, seq)
            if was_oversized:
                self._write_legacy_output_archive(archive_path, record)
                after_migration_phase("archive", path)
                current = queue_store_read.read_bounded_record_bytes_once(
                    path,
                    limit=queue_layout.MAX_LEGACY_OUTPUT_RECORD_BYTES,
                )
                if current != record.original_bytes:
                    raise QueueConflictError(
                        f"legacy output event changed after validation: {path}"
                    )
                queue_store_write.write_bytes(
                    self._storage_root,
                    path,
                    record.replacement_bytes,
                    max_bytes=queue_layout.RECORD_FAMILY_MAX_BYTES["events"],
                )
                after_migration_phase("replacement", path)
            self._write_legacy_output_receipt(receipt_path, record)
            after_migration_phase("receipt", path)
        observed = LegacyOutputAudit(
            marker_complete=False,
            event_records=audit.event_records,
            migration_records=migration_records,
            archive_bytes=archive_bytes,
            migration_keys=audit.migration_keys,
        )
        if observed != audit:
            raise QueueConflictError("legacy output state changed after its complete audit")
        self._audit_legacy_output_auxiliary_state()
        receipt_paths = {
            (job_id, seq): path
            for path, job_id, seq in self._iter_legacy_output_auxiliary_paths(
                "legacy_output_receipts"
            )
        }
        if len(receipt_paths) != migration_records:
            raise QueueConflictError("legacy output receipt manifest is incomplete")
        manifest = queue_legacy_output_codec.legacy_output_receipt_manifest_sha256(receipt_paths)
        marker: dict[str, object] = {
            "schema_version": queue_layout.LEGACY_OUTPUT_MIGRATION_SCHEMA,
            "complete": True,
            "event_records": audit.event_records,
            "migration_records": migration_records,
            "archive_bytes": archive_bytes,
            "receipt_manifest_sha256": manifest,
        }
        marker_path = queue_legacy_output_codec.marker_path(self._storage_root)
        queue_store_write.write_json(self._storage_root, marker_path, marker)
        after_migration_phase("marker", marker_path)
        durable_marker = queue_legacy_output_codec.read_legacy_output_marker(self._storage_root)
        if durable_marker is None or durable_marker != LegacyOutputAudit(
            marker_complete=True,
            event_records=audit.event_records,
            migration_records=migration_records,
            archive_bytes=archive_bytes,
            receipt_manifest_sha256=manifest,
        ):
            raise QueueConflictError("legacy output migration marker was not durable")

    def _require_legacy_output_migration_authorized(
        self,
        audit: LegacyOutputAudit,
        *,
        migrate_legacy_output: bool,
    ) -> None:
        if audit.marker_complete or audit.migration_records == 0 or migrate_legacy_output:
            return
        raise queue_store_lock.LegacyQueueStateError(
            family="events",
            path=self._storage_root / "events",
            reason=(
                f"{audit.migration_records} exact v0.9 output event(s) require an explicitly "
                "authorized compatibility migration"
            ),
            action=(
                "stop and verify every process that can write this queue, then run "
                "clio-relay init --migrate-legacy-output"
            ),
        )

    def _retire_legacy_output_receipts_unlocked(self, tombstone: JobTombstone) -> bool:
        if not tombstone.records_trash_started:
            raise QueueConflictError(
                f"legacy output receipts cannot retire before GC authorization: {tombstone.job_id}"
            )
        job_id = queue_layout.QueueLayout.durable_key(tombstone.job_id)
        source = self._storage_root / "legacy_output_receipts" / job_id
        destination = self._storage_root / "legacy_output_retired" / job_id
        source_stat = queue_store_read.path_lstat(source)
        destination_stat = queue_store_read.path_lstat(destination)
        if source_stat is not None and destination_stat is not None:
            raise QueueConflictError(
                f"active and retired legacy output receipts both exist: {tombstone.job_id}"
            )
        if source_stat is None:
            if destination_stat is not None and (
                not stat.S_ISDIR(destination_stat.st_mode)
                or queue_layout.record_is_reparse(destination_stat)
            ):
                raise QueueConflictError(
                    f"retired legacy output receipt root is unsafe: {destination}"
                )
            return False
        if not stat.S_ISDIR(source_stat.st_mode) or queue_layout.record_is_reparse(source_stat):
            raise QueueConflictError(f"active legacy output receipt root is unsafe: {source}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        current = destination.parent
        while True:
            current_stat = os.lstat(current)
            if not stat.S_ISDIR(current_stat.st_mode) or queue_layout.record_is_reparse(
                current_stat
            ):
                raise QueueConflictError(f"GC destination contains an unsafe directory: {current}")
            if current.parent == current:
                break
            current = current.parent
        if os.stat(source.parent).st_dev != os.stat(destination.parent).st_dev:
            raise QueueConflictError(f"GC move would cross filesystems: {source}")
        try:
            source.replace(destination)
        except OSError as error:
            raise queue_conflict_from_cause(
                f"GC could not quarantine {source}", cause=error, logger=logger
            ) from error
        queue_store_write.fsync_write_directory(source.parent)
        queue_store_write.fsync_write_directory(destination.parent)
        retired_stat = os.lstat(destination)
        if not stat.S_ISDIR(retired_stat.st_mode) or queue_layout.record_is_reparse(retired_stat):
            raise QueueConflictError(f"retired legacy output receipt root is unsafe: {destination}")
        return True
