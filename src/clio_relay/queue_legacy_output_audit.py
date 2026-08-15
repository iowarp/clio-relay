"""Bounded legacy-output audit and compatibility validation."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable
from pathlib import Path
from typing import Literal, cast

from clio_relay import (
    queue_context,
    queue_layout,
    queue_legacy_output_codec,
    queue_store_lock,
    queue_store_read,
)
from clio_relay.errors import QueueConflictError
from clio_relay.models import JobTombstone, RelayEvent

LegacyQueueStateError = queue_store_lock.LegacyQueueStateError
LegacyOutputAudit = queue_legacy_output_codec.LegacyOutputAudit
LegacyOutputRecord = queue_legacy_output_codec.LegacyOutputRecord


class QueueLegacyOutputAuditMixin:
    """Own legacy-output audit behavior on the composed queue facade."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol

    def _record_from_legacy_compatibility_event(
        self,
        path: Path,
        event: RelayEvent,
        event_bytes: bytes,
        *,
        job_id: str,
        seq: int,
    ) -> LegacyOutputRecord:
        payload = event.payload
        compatibility = payload.get("legacy_output")
        if not isinstance(compatibility, dict):
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason="legacy output compatibility metadata is not an object",
            )
        typed_compatibility = cast(dict[str, object], compatibility)
        expected_keys = {
            "schema_version",
            "archive_path",
            "archive_sha256",
            "archive_size_bytes",
            "original_message_utf8_bytes",
            "original_payload_text_utf8_bytes",
            "representation",
        }
        if (
            set(typed_compatibility) != expected_keys
            or typed_compatibility.get("schema_version")
            != queue_layout.LEGACY_OUTPUT_COMPATIBILITY_SCHEMA
        ):
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason="legacy output compatibility metadata has an unknown schema",
            )
        representation = typed_compatibility.get("representation")
        expected_payload_keys = (
            {"stream", "text", "legacy_output"}
            if representation == "payload_text"
            else {"stream", "legacy_output"}
        )
        if (
            representation not in {"payload_text", "archive"}
            or set(payload) != expected_payload_keys
        ):
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason="legacy output compatibility payload has an unknown shape",
            )
        archive_path = queue_legacy_output_codec.archive_path(self._storage_root, job_id, seq)
        if (
            typed_compatibility.get("archive_path")
            != archive_path.relative_to(self._storage_root).as_posix()
        ):
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason="legacy output compatibility archive path is not canonical",
            )
        try:
            original = queue_legacy_output_codec.read_v09_legacy_output_record(
                archive_path,
                job_id=job_id,
                seq=seq,
            )
        except (OSError, ValueError, QueueConflictError) as error:
            raise LegacyQueueStateError(
                family="legacy_output_archives",
                path=archive_path,
                reason=f"legacy output archive is invalid: {type(error).__name__}",
            ) from error
        if event != original.replacement or event_bytes != original.replacement_bytes:
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason="legacy output compatibility event does not match its exact archive",
            )
        return original

    def _audit_one_legacy_output_event(
        self,
        path: Path,
        *,
        job_id: str,
        seq: int,
    ) -> LegacyOutputRecord | None:
        try:
            path_stat = os.lstat(path)
            if path_stat.st_size > queue_layout.RECORD_FAMILY_MAX_BYTES["events"]:
                record = queue_legacy_output_codec.read_v09_legacy_output_record(
                    path,
                    job_id=job_id,
                    seq=seq,
                )
                archive_path = queue_legacy_output_codec.archive_path(
                    self._storage_root, job_id, seq
                )
                if queue_store_read.path_lstat(archive_path) is not None:
                    queue_legacy_output_codec.validate_legacy_output_archive(archive_path, record)
                receipt_path = queue_legacy_output_codec.receipt_path(
                    self._storage_root, job_id, seq
                )
                if queue_store_read.path_lstat(receipt_path) is not None:
                    raise LegacyQueueStateError(
                        family="legacy_output_receipts",
                        path=receipt_path,
                        reason="receipt exists before the compatibility event replacement",
                    )
                return record
            event_bytes = queue_store_read.read_bounded_record_bytes(path)
            event = RelayEvent.model_validate_json(event_bytes)
        except LegacyQueueStateError:
            raise
        except (OSError, ValueError, QueueConflictError) as error:
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason=f"event record is invalid: {type(error).__name__}: {error}",
            ) from error
        if event.job_id != job_id:
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason="event directory/content identity mismatch for job_id",
            )
        if event.seq != seq:
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason="event filename/content sequence mismatch",
            )
        if "legacy_output" not in event.payload:
            return None
        record = self._record_from_legacy_compatibility_event(
            path,
            event,
            event_bytes,
            job_id=job_id,
            seq=seq,
        )
        receipt_path = queue_legacy_output_codec.receipt_path(self._storage_root, job_id, seq)
        if queue_store_read.path_lstat(receipt_path) is not None:
            queue_legacy_output_codec.validate_legacy_output_receipt(receipt_path, record)
        return record

    def _validate_legacy_output_event_access(self, path: Path, event: RelayEvent) -> None:
        """Validate one compatibility archive and receipt when its event is read."""
        if "legacy_output" not in event.payload:
            return
        try:
            event_bytes = queue_store_read.read_bounded_record_bytes(path)
        except (OSError, ValueError, QueueConflictError) as error:
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason=f"legacy output event is invalid on access: {type(error).__name__}",
            ) from error
        record = self._record_from_legacy_compatibility_event(
            path,
            event,
            event_bytes,
            job_id=event.job_id,
            seq=event.seq,
        )
        receipt_path = queue_legacy_output_codec.receipt_path(
            self._storage_root, event.job_id, event.seq
        )
        if queue_store_read.path_lstat(receipt_path) is None:
            raise LegacyQueueStateError(
                family="legacy_output_receipts",
                path=receipt_path,
                reason="legacy output compatibility event has no durable receipt",
            )
        queue_legacy_output_codec.validate_legacy_output_receipt(receipt_path, record)

    def _iter_legacy_output_auxiliary_paths(
        self,
        family: Literal[
            "legacy_output_archives",
            "legacy_output_receipts",
            "legacy_output_retired",
        ],
    ) -> Iterable[tuple[Path, str, int]]:
        yield from queue_legacy_output_codec.iter_legacy_event_paths(
            self._storage_root,
            family,
            max_directories=queue_layout.MAX_LEGACY_OUTPUT_MIGRATION_RECORDS,
            max_records=queue_layout.MAX_LEGACY_OUTPUT_MIGRATION_RECORDS,
        )

    def _audit_legacy_output_auxiliary_state(self) -> None:
        for family in ("legacy_output_archives", "legacy_output_receipts"):
            for path, job_id, seq in self._iter_legacy_output_auxiliary_paths(family):
                event_path = self._storage_root / "events" / job_id / f"{seq:020d}.json"
                if queue_store_read.path_lstat(event_path) is None:
                    raise LegacyQueueStateError(
                        family=family,
                        path=path,
                        reason="legacy output auxiliary record has no canonical event",
                    )
                record = self._audit_one_legacy_output_event(
                    event_path,
                    job_id=job_id,
                    seq=seq,
                )
                if record is None:
                    raise LegacyQueueStateError(
                        family=family,
                        path=path,
                        reason="legacy output auxiliary record points to an ordinary event",
                    )
                archive_path = queue_legacy_output_codec.archive_path(
                    self._storage_root, job_id, seq
                )
                if queue_store_read.path_lstat(archive_path) is None:
                    raise LegacyQueueStateError(
                        family=family, path=path, reason="legacy output archive is missing"
                    )
                queue_legacy_output_codec.validate_legacy_output_archive(archive_path, record)
                if family == "legacy_output_receipts":
                    if event_path.stat().st_size > queue_layout.RECORD_FAMILY_MAX_BYTES["events"]:
                        raise LegacyQueueStateError(
                            family=family,
                            path=path,
                            reason="receipt exists before the compatibility event replacement",
                        )
                    queue_legacy_output_codec.validate_legacy_output_receipt(path, record)

        for path, _job_id, _seq in self._iter_legacy_output_auxiliary_paths(
            "legacy_output_retired"
        ):
            raise LegacyQueueStateError(
                family="legacy_output_retired",
                path=path,
                reason="retired receipt exists before legacy-output migration completed",
            )

    def _validate_retired_legacy_output_receipt(
        self,
        path: Path,
        receipt: dict[str, object],
        *,
        job_id: str,
        seq: int,
    ) -> None:
        tombstone_path = (
            self._storage_root
            / "job_tombstones"
            / f"{queue_layout.QueueLayout.durable_key(job_id)}.json"
        )
        tombstone = queue_store_read.read_optional(self._storage_root, tombstone_path, JobTombstone)
        if tombstone is None or tombstone.job_id != job_id or not tombstone.records_trash_started:
            raise LegacyQueueStateError(
                family="legacy_output_retired",
                path=path,
                reason="retired receipt has no authorized terminal-job GC tombstone",
            )
        archive_path = queue_legacy_output_codec.archive_path(self._storage_root, job_id, seq)
        event_path = self._storage_root / "events" / job_id / f"{seq:020d}.json"
        archive_present = queue_store_read.path_lstat(archive_path) is not None
        event_present = queue_store_read.path_lstat(event_path) is not None
        record: LegacyOutputRecord | None = None
        if archive_present:
            try:
                record = queue_legacy_output_codec.read_v09_legacy_output_record(
                    archive_path,
                    job_id=job_id,
                    seq=seq,
                )
            except (OSError, ValueError, QueueConflictError) as error:
                raise LegacyQueueStateError(
                    family="legacy_output_archives",
                    path=archive_path,
                    reason=f"retired legacy output archive is invalid: {type(error).__name__}",
                ) from error
            queue_legacy_output_codec.validate_legacy_output_archive(archive_path, record)
            if receipt != queue_legacy_output_codec.legacy_output_receipt(
                record,
                receipt_schema=queue_layout.LEGACY_OUTPUT_RECEIPT_SCHEMA,
            ):
                raise LegacyQueueStateError(
                    family="legacy_output_retired",
                    path=path,
                    reason="retired receipt does not match its remaining archive",
                )
        if not event_present:
            return
        try:
            event_bytes = queue_store_read.read_bounded_record_bytes(event_path)
            event = RelayEvent.model_validate_json(event_bytes)
        except (OSError, ValueError, QueueConflictError) as error:
            raise LegacyQueueStateError(
                family="events",
                path=event_path,
                reason=f"retired legacy output event is invalid: {type(error).__name__}",
            ) from error
        if (
            event.job_id != job_id
            or event.seq != seq
            or event.event_type != receipt.get("event_type")
            or len(event_bytes) != receipt.get("replacement_size_bytes")
            or hashlib.sha256(event_bytes).hexdigest() != receipt.get("replacement_sha256")
        ):
            raise LegacyQueueStateError(
                family="events",
                path=event_path,
                reason="retired legacy output event does not match its receipt",
            )
        compatibility = event.payload.get("legacy_output")
        if not isinstance(compatibility, dict):
            raise LegacyQueueStateError(
                family="events",
                path=event_path,
                reason="retired legacy output event has no compatibility metadata",
            )
        typed_compatibility = cast(dict[str, object], compatibility)
        if (
            typed_compatibility.get("schema_version")
            != queue_layout.LEGACY_OUTPUT_COMPATIBILITY_SCHEMA
            or typed_compatibility.get("archive_path") != receipt.get("archive_path")
            or typed_compatibility.get("archive_sha256") != receipt.get("archive_sha256")
            or typed_compatibility.get("archive_size_bytes") != receipt.get("archive_size_bytes")
            or typed_compatibility.get("representation") != receipt.get("representation")
        ):
            raise LegacyQueueStateError(
                family="events",
                path=event_path,
                reason="retired compatibility metadata does not match its receipt",
            )
        if record is not None and (
            event != record.replacement or event_bytes != record.replacement_bytes
        ):
            raise LegacyQueueStateError(
                family="events",
                path=event_path,
                reason="retired compatibility event does not match its remaining archive",
            )

    def _audit_completed_legacy_output_state(self, marker: LegacyOutputAudit) -> None:
        """Boundedly verify every active or GC-retired migration receipt."""
        receipts: dict[tuple[str, int], tuple[str, Path, dict[str, object]]] = {}
        receipt_paths: dict[tuple[str, int], Path] = {}
        archive_bytes = 0
        for family in ("legacy_output_receipts", "legacy_output_retired"):
            for path, job_id, seq in self._iter_legacy_output_auxiliary_paths(family):
                key = (job_id, seq)
                if key in receipts:
                    raise LegacyQueueStateError(
                        family=family,
                        path=path,
                        reason="legacy output identity exists in active and retired receipts",
                    )
                receipt = queue_legacy_output_codec.read_legacy_output_receipt_document(
                    path,
                    job_id=job_id,
                    seq=seq,
                )
                receipts[key] = (family, path, receipt)
                receipt_paths[key] = path
                archive_bytes += cast(int, receipt["archive_size_bytes"])
                if len(receipts) > queue_layout.MAX_LEGACY_OUTPUT_MIGRATION_RECORDS:
                    raise LegacyQueueStateError(
                        family=family,
                        path=path,
                        reason="legacy output receipts exceed the bounded migration limit",
                    )

        if (
            len(receipts) != marker.migration_records
            or archive_bytes != marker.archive_bytes
            or queue_legacy_output_codec.legacy_output_receipt_manifest_sha256(receipt_paths)
            != marker.receipt_manifest_sha256
        ):
            raise LegacyQueueStateError(
                family="migrations",
                path=queue_legacy_output_codec.marker_path(self._storage_root),
                reason="legacy-output marker totals do not match active and retired receipts",
            )

        for (job_id, seq), (family, path, receipt) in receipts.items():
            if family == "legacy_output_retired":
                self._validate_retired_legacy_output_receipt(
                    path,
                    receipt,
                    job_id=job_id,
                    seq=seq,
                )
                continue
            event_path = self._storage_root / "events" / job_id / f"{seq:020d}.json"
            archive_path = queue_legacy_output_codec.archive_path(self._storage_root, job_id, seq)
            if (
                queue_store_read.path_lstat(event_path) is None
                or queue_store_read.path_lstat(archive_path) is None
            ):
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason="active receipt is missing its canonical event or archive",
                )
            record = self._audit_one_legacy_output_event(
                event_path,
                job_id=job_id,
                seq=seq,
            )
            if (
                record is None
                or event_path.stat().st_size > queue_layout.RECORD_FAMILY_MAX_BYTES["events"]
            ):
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason="active receipt does not point to a compatibility event",
                )
            queue_legacy_output_codec.validate_legacy_output_archive(archive_path, record)
            queue_legacy_output_codec.validate_legacy_output_receipt(path, record)

        for archive_path, job_id, seq in self._iter_legacy_output_auxiliary_paths(
            "legacy_output_archives"
        ):
            if (job_id, seq) not in receipts:
                raise LegacyQueueStateError(
                    family="legacy_output_archives",
                    path=archive_path,
                    reason="legacy output archive has no active or retired receipt",
                )

    def _audit_legacy_output_state_before_initialization(self) -> LegacyOutputAudit:
        marker = queue_legacy_output_codec.read_legacy_output_marker(self._storage_root)
        if marker is not None:
            with self._lock:
                locked_marker = queue_legacy_output_codec.read_legacy_output_marker(
                    self._storage_root
                )
                if locked_marker is None or locked_marker != marker:
                    raise QueueConflictError(
                        "legacy-output completion marker changed while taking the queue lock"
                    )
                self._audit_completed_legacy_output_state(locked_marker)
                return locked_marker
        event_records = migration_records = archive_bytes = 0
        migration_keys: list[tuple[str, int]] = []
        for path, job_id, seq in queue_legacy_output_codec.iter_legacy_event_paths(
            self._storage_root,
            "events",
            max_directories=queue_layout.MAX_LEGACY_EVENT_AUDIT_DIRECTORIES,
            max_records=queue_layout.MAX_LEGACY_EVENT_AUDIT_RECORDS,
        ):
            event_records += 1
            record = self._audit_one_legacy_output_event(path, job_id=job_id, seq=seq)
            if record is None:
                continue
            migration_records += 1
            migration_keys.append((job_id, seq))
            archive_bytes += len(record.original_bytes)
            if migration_records > queue_layout.MAX_LEGACY_OUTPUT_MIGRATION_RECORDS:
                raise LegacyQueueStateError(
                    family="events",
                    path=path,
                    reason=(
                        "legacy output migration exceeds the bounded record limit of "
                        f"{queue_layout.MAX_LEGACY_OUTPUT_MIGRATION_RECORDS}"
                    ),
                )
            if archive_bytes > queue_layout.MAX_LEGACY_OUTPUT_MIGRATION_BYTES:
                raise LegacyQueueStateError(
                    family="events",
                    path=path,
                    reason=(
                        "legacy output migration exceeds the bounded aggregate byte limit "
                        f"of {queue_layout.MAX_LEGACY_OUTPUT_MIGRATION_BYTES}"
                    ),
                )
        self._audit_legacy_output_auxiliary_state()
        return LegacyOutputAudit(
            marker_complete=False,
            event_records=event_records,
            migration_records=migration_records,
            archive_bytes=archive_bytes,
            migration_keys=tuple(migration_keys),
        )


audit_completed_state = QueueLegacyOutputAuditMixin._audit_completed_legacy_output_state  # pyright: ignore[reportPrivateUsage]
audit_state_before_initialization = (
    QueueLegacyOutputAuditMixin._audit_legacy_output_state_before_initialization  # pyright: ignore[reportPrivateUsage]
)
