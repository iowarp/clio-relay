"""Bounded legacy-record audit and indexed-era seal ownership."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import stat
from pathlib import Path
from typing import Protocol, TypeVar, cast

from pydantic import BaseModel

from clio_relay import (
    cluster_config,
    queue_context,
    queue_index_state,
    queue_layout,
    queue_legacy_output_audit,
    queue_legacy_output_codec,
    queue_store_lock,
    queue_store_read,
    queue_store_write,
)
from clio_relay.errors import QueueConflictError, queue_conflict_from_cause
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.models import (
    ArtifactRef,
    Cursor,
    EndpointRegistration,
    GatewaySession,
    Lease,
    MonitorRule,
    ProgressRecord,
    RelayJob,
    RelayTask,
    TaskTimelineEvent,
)

logger = logging.getLogger(__name__)
Record = TypeVar("Record", bound=BaseModel)
LegacyQueueStateError = queue_store_lock.LegacyQueueStateError
QueueSealRequiresExclusive = queue_store_lock.QueueSealRequiresExclusive
LegacyOutputAudit = queue_legacy_output_codec.LegacyOutputAudit


class _LegacyLockProtocol(queue_context.QueueLockProtocol, Protocol):
    @property
    def is_locked(self) -> bool: ...


def after_audit_phase(_phase: str, _path: Path) -> None:
    """Fault-injection seam after the indexed-era seal becomes durable."""


def _unique_json(path: Path) -> object:
    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise QueueConflictError(f"duplicate JSON key {key!r} in {path}")
            result[key] = value
        return result

    try:
        return json.loads(
            queue_store_read.read_bounded_record_bytes(path),
            object_pairs_hook=unique,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise queue_conflict_from_cause(
            f"invalid JSON record {path}",
            cause=exc,
            logger=logger,
        ) from exc


class QueueLegacyAuditMixin:
    """Own bounded legacy-state audits on the composed queue facade."""

    _storage_root: Path
    _layout: queue_layout.QueueLayout
    _lock: queue_context.QueueLockProtocol
    _migration_lifetime_guarded: bool

    def _legacy_record_audit_marker_path(self) -> Path:
        return self._storage_root / "migrations" / "legacy-record-audit-v1.json"

    def _prepare_queue_root_for_lock(self) -> None:
        """Create only a missing queue root and reject an unsafe lock path."""
        try:
            root_stat = self._layout.storage_root_stat()
        except FileNotFoundError:
            try:
                cluster_config.ensure_private_configuration_directory(self._storage_root)
                root_stat = self._layout.storage_root_stat()
            except OSError as error:
                raise LegacyQueueStateError(
                    family="root",
                    path=self._storage_root,
                    reason=f"cannot create queue root: {type(error).__name__}",
                ) from error
        except OSError as error:
            raise LegacyQueueStateError(
                family="root",
                path=self._storage_root,
                reason=f"cannot inspect queue root: {type(error).__name__}",
            ) from error
        if not stat.S_ISDIR(root_stat.st_mode) or queue_layout.record_is_reparse(root_stat):
            raise LegacyQueueStateError(
                family="root",
                path=self._storage_root,
                reason="queue root is not an owned directory",
            )
        self._require_owner_private_queue_directory("root", self._storage_root, root_stat)
        lock_path = self._storage_root / ".lock"
        try:
            lock_stat = os.lstat(lock_path)
        except FileNotFoundError:
            return
        except OSError as error:
            raise LegacyQueueStateError(
                family="root",
                path=lock_path,
                reason=f"cannot inspect queue lock: {type(error).__name__}",
            ) from error
        if not stat.S_ISREG(lock_stat.st_mode) or queue_layout.record_is_reparse(lock_stat):
            raise LegacyQueueStateError(
                family="root", path=lock_path, reason="queue lock is not an owned regular file"
            )

    @staticmethod
    def _legacy_record_audit_marker() -> dict[str, object]:
        """Return the exact durable seal for the indexed queue era."""
        return {
            "schema_version": queue_layout.LEGACY_RECORD_AUDIT_SCHEMA,
            "complete": True,
            "queue_layout_schema": queue_layout.QUEUE_LAYOUT_SCHEMA,
            "canonical_record_access_schema": queue_layout.CANONICAL_RECORD_ACCESS_SCHEMA,
            "index_migration_schema": queue_layout.INDEX_MIGRATION_SCHEMA,
            "legacy_output_migration_schema": queue_layout.LEGACY_OUTPUT_MIGRATION_SCHEMA,
        }

    @staticmethod
    def _require_owner_private_queue_directory(
        family: str, path: Path, details: os.stat_result
    ) -> None:
        if os.name == "nt":
            return
        getuid = getattr(os, "getuid", None)
        current_uid = getuid() if callable(getuid) else None
        if current_uid is not None and details.st_uid != current_uid:
            raise LegacyQueueStateError(
                family=family, path=path, reason="queue directory is not owned by the current user"
            )
        if stat.S_IMODE(details.st_mode) & 0o077:
            raise LegacyQueueStateError(
                family=family,
                path=path,
                reason="queue directory is readable or writable by another user",
            )

    def _require_legacy_family_directory(self, family: str) -> Path | None:
        directory = self._storage_root / family
        try:
            details = os.lstat(directory)
        except FileNotFoundError:
            return None
        except OSError as error:
            raise LegacyQueueStateError(
                family=family,
                path=directory,
                reason=f"cannot inspect canonical family: {type(error).__name__}",
            ) from error
        if not stat.S_ISDIR(details.st_mode) or queue_layout.record_is_reparse(details):
            raise LegacyQueueStateError(
                family=family,
                path=directory,
                reason="canonical family is not an owned directory",
            )
        self._require_owner_private_queue_directory(family, directory, details)
        return directory

    def _require_indexed_queue_directory(self, *, family: str, directory: Path) -> None:
        try:
            details = os.lstat(directory)
        except OSError as error:
            raise LegacyQueueStateError(
                family=family,
                path=directory,
                reason=f"cannot inspect indexed queue directory: {type(error).__name__}",
            ) from error
        if not stat.S_ISDIR(details.st_mode) or queue_layout.record_is_reparse(details):
            raise LegacyQueueStateError(
                family=family,
                path=directory,
                reason="indexed queue path is not an owned directory",
            )
        self._require_owner_private_queue_directory(family, directory, details)

    def _require_indexed_queue_layout(self) -> None:
        try:
            root_stat = self._layout.storage_root_stat()
        except OSError as error:
            raise LegacyQueueStateError(
                family="root",
                path=self._storage_root,
                reason=f"cannot inspect indexed queue root: {type(error).__name__}",
            ) from error
        if not stat.S_ISDIR(root_stat.st_mode) or queue_layout.record_is_reparse(root_stat):
            raise LegacyQueueStateError(
                family="root",
                path=self._storage_root,
                reason="indexed queue root is not an owned directory",
            )
        self._require_owner_private_queue_directory("root", self._storage_root, root_stat)
        lock_path = self._storage_root / ".lock"
        try:
            lock_stat = os.lstat(lock_path)
        except FileNotFoundError:
            lock_stat = None
        except OSError as error:
            raise LegacyQueueStateError(
                family="root",
                path=lock_path,
                reason=f"cannot inspect indexed queue lock: {type(error).__name__}",
            ) from error
        if lock_stat is not None and (
            not stat.S_ISREG(lock_stat.st_mode) or queue_layout.record_is_reparse(lock_stat)
        ):
            raise LegacyQueueStateError(
                family="root",
                path=lock_path,
                reason="indexed queue lock is not an owned regular file",
            )
        for family in queue_store_lock.INITIALIZED_QUEUE_FAMILIES:
            if self._require_legacy_family_directory(family) is None:
                raise LegacyQueueStateError(
                    family=family,
                    path=self._storage_root / family,
                    reason="indexed queue seal requires its owned record directory",
                )
        global_root = self._storage_root / "global_order"
        self._require_indexed_queue_directory(family="global_order", directory=global_root)
        for family in queue_store_lock.GLOBAL_ORDER_FAMILIES:
            family_root = global_root / family
            self._require_indexed_queue_directory(family="global_order", directory=family_root)
            for child in ("by_id", "entries"):
                self._require_indexed_queue_directory(
                    family="global_order", directory=family_root / child
                )

    @staticmethod
    def _require_sealed_checkpoint(
        raw: object, *, label: str, schema_version: str | None = None
    ) -> dict[str, object]:
        if not isinstance(raw, dict):
            raise QueueConflictError(f"sealed {label} checkpoint is not an object")
        checkpoint = cast(dict[str, object], raw)
        keys = {"cursor", "complete"}
        if schema_version is not None:
            keys.add("schema_version")
        if set(checkpoint) != keys:
            raise QueueConflictError(f"sealed {label} checkpoint has an unknown shape")
        if not isinstance(checkpoint.get("complete"), bool):
            raise QueueConflictError(f"sealed {label} checkpoint completion is invalid")
        cursor = checkpoint.get("cursor")
        if cursor is not None:
            if (
                not isinstance(cursor, str)
                or len(cursor) > 512
                or Path(cursor).name != cursor
                or not cursor.endswith(".json")
            ):
                raise QueueConflictError(f"sealed {label} checkpoint cursor is invalid")
            try:
                validate_durable_record_id(cursor.removesuffix(".json"))
            except ValueError as error:
                raise QueueConflictError(
                    f"sealed {label} checkpoint cursor identity is invalid"
                ) from error
        if schema_version is not None and checkpoint.get("schema_version") != schema_version:
            raise QueueConflictError(f"sealed {label} checkpoint schema is invalid")
        return checkpoint

    def _read_sealed_state(self, *, allow_legacy_lease_schema: bool = False) -> dict[str, object]:
        return queue_index_state.read_sealed_index_migration_state(
            self._storage_root,
            checkpoint_validator=self._require_sealed_checkpoint,
            document_reader=_unique_json,
            allow_legacy_lease_schema=allow_legacy_lease_schema,
        )

    def _read_legacy_record_audit_marker(
        self, *, allow_legacy_lease_schema: bool = False
    ) -> LegacyOutputAudit | None:
        """Return constant-size indexed-era evidence, or ``None`` for bounded repair."""
        path = self._legacy_record_audit_marker_path()
        if queue_store_read.path_lstat(path) is None:
            index_path = self._storage_root / "migrations" / "index-v1.json"
            if queue_store_read.path_lstat(index_path) is not None:
                self._require_indexed_queue_layout()
                try:
                    queue_index_state.read_index_migration_state(self._storage_root)
                except (OSError, ValueError, QueueConflictError) as error:
                    raise LegacyQueueStateError(
                        family="migrations",
                        path=index_path,
                        reason=(
                            f"missing-seal queue migration state is invalid: {type(error).__name__}"
                        ),
                    ) from error
            return None
        self._require_indexed_queue_layout()
        try:
            raw = _unique_json(path)
        except (OSError, ValueError, QueueConflictError) as error:
            raise LegacyQueueStateError(
                family="migrations",
                path=path,
                reason=f"legacy-record audit marker is invalid: {type(error).__name__}",
            ) from error
        if raw != self._legacy_record_audit_marker():
            raise LegacyQueueStateError(
                family="migrations",
                path=path,
                reason="legacy-record audit marker has an unknown or incomplete contract",
            )
        self._read_sealed_state(allow_legacy_lease_schema=allow_legacy_lease_schema)
        output = queue_legacy_output_codec.read_legacy_output_marker(self._storage_root)
        if output is None:
            raise LegacyQueueStateError(
                family="migrations",
                path=queue_legacy_output_codec.marker_path(self._storage_root),
                reason="indexed queue seal requires the legacy-output completion marker",
            )
        return output

    def _write_legacy_record_audit_marker_unlocked(self) -> None:
        lock = cast(_LegacyLockProtocol, self._lock)
        if not lock.is_locked:
            raise RuntimeError("legacy-record audit seal requires the queue lock")
        if not self._migration_lifetime_guarded:
            raise QueueSealRequiresExclusive(
                "legacy-record audit seal requires exclusive writer-lifetime ownership"
            )
        path = self._legacy_record_audit_marker_path()
        if self._read_legacy_record_audit_marker() is not None:
            return
        self._read_sealed_state()
        queue_store_write.write_json(self._storage_root, path, self._legacy_record_audit_marker())
        after_audit_phase("marker", path)
        if self._read_legacy_record_audit_marker() is None:
            raise QueueConflictError("legacy-record audit marker was not durable")

    def _audit_legacy_state_before_initialization(self) -> LegacyOutputAudit:
        """Refuse unsafe v0.9 canonical state before creating or changing files."""
        try:
            root_stat = self._layout.storage_root_stat()
        except FileNotFoundError:
            return LegacyOutputAudit(False, 0, 0, 0)
        except OSError as error:
            raise LegacyQueueStateError(
                family="root",
                path=self._storage_root,
                reason=f"cannot inspect queue root: {type(error).__name__}",
            ) from error
        if not stat.S_ISDIR(root_stat.st_mode) or queue_layout.record_is_reparse(root_stat):
            raise LegacyQueueStateError(
                family="root",
                path=self._storage_root,
                reason="queue root is not an owned directory",
            )
        lock_path = self._storage_root / ".lock"
        try:
            lock_stat = os.lstat(lock_path)
        except FileNotFoundError:
            pass
        except OSError as error:
            raise LegacyQueueStateError(
                family="root",
                path=lock_path,
                reason=f"cannot inspect queue lock: {type(error).__name__}",
            ) from error
        else:
            if not stat.S_ISREG(lock_stat.st_mode) or queue_layout.record_is_reparse(lock_stat):
                raise LegacyQueueStateError(
                    family="root",
                    path=lock_path,
                    reason="queue lock is not an owned regular file",
                )
        families: tuple[tuple[str, type[BaseModel], str], ...] = (
            ("endpoints", EndpointRegistration, "endpoint_id"),
            ("jobs", RelayJob, "job_id"),
            ("tasks", RelayTask, "task_id"),
            ("leases", Lease, "lease_id"),
            ("artifacts", ArtifactRef, "artifact_id"),
            ("progress", ProgressRecord, "progress_id"),
            ("gateway_sessions", GatewaySession, "session_id"),
            ("monitor_rules", MonitorRule, "rule_id"),
        )
        for family, model, identity_field in families:
            self._audit_legacy_record_family(family, model=model, identity_field=identity_field)
        output_owner = cast(queue_legacy_output_audit.QueueLegacyOutputAuditMixin, self)
        output = queue_legacy_output_audit.audit_state_before_initialization(output_owner)
        self._audit_legacy_event_family(
            "task_events", model=TaskTimelineEvent, identity_field="task_id"
        )
        self._audit_legacy_record_family("cursors", model=Cursor, identity_field="job_id")
        self._audit_legacy_idempotency_family()
        return output

    def _bounded_legacy_family_entries(self, family: str) -> list[Path]:
        directory = self._require_legacy_family_directory(family)
        if directory is None:
            return []
        paths: list[Path] = []
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if len(paths) >= queue_layout.MAX_BOUNDED_SCAN_RECORDS:
                        raise LegacyQueueStateError(
                            family=family,
                            path=directory,
                            reason=(
                                "canonical family exceeds the bounded legacy audit limit of "
                                f"{queue_layout.MAX_BOUNDED_SCAN_RECORDS} entries"
                            ),
                        )
                    paths.append(Path(entry.path))
        except LegacyQueueStateError:
            raise
        except OSError as error:
            raise LegacyQueueStateError(
                family=family,
                path=directory,
                reason=f"cannot scan canonical family: {type(error).__name__}",
            ) from error
        return paths

    @staticmethod
    def _require_legacy_regular_json(family: str, path: Path) -> None:
        try:
            details = os.lstat(path)
        except OSError as error:
            raise LegacyQueueStateError(
                family=family,
                path=path,
                reason=f"cannot inspect canonical record: {type(error).__name__}",
            ) from error
        if (
            not path.name.endswith(".json")
            or not stat.S_ISREG(details.st_mode)
            or queue_layout.record_is_reparse(details)
        ):
            raise LegacyQueueStateError(
                family=family,
                path=path,
                reason="canonical record is not an owned .json regular file",
            )

    @staticmethod
    def _require_legacy_durable_id(family: str, path: Path, value: object) -> str:
        if not isinstance(value, str):
            raise LegacyQueueStateError(
                family=family, path=path, reason="canonical identity is not a string"
            )
        try:
            return validate_durable_record_id(value)
        except ValueError as error:
            raise LegacyQueueStateError(
                family=family,
                path=path,
                reason=f"canonical identity is not portable: {error}",
            ) from error

    def _audit_legacy_record_family(
        self, family: str, *, model: type[Record], identity_field: str
    ) -> None:
        for path in self._bounded_legacy_family_entries(family):
            self._require_legacy_regular_json(family, path)
            record_id = path.name.removesuffix(".json")
            self._require_legacy_durable_id(family, path, record_id)
            try:
                record = queue_store_read.read_json_file(path, model)
            except (OSError, ValueError, QueueConflictError) as error:
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason=f"canonical record is invalid: {type(error).__name__}",
                ) from error
            if getattr(record, identity_field, None) != record_id:
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason=f"filename/content identity mismatch for {identity_field}",
                )

    def _audit_legacy_event_family(
        self, family: str, *, model: type[Record], identity_field: str
    ) -> None:
        record_count = 0
        for directory in self._bounded_legacy_family_entries(family):
            try:
                details = os.lstat(directory)
            except OSError as error:
                raise LegacyQueueStateError(
                    family=family,
                    path=directory,
                    reason=f"cannot inspect event identity directory: {type(error).__name__}",
                ) from error
            if not stat.S_ISDIR(details.st_mode) or queue_layout.record_is_reparse(details):
                raise LegacyQueueStateError(
                    family=family,
                    path=directory,
                    reason="event identity entry is not an owned directory",
                )
            self._require_legacy_durable_id(family, directory, directory.name)
            try:
                with os.scandir(directory) as entries:
                    for entry in entries:
                        record_count += 1
                        path = Path(entry.path)
                        if record_count > queue_layout.MAX_BOUNDED_SCAN_RECORDS:
                            raise LegacyQueueStateError(
                                family=family,
                                path=directory,
                                reason=(
                                    "event family exceeds the bounded legacy audit limit of "
                                    f"{queue_layout.MAX_BOUNDED_SCAN_RECORDS} records"
                                ),
                            )
                        self._require_legacy_regular_json(family, path)
                        sequence_text = path.name.removesuffix(".json")
                        if (
                            len(sequence_text) != 20
                            or not sequence_text.isascii()
                            or not sequence_text.isdigit()
                        ):
                            raise LegacyQueueStateError(
                                family=family,
                                path=path,
                                reason="event filename is not a canonical 20-digit sequence",
                            )
                        try:
                            record = queue_store_read.read_json_file(path, model)
                        except (OSError, ValueError, QueueConflictError) as error:
                            raise LegacyQueueStateError(
                                family=family,
                                path=path,
                                reason=f"event record is invalid: {type(error).__name__}",
                            ) from error
                        if getattr(record, identity_field, None) != directory.name:
                            raise LegacyQueueStateError(
                                family=family,
                                path=path,
                                reason=(
                                    "event directory/content identity mismatch for "
                                    f"{identity_field}"
                                ),
                            )
                        if getattr(record, "seq", None) != int(sequence_text):
                            raise LegacyQueueStateError(
                                family=family,
                                path=path,
                                reason="event filename/content sequence mismatch",
                            )
            except LegacyQueueStateError:
                raise
            except OSError as error:
                raise LegacyQueueStateError(
                    family=family,
                    path=directory,
                    reason=f"cannot scan event identity directory: {type(error).__name__}",
                ) from error

    def _audit_legacy_idempotency_family(self) -> None:
        family = "idempotency"
        for path in self._bounded_legacy_family_entries(family):
            self._require_legacy_regular_json(family, path)
            filename = path.name.removesuffix(".json")
            digest = filename.removeprefix("key_")
            if (
                not filename.startswith("key_")
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason="idempotency filename is not a canonical SHA-256",
                )
            try:
                raw = queue_store_read.read_json_document(path)
            except (OSError, ValueError, QueueConflictError) as error:
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason=f"idempotency record is invalid: {type(error).__name__}",
                ) from error
            if not isinstance(raw, dict):
                raise LegacyQueueStateError(
                    family=family, path=path, reason="idempotency record is not an object"
                )
            document = cast(dict[str, object], raw)
            self._require_legacy_durable_id(family, path, document.get("job_id"))
            key = document.get("idempotency_key")
            if not isinstance(key, str) or not key:
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason="idempotency record has no string idempotency_key",
                )
            if f"key_{hashlib.sha256(key.encode('utf-8')).hexdigest()}" != filename:
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason="idempotency filename/content identity mismatch",
                )
