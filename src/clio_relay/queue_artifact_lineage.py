"""Durable artifact-use lineage ownership."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from clio_relay import queue_context, queue_layout, queue_store_read, queue_store_write
from clio_relay.errors import QueueConflictError as _Conflict
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.models import (
    ArtifactRef,
    ArtifactUseProvenance,
    ArtifactUserOrderHead,
    RelayJob,
    UsedArtifactRef,
)
from clio_relay.pagination import validate_response_page_limit

_C = _Conflict
MAX_ARTIFACT_USES_PER_JOB = queue_layout.MAX_ARTIFACT_USES_PER_JOB
MAX_ARTIFACT_CONSUMERS = queue_layout.MAX_ARTIFACT_CONSUMERS
ARTIFACT_USER_CURSOR_PREFIX = queue_layout.ARTIFACT_USER_CURSOR_PREFIX
ARTIFACT_USER_CURSOR_DIGITS = queue_layout.ARTIFACT_USER_CURSOR_DIGITS


def _identity(metadata: dict[str, object], *, allow_legacy: bool) -> tuple[str, str | None] | None:
    owner_session_id = metadata.get("owner_session_id")
    generation_id = metadata.get("owner_session_generation_id")
    admission_session_id = metadata.get("owner_session_admission_id")
    if owner_session_id is None:
        if generation_id is not None or admission_session_id is not None:
            raise _Conflict(
                "owner_session_generation_id and owner_session_admission_id require "
                "owner_session_id"
            )
        return None
    if not isinstance(owner_session_id, str) or not owner_session_id:
        raise _Conflict("owner_session_id must be a non-empty string")
    if admission_session_id is not None and (
        not isinstance(admission_session_id, str)
        or not queue_layout.safe_global_record_id(admission_session_id)
    ):
        raise _Conflict("owner_session_admission_id must be a safe identifier")
    if generation_id is None and allow_legacy:
        return owner_session_id, None
    if not isinstance(generation_id, str):
        raise _Conflict("new owner-session records require owner_session_generation_id")
    try:
        validate_durable_record_id(generation_id)
    except ValueError as error:
        raise _C("owner_session_generation_id must be a portable durable identifier") from error
    return owner_session_id, generation_id


def require_artifact_lineage_owner_match(
    *,
    consumer: RelayJob,
    producer: RelayJob,
) -> None:
    """Forbid lineage edges across exact owner-session generation boundaries."""
    consumer_identity = _identity(consumer.metadata, allow_legacy=True)
    producer_identity = _identity(producer.metadata, allow_legacy=True)
    if consumer_identity is None and producer_identity is None:
        return
    if (
        consumer_identity is None
        or producer_identity is None
        or consumer_identity[1] is None
        or producer_identity[1] is None
        or consumer_identity != producer_identity
    ):
        raise _Conflict("used artifact owner session generation does not match the consuming job")


def _is_sha256_digest(value: object) -> bool:
    return (
        isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)
    )


def _artifact_user_cursor(sequence: int) -> str:
    if sequence < 1 or sequence >= 2**63:
        raise _Conflict("artifact-user cursor sequence is outside its durable range")
    return f"{ARTIFACT_USER_CURSOR_PREFIX}{sequence:0{ARTIFACT_USER_CURSOR_DIGITS}d}"


def _artifact_user_cursor_sequence(cursor: str) -> int:
    try:
        validate_durable_record_id(cursor)
    except (TypeError, ValueError) as error:
        raise ValueError(f"invalid cursor: {error}") from error
    digits = cursor.removeprefix(ARTIFACT_USER_CURSOR_PREFIX)
    if (
        not cursor.startswith(ARTIFACT_USER_CURSOR_PREFIX)
        or len(digits) != ARTIFACT_USER_CURSOR_DIGITS
        or not digits.isdecimal()
    ):
        raise ValueError("invalid cursor: expected an artifact-user edge cursor")
    sequence = int(digits)
    if sequence < 1 or sequence >= 2**63:
        raise ValueError("invalid cursor: artifact-user edge sequence is outside its range")
    return sequence


def _artifact_user_entry_sequence(path: Path) -> int:
    stem = path.stem
    if len(stem) != ARTIFACT_USER_CURSOR_DIGITS or not stem.isdecimal():
        raise _Conflict(f"artifact-user order entry filename is invalid: {path}")
    sequence = int(stem)
    if sequence < 1 or sequence >= 2**63 or stem != f"{sequence:020d}":
        raise _Conflict(f"artifact-user order entry sequence is invalid: {path}")
    return sequence


def write_immutable_use_record(
    store: queue_context.QueueStoreProtocol,
    path: Path,
    record: UsedArtifactRef,
) -> None:
    """Write one immutable artifact-use edge through the lineage owner lookup."""
    existing = queue_store_read.read_optional(store.storage_root, path, UsedArtifactRef)
    if existing is not None:
        if existing != record:
            raise _Conflict(f"immutable used-artifact edge changed: {path}")
        return
    queue_store_write.write_model(store.storage_root, path, record)


class QueueArtifactLineageMixin:
    """Own immutable artifact-use edges and their ordered reverse index."""

    _storage_root: Path
    _store_adapter: queue_context.QueueStoreProtocol
    if TYPE_CHECKING:

        def get_artifact(self, artifact_id: str) -> ArtifactRef: ...

        def get_job(self, job_id: str) -> RelayJob: ...

    def list_used_artifacts_page(
        self,
        job_id: str,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> tuple[list[UsedArtifactRef], str | None, int]:
        """Return one bounded stable page of artifacts consumed by a job."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        if cursor is not None:
            cursor = queue_layout.QueueLayout.require_durable_record_id(cursor, field="cursor")
        limit = validate_response_page_limit(limit)
        self._store_adapter.initialize()
        job = self.get_job(job_id)
        records, next_cursor, total = self._read_artifact_use_page(
            self._storage_root
            / "used_artifacts_by_job"
            / queue_layout.QueueLayout.durable_key(job_id),
            cursor=cursor,
            limit=limit,
            capacity=MAX_ARTIFACT_USES_PER_JOB,
            identity_field="artifact_id",
            label=f"used artifacts for job {job_id}",
        )
        expected = {item.artifact_id: item for item in job.used_artifact_refs}
        if len(expected) != total:
            raise _Conflict(f"used-artifact index is incomplete for job: {job_id}")
        for record in records:
            expected_use = expected.get(record.artifact_id)
            if (
                record.consumer_job_id != job_id
                or expected_use is None
                or expected_use.sha256 != record.sha256
                or expected_use.provenance != record.provenance
            ):
                raise _Conflict(f"used-artifact index identity mismatch for job: {job_id}")
            self._validate_artifact_use_record(record)
        return records, next_cursor, total

    def list_artifact_users_page(
        self,
        artifact_id: str,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> tuple[list[UsedArtifactRef], str | None, int]:
        """Return one bounded stable page of jobs that consumed an artifact."""
        artifact_id = queue_layout.QueueLayout.require_durable_record_id(
            artifact_id, field="artifact_id"
        )
        cursor_sequence = 0 if cursor is None else _artifact_user_cursor_sequence(cursor)
        limit = validate_response_page_limit(limit)
        self._store_adapter.initialize()
        self.get_artifact(artifact_id)
        order_root = self._artifact_user_order_root(artifact_id)
        entry_paths = queue_store_read.bounded_json_record_paths(
            order_root / "entries",
            limit=MAX_ARTIFACT_CONSUMERS,
            label=f"ordered consumers of artifact {artifact_id}",
        )
        reverse_paths = queue_store_read.bounded_json_record_paths(
            self._storage_root / "artifact_users" / artifact_id,
            limit=MAX_ARTIFACT_CONSUMERS,
            label=f"consumers of artifact {artifact_id}",
        )
        mapping_paths = queue_store_read.bounded_json_record_paths(
            order_root / "by_consumer",
            limit=MAX_ARTIFACT_CONSUMERS,
            label=f"consumer order mappings for artifact {artifact_id}",
        )
        if len(entry_paths) != len(reverse_paths) or len(mapping_paths) < len(entry_paths):
            raise _C(f"artifact-user ordered index is incomplete for artifact: {artifact_id}")
        latest_sequence = self._read_artifact_user_order_head(artifact_id)
        ordered_paths: list[tuple[int, Path]] = []
        for path in entry_paths:
            sequence = _artifact_user_entry_sequence(path)
            if sequence > latest_sequence:
                raise _Conflict(f"artifact-user order entry exceeds its head: {artifact_id}")
            ordered_paths.append((sequence, path))
        ordered_paths.sort(key=lambda item: item[0])
        remaining = [item for item in ordered_paths if item[0] > cursor_sequence]
        window = remaining[: limit + 1]
        has_more = len(window) > limit
        records: list[UsedArtifactRef] = []
        for sequence, path in window[:limit]:
            record = queue_store_read.read_json_file(path, UsedArtifactRef)
            if record.artifact_id != artifact_id or record.sequence != sequence:
                raise _C(f"artifact-user order identity mismatch for artifact: {artifact_id}")
            reverse = self._store_adapter.read_optional(
                self._storage_root
                / "artifact_users"
                / artifact_id
                / f"{record.consumer_job_id}.json",
                UsedArtifactRef,
            )
            mapping = self._store_adapter.read_optional(
                order_root / "by_consumer" / f"{record.consumer_job_id}.json",
                UsedArtifactRef,
            )
            if reverse != record or mapping != record:
                raise _C(f"artifact-user ordered index disagrees for artifact: {artifact_id}")
            records.append(record)
        for record in records:
            self._validate_artifact_use_record(record)
        next_cursor = _artifact_user_cursor(records[-1].sequence) if has_more and records else None
        return records, next_cursor, len(ordered_paths)

    @staticmethod
    def _read_artifact_use_page(
        directory: Path,
        *,
        cursor: str | None,
        limit: int,
        capacity: int,
        identity_field: Literal["artifact_id", "consumer_job_id"],
        label: str,
    ) -> tuple[list[UsedArtifactRef], str | None, int]:
        paths = queue_store_read.bounded_json_record_paths(
            directory,
            limit=capacity,
            label=label,
        )
        paths.sort(key=lambda path: path.name)
        total = len(paths)
        if cursor is not None:
            paths = [path for path in paths if path.stem > cursor]
        window = paths[: limit + 1]
        has_more = len(window) > limit
        records: list[UsedArtifactRef] = []
        for path in window[:limit]:
            record = queue_store_read.read_json_file(path, UsedArtifactRef)
            identity = getattr(record, identity_field)
            if identity != path.stem:
                raise _Conflict(f"{label} filename/content identity mismatch: {path}")
            records.append(record)
        next_cursor = getattr(records[-1], identity_field) if has_more and records else None
        return records, next_cursor, total

    def _ensure_artifact_use_indexes_unlocked(self, job: RelayJob) -> None:
        records = self._artifact_use_records_unlocked(job, allocate_sequences=True)
        forward_directory = self._storage_root / "used_artifacts_by_job" / job.job_id
        for record in records:
            order_root = self._artifact_user_order_root(record.artifact_id)
            write_immutable_use_record(
                self._store_adapter,
                order_root / "by_consumer" / f"{record.consumer_job_id}.json",
                record,
            )
            write_immutable_use_record(
                self._store_adapter,
                forward_directory / f"{record.artifact_id}.json",
                record,
            )
            reverse_directory = self._storage_root / "artifact_users" / record.artifact_id
            reverse_directory.mkdir(parents=True, exist_ok=True)
            write_immutable_use_record(
                self._store_adapter,
                reverse_directory / f"{record.consumer_job_id}.json",
                record,
            )
            write_immutable_use_record(
                self._store_adapter,
                order_root / "entries" / f"{record.sequence:020d}.json",
                record,
            )

    def _artifact_use_records_unlocked(
        self,
        job: RelayJob,
        *,
        allocate_sequences: bool,
    ) -> list[UsedArtifactRef]:
        expected_ids = {item.artifact_id for item in job.used_artifact_refs}
        forward_directory = self._storage_root / "used_artifacts_by_job" / job.job_id
        existing_paths = queue_store_read.bounded_json_record_paths(
            forward_directory,
            limit=MAX_ARTIFACT_USES_PER_JOB,
            label=f"used artifacts for job {job.job_id}",
        )
        unexpected = {path.stem for path in existing_paths}.difference(expected_ids)
        if unexpected:
            raise _C(
                f"used-artifact edge set changed for job {job.job_id}: {sorted(unexpected)[0]}"
            )
        records: list[UsedArtifactRef] = []
        for use in job.used_artifact_refs:
            artifact = self._store_adapter.read_optional(
                self._storage_root / "artifacts" / f"{use.artifact_id}.json",
                ArtifactRef,
            )
            if artifact is None:
                raise _Conflict(f"used artifact not found: {use.artifact_id}")
            if artifact.artifact_id != use.artifact_id:
                raise _Conflict(f"canonical artifact identity mismatch: {use.artifact_id}")
            canonical_sha256 = artifact.sha256
            if not _is_sha256_digest(canonical_sha256):
                raise _Conflict(f"used artifact is not content-addressed: {use.artifact_id}")
            canonical_sha256 = cast(str, canonical_sha256)
            if canonical_sha256 != use.sha256:
                raise _Conflict(f"used artifact digest mismatch: {use.artifact_id}")
            producer = self._store_adapter.read_optional(
                self._storage_root / "jobs" / f"{artifact.job_id}.json",
                RelayJob,
            )
            if producer is None or producer.job_id != artifact.job_id:
                raise _Conflict(f"used artifact producer is not retained: {use.artifact_id}")
            require_artifact_lineage_owner_match(consumer=job, producer=producer)
            existing_forward = self._store_adapter.read_optional(
                forward_directory / f"{artifact.artifact_id}.json",
                UsedArtifactRef,
            )
            reverse_directory = self._storage_root / "artifact_users" / artifact.artifact_id
            reverse_path = reverse_directory / f"{job.job_id}.json"
            existing_reverse = self._store_adapter.read_optional(reverse_path, UsedArtifactRef)
            order_root = self._artifact_user_order_root(artifact.artifact_id)
            order_head = self._read_artifact_user_order_head(artifact.artifact_id)
            mapping_path = order_root / "by_consumer" / f"{job.job_id}.json"
            existing_mapping = self._store_adapter.read_optional(mapping_path, UsedArtifactRef)
            reverse_paths = queue_store_read.bounded_json_record_paths(
                reverse_directory,
                limit=MAX_ARTIFACT_CONSUMERS,
                label=f"consumers of artifact {artifact.artifact_id}",
            )
            mapping_paths = queue_store_read.bounded_json_record_paths(
                order_root / "by_consumer",
                limit=MAX_ARTIFACT_CONSUMERS,
                label=f"consumer order mappings for artifact {artifact.artifact_id}",
            )
            existing_records = [
                record
                for record in (existing_forward, existing_reverse, existing_mapping)
                if record is not None
            ]
            if not existing_records and max(len(reverse_paths), len(mapping_paths)) >= (
                MAX_ARTIFACT_CONSUMERS
            ):
                raise _Conflict(f"artifact consumer capacity is exhausted: {artifact.artifact_id}")
            if existing_records:
                record = existing_records[0]
                if any(existing != record for existing in existing_records[1:]) or (
                    record.artifact_id != artifact.artifact_id
                    or record.consumer_job_id != job.job_id
                    or record.producer_job_id != artifact.job_id
                    or record.sha256 != canonical_sha256
                    or record.provenance != use.provenance
                ):
                    raise _C(
                        f"immutable used-artifact edge identity changed: {artifact.artifact_id}"
                    )
                if order_head < record.sequence:
                    raise _C(f"artifact-user order head is behind its edge: {artifact.artifact_id}")
                entry = self._store_adapter.read_optional(
                    order_root / "entries" / f"{record.sequence:020d}.json",
                    UsedArtifactRef,
                )
                if entry is not None and entry != record:
                    raise _Conflict(f"artifact-user order entry changed: {artifact.artifact_id}")
                records.append(record)
                continue
            if not allocate_sequences:
                continue
            record = self._reserve_artifact_user_order_unlocked(
                artifact_id=artifact.artifact_id,
                consumer_job_id=job.job_id,
                producer_job_id=artifact.job_id,
                sha256=canonical_sha256,
                provenance=use.provenance,
                created_at=job.created_at,
            )
            records.append(record)
        return records

    def _artifact_user_order_root(self, artifact_id: str) -> Path:
        return self._storage_root / "artifact_user_order" / artifact_id

    def _initialize_artifact_user_order_unlocked(self, artifact_id: str) -> None:
        root = self._artifact_user_order_root(artifact_id)
        root_existed = root.exists()
        head_path = root / "head.json"
        if not root_existed:
            self._store_adapter.write(
                head_path,
                ArtifactUserOrderHead(artifact_id=artifact_id, latest_sequence=0),
            )
        elif not head_path.exists():
            raise _Conflict(
                f"artifact-user order head is missing from initialized index: {artifact_id}"
            )
        (root / "entries").mkdir(parents=True, exist_ok=True)
        (root / "by_consumer").mkdir(parents=True, exist_ok=True)

    def _read_artifact_user_order_head(self, artifact_id: str) -> int:
        path = self._artifact_user_order_root(artifact_id) / "head.json"
        head = self._store_adapter.read_optional(path, ArtifactUserOrderHead)
        if head is None:
            if path.parent.exists():
                raise _Conflict(f"artifact-user order head is missing: {path}")
            return 0
        if head.artifact_id != artifact_id:
            raise _Conflict(f"artifact-user order head identity mismatch: {path}")
        return head.latest_sequence

    def _reserve_artifact_user_order_unlocked(
        self,
        *,
        artifact_id: str,
        consumer_job_id: str,
        producer_job_id: str,
        sha256: str,
        provenance: ArtifactUseProvenance | None,
        created_at: datetime,
    ) -> UsedArtifactRef:
        root = self._artifact_user_order_root(artifact_id)
        self._initialize_artifact_user_order_unlocked(artifact_id)
        mapping_path = root / "by_consumer" / f"{consumer_job_id}.json"
        existing = self._store_adapter.read_optional(mapping_path, UsedArtifactRef)
        if existing is not None:
            return existing
        latest_sequence = self._read_artifact_user_order_head(artifact_id)
        if latest_sequence >= 2**63 - 1:
            raise _Conflict(f"artifact-user sequence exhausted: {artifact_id}")
        sequence = latest_sequence + 1
        record = UsedArtifactRef(
            artifact_id=artifact_id,
            consumer_job_id=consumer_job_id,
            producer_job_id=producer_job_id,
            sequence=sequence,
            sha256=sha256,
            provenance=provenance,
            created_at=created_at,
        )
        self._store_adapter.write(
            root / "head.json",
            ArtifactUserOrderHead(
                artifact_id=artifact_id,
                latest_sequence=sequence,
            ),
        )
        write_immutable_use_record(self._store_adapter, mapping_path, record)
        return record

    def _validate_artifact_use_record(self, record: UsedArtifactRef) -> None:
        artifact = self.get_artifact(record.artifact_id)
        if artifact.job_id != record.producer_job_id or artifact.sha256 != record.sha256:
            raise _C(
                f"used-artifact edge no longer matches canonical artifact: {record.artifact_id}"
            )
        consumer = self.get_job(record.consumer_job_id)
        producer = self.get_job(record.producer_job_id)
        require_artifact_lineage_owner_match(consumer=consumer, producer=producer)
        pinned = {item.artifact_id: item for item in consumer.used_artifact_refs}
        expected = pinned.get(record.artifact_id)
        if (
            expected is None
            or expected.sha256 != record.sha256
            or expected.provenance != record.provenance
        ):
            raise _C(f"used-artifact edge no longer matches consumer job: {record.consumer_job_id}")
