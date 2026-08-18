"""Durable artifact and transform record ownership."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from clio_relay import (
    queue_context,
    queue_index_state,
    queue_layout,
    queue_order_index,
    queue_store_read,
)
from clio_relay.errors import QueueConflictError
from clio_relay.models import ArtifactRef, ArtifactUseProvenance, RelayEvent, RelayJob, TransformRef

_require_durable_record_id = queue_layout.QueueLayout.require_durable_record_id


def artifact_with_sequence(artifact: ArtifactRef, sequence: int) -> ArtifactRef:
    """Return an indexed artifact with relay order mirrored into CLIO provenance."""
    metadata = artifact.metadata
    raw_clio_provenance = metadata.get("clio.provenance.v1")
    if isinstance(raw_clio_provenance, dict):
        clio_provenance = cast(dict[str, Any], raw_clio_provenance)
        recorded_sequence = clio_provenance.get("sequence")
        if recorded_sequence is not None and recorded_sequence != sequence:
            raise QueueConflictError("CLIO artifact provenance sequence does not match relay order")
        metadata = {
            **metadata,
            "clio.provenance.v1": {
                **clio_provenance,
                "sequence": sequence,
            },
        }
    payload = artifact.model_dump(mode="python")
    payload.update(sequence=sequence, metadata=metadata)
    return ArtifactRef.model_validate(payload)


class QueueArtifactsMixin(queue_order_index.QueueOrderIndexMixin):
    """Own artifact registration, reads, and immutable transform records."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol
    if TYPE_CHECKING:

        def _initialize_artifact_user_order_unlocked(self, artifact_id: str) -> None: ...

        def _link_gateways_for_artifact_unlocked(self, artifact: ArtifactRef) -> None: ...

        def append_event(
            self,
            job_id: str,
            event_type: str,
            message: str,
            *,
            locked: bool = False,
            payload: dict[str, object] | None = None,
        ) -> RelayEvent: ...

    def append_artifact(self, artifact: ArtifactRef) -> ArtifactRef:
        """Index an artifact reference."""
        _require_durable_record_id(artifact.artifact_id, field="artifact_id")
        _require_durable_record_id(artifact.job_id, field="job_id")
        self._store_adapter.initialize()
        with self._lock:
            root = self._storage_root
            queue_store_read.read_required_job(self._storage_root, artifact.job_id)
            sequence = self._next_job_record_sequence_unlocked(artifact.job_id, "artifact_count")
            saved = artifact_with_sequence(artifact, sequence)
            self._store_adapter.write(root / "artifacts" / f"{saved.artifact_id}.json", saved)
            self._store_adapter.write(
                root
                / "artifacts_by_job"
                / queue_layout.QueueLayout.durable_key(saved.job_id)
                / f"{saved.artifact_id}.json",
                saved,
            )
            users = root / "artifact_users" / saved.artifact_id
            users.mkdir(parents=True, exist_ok=True)
            self._initialize_artifact_user_order_unlocked(saved.artifact_id)
            self._write_ordered_job_record("artifact", saved.job_id, sequence, saved)
            self._link_gateways_for_artifact_unlocked(saved)
            queue_order_index.increment_job_index(
                self._store_adapter, artifact.job_id, "artifact_count"
            )
            self.append_event(
                artifact.job_id,
                "artifact.created",
                f"Artifact indexed: {artifact.uri}",
                locked=True,
                payload={"artifact_id": artifact.artifact_id, "uri": artifact.uri},
            )
        return saved

    def list_artifacts(self, job_id: str) -> list[ArtifactRef]:
        """Return artifact refs for a job."""
        job_id = _require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
        root = self._storage_root
        key = queue_layout.QueueLayout.durable_key(job_id)
        if (root / "job_indexes" / f"{key}.json").is_file():
            return list(
                queue_store_read.read_many(
                    root / "artifacts_by_job" / key,
                    ArtifactRef,
                    identity_field="artifact_id",
                )
            )
        return [
            artifact
            for artifact in queue_store_read.read_many(
                root / "artifacts",
                ArtifactRef,
                identity_field="artifact_id",
            )
            if artifact.job_id == job_id
        ]

    def list_artifacts_page(
        self,
        job_id: str,
        *,
        cursor: int = 1,
        limit: int = 100,
    ) -> tuple[list[ArtifactRef], int | None, int]:
        """Read one stable artifact page from the per-job sequence index."""
        job_id = _require_durable_record_id(job_id, field="job_id")
        return self._read_ordered_job_page(
            job_id,
            family="artifact",
            model=ArtifactRef,
            cursor=cursor,
            limit=limit,
            count_field="artifact_count",
        )

    def job_artifact_count(self, job_id: str) -> tuple[int, bool]:
        """Return the exact indexed artifact count or a bounded legacy lower bound."""
        job_id = _require_durable_record_id(job_id, field="job_id")
        index = queue_order_index.read_job_index(self._store_adapter, job_id)
        if index is not None:
            return queue_index_state.index_integer(index, "artifact_count"), False
        artifacts, truncated = queue_store_read.scan_many(
            self._storage_root / "artifacts",
            ArtifactRef,
            limit=queue_layout.DEFAULT_EXACT_RECORD_LIMIT,
            identity_field="artifact_id",
        )
        return sum(artifact.job_id == job_id for artifact in artifacts), truncated

    def get_artifact(self, artifact_id: str) -> ArtifactRef:
        """Return an artifact by id."""
        return queue_store_read.read_required_artifact(self._storage_root, artifact_id)

    def record_transform_ref(self, transform: TransformRef) -> TransformRef:
        """Persist one immutable transform independently from its used-edge count."""
        job_id = _require_durable_record_id(transform.job_id, field="job_id")
        self._store_adapter.initialize()
        path = self._storage_root / "transforms" / f"{job_id}.json"
        with self._lock:
            job = queue_store_read.read_required_job(self._storage_root, job_id)
            self._validate_transform_ref_unlocked(job, transform)
            existing = self._store_adapter.read_optional(path, TransformRef)
            if existing is not None:
                if existing != transform:
                    raise QueueConflictError(f"immutable transform ref changed for job: {job_id}")
                return existing
            self._store_adapter.write(path, transform)
            return transform

    def get_transform_ref(self, job_id: str) -> TransformRef | None:
        """Return the nullable immutable transform associated with one retained job."""
        job_id = _require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
        queue_store_read.read_required_job(self._storage_root, job_id)
        record = self._store_adapter.read_optional(
            self._storage_root / "transforms" / f"{job_id}.json",
            TransformRef,
        )
        if record is not None and record.job_id != job_id:
            raise QueueConflictError(f"transform ref identity mismatch for job: {job_id}")
        return record

    @staticmethod
    def _validate_transform_ref_unlocked(job: RelayJob, transform: TransformRef) -> None:
        if transform.job_id != job.job_id:
            raise QueueConflictError("transform ref belongs to a different job")
        expected = {item.artifact_id: item for item in job.used_artifact_refs}
        observed: set[str] = set()
        for evidence in transform.used_evidence:
            if evidence.artifact_id is None:
                continue
            use = expected.get(evidence.artifact_id)
            if use is None or evidence.sha256 != use.sha256:
                raise QueueConflictError(
                    f"transform evidence does not match job dependency: {evidence.artifact_id}"
                )
            edge_provenance = ArtifactUseProvenance.model_validate(
                evidence.model_dump(mode="python", exclude={"artifact_id", "sha256"})
            )
            if edge_provenance != use.provenance:
                raise QueueConflictError(
                    f"transform evidence provenance changed: {evidence.artifact_id}"
                )
            if evidence.artifact_id in observed:
                raise QueueConflictError(
                    f"transform evidence repeats artifact: {evidence.artifact_id}"
                )
            observed.add(evidence.artifact_id)
        missing = set(expected).difference(observed)
        if missing:
            raise QueueConflictError(
                f"transform evidence omits job dependency: {sorted(missing)[0]}"
            )
