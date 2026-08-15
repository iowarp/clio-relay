"""Durable idempotency-key admission ownership."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

from clio_relay import (
    queue_context,
    queue_index_state,
    queue_layout,
    queue_lease_records,
    queue_order_index,
    queue_store_read,
    queue_store_write,
)
from clio_relay.errors import QueueConflictError
from clio_relay.models import (
    INPUT_INGEST_POLICY_METADATA_KEY,
    JobKind,
    JobTombstone,
    McpAdmissionClass,
    McpCallSpec,
    RelayJob,
    artifact_use_payload,
    is_owned_jarvis_run_spec,
    prepare_owned_jarvis_run_submission,
    utc_now,
)


@dataclass(frozen=True, slots=True)
class IdempotentSubmissionResolution:
    """Read-only canonical identity resolution for storage admission."""

    state: Literal["new", "reserved", "existing", "retired"]
    canonical_job_id: str
    existing_job: RelayJob | None = None


class QueueIdempotencyMixin(queue_order_index.QueueOrderIndexMixin):
    """Own canonical idempotency-key admission and replay behavior."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol
    _validate_new_owner_session_metadata: Callable[[dict[str, object]], None]

    def resolve_idempotent_submission(
        self,
        job: RelayJob,
    ) -> IdempotentSubmissionResolution:
        """Resolve canonical idempotency identity without repairing or writing records."""
        queue_layout.QueueLayout.require_durable_record_id(job.job_id, field="job_id")
        self._validate_new_owner_session_metadata(job.metadata)
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._storage_root)
        key_path = (
            self._storage_root
            / "idempotency"
            / f"{_idempotency_key_filename(job.idempotency_key)}.json"
        )
        with self._lock:
            try:
                raw = queue_store_read.read_json_document(key_path)
            except FileNotFoundError:
                job = prepare_owned_jarvis_run_submission(job)
                job_digest = _job_idempotency_digest(job)
                if job.submission_digest not in {None, job_digest}:
                    raise QueueConflictError(
                        "submitted job carries a mismatched submission_digest"
                    ) from None
                return IdempotentSubmissionResolution(
                    state="new",
                    canonical_job_id=job.job_id,
                )
            if not isinstance(raw, dict):
                raise QueueConflictError(f"idempotency record is not an object: {key_path}")
            record = cast(dict[str, object], raw)
            canonical_job_id = record.get("job_id")
            recorded_digest = record.get("job_digest")
            state = record.get("state")
            if (
                not queue_layout.safe_global_record_id(canonical_job_id)
                or record.get("idempotency_key") != job.idempotency_key
                or state not in {"reserved", "committed", "retired"}
            ):
                raise QueueConflictError(
                    f"idempotency key was reused with a different or invalid job payload: "
                    f"{job.idempotency_key}"
                )
            canonical_job_id = cast(str, canonical_job_id)
            job = prepare_owned_jarvis_run_submission(
                job.model_copy(update={"job_id": canonical_job_id})
            )
            job_digest = _job_idempotency_digest(job)
            if job.submission_digest not in {None, job_digest}:
                raise QueueConflictError("submitted job carries a mismatched submission_digest")
            if (
                not queue_lease_records._is_sha256_digest(recorded_digest)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                or recorded_digest != job_digest
            ):
                raise QueueConflictError(
                    f"idempotency key was reused with a different or invalid job payload: "
                    f"{job.idempotency_key}"
                )
            submitted = job.model_copy(update={"submission_digest": job_digest})
            if state == "retired":
                retired = self._replay_retired_job(
                    submitted,
                    record,
                    job_digest=job_digest,
                )
                return IdempotentSubmissionResolution(
                    state="retired",
                    canonical_job_id=canonical_job_id,
                    existing_job=retired,
                )
            existing = queue_store_read.read_optional(
                self._storage_root,
                self._storage_root / "jobs" / f"{canonical_job_id}.json",
                RelayJob,
            )
            if existing is not None:
                if existing.idempotency_key != job.idempotency_key or (
                    existing.submission_digest is not None
                    and existing.submission_digest != job_digest
                ):
                    raise QueueConflictError(
                        f"idempotency target identity mismatch: {canonical_job_id}"
                    )
                return IdempotentSubmissionResolution(
                    state="existing",
                    canonical_job_id=canonical_job_id,
                    existing_job=existing,
                )
            if state == "committed":
                raise QueueConflictError(
                    f"idempotency key points to missing job: {job.idempotency_key}"
                )
            return IdempotentSubmissionResolution(
                state="reserved",
                canonical_job_id=canonical_job_id,
            )

    def _replay_retired_job(
        self,
        submitted: RelayJob,
        idempotency_record: dict[str, object],
        *,
        job_digest: str,
    ) -> RelayJob:
        job_id = idempotency_record.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            raise QueueConflictError("retired idempotency record has no job_id")
        tombstone = queue_store_read.read_optional(
            self._storage_root,
            self._storage_root
            / "job_tombstones"
            / f"{queue_layout.QueueLayout.durable_key(job_id)}.json",
            JobTombstone,
        )
        if tombstone is None:
            raise QueueConflictError(
                f"retired idempotency record points to a missing tombstone: {job_id}"
            )
        if tombstone.job_digest != job_digest or tombstone.idempotency_key != (
            submitted.idempotency_key
        ):
            raise QueueConflictError(f"retired idempotency identity mismatch: {job_id}")
        metadata = dict(submitted.metadata)
        metadata["retired_job"] = {
            "schema_version": tombstone.schema_version,
            "phase": tombstone.phase.value,
            "gc_started_at": tombstone.gc_started_at.isoformat(),
        }
        return submitted.model_copy(
            update={
                "job_id": tombstone.job_id,
                "cluster": tombstone.cluster,
                "kind": tombstone.kind,
                "state": tombstone.final_state,
                "created_at": tombstone.created_at,
                "updated_at": tombstone.updated_at,
                "attempts": tombstone.attempts,
                "last_error": tombstone.last_error,
                "leased_by": None,
                "metadata": metadata,
            }
        )

    def _write_committed_idempotency_record(
        self,
        key_path: Path,
        job: RelayJob,
        job_digest: str,
    ) -> None:
        queue_store_write.write_json(
            self._storage_root,
            key_path,
            _committed_idempotency_record(job, job_digest),
        )


def _job_idempotency_digest(job: RelayJob) -> str:
    payload = job.model_dump(mode="json")
    for generated_field in {
        "job_id",
        "state",
        "created_at",
        "updated_at",
        "leased_by",
        "attempts",
        "last_error",
        "submission_digest",
    }:
        payload.pop(generated_field, None)
    if not payload.get("used_artifact_refs"):
        payload.pop("used_artifact_refs", None)
    else:
        payload["used_artifact_refs"] = [
            artifact_use_payload(item) for item in job.used_artifact_refs
        ]
    raw_metadata = payload.get("metadata")
    if job.kind is JobKind.INPUT_INGEST and isinstance(raw_metadata, dict):
        typed_metadata = cast(dict[str, object], raw_metadata)
        for key in (
            INPUT_INGEST_POLICY_METADATA_KEY,
            queue_layout.INPUT_INGEST_ORIGINAL_POLICY_METADATA_KEY,
            queue_layout.INPUT_INGEST_ATTEMPT_METADATA_KEY,
        ):
            typed_metadata.pop(key, None)
    raw_spec = payload.get("spec")
    if isinstance(raw_spec, dict):
        spec_payload = cast(dict[str, object], raw_spec)
        if spec_payload.get("expected_jarvis_cd_lock_binding") is None:
            spec_payload.pop("expected_jarvis_cd_lock_binding", None)
        if (
            job.kind is JobKind.MCP_CALL
            and isinstance(job.spec, McpCallSpec)
            and job.spec.admission_class is McpAdmissionClass.WORKLOAD
        ):
            spec_payload.pop("admission_class", None)
        if is_owned_jarvis_run_spec(job.kind, job.spec):
            raw_arguments = spec_payload.get("arguments")
            if isinstance(raw_arguments, dict):
                cast(dict[str, object], raw_arguments).pop("execution_id", None)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _idempotency_key_filename(key: str) -> str:
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return f"key_{digest}"


def _committed_idempotency_record(job: RelayJob, job_digest: str) -> dict[str, object]:
    return {
        "state": "committed",
        "job_id": job.job_id,
        "idempotency_key": job.idempotency_key,
        "job_digest": job_digest,
        "created_at": job.created_at.isoformat(),
        "committed_at": utc_now().isoformat(),
    }
