"""Durable input-artifact ingest ownership: begin/fail/complete lifecycle,
quota-consumption predicate, and artifact reconciliation.

Owns every public facade method that claims, terminalizes, recovers, or
reconciles a synchronous ``JobKind.INPUT_INGEST`` job -- input ingestion is
intentionally never worker-leased (``begin_input_ingest``'s own docstring),
so this lifecycle is entirely distinct from ``queue_jobs``'s general
job-state transitions.

Typed deviation (CQ13-IO-01): the design doc's second ``input_ingest``
inventory band pairs ``_assert_input_ingest_quota_unlocked`` with
``_input_ingest_consumes_quota_unlocked``. Only the latter (a one-caller,
no-business-logic "does this terminal job still own its artifact" read)
moved here. ``_assert_input_ingest_quota_unlocked`` stays facade-resident on
``ClioCoreQueue`` (``core_queue.py``): its only external caller is
``queue_jobs.submit_job`` -- an already-landed, budget-pinned owner
(786/800) that must invoke it inline inside the submission lock. Extracting
it would make ``queue_jobs.py`` reference this later-landed owner, a
reverse-rank ``queue_jobs -> queue_input_ingest`` edge the architecture
guard rejects (design doc §3's DAG requires a caller's collaborators to
already exist), and no earlier-ranked owner has the ~90-line headroom to
host it as a shared primitive instead (the section-9.4-style ratchet table
lives in ``tests/test_core_queue_split_architecture.py``). This mirrors the
already-documented CQ4-IO-01 deviation on ``queue_scheduler_cancel_state.py``
for the same class of problem. ``begin_input_ingest`` below still calls it
by name (``self._assert_input_ingest_quota_unlocked(...)``, stubbed under
``TYPE_CHECKING``); the call resolves through the composed ``ClioCoreQueue``
MRO exactly like the other still-facade-resident helpers stubbed here.

Since none of this owner's own bodies call ``_is_sha256_digest``, the design
doc §9.6 ledger's ``queue_jobs``/``queue_artifact_lineage`` duplication was
intentionally *not* touched by this slice. ``queue_job_gc`` (CQ18) has since
landed and resolved it as ledger §13.3 records: six per-owner holders plus
one consumer (``queue_idempotency``, reaching into ``queue_lease_records``'s
copy), not a shared import -- per-owner duplication of this six-line pure
predicate, not an oversight.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from clio_relay import (
    queue_context,
    queue_index_state,
    queue_jobs,
    queue_layout,
    queue_store_read,
)
from clio_relay.command_evidence import bounded_error_detail
from clio_relay.errors import QueueConflictError
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.models import (
    INPUT_INGEST_POLICY_METADATA_KEY,
    ArtifactRef,
    InputArtifactIngestPolicy,
    InputArtifactSpec,
    JobKind,
    JobState,
    RelayEvent,
    RelayJob,
    deterministic_input_artifact_id,
    utc_now,
)


def _input_ingest_attempt(job: RelayJob) -> dict[str, str] | None:
    """Validate and return one durable synchronous-ingest attempt record."""
    raw = job.metadata.get(queue_layout.INPUT_INGEST_ATTEMPT_METADATA_KEY)
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise QueueConflictError(f"input ingest attempt metadata is invalid: {job.job_id}")
    attempt = cast(dict[str, object], raw)
    schema = attempt.get("schema_version")
    attempt_id = attempt.get("attempt_id")
    started_at = attempt.get("started_at")
    outcome = attempt.get("outcome")
    if (
        schema != queue_layout.INPUT_INGEST_ATTEMPT_SCHEMA
        or not isinstance(attempt_id, str)
        or not isinstance(started_at, str)
        or outcome not in {"running", "succeeded", "failed", "abandoned"}
    ):
        raise QueueConflictError(f"input ingest attempt metadata is invalid: {job.job_id}")
    try:
        validate_durable_record_id(attempt_id)
        started = datetime.fromisoformat(started_at)
    except (TypeError, ValueError) as exc:
        raise QueueConflictError(f"input ingest attempt metadata is invalid: {job.job_id}") from exc
    if started.tzinfo is None or started.utcoffset() is None:
        raise QueueConflictError(f"input ingest attempt timestamp is naive: {job.job_id}")
    result: dict[str, str] = {
        "schema_version": queue_layout.INPUT_INGEST_ATTEMPT_SCHEMA,
        "attempt_id": attempt_id,
        "started_at": started_at,
        "outcome": cast(str, outcome),
    }
    for field in ("completed_at", "error"):
        value = attempt.get(field)
        if value is not None:
            if not isinstance(value, str):
                raise QueueConflictError(f"input ingest attempt metadata is invalid: {job.job_id}")
            result[field] = value
    return result


def _same_input_artifact(existing: ArtifactRef, requested: ArtifactRef) -> bool:
    """Compare immutable input-artifact identity while preserving its first timestamp."""
    return existing.model_dump(exclude={"sequence", "created_at"}) == requested.model_dump(
        exclude={"sequence", "created_at"}
    )


class QueueInputIngestMixin:
    """Own the synchronous input-artifact ingest lifecycle and its quota."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        def append_event(
            self,
            job_id: str,
            event_type: str,
            message: str,
            *,
            locked: bool = False,
            payload: dict[str, object] | None = None,
        ) -> RelayEvent: ...
        def _recover_pending_transitions_unlocked(self) -> list[RelayJob]: ...
        def _repair_active_job_index_unlocked(self) -> None: ...
        def _job_submission_order_key_unlocked(
            self, job: RelayJob
        ) -> tuple[int, datetime, str]: ...
        def _read_job_index(self, job_id: str) -> dict[str, object] | None: ...
        def _update_job_index_unlocked(self, job_id: str, **updates: object) -> None: ...
        def _next_job_record_sequence_unlocked(self, job_id: str, count_field: str) -> int: ...
        def _job_record_path(self, family: str, job_id: str, record_id: str) -> Path: ...
        def _initialize_artifact_user_order_unlocked(self, artifact_id: str) -> None: ...
        def _link_gateways_for_artifact_unlocked(self, artifact: ArtifactRef) -> None: ...
        def _assert_input_ingest_quota_unlocked(
            self, job: RelayJob, *, policy: InputArtifactIngestPolicy | None = None
        ) -> None: ...

    def begin_input_ingest(
        self,
        job_id: str,
        *,
        attempt_id: str,
        policy: InputArtifactIngestPolicy | None = None,
    ) -> tuple[RelayJob, bool]:
        """Claim one synchronous ingest attempt, including an exact failed-job retry.

        Input ingestion is intentionally never worker-leased.  This explicit claim
        prevents concurrent HTTP requests from racing completion and gives crash
        recovery a durable timestamp and identity to terminalize.
        """
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        attempt_id = queue_layout.QueueLayout.require_durable_record_id(
            attempt_id, field="attempt_id"
        )
        self._store_adapter.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = queue_store_read.read_required_job(self._storage_root, job_id)
            if job.kind is not JobKind.INPUT_INGEST or not isinstance(
                job.spec,
                InputArtifactSpec,
            ):
                raise QueueConflictError(f"job is not an input ingest: {job_id}")
            if job.state is JobState.SUCCEEDED:
                return job, False
            existing_attempt = _input_ingest_attempt(job)
            if job.state is JobState.RUNNING:
                if existing_attempt is not None and existing_attempt["attempt_id"] == attempt_id:
                    return job, False
                raise QueueConflictError(f"input ingest already has an active attempt: {job_id}")
            if job.state not in {JobState.QUEUED, JobState.FAILED}:
                raise QueueConflictError(
                    f"input ingest cannot begin from state {job.state.value}: {job_id}"
                )
            stored_policy_raw = job.metadata.get(INPUT_INGEST_POLICY_METADATA_KEY)
            try:
                stored_policy = InputArtifactIngestPolicy.model_validate(stored_policy_raw)
            except ValueError as exc:
                raise QueueConflictError("input ingest has no valid server quota policy") from exc
            effective_policy = policy or stored_policy
            self._assert_input_ingest_quota_unlocked(job, policy=effective_policy)
            now = utc_now()
            metadata = dict(job.metadata)
            original_policy_raw = metadata.get(
                queue_layout.INPUT_INGEST_ORIGINAL_POLICY_METADATA_KEY
            )
            if original_policy_raw is not None:
                try:
                    InputArtifactIngestPolicy.model_validate(original_policy_raw)
                except ValueError as exc:
                    raise QueueConflictError(
                        "input ingest original quota policy is invalid"
                    ) from exc
            policy_changed = effective_policy != stored_policy
            if (
                policy_changed
                and queue_layout.INPUT_INGEST_ORIGINAL_POLICY_METADATA_KEY not in metadata
            ):
                metadata[queue_layout.INPUT_INGEST_ORIGINAL_POLICY_METADATA_KEY] = (
                    stored_policy.model_dump(mode="json")
                )
            metadata[INPUT_INGEST_POLICY_METADATA_KEY] = effective_policy.model_dump(mode="json")
            metadata[queue_layout.INPUT_INGEST_ATTEMPT_METADATA_KEY] = {
                "schema_version": queue_layout.INPUT_INGEST_ATTEMPT_SCHEMA,
                "attempt_id": attempt_id,
                "started_at": now.isoformat(),
                "outcome": "running",
            }
            started = job.model_copy(
                update={
                    "state": JobState.RUNNING,
                    "updated_at": now,
                    "last_error": None,
                    "leased_by": None,
                    "metadata": metadata,
                }
            )
            queue_jobs.write_job(cast(queue_jobs.QueueJobsMixin, self), started)
            self.append_event(
                job_id,
                "input_ingest.started",
                "Input artifact ingest attempt started",
                locked=True,
                payload={
                    "attempt_id": attempt_id,
                    "retry": job.state is JobState.FAILED,
                    "policy_changed": policy_changed,
                },
            )
            return started, True

    def fail_input_ingest(
        self,
        job_id: str,
        *,
        attempt_id: str,
        error: str,
    ) -> tuple[RelayJob, bool]:
        """Terminalize the exact failed ingest attempt without stranding capacity."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        attempt_id = queue_layout.QueueLayout.require_durable_record_id(
            attempt_id, field="attempt_id"
        )
        bounded_error = bounded_error_detail(error) or "input artifact ingest failed"
        self._store_adapter.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = queue_store_read.read_required_job(self._storage_root, job_id)
            if job.kind is not JobKind.INPUT_INGEST or not isinstance(
                job.spec,
                InputArtifactSpec,
            ):
                raise QueueConflictError(f"job is not an input ingest: {job_id}")
            attempt = _input_ingest_attempt(job)
            if job.state is JobState.SUCCEEDED:
                return job, False
            if job.state is JobState.FAILED:
                if attempt is not None and attempt["attempt_id"] == attempt_id:
                    return job, False
                raise QueueConflictError(f"input ingest failed under another attempt: {job_id}")
            if (
                job.state is not JobState.RUNNING
                or attempt is None
                or attempt["attempt_id"] != attempt_id
                or attempt["outcome"] != "running"
            ):
                raise QueueConflictError(f"input ingest attempt identity changed: {job_id}")
            now = utc_now()
            metadata = dict(job.metadata)
            metadata[queue_layout.INPUT_INGEST_ATTEMPT_METADATA_KEY] = {
                **attempt,
                "completed_at": now.isoformat(),
                "outcome": "failed",
                "error": bounded_error,
            }
            failed = job.model_copy(
                update={
                    "state": JobState.FAILED,
                    "updated_at": now,
                    "last_error": bounded_error,
                    "leased_by": None,
                    "metadata": metadata,
                }
            )
            queue_jobs.write_job(cast(queue_jobs.QueueJobsMixin, self), failed)
            self.append_event(
                job_id,
                "job.failed",
                "Input artifact ingest failed",
                locked=True,
                payload={
                    "state": JobState.FAILED.value,
                    "attempt_id": attempt_id,
                    "error": bounded_error,
                },
            )
            return failed, True

    def recover_abandoned_input_ingests(
        self,
        *,
        cluster: str,
        stale_before: datetime | None = None,
        limit: int = queue_layout.MAX_INPUT_INGEST_RECOVERY_BATCH,
    ) -> list[RelayJob]:
        """Fail bounded orphaned synchronous ingests so quota and storage can recover."""
        if limit < 1 or limit > queue_layout.MAX_INPUT_INGEST_RECOVERY_BATCH:
            raise ValueError(
                "input ingest recovery limit must be between 1 and "
                f"{queue_layout.MAX_INPUT_INGEST_RECOVERY_BATCH}"
            )
        cutoff = stale_before or (
            utc_now() - timedelta(seconds=queue_layout.DEFAULT_INPUT_INGEST_ABANDONED_AFTER_SECONDS)
        )
        if cutoff.tzinfo is None or cutoff.utcoffset() is None:
            raise ValueError("input ingest recovery cutoff must include a timezone")
        self._store_adapter.initialize()
        recovered: list[RelayJob] = []
        with self._lock:
            self._recover_pending_transitions_unlocked()
            self._repair_active_job_index_unlocked()
            active, truncated = queue_store_read.scan_many(
                self._storage_root / "jobs_active",
                RelayJob,
                limit=queue_layout.MAX_ACTIVE_JOB_RECORDS,
            )
            if truncated:
                raise QueueConflictError("active job index exceeded its safety bound")
            for indexed in sorted(active, key=self._job_submission_order_key_unlocked):
                if len(recovered) >= limit:
                    break
                job = queue_store_read.read_required_job(self._storage_root, indexed.job_id)
                if (
                    job.cluster != cluster
                    or job.kind is not JobKind.INPUT_INGEST
                    or job.state not in {JobState.QUEUED, JobState.RUNNING}
                    or job.updated_at > cutoff
                ):
                    continue
                now = utc_now()
                existing_attempt = _input_ingest_attempt(job)
                attempt_id = (
                    existing_attempt["attempt_id"]
                    if existing_attempt is not None
                    else f"ingest_recovery_{uuid4().hex}"
                )
                metadata = dict(job.metadata)
                metadata[queue_layout.INPUT_INGEST_ATTEMPT_METADATA_KEY] = {
                    "schema_version": queue_layout.INPUT_INGEST_ATTEMPT_SCHEMA,
                    "attempt_id": attempt_id,
                    "started_at": (
                        existing_attempt["started_at"]
                        if existing_attempt is not None
                        else job.updated_at.isoformat()
                    ),
                    "completed_at": now.isoformat(),
                    "outcome": "abandoned",
                    "error": "input ingest attempt ended without terminal reconciliation",
                }
                failed = job.model_copy(
                    update={
                        "state": JobState.FAILED,
                        "updated_at": now,
                        "last_error": (
                            "input ingest attempt ended without terminal reconciliation"
                        ),
                        "leased_by": None,
                        "metadata": metadata,
                    }
                )
                queue_jobs.write_job(cast(queue_jobs.QueueJobsMixin, self), failed)
                self.append_event(
                    job.job_id,
                    "job.failed",
                    "Abandoned input artifact ingest recovered",
                    locked=True,
                    payload={
                        "state": JobState.FAILED.value,
                        "attempt_id": attempt_id,
                        "previous_state": job.state.value,
                        "error": failed.last_error,
                    },
                )
                recovered.append(failed)
        return recovered

    def reconcile_input_artifact(
        self,
        artifact: ArtifactRef,
        *,
        attempt_id: str | None = None,
    ) -> ArtifactRef:
        """Idempotently index the single verified artifact of an ingest job."""
        queue_layout.QueueLayout.require_durable_record_id(
            artifact.artifact_id, field="artifact_id"
        )
        queue_layout.QueueLayout.require_durable_record_id(artifact.job_id, field="job_id")
        self._store_adapter.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = queue_store_read.read_required_job(self._storage_root, artifact.job_id)
            if job.kind is not JobKind.INPUT_INGEST or not isinstance(
                job.spec,
                InputArtifactSpec,
            ):
                raise QueueConflictError(
                    f"input artifact producer is not an ingest job: {artifact.job_id}"
                )
            if job.state not in {JobState.QUEUED, JobState.RUNNING, JobState.SUCCEEDED}:
                raise QueueConflictError(
                    f"input ingest job is not reconcilable from state {job.state.value}: "
                    f"{job.job_id}"
                )
            if attempt_id is not None:
                attempt_id = queue_layout.QueueLayout.require_durable_record_id(
                    attempt_id, field="attempt_id"
                )
                attempt = _input_ingest_attempt(job)
                if (
                    job.state is not JobState.RUNNING
                    or attempt is None
                    or attempt["attempt_id"] != attempt_id
                    or attempt["outcome"] != "running"
                ):
                    raise QueueConflictError(
                        f"input ingest attempt identity changed before reconciliation: {job.job_id}"
                    )
            if (
                job.metadata.get("owner") != "clio-relay"
                or not isinstance(job.metadata.get("owner_session_id"), str)
                or not isinstance(job.metadata.get("owner_session_generation_id"), str)
            ):
                raise QueueConflictError(
                    f"input ingest job has no exact owner-session generation: {job.job_id}"
                )
            expected_artifact_id = deterministic_input_artifact_id(job.job_id)
            if artifact.artifact_id != expected_artifact_id:
                raise QueueConflictError(f"input artifact identity changed for job: {job.job_id}")
            if (
                artifact.kind != "input"
                or artifact.size_bytes != job.spec.size_bytes
                or artifact.sha256 != job.spec.sha256
                or artifact.metadata.get("schema_version") != "clio-relay.input-artifact.v1"
                or artifact.metadata.get("logical_name") != job.spec.logical_name
            ):
                raise QueueConflictError(
                    f"input artifact content identity changed for job: {job.job_id}"
                )

            canonical_path = self._storage_root / "artifacts" / f"{artifact.artifact_id}.json"
            existing = self._store_adapter.read_optional(canonical_path, ArtifactRef)
            if existing is None:
                sequence = self._next_job_record_sequence_unlocked(
                    artifact.job_id,
                    "artifact_count",
                )
                if sequence != 1:
                    raise QueueConflictError(
                        f"input ingest job already has another artifact: {job.job_id}"
                    )
                saved = artifact.model_copy(update={"sequence": sequence})
                self._store_adapter.write(canonical_path, saved)
            else:
                if existing.sequence != 1 or not _same_input_artifact(existing, artifact):
                    raise QueueConflictError(
                        f"canonical input artifact identity changed: {artifact.artifact_id}"
                    )
                saved = existing

            artifact_directory = (
                self._storage_root
                / "artifacts_by_job"
                / queue_layout.QueueLayout.durable_key(job.job_id)
            )
            paths = queue_store_read.bounded_json_record_paths(
                artifact_directory,
                limit=2,
                label=f"input artifacts for job {job.job_id}",
            )
            unexpected = [path for path in paths if path.stem != saved.artifact_id]
            if unexpected:
                raise QueueConflictError(
                    f"input ingest job has an unexpected artifact: {unexpected[0].stem}"
                )
            by_job_path = self._job_record_path(
                "artifacts_by_job",
                saved.job_id,
                saved.artifact_id,
            )
            existing_by_job = self._store_adapter.read_optional(by_job_path, ArtifactRef)
            if existing_by_job is not None and existing_by_job != saved:
                raise QueueConflictError(f"input artifact job index changed: {saved.artifact_id}")
            self._store_adapter.write(by_job_path, saved)
            order_path = (
                self._storage_root
                / "artifact_order_by_job"
                / queue_layout.QueueLayout.durable_key(saved.job_id)
                / f"{saved.sequence:020d}.json"
            )
            existing_order = self._store_adapter.read_optional(order_path, ArtifactRef)
            if existing_order is not None and existing_order != saved:
                raise QueueConflictError(f"input artifact order index changed: {saved.artifact_id}")
            self._store_adapter.write(order_path, saved)
            index = self._read_job_index(saved.job_id)
            if index is None:
                raise QueueConflictError(f"input ingest job index is missing: {saved.job_id}")
            artifact_count = queue_index_state.index_integer(index, "artifact_count")
            if artifact_count not in {0, 1}:
                raise QueueConflictError(f"input ingest artifact count is invalid: {saved.job_id}")
            if artifact_count == 0:
                self._update_job_index_unlocked(saved.job_id, artifact_count=1)
            (self._storage_root / "artifact_users" / saved.artifact_id).mkdir(
                parents=True,
                exist_ok=True,
            )
            self._initialize_artifact_user_order_unlocked(saved.artifact_id)
            self._link_gateways_for_artifact_unlocked(saved)
            if not self._input_artifact_event_exists_unlocked(saved):
                self.append_event(
                    saved.job_id,
                    "artifact.created",
                    f"Input artifact indexed: {job.spec.logical_name}",
                    locked=True,
                    payload={
                        "artifact_id": saved.artifact_id,
                        "uri": saved.uri,
                        "kind": "input",
                        "logical_name": job.spec.logical_name,
                    },
                )
            return saved

    def _input_artifact_event_exists_unlocked(self, artifact: ArtifactRef) -> bool:
        index = self._read_job_index(artifact.job_id)
        if index is None:
            raise QueueConflictError(f"input ingest job index is missing: {artifact.job_id}")
        latest_event_seq = queue_index_state.index_integer(index, "latest_event_seq")
        if latest_event_seq > queue_layout.DEFAULT_EXACT_RECORD_LIMIT:
            raise QueueConflictError(
                f"input ingest event history exceeds its reconciliation bound: {artifact.job_id}"
            )
        for sequence in range(1, latest_event_seq + 1):
            event = self._store_adapter.read_optional(
                self._storage_root / "events" / artifact.job_id / f"{sequence:020d}.json",
                RelayEvent,
            )
            if event is None or event.job_id != artifact.job_id or event.seq != sequence:
                raise QueueConflictError(
                    f"input ingest event history is incomplete: {artifact.job_id}"
                )
            if (
                event.event_type == "artifact.created"
                and event.payload.get("artifact_id") == artifact.artifact_id
            ):
                return True
        return False

    def complete_input_ingest(
        self,
        job_id: str,
        *,
        attempt_id: str | None = None,
    ) -> tuple[RelayJob, bool]:
        """Idempotently terminalize an ingest job after its artifact is durable."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = queue_store_read.read_required_job(self._storage_root, job_id)
            if job.kind is not JobKind.INPUT_INGEST or not isinstance(
                job.spec,
                InputArtifactSpec,
            ):
                raise QueueConflictError(f"job is not an input ingest: {job_id}")
            if job.state not in {JobState.QUEUED, JobState.RUNNING, JobState.SUCCEEDED}:
                raise QueueConflictError(
                    f"input ingest job has unexpected state: {job.state.value}"
                )
            attempt = _input_ingest_attempt(job)
            if attempt_id is not None:
                attempt_id = queue_layout.QueueLayout.require_durable_record_id(
                    attempt_id, field="attempt_id"
                )
                if (
                    job.state is not JobState.RUNNING
                    or attempt is None
                    or attempt["attempt_id"] != attempt_id
                    or attempt["outcome"] != "running"
                ):
                    raise QueueConflictError(
                        f"input ingest attempt identity changed before completion: {job_id}"
                    )
            artifact_id = deterministic_input_artifact_id(job.job_id)
            artifact = self._store_adapter.read_optional(
                self._storage_root / "artifacts" / f"{artifact_id}.json",
                ArtifactRef,
            )
            if (
                artifact is None
                or artifact.job_id != job.job_id
                or artifact.kind != "input"
                or artifact.size_bytes != job.spec.size_bytes
                or artifact.sha256 != job.spec.sha256
            ):
                raise QueueConflictError(
                    f"input ingest cannot complete without its exact artifact: {job_id}"
                )
            changed = job.state in {JobState.QUEUED, JobState.RUNNING}
            if changed:
                metadata = dict(job.metadata)
                if attempt is not None:
                    metadata[queue_layout.INPUT_INGEST_ATTEMPT_METADATA_KEY] = {
                        **attempt,
                        "completed_at": utc_now().isoformat(),
                        "outcome": "succeeded",
                    }
                job = job.model_copy(
                    update={
                        "state": JobState.SUCCEEDED,
                        "updated_at": utc_now(),
                        "last_error": None,
                        "leased_by": None,
                        "metadata": metadata,
                    }
                )
                queue_jobs.write_job(cast(queue_jobs.QueueJobsMixin, self), job)
            if not self._input_ingest_succeeded_event_exists_unlocked(job.job_id):
                self.append_event(
                    job.job_id,
                    "job.succeeded",
                    "Input artifact ingested",
                    locked=True,
                    payload={"state": JobState.SUCCEEDED.value, "error": None},
                )
            return job, changed

    def _input_ingest_succeeded_event_exists_unlocked(self, job_id: str) -> bool:
        index = self._read_job_index(job_id)
        if index is None:
            raise QueueConflictError(f"input ingest job index is missing: {job_id}")
        latest_event_seq = queue_index_state.index_integer(index, "latest_event_seq")
        if latest_event_seq > queue_layout.DEFAULT_EXACT_RECORD_LIMIT:
            raise QueueConflictError(
                f"input ingest event history exceeds its reconciliation bound: {job_id}"
            )
        for sequence in range(1, latest_event_seq + 1):
            event = self._store_adapter.read_optional(
                self._storage_root / "events" / job_id / f"{sequence:020d}.json",
                RelayEvent,
            )
            if event is None or event.job_id != job_id or event.seq != sequence:
                raise QueueConflictError(f"input ingest event history is incomplete: {job_id}")
            if event.event_type == "job.succeeded":
                return True
        return False

    def _input_ingest_consumes_quota_unlocked(self, job: RelayJob) -> bool:
        """Return whether an admitted ingest owns bytes or can still create them."""
        if job.kind is not JobKind.INPUT_INGEST or not isinstance(
            job.spec,
            InputArtifactSpec,
        ):
            raise QueueConflictError(f"input ingest quota producer is invalid: {job.job_id}")
        if job.state not in {JobState.FAILED, JobState.CANCELED}:
            return True
        artifact_id = deterministic_input_artifact_id(job.job_id)
        artifact = self._store_adapter.read_optional(
            self._storage_root / "artifacts" / f"{artifact_id}.json",
            ArtifactRef,
        )
        if artifact is None:
            return False
        if (
            artifact.artifact_id != artifact_id
            or artifact.job_id != job.job_id
            or artifact.kind != "input"
            or artifact.size_bytes != job.spec.size_bytes
            or artifact.sha256 != job.spec.sha256
            or artifact.metadata.get("schema_version") != job.spec.schema_version
            or artifact.metadata.get("logical_name") != job.spec.logical_name
        ):
            raise QueueConflictError(
                f"terminal input ingest artifact identity changed: {job.job_id}"
            )
        return True
