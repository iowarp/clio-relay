"""Durable queue/state boundary used as the relay's clio-core adapter.

The implementation in this repository is intentionally a filesystem-backed
record store so it can run everywhere during development. The public class is
named around the clio-core contract: callers depend on record families,
idempotency, leases, and cursor replay rather than a database choice.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Iterable
from pathlib import Path
from typing import TypeVar
from uuid import uuid4

from filelock import FileLock
from pydantic import BaseModel

from clio_relay.errors import NotFoundError, QueueConflictError
from clio_relay.models import (
    TERMINAL_STATES,
    ArtifactRef,
    Cursor,
    EndpointRegistration,
    JobState,
    Lease,
    MonitorRule,
    ProgressRecord,
    RelayEvent,
    RelayJob,
    RelayTask,
    utc_now,
)

Record = TypeVar("Record", bound=BaseModel)


class ClioCoreQueue:
    """Durable queue facade for endpoint, job, task, lease, event, cursor, and artifact records."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._lock = FileLock(str(root / ".lock"))

    def initialize(self) -> None:
        """Create the record families used by the queue."""
        for family in (
            "endpoints",
            "jobs",
            "tasks",
            "leases",
            "events",
            "cursors",
            "artifacts",
            "progress",
            "checkpoints",
            "idempotency",
            "monitor_rules",
        ):
            (self.root / family).mkdir(parents=True, exist_ok=True)

    def register_endpoint(self, endpoint: EndpointRegistration) -> EndpointRegistration:
        """Create or refresh an endpoint registration."""
        self.initialize()
        with self._lock:
            existing = self._read_optional(
                self.root / "endpoints" / f"{endpoint.endpoint_id}.json",
                EndpointRegistration,
            )
            if existing is not None:
                endpoint = existing.model_copy(
                    update={"last_seen_at": utc_now(), "metadata": endpoint.metadata}
                )
            self._write(self.root / "endpoints" / f"{endpoint.endpoint_id}.json", endpoint)
        return endpoint

    def submit_job(self, job: RelayJob) -> RelayJob:
        """Submit a job, returning the existing record for a repeated idempotency key."""
        self.initialize()
        key_path = (
            self.root / "idempotency" / f"{_idempotency_key_filename(job.idempotency_key)}.json"
        )
        job_digest = _job_idempotency_digest(job)
        with self._lock:
            if key_path.exists():
                existing = json.loads(key_path.read_text(encoding="utf-8"))
                existing_job_id = existing["job_id"]
                existing_digest = existing.get("job_digest")
                if isinstance(existing_digest, str) and existing_digest != job_digest:
                    raise QueueConflictError(
                        f"idempotency key was reused with a different job payload: "
                        f"{job.idempotency_key}"
                    )
                existing_job = self._read_optional(
                    self.root / "jobs" / f"{existing_job_id}.json",
                    RelayJob,
                )
                if existing_job is not None:
                    self._ensure_job_queued_event(existing_job)
                    if existing.get("state") == "reserved":
                        self._write_committed_idempotency_record(key_path, existing_job, job_digest)
                    return existing_job
                if existing.get("state") != "reserved":
                    raise QueueConflictError(
                        f"idempotency key points to missing job: {job.idempotency_key}"
                    )
                job = job.model_copy(update={"job_id": existing_job_id})
            else:
                self._write_json(
                    key_path,
                    {
                        "state": "reserved",
                        "job_id": job.job_id,
                        "idempotency_key": job.idempotency_key,
                        "job_digest": job_digest,
                        "created_at": utc_now().isoformat(),
                    },
                )
            self._write(self.root / "jobs" / f"{job.job_id}.json", job)
            self._write_json(
                key_path,
                _committed_idempotency_record(job, job_digest),
            )
            self.append_event(job.job_id, "job.queued", "Job queued", locked=True)
        return job

    def get_job(self, job_id: str) -> RelayJob:
        """Return a job by id."""
        path = self.root / "jobs" / f"{job_id}.json"
        job = self._read_optional(path, RelayJob)
        if job is None:
            raise NotFoundError(f"job not found: {job_id}")
        return job

    def list_jobs(self) -> list[RelayJob]:
        """Return all jobs sorted by creation time."""
        self.initialize()
        return sorted(
            self._read_many(self.root / "jobs", RelayJob),
            key=lambda job: job.created_at,
        )

    def update_job_state(
        self,
        job_id: str,
        state: JobState,
        *,
        message: str | None = None,
        error: str | None = None,
    ) -> RelayJob:
        """Update a job state and append a state event."""
        with self._lock:
            job = self.get_job(job_id)
            if job.state in TERMINAL_STATES and state not in TERMINAL_STATES:
                raise QueueConflictError(f"cannot move terminal job {job_id} back to {state}")
            job = job.model_copy(
                update={"state": state, "updated_at": utc_now(), "last_error": error}
            )
            self._write(self.root / "jobs" / f"{job_id}.json", job)
            self.append_event(
                job_id,
                f"job.{state.value}",
                message or f"Job {state.value}",
                locked=True,
                payload={"state": state.value, "error": error},
            )
        return job

    def acquire_next_job(
        self,
        endpoint_id: str,
        *,
        cluster: str,
        ttl_seconds: int = 300,
        max_attempts: int = 3,
    ) -> Lease | None:
        """Lease the next queued job for a cluster worker."""
        self.initialize()
        with self._lock:
            self._recover_stale_jobs_unlocked(
                cluster=cluster,
                max_attempts=max_attempts,
            )
            active = self._active_lease_for_endpoint(endpoint_id)
            if active is not None:
                return active
            for job in self.list_jobs():
                if job.cluster != cluster or job.state != JobState.QUEUED:
                    continue
                lease = Lease.new(job.job_id, endpoint_id, ttl_seconds)
                job = job.model_copy(
                    update={
                        "state": JobState.LEASED,
                        "leased_by": endpoint_id,
                        "attempts": job.attempts + 1,
                        "updated_at": utc_now(),
                    }
                )
                self._write(self.root / "jobs" / f"{job.job_id}.json", job)
                self._write(self.root / "leases" / f"{lease.lease_id}.json", lease)
                self.append_event(
                    job.job_id,
                    "job.leased",
                    f"Job leased by {endpoint_id}",
                    locked=True,
                    payload={"lease_id": lease.lease_id},
                )
                return lease
        return None

    def renew_lease(self, lease_id: str, *, ttl_seconds: int = 300) -> Lease | None:
        """Extend an active lease TTL."""
        self.initialize()
        with self._lock:
            path = self.root / "leases" / f"{lease_id}.json"
            lease = self._read_optional(path, Lease)
            if lease is None:
                return None
            renewed = Lease.new(lease.job_id, lease.endpoint_id, ttl_seconds)
            renewed = renewed.model_copy(update={"lease_id": lease.lease_id})
            self._write(path, renewed)
            return renewed

    def recover_stale_jobs(self, *, cluster: str, max_attempts: int = 3) -> list[RelayJob]:
        """Requeue or fail jobs whose worker lease expired."""
        self.initialize()
        with self._lock:
            return self._recover_stale_jobs_unlocked(
                cluster=cluster,
                max_attempts=max_attempts,
            )

    def release_lease(self, lease_id: str) -> None:
        """Remove a lease record."""
        with self._lock:
            path = self.root / "leases" / f"{lease_id}.json"
            if path.exists():
                path.unlink()

    def append_task(self, task: RelayTask) -> RelayTask:
        """Create a task record."""
        self.initialize()
        with self._lock:
            self.get_job(task.job_id)
            self._write(self.root / "tasks" / f"{task.task_id}.json", task)
            self.append_event(
                task.job_id,
                "task.queued",
                f"Task queued: {task.name}",
                locked=True,
                payload={"task_id": task.task_id, "name": task.name},
            )
        return task

    def update_task_state(
        self,
        task_id: str,
        state: JobState,
        *,
        message: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> RelayTask:
        """Update a task state and append a task event."""
        self.initialize()
        with self._lock:
            path = self.root / "tasks" / f"{task_id}.json"
            task = self._read_optional(path, RelayTask)
            if task is None:
                raise NotFoundError(f"task not found: {task_id}")
            update_metadata = dict(task.metadata)
            if metadata:
                update_metadata.update(metadata)
            updated = task.model_copy(
                update={
                    "state": state,
                    "updated_at": utc_now(),
                    "metadata": update_metadata,
                }
            )
            self._write(path, updated)
            self.append_event(
                updated.job_id,
                f"task.{state.value}",
                message or f"Task {updated.name} {state.value}",
                locked=True,
                payload={
                    "task_id": updated.task_id,
                    "name": updated.name,
                    "state": state.value,
                },
            )
            return updated

    def list_tasks(self, job_id: str | None = None) -> list[RelayTask]:
        """Return durable task records, optionally filtered by job id."""
        self.initialize()
        tasks = list(self._read_many(self.root / "tasks", RelayTask))
        if job_id is not None:
            tasks = [task for task in tasks if task.job_id == job_id]
        return sorted(tasks, key=lambda task: task.created_at)

    def append_event(
        self,
        job_id: str,
        event_type: str,
        message: str,
        *,
        locked: bool = False,
        payload: dict[str, object] | None = None,
    ) -> RelayEvent:
        """Append an event with a per-job monotonic sequence number."""
        if locked:
            return self._append_event_unlocked(job_id, event_type, message, payload or {})
        with self._lock:
            return self._append_event_unlocked(job_id, event_type, message, payload or {})

    def drain_events(self, cursor: Cursor, *, limit: int = 100) -> tuple[list[RelayEvent], Cursor]:
        """Drain events from a cursor and return the advanced cursor."""
        self.initialize()
        events = [
            event
            for event in self._read_many(self.root / "events" / cursor.job_id, RelayEvent)
            if event.seq >= cursor.next_seq
        ]
        events.sort(key=lambda event: event.seq)
        drained = events[:limit]
        next_seq = cursor.next_seq if not drained else drained[-1].seq + 1
        advanced = Cursor(job_id=cursor.job_id, next_seq=next_seq)
        self._write(self.root / "cursors" / f"{cursor.job_id}.json", advanced)
        return drained, advanced

    def append_artifact(self, artifact: ArtifactRef) -> ArtifactRef:
        """Index an artifact reference."""
        self.initialize()
        with self._lock:
            self._write(self.root / "artifacts" / f"{artifact.artifact_id}.json", artifact)
            self.append_event(
                artifact.job_id,
                "artifact.created",
                f"Artifact indexed: {artifact.uri}",
                locked=True,
                payload={"artifact_id": artifact.artifact_id, "uri": artifact.uri},
            )
        return artifact

    def list_artifacts(self, job_id: str) -> list[ArtifactRef]:
        """Return artifact refs for a job."""
        self.initialize()
        return [
            artifact
            for artifact in self._read_many(self.root / "artifacts", ArtifactRef)
            if artifact.job_id == job_id
        ]

    def get_artifact(self, artifact_id: str) -> ArtifactRef:
        """Return an artifact by id."""
        path = self.root / "artifacts" / f"{artifact_id}.json"
        artifact = self._read_optional(path, ArtifactRef)
        if artifact is None:
            raise NotFoundError(f"artifact not found: {artifact_id}")
        return artifact

    def append_progress(self, progress: ProgressRecord) -> ProgressRecord:
        """Record a structured job progress observation."""
        self.initialize()
        with self._lock:
            self.get_job(progress.job_id)
            self._write(self.root / "progress" / f"{progress.progress_id}.json", progress)
            self.append_event(
                progress.job_id,
                "progress.updated",
                progress.message or f"Progress updated: {progress.label}",
                locked=True,
                payload={
                    "progress_id": progress.progress_id,
                    "label": progress.label,
                    "current": progress.current,
                    "total": progress.total,
                    "unit": progress.unit,
                    "message": progress.message,
                    "source_event_seq": progress.source_event_seq,
                },
            )
        return progress

    def list_progress(self, job_id: str) -> list[ProgressRecord]:
        """Return structured progress observations for a job."""
        self.initialize()
        return sorted(
            [
                progress
                for progress in self._read_many(self.root / "progress", ProgressRecord)
                if progress.job_id == job_id
            ],
            key=lambda progress: progress.created_at,
        )

    def append_monitor_rule(self, rule: MonitorRule) -> MonitorRule:
        """Create a durable monitor rule."""
        self.initialize()
        with self._lock:
            self.get_job(rule.job_id)
            self._write(self.root / "monitor_rules" / f"{rule.rule_id}.json", rule)
            self.append_event(
                rule.job_id,
                "monitor.rule.created",
                f"Monitor rule created: {rule.rule_id}",
                locked=True,
                payload={"rule_id": rule.rule_id, "pattern": rule.pattern},
            )
        return rule

    def list_monitor_rules(self, job_id: str | None = None) -> list[MonitorRule]:
        """Return monitor rules, optionally filtered by job id."""
        self.initialize()
        rules = list(self._read_many(self.root / "monitor_rules", MonitorRule))
        if job_id is not None:
            rules = [rule for rule in rules if rule.job_id == job_id]
        return sorted(rules, key=lambda rule: rule.created_at)

    def update_monitor_rule(self, rule: MonitorRule) -> MonitorRule:
        """Persist a monitor rule update."""
        self.initialize()
        with self._lock:
            self._write(self.root / "monitor_rules" / f"{rule.rule_id}.json", rule)
        return rule

    def _append_event_unlocked(
        self,
        job_id: str,
        event_type: str,
        message: str,
        payload: dict[str, object],
    ) -> RelayEvent:
        event_dir = self.root / "events" / job_id
        event_dir.mkdir(parents=True, exist_ok=True)
        seq = self._next_event_seq(event_dir)
        event = RelayEvent(
            job_id=job_id,
            seq=seq,
            event_type=event_type,
            message=message,
            payload=payload,
        )
        self._write(event_dir / f"{seq:020d}.json", event)
        return event

    def _next_event_seq(self, event_dir: Path) -> int:
        existing = [int(path.stem) for path in event_dir.glob("*.json")]
        return max(existing, default=0) + 1

    def _active_lease_for_endpoint(self, endpoint_id: str) -> Lease | None:
        for lease in self._read_many(self.root / "leases", Lease):
            if lease.endpoint_id == endpoint_id and not lease.is_expired():
                return lease
        return None

    def _recover_stale_jobs_unlocked(self, *, cluster: str, max_attempts: int) -> list[RelayJob]:
        recovered: list[RelayJob] = []
        for lease in self._read_many(self.root / "leases", Lease):
            if not lease.is_expired():
                continue
            lease_path = self.root / "leases" / f"{lease.lease_id}.json"
            job = self._read_optional(self.root / "jobs" / f"{lease.job_id}.json", RelayJob)
            if job is None or job.cluster != cluster or job.state in TERMINAL_STATES:
                lease_path.unlink(missing_ok=True)
                continue
            if job.state not in {JobState.LEASED, JobState.RUNNING}:
                lease_path.unlink(missing_ok=True)
                continue
            previous_state = job.state
            if job.attempts >= max_attempts:
                updated = job.model_copy(
                    update={
                        "state": JobState.FAILED,
                        "leased_by": None,
                        "updated_at": utc_now(),
                        "last_error": "expired lease exceeded retry limit",
                    }
                )
                self._write(self.root / "jobs" / f"{job.job_id}.json", updated)
                self.append_event(
                    job.job_id,
                    "job.failed",
                    "Job failed after expired lease retry limit",
                    locked=True,
                    payload={
                        "state": JobState.FAILED.value,
                        "error": "expired lease exceeded retry limit",
                        "expired_lease_id": lease.lease_id,
                        "previous_state": previous_state.value,
                    },
                )
            else:
                updated = job.model_copy(
                    update={
                        "state": JobState.QUEUED,
                        "leased_by": None,
                        "updated_at": utc_now(),
                    }
                )
                self._write(self.root / "jobs" / f"{job.job_id}.json", updated)
                self.append_event(
                    job.job_id,
                    "job.requeued",
                    "Job requeued after expired worker lease",
                    locked=True,
                    payload={
                        "state": JobState.QUEUED.value,
                        "expired_lease_id": lease.lease_id,
                        "previous_state": previous_state.value,
                    },
                )
            lease_path.unlink(missing_ok=True)
            recovered.append(updated)
        return recovered

    def _ensure_job_queued_event(self, job: RelayJob) -> None:
        event_dir = self.root / "events" / job.job_id
        if any(event_dir.glob("*.json")):
            return
        self.append_event(job.job_id, "job.queued", "Job queued", locked=True)

    def _write_committed_idempotency_record(
        self,
        key_path: Path,
        job: RelayJob,
        job_digest: str,
    ) -> None:
        self._write_json(key_path, _committed_idempotency_record(job, job_digest))

    @staticmethod
    def _safe_key(value: str) -> str:
        return "".join(
            character if character.isalnum() or character in "-_." else "_" for character in value
        )

    @staticmethod
    def _write(path: Path, record: BaseModel) -> None:
        ClioCoreQueue._write_text(path, record.model_dump_json(indent=2))

    @staticmethod
    def _write_json(path: Path, record: dict[str, object]) -> None:
        ClioCoreQueue._write_text(path, json.dumps(record))

    @staticmethod
    def _write_text(path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    @staticmethod
    def _read_optional(path: Path, model: type[Record]) -> Record | None:
        if not path.exists():
            return None
        try:
            return ClioCoreQueue._read_json_file(path, model)
        except FileNotFoundError:
            return None

    @classmethod
    def _read_many(cls, directory: Path, model: type[Record]) -> Iterable[Record]:
        if not directory.exists():
            return []
        records: list[Record] = []
        for path in directory.glob("*.json"):
            try:
                records.append(cls._read_json_file(path, model))
            except FileNotFoundError:
                continue
        return records

    @staticmethod
    def _read_json_file(path: Path, model: type[Record]) -> Record:
        last_error: OSError | json.JSONDecodeError | None = None
        for _ in range(5):
            try:
                return model.model_validate_json(path.read_text(encoding="utf-8"))
            except (PermissionError, json.JSONDecodeError) as exc:
                last_error = exc
                time.sleep(0.02)
        if last_error is not None:
            raise last_error
        return model.model_validate_json(path.read_text(encoding="utf-8"))


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
    }:
        payload.pop(generated_field, None)
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
