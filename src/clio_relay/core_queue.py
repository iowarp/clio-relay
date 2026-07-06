"""Durable queue/state boundary used as the relay's clio-core adapter.

The implementation in this repository is intentionally a filesystem-backed
record store so it can run everywhere during development. The public class is
named around the clio-core contract: callers depend on record families,
idempotency, leases, and cursor replay rather than a database choice.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import TypeVar

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
            "checkpoints",
            "idempotency",
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
        key_path = self.root / "idempotency" / f"{self._safe_key(job.idempotency_key)}.json"
        with self._lock:
            if key_path.exists():
                existing_job_id = json.loads(key_path.read_text(encoding="utf-8"))["job_id"]
                return self.get_job(existing_job_id)
            self._write(self.root / "jobs" / f"{job.job_id}.json", job)
            key_path.write_text(json.dumps({"job_id": job.job_id}), encoding="utf-8")
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
    ) -> Lease | None:
        """Lease the next queued job for a cluster worker."""
        self.initialize()
        with self._lock:
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
            self._write(self.root / "tasks" / f"{task.task_id}.json", task)
        return task

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

    @staticmethod
    def _safe_key(value: str) -> str:
        return "".join(
            character if character.isalnum() or character in "-_." else "_" for character in value
        )

    @staticmethod
    def _write(path: Path, record: BaseModel) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(record.model_dump_json(indent=2), encoding="utf-8")

    @staticmethod
    def _read_optional(path: Path, model: type[Record]) -> Record | None:
        if not path.exists():
            return None
        return model.model_validate_json(path.read_text(encoding="utf-8"))

    @classmethod
    def _read_many(cls, directory: Path, model: type[Record]) -> Iterable[Record]:
        if not directory.exists():
            return []
        return [cls._read_json_file(path, model) for path in directory.glob("*.json")]

    @staticmethod
    def _read_json_file(path: Path, model: type[Record]) -> Record:
        return model.model_validate_json(path.read_text(encoding="utf-8"))
