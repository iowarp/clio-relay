"""Hard-exit at canonical/derived queue transition boundaries."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import (
    TERMINAL_STATES,
    EndpointRegistration,
    EndpointRole,
    GatewaySession,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    JobState,
    Lease,
    RelayJob,
    RelayTask,
)


class _CrashQueue(ClioCoreQueue):
    mode: str
    marker: Path
    armed: bool = False

    def _mark_and_exit(self, payload: dict[str, object]) -> None:
        descriptor = os.open(self.marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            os.write(descriptor, json.dumps(payload).encode("utf-8"))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os._exit(83)

    def _sync_job_derived_unlocked(self, job: RelayJob) -> None:
        if self.armed and self.mode == "terminal" and job.state in TERMINAL_STATES:
            self._mark_and_exit({"job_id": job.job_id})
        super()._sync_job_derived_unlocked(job)

    def _write_job_unlocked(self, job: RelayJob) -> None:
        super()._write_job_unlocked(job)
        if self.armed and self.mode == "lease" and job.state is JobState.LEASED:
            self._mark_and_exit({"job_id": job.job_id})

    def _sync_task_derived_unlocked(self, task: RelayTask) -> None:
        if self.armed and self.mode == "task":
            self._mark_and_exit({"job_id": task.job_id, "task_id": task.task_id})
        super()._sync_task_derived_unlocked(task)

    def _after_lease_canonical_delete(self, lease: Lease) -> None:
        if self.armed and "after_canonical" in self.mode:
            self._mark_and_exit({"job_id": lease.job_id, "lease_id": lease.lease_id})

    def _after_lease_index_delete(self, lease: Lease) -> None:
        if self.armed and "after_index" in self.mode:
            self._mark_and_exit({"job_id": lease.job_id, "lease_id": lease.lease_id})

    def _after_lease_operational_index_write(self, lease: Lease) -> None:
        if self.armed and self.mode == "lease_after_index":
            self._mark_and_exit({"job_id": lease.job_id, "lease_id": lease.lease_id})

    def _before_stale_recovery_job_write(
        self,
        target: RelayJob,
        leases: list[Lease],
    ) -> None:
        if self.armed and "before_job" in self.mode:
            self._mark_and_exit({"job_id": target.job_id, "lease_id": leases[0].lease_id})

    def _after_stale_recovery_job_write(
        self,
        target: RelayJob,
        leases: list[Lease],
    ) -> None:
        if self.armed and "after_job" in self.mode:
            self._mark_and_exit({"job_id": target.job_id, "lease_id": leases[0].lease_id})

    def _after_gateway_canonical_write(self, session: GatewaySession) -> None:
        if (
            self.armed
            and self.mode == "gateway_close"
            and session.state is GatewaySessionState.CLOSED
        ):
            self._mark_and_exit(
                {
                    "job_id": session.metadata["job_id"],
                    "session_id": session.session_id,
                }
            )


def main() -> None:
    """Create one transition and terminate before its derived writes complete."""
    root = Path(sys.argv[1])
    marker = Path(sys.argv[2])
    mode = sys.argv[3]
    queue = _CrashQueue(root / "core")
    queue.mode = mode
    queue.marker = marker
    job = queue.submit_job(
        RelayJob(
            cluster="configured-target",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key=f"hard-crash-{mode}",
        )
    )
    queue.armed = True
    if mode == "terminal":
        queue.update_job_state(job.job_id, JobState.SUCCEEDED)
    elif mode in {"lease", "lease_after_index"}:
        endpoint = queue.register_endpoint(
            EndpointRegistration(
                role=EndpointRole.WORKER,
                cluster=job.cluster,
                hostname="crash-worker",
                pid=os.getpid(),
            )
        )
        queue.acquire_next_job(endpoint.endpoint_id, cluster=job.cluster)
    elif mode == "task":
        queue.append_task(
            RelayTask(
                job_id=job.job_id,
                name="scheduler-owned-task",
                metadata={"scheduler_job_ids": ["scheduler-hard-crash"]},
            )
        )
    elif mode.startswith("stale_"):
        endpoint = queue.register_endpoint(
            EndpointRegistration(
                role=EndpointRole.WORKER,
                cluster=job.cluster,
                hostname="lease-delete-crash-worker",
                pid=os.getpid(),
            )
        )
        lease = queue.acquire_job(
            job.job_id,
            endpoint.endpoint_id,
            cluster=job.cluster,
            ttl_seconds=-1,
        )
        assert lease is not None
        second_lease = Lease.new(job.job_id, f"{endpoint.endpoint_id}-second", -1)
        queue._write(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue.root / "leases" / f"{second_lease.lease_id}.json",
            second_lease,
        )
        queue._write(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue._job_record_path(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                "leases_by_job",
                job.job_id,
                second_lease.lease_id,
            ),
            second_lease,
        )
        queue._sync_lease_operational_indexes_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            second_lease,
            job=queue.get_job(job.job_id),
        )
        if mode.endswith("_exact"):
            queue.recover_stale_job(job.job_id, cluster=job.cluster)
        else:
            queue.recover_stale_jobs(cluster=job.cluster)
    elif mode.startswith("release_"):
        endpoint = queue.register_endpoint(
            EndpointRegistration(
                role=EndpointRole.WORKER,
                cluster=job.cluster,
                hostname="lease-release-crash-worker",
                pid=os.getpid(),
            )
        )
        lease = queue.acquire_job(job.job_id, endpoint.endpoint_id, cluster=job.cluster)
        assert lease is not None
        queue.release_lease(lease.lease_id)
    elif mode == "gateway_close":
        session = queue.create_gateway_session(
            GatewaySession(
                cluster=job.cluster,
                name="gateway-close-crash",
                state=GatewaySessionState.READY,
                metadata={"job_id": job.job_id},
            )
        )
        queue.update_job_state(job.job_id, JobState.SUCCEEDED)
        queue.update_gateway_session(session.session_id, state=GatewaySessionState.CLOSED)
    else:
        raise ValueError(f"unsupported crash fixture mode: {mode}")
    raise AssertionError("queue crash injection did not terminate")


if __name__ == "__main__":
    main()
