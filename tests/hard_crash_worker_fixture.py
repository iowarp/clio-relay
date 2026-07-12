"""Subprocess fixture that crashes after persisting live execution ownership."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

from clio_relay import process_containment
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.models import (
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    RelayJob,
    RelayTask,
)
from clio_relay.relay_ops import cancel_job


def _create_anchored_sidecar(path: Path) -> dict[str, int]:
    """Create one private sidecar and return its durable filesystem anchor."""
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    try:
        if os.name != "nt":
            os.fchmod(descriptor, 0o600)
        observed = os.fstat(descriptor)
        return {
            "device": int(observed.st_dev),
            "inode": int(observed.st_ino),
            "owner": int(observed.st_uid),
            "link_count": int(observed.st_nlink),
            "mode": stat.S_IMODE(observed.st_mode),
        }
    finally:
        os.close(descriptor)


def main() -> None:
    """Create one running job, request cancellation, then hard-exit the worker."""
    root = Path(sys.argv[1])
    marker = Path(sys.argv[2])
    settings = RelaySettings(core_dir=root / "core", spool_dir=root / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=[sys.executable, "-c", "import time;time.sleep(60)"]),
            idempotency_key="hard-crash-worker-fixture",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
    )
    endpoint = worker.register()
    lease = queue.acquire_next_job(endpoint.endpoint_id, cluster="ares", ttl_seconds=-1)
    if lease is None:
        raise RuntimeError("hard-crash fixture could not lease its job")
    queue.update_job_state(job.job_id, JobState.RUNNING)
    task = queue.append_task(
        RelayTask(
            job_id=job.job_id,
            name="jarvis.execution",
            metadata={"cluster": "ares"},
        )
    )
    queue.update_task_state(task.task_id, JobState.RUNNING)
    restart_spool = settings.spool_dir / job.job_id
    restart_spool.mkdir(parents=True)
    progress_name = ".progress-hard-crash.jsonl"
    runtime_name = ".runtime-hard-crash.jsonl"
    progress_anchor = _create_anchored_sidecar(restart_spool / progress_name)
    runtime_anchor = _create_anchored_sidecar(restart_spool / runtime_name)
    queue.register_execution_cleanup(
        task.task_id,
        {
            "execution_sidecars": {
                "schema_version": "clio-relay.execution-sidecars.v1",
                "progress": progress_name,
                "progress_anchor": progress_anchor,
                "runtime": runtime_name,
                "runtime_anchor": runtime_anchor,
            },
            "execution_cleanup": {
                "schema_version": "clio-relay.execution-cleanup.v1",
                "launch_protocol": "broker-release-after-ownership-v1",
            },
        },
    )
    process = process_containment.spawn_owned_process(
        [sys.executable, "-c", "import time;time.sleep(60)"],
        env=process_containment.owner_environment(None),
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    worker._append_execution_start(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        job,
        task,
        process.pid,
    )
    cancel_job(queue, job.job_id)
    payload = json.dumps(
        {"job_id": job.job_id, "task_id": task.task_id, "process_id": process.pid}
    ).encode("utf-8")
    descriptor = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os._exit(0)


if __name__ == "__main__":
    main()
