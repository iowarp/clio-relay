"""Crash a worker after durable containment ownership but before workload release."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import cast

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.jarvis_execution import sanitized_jarvis_environment
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import EndpointRole, JarvisRunSpec, JobKind, RelayJob, RelayTask
from clio_relay.relay_ops import cancel_job


class _MarkerProvider(JarvisCdProvider):
    def __init__(self, workload_marker: Path) -> None:
        super().__init__()
        self.workload_marker = workload_marker

    def require_available(self) -> None:
        """Use the current interpreter as the controlled workload."""

    def pipeline_command(self, pipeline_path: Path) -> list[str]:
        """Return a command that proves whether broker release occurred."""
        del pipeline_path
        script = (
            "from pathlib import Path;import sys,time;"
            "Path(sys.argv[1]).write_text('started');time.sleep(60)"
        )
        return [sys.executable, "-c", script, str(self.workload_marker)]

    def run_pipeline_streaming(
        self,
        pipeline_path: Path,
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        on_start: Callable[[int], None] | None = None,
        should_cancel: Callable[[], bool] | None = None,
        on_poll: Callable[[], None] | None = None,
        timeout_seconds: int | None = None,
        on_timeout: Callable[[], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run the non-JARVIS fixture without forwarding relay credentials."""
        return self.run_command_streaming(
            self.pipeline_command(pipeline_path),
            cwd=cwd,
            env=sanitized_jarvis_environment(env),
            credential_payload=None,
            on_stdout=on_stdout,
            on_stderr=on_stderr,
            on_start=on_start,
            should_cancel=should_cancel,
            on_poll=on_poll,
            timeout_seconds=timeout_seconds,
            on_timeout=on_timeout,
        )


class _CrashAfterOwnershipWorker(EndpointWorker):
    crash_marker: Path
    cancel_before_crash: bool = True

    def _append_execution_start(self, job: RelayJob, task: RelayTask, pid: int) -> None:
        super()._append_execution_start(job, task, pid)
        if self.cancel_before_crash:
            cancel_job(self.queue, job.job_id)
        durable_task = self.queue.get_task(task.task_id)
        raw_sidecars = durable_task.metadata["execution_sidecars"]
        if not isinstance(raw_sidecars, dict):
            raise RuntimeError("execution sidecar ownership was not persisted")
        sidecars = cast(dict[str, object], raw_sidecars)
        spool = self.settings.spool_dir / job.job_id
        for role in ("progress", "runtime"):
            name = sidecars.get(role)
            if not isinstance(name, str):
                raise RuntimeError(f"missing {role} sidecar ownership")
            (spool / name).write_text("owned\n", encoding="utf-8")
        payload = json.dumps(
            {
                "job_id": job.job_id,
                "task_id": task.task_id,
                "process_id": pid,
                "execution_ownership": durable_task.metadata["execution_ownership"],
                "execution_sidecars": durable_task.metadata["execution_sidecars"],
            }
        ).encode("utf-8")
        descriptor = os.open(self.crash_marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            os.write(descriptor, payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os._exit(0)


def main() -> None:
    """Run one job until the pre-release crash injection terminates this process."""
    root = Path(sys.argv[1])
    crash_marker = Path(sys.argv[2])
    workload_marker = Path(sys.argv[3])
    cancel_before_crash = len(sys.argv) < 5 or sys.argv[4] != "no-cancel"
    settings = RelaySettings(core_dir=root / "core", spool_dir=root / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["controlled"]),
            idempotency_key="pre-release-hard-crash",
        )
    )
    worker = _CrashAfterOwnershipWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=_MarkerProvider(workload_marker),
    )
    worker.crash_marker = crash_marker
    worker.cancel_before_crash = cancel_before_crash
    worker.lease_ttl_seconds = -1
    worker.run_once()
    raise RuntimeError("fault injection did not terminate the worker")


if __name__ == "__main__":
    main()
