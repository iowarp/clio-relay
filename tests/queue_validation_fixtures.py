"""Real-process worker fixtures for queue live-validation tests."""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path

import yaml

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.jarvis_execution import sanitized_jarvis_environment
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import (
    EndpointRegistration,
    EndpointRole,
    JobKind,
    SchedulerPhase,
    SchedulerStatus,
)
from clio_relay.process_containment import containment_capability


class DeterministicQueueValidationProvider:
    """Stateful scheduler provider for queue orchestration tests."""

    name = "slurm"

    def __init__(self) -> None:
        self.scheduler_job_id = "queue-validation-123"
        self.released = False
        self.canceled = False
        self.post_release_polls = 0

    def scheduler_cluster_name(self) -> str:
        """Return the deterministic scheduler-native identity for validation."""
        return "queue-validation-cluster"

    def submit_held_validation_job(self, *, job_name: str, run_seconds: int) -> str:
        assert job_name.startswith("clio-relay-queue-")
        assert run_seconds == 5
        return self.scheduler_job_id

    def release_validation_job(
        self,
        scheduler_job_id: str,
    ) -> subprocess.CompletedProcess[str]:
        assert scheduler_job_id == self.scheduler_job_id
        self.released = True
        return subprocess.CompletedProcess(["release", scheduler_job_id], 0, "", "")

    def poll(self, scheduler_job_id: str) -> SchedulerStatus:
        assert scheduler_job_id == self.scheduler_job_id
        if self.canceled:
            phase = SchedulerPhase.CANCELED
        elif not self.released:
            phase = SchedulerPhase.PENDING
        else:
            self.post_release_polls += 1
            phase = (
                SchedulerPhase.RUNNING if self.post_release_polls == 1 else SchedulerPhase.COMPLETED
            )
        return SchedulerStatus(
            scheduler=self.name,
            scheduler_job_id=scheduler_job_id,
            phase=phase,
            nodes=1 if phase in {SchedulerPhase.RUNNING, SchedulerPhase.COMPLETED} else None,
        )

    def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        assert scheduler_job_id == self.scheduler_job_id
        self.canceled = True
        return subprocess.CompletedProcess(["cancel", scheduler_job_id], 0, "", "")


class YamlCommandProcessProvider(JarvisCdProvider):
    """Execute the bounded command through a separate-session package child."""

    def require_available(self) -> None:
        """The test provider uses the current interpreter rather than JARVIS."""

    def pipeline_command(self, pipeline_path: Path) -> list[str]:
        """Extract and execute the rendered bounded command through an outer process."""
        document = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
        command = document["pkgs"][0]["command"]
        wrapper = """
import json
import os
import subprocess
import sys

command = json.loads(sys.argv[1])
child = subprocess.Popen(
    command,
    start_new_session=os.name != "nt",
    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
)
raise SystemExit(child.wait())
        """
        return [sys.executable, "-u", "-c", wrapper, json.dumps(command)]

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
        """Run this non-JARVIS fixture without forwarding relay credentials."""
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


class LiveWorkerFleet:
    """Three actual EndpointWorker loops sharing one durable queue."""

    def __init__(self, root: Path, *, cluster: str = "test-cluster") -> None:
        self.cluster = cluster
        self.settings = RelaySettings(core_dir=root / "core", spool_dir=root / "spool")
        self.queue = ClioCoreQueue(self.settings.core_dir)
        self.scheduler = DeterministicQueueValidationProvider()
        self._stop = threading.Event()
        self._errors: list[BaseException] = []
        self._threads: list[threading.Thread] = []
        self._workers: list[EndpointWorker] = []

    def start(self) -> LiveWorkerFleet:
        """Register three slots and start their real worker loops."""
        for index in range(3):
            worker = EndpointWorker(
                role=EndpointRole.WORKER,
                settings=self.settings,
                cluster=self.cluster,
                concurrency=1,
                kind_concurrency={JobKind.JARVIS: 2},
                queue=self.queue,
                provider=YamlCommandProcessProvider(),
                scheduler_provider=self.scheduler,
            )
            endpoint = EndpointRegistration(
                endpoint_id=f"test-worker-slot-{index}",
                role=EndpointRole.WORKER,
                cluster=self.cluster,
                hostname="test-host",
                pid=10_000 + index,
                metadata={
                    "worker_slot": index,
                    "parent_endpoint_id": "test-supervisor",
                    "concurrency": 1,
                    "kind_concurrency": {"jarvis": 2},
                    "scheduler_provider": "slurm",
                    "process_containment": containment_capability(),
                },
            )
            worker.endpoint = self.queue.register_endpoint(endpoint)
            self._workers.append(worker)
            thread = threading.Thread(target=self._run_worker, args=(worker,), daemon=True)
            thread.start()
            self._threads.append(thread)
        return self

    def close(self) -> None:
        """Stop idle loops and require every worker thread to exit cleanly."""
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=15)
        alive = [thread.name for thread in self._threads if thread.is_alive()]
        if alive:
            raise AssertionError(f"queue validation worker threads did not stop: {alive}")
        if self._errors:
            raise AssertionError(f"queue validation worker failed: {self._errors[0]}")

    def __enter__(self) -> LiveWorkerFleet:
        return self.start()

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _run_worker(self, worker: EndpointWorker) -> None:
        try:
            while not self._stop.is_set():
                worker.run_once()
                time.sleep(0.01)
        except BaseException as exc:
            self._errors.append(exc)
            self._stop.set()
