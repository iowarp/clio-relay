from __future__ import annotations

import subprocess
from collections.abc import Callable
from pathlib import Path

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import Cursor, EndpointRole, JarvisRunSpec, JobKind, JobState, RelayJob
from clio_relay.relay_ops import cancel_job


class RecordingProvider(JarvisCdProvider):
    def __init__(self) -> None:
        super().__init__(jarvis_bin="jarvis")
        self.runs: list[Path] = []

    def run_pipeline(
        self,
        pipeline_path: Path,
        *,
        cwd: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.runs.append(pipeline_path)
        return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="ok\n", stderr="")

    def run_pipeline_streaming(
        self,
        pipeline_path: Path,
        *,
        cwd: Path | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        on_start: Callable[[int], None] | None = None,
        should_cancel: Callable[[], bool] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.runs.append(pipeline_path)
        if on_start is not None:
            on_start(123)
        if on_stdout is not None:
            on_stdout("ok\n")
        if on_stderr is not None:
            on_stderr("warn\n")
        if should_cancel is not None and should_cancel():
            return subprocess.CompletedProcess(
                args=["jarvis"],
                returncode=-15,
                stdout="ok\n",
                stderr="warn\n",
            )
        return subprocess.CompletedProcess(
            args=["jarvis"],
            returncode=0,
            stdout="ok\n",
            stderr="warn\n",
        )


def test_worker_runs_one_job_and_indexes_artifacts(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    provider = RecordingProvider()
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="worker",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=provider,
    )
    worker.register()

    result = worker.run_once()

    assert result is not None
    assert result.job_id == job.job_id
    assert result.state == JobState.SUCCEEDED
    assert len(provider.runs) == 1
    artifacts = queue.list_artifacts(job.job_id)
    assert {artifact.kind for artifact in artifacts} == {"jarvis_pipeline", "stdout", "stderr"}
    events, _ = queue.drain_events(Cursor(job_id=job.job_id))
    event_types = [event.event_type for event in events]
    assert "jarvis.started" in event_types
    assert "stdout.delta" in event_types
    assert "stderr.delta" in event_types
    stdout_text = (settings.spool_dir / job.job_id / "stdout.log").read_text(encoding="utf-8")
    stderr_text = (settings.spool_dir / job.job_id / "stderr.log").read_text(encoding="utf-8")
    assert stdout_text == "ok\n"
    assert stderr_text == "warn\n"


def test_worker_preserves_canceled_state_for_running_job(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="cancel-running",
        )
    )

    class CancelingProvider(RecordingProvider):
        def run_pipeline_streaming(
            self,
            pipeline_path: Path,
            *,
            cwd: Path | None = None,
            on_stdout: Callable[[str], None] | None = None,
            on_stderr: Callable[[str], None] | None = None,
            on_start: Callable[[int], None] | None = None,
            should_cancel: Callable[[], bool] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            self.runs.append(pipeline_path)
            if on_start is not None:
                on_start(456)
            if on_stdout is not None:
                on_stdout("started\n")
            cancel_job(queue, job.job_id)
            assert should_cancel is not None
            assert should_cancel() is True
            return subprocess.CompletedProcess(
                args=["jarvis"],
                returncode=-15,
                stdout="started\n",
                stderr="",
            )

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=CancelingProvider(),
    )
    worker.register()

    result = worker.run_once()
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=50)

    assert result is not None
    assert result.state == JobState.CANCELED
    event_types = [event.event_type for event in events]
    assert "job.cancel_requested" in event_types
    assert "execution.started" in event_types
    assert "execution.canceled" in event_types
