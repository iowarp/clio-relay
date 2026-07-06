from __future__ import annotations

import subprocess
from pathlib import Path

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import EndpointRole, JarvisRunSpec, JobKind, JobState, RelayJob


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


def test_ares_worker_runs_one_job_and_indexes_artifacts(tmp_path: Path) -> None:
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
        role=EndpointRole.ARES,
        settings=settings,
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
