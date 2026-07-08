from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Callable
from pathlib import Path

from pytest import MonkeyPatch

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import (
    Cursor,
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    Lease,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
    SchedulerPhase,
    SchedulerStatus,
)
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
        on_poll: Callable[[], None] | None = None,
        timeout_seconds: int | None = None,
        on_timeout: Callable[[], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.runs.append(pipeline_path)
        if on_start is not None:
            on_start(123)
        if timeout_seconds is not None and on_timeout is not None:
            on_timeout()
            return subprocess.CompletedProcess(
                args=["jarvis"],
                returncode=124,
                stdout="",
                stderr="",
            )
        if on_stdout is not None:
            on_stdout("ok\n")
        if on_stderr is not None:
            on_stderr("warn\n")
        if on_poll is not None:
            on_poll()
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
    tasks = queue.list_tasks(job.job_id)
    assert {artifact.kind for artifact in artifacts} == {
        "jarvis_pipeline",
        "stdout",
        "stderr",
        "provenance",
    }
    events, _ = queue.drain_events(Cursor(job_id=job.job_id))
    event_types = [event.event_type for event in events]
    assert "jarvis.started" in event_types
    assert "stdout.delta" in event_types
    assert "stderr.delta" in event_types
    assert "task.queued" in event_types
    assert "task.running" in event_types
    assert "task.succeeded" in event_types
    assert "provenance.available" in event_types
    stdout_text = (settings.spool_dir / job.job_id / "stdout.log").read_text(encoding="utf-8")
    stderr_text = (settings.spool_dir / job.job_id / "stderr.log").read_text(encoding="utf-8")
    provenance = json.loads(
        (settings.spool_dir / job.job_id / "provenance.json").read_text(encoding="utf-8")
    )
    assert stdout_text == "ok\n"
    assert stderr_text == "warn\n"
    assert len(tasks) == 1
    assert tasks[0].name == "jarvis.execution"
    assert tasks[0].state == JobState.SUCCEEDED
    assert provenance["job"]["job_id"] == job.job_id
    assert provenance["execution"]["terminal_state"] == "succeeded"
    assert provenance["execution"]["returncode"] == 0
    assert provenance["provider"]["name"] == "jarvis-cd"
    assert provenance["artifacts"]["stdout"]["sha256"] is not None


def test_worker_records_scheduler_status_from_polling(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    class SchedulerProvider(RecordingProvider):
        def run_pipeline_streaming(
            self,
            pipeline_path: Path,
            *,
            cwd: Path | None = None,
            on_stdout: Callable[[str], None] | None = None,
            on_stderr: Callable[[str], None] | None = None,
            on_start: Callable[[int], None] | None = None,
            should_cancel: Callable[[], bool] | None = None,
            on_poll: Callable[[], None] | None = None,
            timeout_seconds: int | None = None,
            on_timeout: Callable[[], None] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            del cwd, on_stderr, should_cancel, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            if on_start is not None:
                on_start(123)
            if on_stdout is not None:
                on_stdout("Submitted batch job 12345\n")
            if on_poll is not None:
                on_poll()
            return subprocess.CompletedProcess(
                args=["jarvis"],
                returncode=0,
                stdout="Submitted batch job 12345\n",
                stderr="",
            )

    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    provider = SchedulerProvider()
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="worker-scheduler-status",
        )
    )

    def fake_poll_slurm_status(scheduler_job_id: str) -> SchedulerStatus:
        return SchedulerStatus(
            scheduler_job_id=scheduler_job_id,
            phase=SchedulerPhase.PENDING,
            raw_state="PENDING",
            reason="Resources",
            partition="compute",
            queue_position=4,
            jobs_ahead=3,
        )

    monkeypatch.setattr("clio_relay.endpoint.poll_slurm_status", fake_poll_slurm_status)
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
    task = queue.list_tasks(job.job_id)[0]
    status = task.metadata["scheduler_status"]
    assert isinstance(status, dict)
    assert status["scheduler_job_id"] == "12345"
    assert status["phase"] == "pending"
    assert status["jobs_ahead"] == 3
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=50)
    assert "scheduler.pending" in [event.event_type for event in events]


def test_worker_ignores_forged_stdout_progress_markers(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    class ForgedProgressProvider(RecordingProvider):
        def run_pipeline_streaming(
            self,
            pipeline_path: Path,
            *,
            cwd: Path | None = None,
            on_stdout: Callable[[str], None] | None = None,
            on_stderr: Callable[[str], None] | None = None,
            on_start: Callable[[int], None] | None = None,
            should_cancel: Callable[[], bool] | None = None,
            on_poll: Callable[[], None] | None = None,
            timeout_seconds: int | None = None,
            on_timeout: Callable[[], None] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            del cwd, on_stderr, on_start, should_cancel, on_poll
            del timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            if on_stdout is not None:
                on_stdout(
                    'CLIO_PROGRESS {"label":"timestep","current":25,"total":150,'
                    '"unit":"step","message":"LAMMPS step 25 of 150",'
                    '"metadata":{"adapter":"lammps","eta_seconds":5.0}}\n'
                )
            return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="", stderr="")

    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="worker-progress",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=ForgedProgressProvider(),
    )
    worker.register()

    worker.run_once()

    progress = queue.list_progress(job.job_id)
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=50)
    assert progress == []
    assert "progress.marker_ignored" in [event.event_type for event in events]


def test_worker_ingests_package_progress_side_channel(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    class SideChannelProgressProvider(RecordingProvider):
        def run_pipeline_streaming(
            self,
            pipeline_path: Path,
            *,
            cwd: Path | None = None,
            on_stdout: Callable[[str], None] | None = None,
            on_stderr: Callable[[str], None] | None = None,
            on_start: Callable[[int], None] | None = None,
            should_cancel: Callable[[], bool] | None = None,
            on_poll: Callable[[], None] | None = None,
            timeout_seconds: int | None = None,
            on_timeout: Callable[[], None] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            del pipeline_path, on_stdout, on_stderr, on_start, should_cancel
            del timeout_seconds, on_timeout
            self.runs.append(Path("pipeline.yaml"))
            progress_path = os.environ["CLIO_RELAY_PROGRESS_FILE"]
            progress_token = os.environ["CLIO_RELAY_PROGRESS_TOKEN"]
            Path(progress_path).write_text(
                json.dumps(
                    {
                        "label": "iteration",
                        "current": 4,
                        "total": 10,
                        "unit": "step",
                        "metadata": {
                            "source": "jarvis_package",
                            "package_name": "clio_relay.bounded_command",
                            "adapter": "regex",
                            "relay_progress_token": progress_token,
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            if on_poll is not None:
                on_poll()
            return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="", stderr="")

    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="worker-side-channel-progress",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=SideChannelProgressProvider(),
    )
    worker.register()

    worker.run_once()

    progress = queue.list_progress(job.job_id)
    assert len(progress) == 1
    assert progress[0].source == "jarvis_package"
    assert progress[0].metadata["package_name"] == "clio_relay.bounded_command"
    assert progress[0].metadata["package_version"] == "builtin"
    assert progress[0].metadata["run_id"] == job.job_id
    assert "relay_progress_token" not in progress[0].metadata


def test_worker_rejects_side_channel_progress_without_token(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    class ForgedSideChannelProvider(RecordingProvider):
        def run_pipeline_streaming(
            self,
            pipeline_path: Path,
            *,
            cwd: Path | None = None,
            on_stdout: Callable[[str], None] | None = None,
            on_stderr: Callable[[str], None] | None = None,
            on_start: Callable[[int], None] | None = None,
            should_cancel: Callable[[], bool] | None = None,
            on_poll: Callable[[], None] | None = None,
            timeout_seconds: int | None = None,
            on_timeout: Callable[[], None] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            del pipeline_path, cwd, on_stdout, on_stderr, on_start, should_cancel
            del on_poll, timeout_seconds, on_timeout
            progress_path = os.environ["CLIO_RELAY_PROGRESS_FILE"]
            Path(progress_path).write_text(
                json.dumps(
                    {
                        "label": "timestep",
                        "current": 100,
                        "total": 100,
                        "metadata": {
                            "source": "jarvis_package",
                            "package_name": "builtin.lammps",
                            "package_version": "builtin",
                            "adapter": "lammps",
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="", stderr="")

    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="worker-forged-side-channel-progress",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=ForgedSideChannelProvider(),
    )
    worker.register()

    worker.run_once()
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=50)

    assert queue.list_progress(job.job_id) == []
    assert "progress.parse_failed" in [event.event_type for event in events]


def test_worker_rewrites_side_channel_package_identity_even_with_valid_token(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    class ForgedIdentitySideChannelProvider(RecordingProvider):
        def run_pipeline_streaming(
            self,
            pipeline_path: Path,
            *,
            cwd: Path | None = None,
            on_stdout: Callable[[str], None] | None = None,
            on_stderr: Callable[[str], None] | None = None,
            on_start: Callable[[int], None] | None = None,
            should_cancel: Callable[[], bool] | None = None,
            on_poll: Callable[[], None] | None = None,
            timeout_seconds: int | None = None,
            on_timeout: Callable[[], None] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            del pipeline_path, cwd, on_stdout, on_stderr, on_start, should_cancel
            del on_poll, timeout_seconds, on_timeout
            progress_path = os.environ["CLIO_RELAY_PROGRESS_FILE"]
            progress_token = os.environ["CLIO_RELAY_PROGRESS_TOKEN"]
            Path(progress_path).write_text(
                json.dumps(
                    {
                        "label": "timestep",
                        "current": 100,
                        "total": 100,
                        "metadata": {
                            "source": "jarvis_package",
                            "package_name": "builtin.lammps",
                            "package_version": "builtin",
                            "adapter": "lammps",
                            "relay_progress_token": progress_token,
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="", stderr="")

    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="worker-forged-side-channel-identity",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=ForgedIdentitySideChannelProvider(),
    )
    worker.register()

    worker.run_once()
    progress = queue.list_progress(job.job_id)

    assert len(progress) == 1
    assert progress[0].metadata["adapter"] == "regex"
    assert progress[0].metadata["package_name"] == "clio_relay.bounded_command"
    assert progress[0].metadata["package_version"] == "builtin"
    assert progress[0].metadata["run_id"] == job.job_id


def test_worker_parses_lammps_progress_only_for_declared_lammps_package(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    class LammpsProvider(RecordingProvider):
        def run_pipeline_streaming(
            self,
            pipeline_path: Path,
            *,
            cwd: Path | None = None,
            on_stdout: Callable[[str], None] | None = None,
            on_stderr: Callable[[str], None] | None = None,
            on_start: Callable[[int], None] | None = None,
            should_cancel: Callable[[], bool] | None = None,
            on_poll: Callable[[], None] | None = None,
            timeout_seconds: int | None = None,
            on_timeout: Callable[[], None] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            del cwd, on_stderr, on_start, should_cancel, on_poll
            del timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            if on_stdout is not None:
                on_stdout(
                    "[builtin.lammps] [START] BEGIN\n"
                    "Step Temp E_pair\n0 1.44 -6.0\n25 1.40 -5.9\n"
                    "[builtin.lammps] [START] END\n"
                )
            return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="", stderr="")

    pipeline_yaml = (
        "name: lammps\npkgs:\n- pkg_type: builtin.lammps\n  progress:\n    total_steps: 100\n"
    )
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml=pipeline_yaml),
            idempotency_key="worker-lammps-progress",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=LammpsProvider(),
    )
    worker.register()

    worker.run_once()

    progress = queue.list_progress(job.job_id)
    assert [item.current for item in progress] == [0, 25]
    assert progress[-1].metadata["source"] == "jarvis_package"
    assert progress[-1].metadata["package_name"] == "builtin.lammps"
    assert progress[-1].metadata["package_version"] == "builtin"
    assert progress[-1].metadata["run_id"] == job.job_id
    assert progress[-1].metadata["execution_id"] == job.job_id


def test_worker_polls_lammps_log_for_live_progress(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    output_dir = tmp_path / "lammps-out"

    class LammpsLogProvider(RecordingProvider):
        def run_pipeline_streaming(
            self,
            pipeline_path: Path,
            *,
            cwd: Path | None = None,
            on_stdout: Callable[[str], None] | None = None,
            on_stderr: Callable[[str], None] | None = None,
            on_start: Callable[[int], None] | None = None,
            should_cancel: Callable[[], bool] | None = None,
            on_poll: Callable[[], None] | None = None,
            timeout_seconds: int | None = None,
            on_timeout: Callable[[], None] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            del cwd, on_stdout, on_stderr, on_start, should_cancel, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            output_dir.mkdir(parents=True)
            (output_dir / "log.lammps").write_text(
                "Step Temp CPU\n0 1.0 0.0\n50 1.0 5.0\n100 1.0 10.0\n",
                encoding="utf-8",
            )
            if on_poll is not None:
                on_poll()
            return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="", stderr="")

    pipeline_yaml = (
        "name: lammps\n"
        "pkgs:\n"
        "- pkg_type: builtin.lammps\n"
        f"  out: {output_dir.as_posix()}\n"
        "  progress:\n"
        "    total_steps: 100\n"
    )
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml=pipeline_yaml),
            idempotency_key="worker-lammps-log-progress",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=LammpsLogProvider(),
    )
    worker.register()

    worker.run_once()

    progress = queue.list_progress(job.job_id)
    assert [item.current for item in progress] == [0, 50, 100]
    assert progress[-1].source_event_seq is None
    assert progress[-1].metadata["timing_source"] == "lammps_thermo_cpu"
    assert progress[-1].metadata["prediction_status"] == "observed_lammps_timing"


def test_worker_ignores_lammps_shaped_stdout_outside_builtin_scope(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    class UnscopedLammpsProvider(RecordingProvider):
        def run_pipeline_streaming(
            self,
            pipeline_path: Path,
            *,
            cwd: Path | None = None,
            on_stdout: Callable[[str], None] | None = None,
            on_stderr: Callable[[str], None] | None = None,
            on_start: Callable[[int], None] | None = None,
            should_cancel: Callable[[], bool] | None = None,
            on_poll: Callable[[], None] | None = None,
            timeout_seconds: int | None = None,
            on_timeout: Callable[[], None] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            del cwd, on_stderr, on_start, should_cancel, on_poll, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            if on_stdout is not None:
                on_stdout("[clio_relay.remote_agent] [START] BEGIN\n")
                on_stdout("Step Temp E_pair\n0 1.44 -6.0\n25 1.40 -5.9\n")
                on_stdout("[clio_relay.remote_agent] [START] END\n")
            return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="", stderr="")

    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: lammps\npkgs:\n- pkg_type: builtin.lammps\n"),
            idempotency_key="worker-unscoped-lammps-progress",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=UnscopedLammpsProvider(),
    )
    worker.register()

    worker.run_once()

    assert queue.list_progress(job.job_id) == []


def test_worker_ignores_fake_lammps_scope_from_mixed_pipeline(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    class FakeScopedLammpsProvider(RecordingProvider):
        def run_pipeline_streaming(
            self,
            pipeline_path: Path,
            *,
            cwd: Path | None = None,
            on_stdout: Callable[[str], None] | None = None,
            on_stderr: Callable[[str], None] | None = None,
            on_start: Callable[[int], None] | None = None,
            should_cancel: Callable[[], bool] | None = None,
            on_poll: Callable[[], None] | None = None,
            timeout_seconds: int | None = None,
            on_timeout: Callable[[], None] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            del cwd, on_stderr, on_start, should_cancel, on_poll, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            if on_stdout is not None:
                on_stdout(
                    "[builtin.lammps] [START] BEGIN\n"
                    "Step Temp E_pair\n0 1.44 -6.0\n25 1.40 -5.9\n"
                    "[builtin.lammps] [START] END\n"
                )
            return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="", stderr="")

    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(
                pipeline_yaml=(
                    "name: mixed\npkgs:\n"
                    "- pkg_type: builtin.lammps\n"
                    "- pkg_type: clio_relay.bounded_command\n"
                    "  command: [echo, fake]\n"
                )
            ),
            idempotency_key="worker-fake-lammps-scope",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=FakeScopedLammpsProvider(),
    )
    worker.register()

    worker.run_once()

    assert queue.list_progress(job.job_id) == []


def test_worker_preserves_canceled_state_for_running_job(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
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
    scancel_commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        assert text is True
        assert capture_output is True
        assert check is False
        scancel_commands.append(command)
        return subprocess.CompletedProcess(command, returncode=0, stdout="", stderr="")

    def fake_scheduler_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check
        if command[0] == "scancel":
            scancel_commands.append(command)
            return subprocess.CompletedProcess(command, returncode=0, stdout="", stderr="")
        if command[0] == "squeue":
            return subprocess.CompletedProcess(command, returncode=0, stdout="", stderr="")
        if command[0] == "sacct":
            return subprocess.CompletedProcess(
                command,
                returncode=0,
                stdout="",
                stderr="",
            )
        raise AssertionError(f"unexpected scheduler command: {command}")

    monkeypatch.setattr("clio_relay.endpoint.subprocess.run", fake_run)
    monkeypatch.setattr("clio_relay.scheduler_status.subprocess.run", fake_scheduler_run)

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
            on_poll: Callable[[], None] | None = None,
            timeout_seconds: int | None = None,
            on_timeout: Callable[[], None] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            del cwd, on_stderr, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            if on_start is not None:
                on_start(456)
            if on_stdout is not None:
                on_stdout("Submitted batch job 12345\nstarted\n")
            if on_poll is not None:
                on_poll()
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
    assert "scheduler.job_detected" in event_types
    assert "scheduler.cancel_requested" in event_types
    assert "execution.canceled" in event_types
    assert [command for command in scancel_commands if command[0] == "scancel"] == [
        ["scancel", "12345"]
    ]
    tasks = queue.list_tasks(job.job_id)
    assert tasks
    assert tasks[0].metadata["scheduler"] == "slurm"
    assert tasks[0].metadata["scheduler_job_ids"] == ["12345"]
    assert tasks[0].metadata["scheduler_status"]["phase"] == "canceled"


def test_worker_timeout_scancels_scheduler_job(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"], timeout_seconds=5),
            idempotency_key="timeout-running",
        )
    )
    scancel_commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check
        scancel_commands.append(command)
        return subprocess.CompletedProcess(command, returncode=0, stdout="", stderr="")

    def fake_scheduler_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check
        if command[0] == "scancel":
            scancel_commands.append(command)
            return subprocess.CompletedProcess(command, returncode=0, stdout="", stderr="")
        if command[0] == "squeue":
            return subprocess.CompletedProcess(command, returncode=0, stdout="", stderr="")
        if command[0] == "sacct":
            return subprocess.CompletedProcess(
                command,
                returncode=0,
                stdout="98765|CANCELLED|compute|(null)|2026-07-07T18:00:00||0:00|20|20|0\n",
                stderr="",
            )
        raise AssertionError(f"unexpected scheduler command: {command}")

    monkeypatch.setattr("clio_relay.endpoint.subprocess.run", fake_run)
    monkeypatch.setattr("clio_relay.scheduler_status.subprocess.run", fake_scheduler_run)

    class TimeoutProvider(RecordingProvider):
        def run_pipeline_streaming(
            self,
            pipeline_path: Path,
            *,
            cwd: Path | None = None,
            on_stdout: Callable[[str], None] | None = None,
            on_stderr: Callable[[str], None] | None = None,
            on_start: Callable[[int], None] | None = None,
            should_cancel: Callable[[], bool] | None = None,
            on_poll: Callable[[], None] | None = None,
            timeout_seconds: int | None = None,
            on_timeout: Callable[[], None] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            del cwd, on_stderr, should_cancel, on_poll
            self.runs.append(pipeline_path)
            if on_start is not None:
                on_start(789)
            if on_stdout is not None:
                on_stdout("Submitted batch job 98765\nstarted\n")
            assert timeout_seconds == 5
            assert on_timeout is not None
            on_timeout()
            return subprocess.CompletedProcess(
                args=["jarvis"],
                returncode=124,
                stdout="started\n",
                stderr="",
            )

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=TimeoutProvider(),
    )
    worker.register()

    result = worker.run_once()
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=50)

    assert result is not None
    assert result.state == JobState.FAILED
    assert "execution.timeout" in [event.event_type for event in events]
    assert [command for command in scancel_commands if command[0] == "scancel"] == [
        ["scancel", "98765"]
    ]
    tasks = queue.list_tasks(job.job_id)
    assert tasks
    assert tasks[0].metadata["scheduler"] == "slurm"
    assert tasks[0].metadata["scheduler_job_ids"] == ["98765"]
    assert tasks[0].metadata["scheduler_status"]["phase"] == "canceled"


def test_worker_reconciles_canceled_scheduler_job_after_restart(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="restart-cancel",
        )
    )
    task = queue.append_task(
        RelayTask(
            job_id=job.job_id,
            name="jarvis.execution",
            state=JobState.RUNNING,
            metadata={"scheduler": "slurm", "scheduler_job_ids": ["24680"]},
        )
    )
    del task
    cancel_job(queue, job.job_id)
    scancel_commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check
        scancel_commands.append(command)
        return subprocess.CompletedProcess(command, returncode=0, stdout="", stderr="")

    def fake_scheduler_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check
        if command[0] == "scancel":
            scancel_commands.append(command)
            return subprocess.CompletedProcess(command, returncode=0, stdout="", stderr="")
        if command[0] == "squeue":
            return subprocess.CompletedProcess(command, returncode=0, stdout="", stderr="")
        if command[0] == "sacct":
            return subprocess.CompletedProcess(
                command,
                returncode=0,
                stdout="24680|CANCELLED|compute|(null)|2026-07-07T18:00:00||0:00|20|20|0\n",
                stderr="",
            )
        raise AssertionError(f"unexpected scheduler command: {command}")

    monkeypatch.setattr("clio_relay.endpoint.subprocess.run", fake_run)
    monkeypatch.setattr("clio_relay.scheduler_status.subprocess.run", fake_scheduler_run)

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=RecordingProvider(),
    )
    worker.register()

    assert worker.run_once() is None
    assert scancel_commands == [["scancel", "24680"]]
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=50)
    event_types = [event.event_type for event in events]
    assert "scheduler.cancel_requested" in event_types
    assert "scheduler.canceled" in event_types


def test_worker_renews_lease_while_pipeline_runs(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")

    class RecordingQueue(ClioCoreQueue):
        def __init__(self, root: Path) -> None:
            super().__init__(root)
            self.renew_count = 0

        def renew_lease(self, lease_id: str, *, ttl_seconds: int = 300) -> Lease | None:
            self.renew_count += 1
            return super().renew_lease(lease_id, ttl_seconds=ttl_seconds)

    queue = RecordingQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="renew-from-worker",
        )
    )

    class PollingProvider(RecordingProvider):
        def run_pipeline_streaming(
            self,
            pipeline_path: Path,
            *,
            cwd: Path | None = None,
            on_stdout: Callable[[str], None] | None = None,
            on_stderr: Callable[[str], None] | None = None,
            on_start: Callable[[int], None] | None = None,
            should_cancel: Callable[[], bool] | None = None,
            on_poll: Callable[[], None] | None = None,
            timeout_seconds: int | None = None,
            on_timeout: Callable[[], None] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            del cwd, on_stdout, on_stderr, should_cancel, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            if on_start is not None:
                on_start(789)
            if on_poll is not None:
                on_poll()
            return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="", stderr="")

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=PollingProvider(),
    )
    worker.lease_renew_seconds = 0
    worker.register()

    result = worker.run_once()

    assert result is not None
    assert result.job_id == job.job_id
    assert result.state == JobState.SUCCEEDED
    assert queue.renew_count == 1


def test_worker_indexes_agent_result_artifacts(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    prompt = tmp_path / "prompt.md"
    prompt.write_text("submit the configured pipeline", encoding="utf-8")
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path=str(prompt)),
            idempotency_key="agent-artifacts",
        )
    )

    class ArtifactProvider(RecordingProvider):
        def run_pipeline_streaming(
            self,
            pipeline_path: Path,
            *,
            cwd: Path | None = None,
            on_stdout: Callable[[str], None] | None = None,
            on_stderr: Callable[[str], None] | None = None,
            on_start: Callable[[int], None] | None = None,
            should_cancel: Callable[[], bool] | None = None,
            on_poll: Callable[[], None] | None = None,
            timeout_seconds: int | None = None,
            on_timeout: Callable[[], None] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            del on_stdout, on_stderr, should_cancel, on_poll, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            assert cwd is not None
            (cwd / "agent-result.json").write_text('{"returncode": 0}', encoding="utf-8")
            (cwd / "agent-last-message.txt").write_text("submitted job_abc", encoding="utf-8")
            if on_start is not None:
                on_start(321)
            return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="", stderr="")

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=ArtifactProvider(),
    )
    worker.register()

    result = worker.run_once()
    artifacts = queue.list_artifacts(job.job_id)
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=50)

    assert result is not None
    assert result.state == JobState.SUCCEEDED
    assert {artifact.kind for artifact in artifacts} >= {
        "agent_result",
        "agent_last_message",
    }
    assert "agent_result.available" in [event.event_type for event in events]
    assert "agent_last_message.available" in [event.event_type for event in events]
