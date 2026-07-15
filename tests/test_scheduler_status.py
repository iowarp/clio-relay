from __future__ import annotations

import getpass
import hashlib
import hmac
import os
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from json import dumps, loads
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest
from pytest import MonkeyPatch

from clio_relay import process_containment
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_execution import (
    named_jarvis_command,
    register_scheduled_jarvis_command,
    run_native_jarvis_broker,
    scheduled_jarvis_command,
    scheduled_runtime_credential_channel,
)
from clio_relay.models import (
    JarvisRunSpec,
    JobKind,
    JobState,
    RelayJob,
    RelayTask,
    SchedulerPhase,
)
from clio_relay.relay_ops import job_status
from clio_relay.runtime_metadata import runtime_metadata_from_sidecar_record
from clio_relay.scheduler_providers import (
    ExternalSchedulerProvider,
    SlurmSchedulerProvider,
    provider_for_scheduler,
    register_scheduler_provider,
)
from clio_relay.scheduler_status import relay_queue_status


def _install_runtime_credential_fd(
    monkeypatch: MonkeyPatch,
    *,
    runtime_path: Path,
    runtime_token: str,
    scheduler_expected: bool | str = True,
    scheduler_provider: str = "slurm",
) -> int:
    """Install the one-shot broker credential channel for an in-process adapter test."""
    progress_path = runtime_path.with_name(f"{runtime_path.name}.progress")
    progress_descriptor = os.open(
        progress_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    if os.name != "nt":
        os.fchmod(progress_descriptor, 0o600)
    os.close(progress_descriptor)
    descriptor = os.open(runtime_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        if os.name != "nt":
            os.fchmod(descriptor, 0o600)
        runtime_stat = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    read_fd, write_fd = os.pipe()
    ready_read_fd, ready_write_fd = os.pipe()
    direct_proof = "test-direct-execution-proof"
    payload = dumps(
        {
            "schema_version": "clio-relay.jarvis-private-credential.v1",
            "progress_file": str(progress_path),
            "progress_token": "test-progress-token",
            "runtime_file": str(runtime_path),
            "runtime_token": runtime_token,
            "runtime_anchor": {
                "device": int(runtime_stat.st_dev),
                "inode": int(runtime_stat.st_ino),
                "owner": int(runtime_stat.st_uid),
                "link_count": int(runtime_stat.st_nlink),
                "mode": runtime_stat.st_mode & 0o7777,
            },
            "runtime_intent": {
                "schema_version": "clio-relay.scheduler-submission-intent.v1",
                "execution_id": "jarvis_test_execution",
                "marker": "clio-relay-0123456789abcdef",
                "created_at": "2026-07-11T00:00:00+00:00",
                "scheduler_user": getpass.getuser(),
                "scheduler_expected": scheduler_expected,
                "direct_proof_sha256": hashlib.sha256(direct_proof.encode("utf-8")).hexdigest(),
            },
            "runtime_direct_proof": direct_proof,
            "scheduler_provider": scheduler_provider,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    try:
        os.write(write_fd, payload)
    finally:
        os.close(write_fd)
    monkeypatch.setenv("CLIO_RELAY_BROKER_CREDENTIAL_FD", str(read_fd))
    monkeypatch.setenv("CLIO_RELAY_BROKER_READY_FD", str(ready_write_fd))
    return ready_read_fd


@dataclass(frozen=True)
class _NativeHandle:
    execution_id: str
    pipeline_id: str
    mode: str
    scheduler_provider: str | None = None
    scheduler_native_id: str | None = None
    cluster: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "jarvis.execution.handle.v1",
            "execution_id": self.execution_id,
            "pipeline_id": self.pipeline_id,
            "mode": self.mode,
            "scheduler_provider": self.scheduler_provider,
            "scheduler_native_id": self.scheduler_native_id,
            "cluster": self.cluster,
        }


@dataclass(frozen=True)
class _NativeRecord:
    execution_id: str
    pipeline_id: str
    mode: str
    state: str
    submitted: bool
    terminal: bool
    scheduler_provider: str | None = None
    scheduler_native_id: str | None = None
    cluster: str | None = None
    return_code: int | None = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=lambda: {})
    created_at: str = "2026-07-12T00:00:00Z"
    updated_at: str = "2026-07-12T00:00:01Z"

    @property
    def handle(self) -> _NativeHandle:
        return _NativeHandle(
            execution_id=self.execution_id,
            pipeline_id=self.pipeline_id,
            mode=self.mode,
            scheduler_provider=self.scheduler_provider,
            scheduler_native_id=self.scheduler_native_id,
            cluster=self.cluster,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "jarvis.execution.record.v1",
            "execution_id": self.execution_id,
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_id,
            "mode": self.mode,
            "scheduler_provider": self.scheduler_provider,
            "scheduler_native_id": self.scheduler_native_id,
            "cluster": self.cluster,
            "state": self.state,
            "submitted": self.submitted,
            "terminal": self.terminal,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "return_code": self.return_code,
            "error": self.error,
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class _NativeProgress:
    record: _NativeRecord

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "jarvis.execution.progress.v1",
            "execution_id": self.record.execution_id,
            "pipeline_id": self.record.pipeline_id,
            "execution_state": self.record.state,
            "terminal": self.record.terminal,
            "packages": [],
        }


class _ScheduledNativePipeline:
    def __init__(
        self,
        *,
        name: str,
        job_id: str,
        duplicate_running_reads: int = 0,
        on_submit: Callable[[], None] | None = None,
    ) -> None:
        self.name = name
        self.job_id = job_id
        self.scheduler: dict[str, object] = {"name": "slurm"}
        self.duplicate_running_reads = duplicate_running_reads
        self.on_submit = on_submit
        self.submit_calls: list[tuple[bool, bool, str]] = []
        self._records: list[_NativeRecord] = []
        self._read_index = 0
        self._last_read: _NativeRecord | None = None

    def submit(self, *, submit: bool, wait: bool, execution_id: str) -> _NativeHandle:
        self.submit_calls.append((submit, wait, execution_id))
        if self.on_submit is not None:
            self.on_submit()
        submission = {
            "schema_version": "jarvis.scheduler.submission.v1",
            "execution_id": execution_id,
            "provider": "slurm",
            "script_path": "/tmp/submit.slurm",
            "scheduler_job_id": self.job_id,
            "scheduler_cluster": None,
            "identity_source": "scheduler_submit_api",
            "submitted": True,
            "reconciliation_marker": self.scheduler["job_name"],
        }
        submitted = _NativeRecord(
            execution_id=execution_id,
            pipeline_id=self.name,
            mode="scheduler",
            state="submitted",
            submitted=True,
            terminal=False,
            scheduler_provider="slurm",
            scheduler_native_id=self.job_id,
            metadata={"submission": submission},
        )
        running = replace(
            submitted,
            state="running",
            updated_at="2026-07-12T00:00:02Z",
        )
        completed = replace(
            submitted,
            state="completed",
            terminal=True,
            return_code=0,
            updated_at="2026-07-12T00:00:03Z",
        )
        self._records = [
            submitted,
            *([running] * (self.duplicate_running_reads + 1)),
            completed,
        ]
        return submitted.handle

    def get_execution(self, execution_id: str) -> _NativeRecord:
        if not self._records:
            raise FileNotFoundError(execution_id)
        index = min(self._read_index, len(self._records) - 1)
        self._read_index += 1
        self._last_read = self._records[index]
        return self._last_read

    def get_execution_progress(self, execution_id: str) -> _NativeProgress:
        if self._last_read is None:
            raise FileNotFoundError(execution_id)
        return _NativeProgress(self._last_read)

    @property
    def read_count(self) -> int:
        """Return the number of authoritative execution-record reads."""
        return self._read_index


def _install_yaml_pipeline_module(
    monkeypatch: MonkeyPatch,
    pipeline: object,
    *,
    on_load: Callable[[], None] | None = None,
) -> None:
    """Install a bounded fake JARVIS YAML loader for the embedded wrapper."""

    def load_yaml_auto(_path: str) -> tuple[None, object]:
        if on_load is not None:
            on_load()
        return None, pipeline

    pipeline_test = ModuleType("jarvis_cd.core.pipeline_test")
    pipeline_test.load_yaml_auto = load_yaml_auto  # type: ignore[attr-defined]
    jarvis_cd = ModuleType("jarvis_cd")
    jarvis_cd.__path__ = []  # type: ignore[attr-defined]
    jarvis_core = ModuleType("jarvis_cd.core")
    jarvis_core.__path__ = []  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "jarvis_cd", jarvis_cd)
    monkeypatch.setitem(sys.modules, "jarvis_cd.core", jarvis_core)
    monkeypatch.setitem(sys.modules, "jarvis_cd.core.pipeline_test", pipeline_test)


def _install_named_pipeline_module(
    monkeypatch: MonkeyPatch,
    pipeline_class: type[object],
) -> None:
    """Install one fake named JARVIS pipeline class for the embedded wrapper."""

    def reject_yaml(path: str) -> tuple[None, object]:
        raise AssertionError(path)

    pipeline_test = ModuleType("jarvis_cd.core.pipeline_test")
    pipeline_test.load_yaml_auto = reject_yaml  # type: ignore[attr-defined]
    pipeline_module = ModuleType("jarvis_cd.core.pipeline")
    pipeline_module.Pipeline = pipeline_class  # type: ignore[attr-defined]
    jarvis_cd = ModuleType("jarvis_cd")
    jarvis_cd.__path__ = []  # type: ignore[attr-defined]
    jarvis_core = ModuleType("jarvis_cd.core")
    jarvis_core.__path__ = []  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "jarvis_cd", jarvis_cd)
    monkeypatch.setitem(sys.modules, "jarvis_cd.core", jarvis_core)
    monkeypatch.setitem(sys.modules, "jarvis_cd.core.pipeline", pipeline_module)
    monkeypatch.setitem(sys.modules, "jarvis_cd.core.pipeline_test", pipeline_test)


def _execute_test_wrapper(
    monkeypatch: MonkeyPatch,
    *,
    command: list[str],
    ready_read_fd: int,
) -> None:
    """Execute a credential-bound wrapper after replacing the platform gate in tests."""
    monkeypatch.setattr(process_containment, "enforce_linux_secret_memory_gate", lambda: None)
    try:
        exec(compile(command[4], "<jarvis-native-adapter>", "exec"), {"__name__": "__main__"})
        assert os.read(ready_read_fd, 1) == b"1"
    finally:
        os.close(ready_read_fd)


def _fast_test_sleep(_seconds: float) -> None:
    """Yield the GIL without adding wall-clock delay to polling tests."""
    time.sleep(0)


def test_relay_queue_status_counts_older_cluster_jobs(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    first = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: first\npkgs: []\n"),
            idempotency_key="first",
        )
    )
    second = queue.submit_job(
        RelayJob(
            cluster="other",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: other\npkgs: []\n"),
            idempotency_key="other",
        )
    )
    third = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: third\npkgs: []\n"),
            idempotency_key="third",
        )
    )

    assert relay_queue_status(queue, first) == {
        "state": "queued",
        "jobs_ahead": 0,
        "position": 1,
    }
    assert relay_queue_status(queue, second)["jobs_ahead"] == 0
    assert relay_queue_status(queue, third) == {
        "state": "queued",
        "jobs_ahead": 1,
        "position": 2,
    }
    queue.update_job_state(first.job_id, JobState.RUNNING)
    assert relay_queue_status(queue, queue.get_job(first.job_id)) == {
        "state": "running",
        "jobs_ahead": None,
        "position": None,
    }


def test_poll_slurm_status_reports_pending_queue_position(monkeypatch: MonkeyPatch) -> None:
    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        assert text is True
        assert capture_output is True
        assert check is False
        assert timeout == 15.0
        if command[:4] == ["squeue", "-h", "-j", "100"]:
            return subprocess.CompletedProcess(
                command,
                0,
                "100|PENDING|Resources|compute|normal|alice|1|4|4G|2026-07-07T10:00:00|N/A|0:00|1:00:00\n",
                "",
            )
        if command[:4] == ["squeue", "-h", "-t", "PD"]:
            return subprocess.CompletedProcess(
                command,
                0,
                "\n".join(
                    [
                        "99|PENDING|Priority|compute|normal|bob|1|4|4G|2026-07-07T09:00:00|N/A|0:00|1:00:00",
                        "100|PENDING|Resources|compute|normal|alice|1|4|4G|2026-07-07T10:00:00|N/A|0:00|1:00:00",
                        "101|PENDING|Priority|debug|normal|bob|1|4|4G|2026-07-07T08:00:00|N/A|0:00|1:00:00",
                    ]
                ),
                "",
            )
        raise AssertionError(command)

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)

    status = SlurmSchedulerProvider().poll("100")

    assert status.phase == SchedulerPhase.PENDING
    assert status.reason == "Resources"
    assert status.partition == "compute"
    assert status.jobs_ahead == 1
    assert status.queue_position == 2
    assert status.queue_position_note is not None


def test_poll_slurm_status_uses_sacct_when_squeue_is_empty(monkeypatch: MonkeyPatch) -> None:
    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check, timeout
        if command[:4] == ["squeue", "-h", "-j", "100"]:
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[:4] == ["sacct", "-n", "-P", "-j"]:
            return subprocess.CompletedProcess(
                command,
                0,
                "100|COMPLETED|compute|normal|2026-07-07T10:00:00|2026-07-07T10:01:00|00:02:00|1|4|4G\n",
                "",
            )
        raise AssertionError(command)

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)

    status = SlurmSchedulerProvider().poll("100")

    assert status.phase == SchedulerPhase.COMPLETED
    assert status.raw_state == "COMPLETED"


def test_poll_slurm_status_uses_scontrol_when_accounting_is_disabled(
    monkeypatch: MonkeyPatch,
) -> None:
    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check, timeout
        if command[:4] == ["squeue", "-h", "-j", "21835"]:
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[:4] == ["sacct", "-n", "-P", "-j"]:
            return subprocess.CompletedProcess(
                command,
                1,
                "",
                "sacct: Slurm accounting storage is disabled",
            )
        if command == ["scontrol", "show", "job", "21835", "-o"]:
            return subprocess.CompletedProcess(
                command,
                0,
                (
                    "JobId=21835 JobName=clio-relay-validation JobState=COMPLETED "
                    "Reason=None UserId=alice(1008) QOS=(null) Partition=compute "
                    "AllocNode:Sid=ares:2732476 "
                    "NumNodes=1 NumCPUs=2 MinMemoryNode=0 "
                    "SubmitTime=2026-07-10T19:51:34 "
                    "EligibleTime=2026-07-10T19:51:37 "
                    "StartTime=2026-07-10T19:51:37 RunTime=00:00:13 "
                    "TimeLimit=00:05:00 ExitCode=0:0\n"
                ),
                "",
            )
        raise AssertionError(command)

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)

    status = SlurmSchedulerProvider().poll("21835")

    assert status.phase == SchedulerPhase.COMPLETED
    assert status.raw_state == "COMPLETED"
    assert status.partition == "compute"
    assert status.user == "alice"
    assert status.nodes == 1
    assert status.cpus == 2
    assert status.elapsed == "00:00:13"
    assert status.queue_position_note == ("historical scheduler status from scontrol; ExitCode=0:0")


def test_slurm_connector_placement_pins_verified_single_batch_host(
    monkeypatch: MonkeyPatch,
) -> None:
    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check, timeout
        if command == ["scontrol", "show", "job", "21835", "-o"]:
            return subprocess.CompletedProcess(
                command,
                0,
                "JobId=21835 JobState=RUNNING BatchHost=compute-07 "
                "NumNodes=1 NodeList=compute-07\n",
                "",
            )
        if command == ["scontrol", "show", "hostnames", "compute-07"]:
            return subprocess.CompletedProcess(command, 0, "compute-07\n", "")
        raise AssertionError(command)

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)
    provider = SlurmSchedulerProvider()

    placement = provider.connector_placement("21835")

    assert placement.model_dump(mode="json") == {
        "schema_version": "clio-relay.scheduler-connector-placement.v1",
        "scheduler": "slurm",
        "scheduler_job_id": "21835",
        "placement_host": "compute-07",
        "allocation_node_count": 1,
        "source": "slurm-scontrol-batch-host",
        "verified": True,
        "observed_at": placement.observed_at.isoformat().replace("+00:00", "Z"),
    }


def test_slurm_connector_placement_rejects_multinode_or_ambiguous_host(
    monkeypatch: MonkeyPatch,
) -> None:
    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check, timeout
        assert command == ["scontrol", "show", "job", "21835", "-o"]
        return subprocess.CompletedProcess(
            command,
            0,
            "JobId=21835 JobState=RUNNING BatchHost=compute-07 "
            "NumNodes=2 NodeList=compute-[07-08]\n",
            "",
        )

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)

    with pytest.raises(RelayError, match="single-node"):
        SlurmSchedulerProvider().connector_placement("21835")


def test_slurm_connector_step_launch_detaches_and_resolves_exact_marker_identity(
    monkeypatch: MonkeyPatch,
) -> None:
    marker = "clio-relay-connector-0123456789abcdef0123456789abcdef"
    expected = [
        "srun",
        "--jobid=21835",
        "--overlap",
        "--exact",
        "--nodes=1",
        "--ntasks=1",
        "--nodelist=compute-07",
        f"--job-name={marker}",
        "--input=none",
        "--output=/home/alice/.local/share/clio-relay/connector.log",
        "--error=/home/alice/.local/share/clio-relay/connector.log",
        "--open-mode=append",
        "--",
        "/home/alice/.local/bin/frpc",
        "-c",
        "/home/alice/.local/share/clio-relay/frpc.toml",
    ]
    popen_calls: list[tuple[list[str], dict[str, object]]] = []
    registered_launchers: list[object] = []

    class DetachedLauncher:
        """Minimal running launcher used to verify provider process semantics."""

        pid = 4242

        def poll(self) -> int | None:
            """Keep the launcher running while the scheduler registers its step."""
            return None

    launcher = DetachedLauncher()

    def fake_popen(command: list[str], **kwargs: object) -> DetachedLauncher:
        popen_calls.append((command, kwargs))
        return launcher

    def fake_register(process: object) -> None:
        registered_launchers.append(process)

    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check, timeout
        assert popen_calls
        assert cast(Any, popen_calls[0][1]["stdout"]).closed is False
        assert command == [
            "squeue",
            "--noheader",
            "--steps",
            "--jobs=21835",
            f"--name={marker}",
            "--format=%i|%j|%N",
        ]
        return subprocess.CompletedProcess(
            command,
            0,
            f"21835.7|{marker}|compute-07\n",
            "",
        )

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.Popen", fake_popen)
    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)
    monkeypatch.setattr(
        "clio_relay.scheduler_providers._register_connector_launcher_for_reaping",
        fake_register,
    )

    identity = SlurmSchedulerProvider().launch_connector_step(
        "21835",
        placement_host="compute-07",
        step_marker=marker,
        command=[
            "/home/alice/.local/bin/frpc",
            "-c",
            "/home/alice/.local/share/clio-relay/frpc.toml",
        ],
        output_path="/home/alice/.local/share/clio-relay/connector.log",
    )

    assert identity.scheduler_step_id == "21835.7"
    assert identity.step_marker == marker
    assert identity.placement_host == "compute-07"
    assert identity.source == "slurm-srun-detached-marker"
    assert len(popen_calls) == 1
    launch_command, launch_kwargs = popen_calls[0]
    assert launch_command == expected
    assert launch_kwargs["stdin"] is subprocess.DEVNULL
    assert launch_kwargs["stdout"] not in {None, subprocess.PIPE}
    assert launch_kwargs["stderr"] is subprocess.STDOUT
    assert launch_kwargs["start_new_session"] is True
    assert launch_kwargs["close_fds"] is True
    assert cast(Any, launch_kwargs["stdout"]).closed is True
    assert registered_launchers == [launcher]


def test_slurm_connector_step_launch_terminates_unregistered_launcher(
    monkeypatch: MonkeyPatch,
) -> None:
    marker = "clio-relay-connector-0123456789abcdef0123456789abcdef"

    class DetachedLauncher:
        """Running launcher that records bounded provider cleanup."""

        pid = 4242

        def __init__(self) -> None:
            self.returncode: int | None = None
            self.terminate_calls = 0
            self.kill_calls = 0

        def poll(self) -> int | None:
            """Return the current fake launcher state."""
            return self.returncode

        def terminate(self) -> None:
            """Record graceful provider cleanup."""
            self.terminate_calls += 1
            self.returncode = -15

        def wait(self, timeout: float | None = None) -> int:
            """Return the terminal fake launcher state."""
            del timeout
            assert self.returncode is not None
            return self.returncode

        def kill(self) -> None:
            """Record forced provider cleanup if graceful cleanup fails."""
            self.kill_calls += 1
            self.returncode = -9

    launcher = DetachedLauncher()

    def fake_popen(command: list[str], **kwargs: object) -> DetachedLauncher:
        del command, kwargs
        return launcher

    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check, timeout
        assert command[0] == "squeue"
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.Popen", fake_popen)
    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)
    monkeypatch.setattr(
        "clio_relay.scheduler_providers.CONNECTOR_STEP_REGISTRATION_TIMEOUT_SECONDS",
        0.0,
    )
    monkeypatch.setattr(
        "clio_relay.scheduler_providers.CONNECTOR_STEP_FAILED_RECONCILIATION_OBSERVATIONS",
        1,
    )

    with pytest.raises(RelayError, match="bounded provider timeout"):
        SlurmSchedulerProvider().launch_connector_step(
            "21835",
            placement_host="compute-07",
            step_marker=marker,
            command=["/home/alice/.local/bin/frpc", "-c", "/home/alice/frpc.toml"],
            output_path="/home/alice/connector.log",
        )

    assert launcher.terminate_calls == 1
    assert launcher.kill_calls == 0


def test_slurm_connector_step_failed_registration_cancels_late_exact_step(
    monkeypatch: MonkeyPatch,
) -> None:
    marker = "clio-relay-connector-0123456789abcdef0123456789abcdef"
    commands: list[list[str]] = []
    marker_queries = 0

    class DetachedLauncher:
        """Running launcher used to expose a registration/termination race."""

        pid = 4242

        def __init__(self) -> None:
            self.returncode: int | None = None
            self.terminate_calls = 0

        def poll(self) -> int | None:
            """Return the current fake launcher state."""
            return self.returncode

        def terminate(self) -> None:
            """Record graceful provider cleanup."""
            self.terminate_calls += 1
            self.returncode = -15

        def wait(self, timeout: float | None = None) -> int:
            """Return the terminal fake launcher state."""
            del timeout
            assert self.returncode is not None
            return self.returncode

        def kill(self) -> None:
            """Provide the Popen fallback contract."""
            self.returncode = -9

    launcher = DetachedLauncher()

    def fake_popen(command: list[str], **kwargs: object) -> DetachedLauncher:
        del command, kwargs
        return launcher

    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal marker_queries
        del text, capture_output, check, timeout
        commands.append(command)
        if command[0] == "scancel":
            return subprocess.CompletedProcess(command, 0, "", "")
        if "--steps=21835.7" in command:
            return subprocess.CompletedProcess(command, 0, "", "")
        marker_queries += 1
        stdout = "" if marker_queries == 1 else f"21835.7|{marker}|compute-07\n"
        return subprocess.CompletedProcess(command, 0, stdout, "")

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.Popen", fake_popen)
    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)
    monkeypatch.setattr(
        "clio_relay.scheduler_providers.CONNECTOR_STEP_REGISTRATION_TIMEOUT_SECONDS",
        0.0,
    )

    with pytest.raises(RelayError, match="bounded provider timeout"):
        SlurmSchedulerProvider().launch_connector_step(
            "21835",
            placement_host="compute-07",
            step_marker=marker,
            command=["/home/alice/.local/bin/frpc", "-c", "/home/alice/frpc.toml"],
            output_path="/home/alice/connector.log",
        )

    assert launcher.terminate_calls == 1
    assert ["scancel", "21835.7"] in commands
    assert ["scancel", "21835"] not in commands
    assert ["squeue", "--noheader", "--steps=21835.7", "--format=%i|%N"] in commands


def test_slurm_connector_step_status_and_cancel_are_exact_step_scoped(
    monkeypatch: MonkeyPatch,
) -> None:
    commands: list[list[str]] = []
    status_outputs = iter(["21835.7|compute-07\n", ""])

    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check, timeout
        commands.append(command)
        if command[0] == "squeue":
            return subprocess.CompletedProcess(command, 0, next(status_outputs), "")
        if command[0] == "scancel":
            return subprocess.CompletedProcess(command, 0, "", "")
        raise AssertionError(command)

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)
    provider = SlurmSchedulerProvider()

    active = provider.poll_connector_step(
        "21835",
        scheduler_step_id="21835.7",
        placement_host="compute-07",
    )
    canceled = provider.cancel_connector_step(
        "21835",
        scheduler_step_id="21835.7",
    )
    absent = provider.poll_connector_step(
        "21835",
        scheduler_step_id="21835.7",
        placement_host="compute-07",
    )

    assert active.state == "active"
    assert canceled.returncode == 0
    assert absent.state == "absent"
    assert commands == [
        ["squeue", "--noheader", "--steps=21835.7", "--format=%i|%N"],
        ["scancel", "21835.7"],
        ["squeue", "--noheader", "--steps=21835.7", "--format=%i|%N"],
    ]
    assert ["scancel", "21835"] not in commands
    with pytest.raises(ConfigurationError, match="exact numeric step"):
        provider.cancel_connector_step("21835", scheduler_step_id="21835")


def test_slurm_connector_step_status_rejects_wrong_compute_host(
    monkeypatch: MonkeyPatch,
) -> None:
    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check, timeout
        return subprocess.CompletedProcess(command, 0, "21835.7|compute-08\n", "")

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)

    with pytest.raises(RelayError, match="provider-verified placement host"):
        SlurmSchedulerProvider().poll_connector_step(
            "21835",
            scheduler_step_id="21835.7",
            placement_host="compute-07",
        )


def test_slurm_connector_step_reconciliation_uses_exact_marker_and_host(
    monkeypatch: MonkeyPatch,
) -> None:
    marker = "clio-relay-connector-0123456789abcdef0123456789abcdef"
    outputs = iter(
        [
            f"21835.7|{marker}|compute-07\n",
            f"21835.7|{marker}|compute-07\n21835.8|{marker}|compute-07\n",
        ]
    )

    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check, timeout
        assert command == [
            "squeue",
            "--noheader",
            "--steps",
            "--jobs=21835",
            f"--name={marker}",
            "--format=%i|%j|%N",
        ]
        return subprocess.CompletedProcess(command, 0, next(outputs), "")

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)
    provider = SlurmSchedulerProvider()

    identity = provider.find_connector_step(
        "21835",
        step_marker=marker,
        placement_host="compute-07",
    )

    assert identity is not None
    assert identity.scheduler_step_id == "21835.7"
    assert identity.source == "slurm-squeue-step-marker"
    with pytest.raises(RelayError, match="multiple active SLURM steps"):
        provider.find_connector_step(
            "21835",
            step_marker=marker,
            placement_host="compute-07",
        )


def test_poll_slurm_status_is_unknown_when_history_backends_have_no_record(
    monkeypatch: MonkeyPatch,
) -> None:
    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check, timeout
        if command[0] == "squeue":
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[0] == "sacct":
            return subprocess.CompletedProcess(command, 1, "", "accounting disabled")
        if command[0] == "scontrol":
            return subprocess.CompletedProcess(command, 1, "", "invalid job id")
        raise AssertionError(command)

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)

    status = SlurmSchedulerProvider().poll("99999")

    assert status.phase is SchedulerPhase.UNKNOWN
    assert status.record_found is None
    assert status.queue_position_note is not None
    assert "accounting disabled" in status.queue_position_note
    assert "invalid job id" in status.queue_position_note


def test_poll_slurm_status_explicitly_marks_confirmed_not_found(
    monkeypatch: MonkeyPatch,
) -> None:
    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check, timeout
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)

    status = SlurmSchedulerProvider().poll("99999")

    assert status.phase is SchedulerPhase.UNKNOWN
    assert status.record_found is False


def test_scheduler_provider_selection_is_explicit() -> None:
    assert isinstance(provider_for_scheduler("slurm"), SlurmSchedulerProvider)
    assert isinstance(provider_for_scheduler("external"), ExternalSchedulerProvider)
    assert isinstance(provider_for_scheduler("unmanaged"), ExternalSchedulerProvider)
    with pytest.raises(ConfigurationError, match="must be explicit"):
        provider_for_scheduler(None)
    with pytest.raises(ConfigurationError, match="unsupported scheduler provider"):
        provider_for_scheduler("batch-system")


def test_slurm_target_identity_is_owned_by_provider(monkeypatch: MonkeyPatch) -> None:
    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        assert command == ["scontrol", "show", "config"]
        assert (text, capture_output, check, timeout) == (True, True, False, 15.0)
        return subprocess.CompletedProcess(
            command,
            0,
            "ControlMachine = ares\nClusterName = linux\n",
            "",
        )

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)

    assert provider_for_scheduler("slurm").scheduler_cluster_name() == "linux"
    assert provider_for_scheduler("external").scheduler_cluster_name() is None


def test_slurm_reconciles_only_exact_active_job_name(monkeypatch: MonkeyPatch) -> None:
    marker = "clio-relay-0123456789abcdef"
    submitted_after = datetime.now(UTC) - timedelta(seconds=30)
    scheduler_user = "alice"
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        assert (text, capture_output, check, timeout) == (True, True, False, 15.0)
        if command[0] == "squeue":
            return subprocess.CompletedProcess(
                command,
                0,
                (
                    f"12345|{marker}|alice|"
                    f"{(submitted_after + timedelta(seconds=1)).isoformat()}\n"
                    f"99999|some-other-name|alice|"
                    f"{(submitted_after + timedelta(seconds=1)).isoformat()}\n"
                ),
                "",
            )
        assert command[0] == "sacct"
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)

    assert SlurmSchedulerProvider().find_job_ids_by_marker(
        marker,
        submitted_after=submitted_after,
        scheduler_user=scheduler_user,
    ) == ["12345"]
    assert [command[0] for command in commands] == ["squeue", "sacct"]


def test_slurm_reconciliation_falls_back_to_bounded_exact_history(
    monkeypatch: MonkeyPatch,
) -> None:
    marker = "clio-relay-fedcba9876543210"
    scheduler_user = "alice"
    submitted_after = datetime.now(UTC) - timedelta(minutes=2)
    submit_time = (submitted_after + timedelta(seconds=30)).astimezone().isoformat()
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check, timeout
        commands.append(command)
        if command[0] == "squeue":
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[0] == "sacct":
            return subprocess.CompletedProcess(
                command,
                0,
                (f"24680|{marker}|alice|{submit_time}\n97531|{marker}|mallory|{submit_time}\n"),
                "",
            )
        raise AssertionError(command)

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)

    assert SlurmSchedulerProvider().find_job_ids_by_marker(
        marker,
        submitted_after=submitted_after,
        scheduler_user=scheduler_user,
    ) == ["24680"]
    assert commands[1][:12] == [
        "sacct",
        "-n",
        "-P",
        "-X",
        "--name",
        marker,
        "--user",
        scheduler_user,
        "--starttime",
        (submitted_after - timedelta(seconds=5)).astimezone().strftime("%Y-%m-%dT%H:%M:%S"),
        "-o",
        "JobIDRaw,JobName,User,Submit",
    ]


def test_slurm_reconciliation_unions_active_and_historical_matches(
    monkeypatch: MonkeyPatch,
) -> None:
    marker = "clio-relay-aaaaaaaaaaaaaaaa"
    scheduler_user = "alice"
    submitted_after = datetime.now(UTC) - timedelta(seconds=20)
    rounded_submit = (submitted_after - timedelta(seconds=2)).replace(microsecond=0).isoformat()

    def fake_run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        if command[0] == "squeue":
            stdout = f"12345|{marker}|{scheduler_user}|{rounded_submit}\n"
        else:
            stdout = (
                f"12345|{marker}|{scheduler_user}|{rounded_submit}\n"
                f"67890|{marker}|{scheduler_user}|{rounded_submit}\n"
            )
        return subprocess.CompletedProcess(command, 0, stdout, "")

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)

    assert SlurmSchedulerProvider().find_job_ids_by_marker(
        marker,
        submitted_after=submitted_after,
        scheduler_user=scheduler_user,
    ) == ["12345", "67890"]


def test_slurm_reconciliation_fails_closed_without_accounting_history(
    monkeypatch: MonkeyPatch,
) -> None:
    marker = "clio-relay-bbbbbbbbbbbbbbbb"
    scheduler_user = "alice"
    submitted_after = datetime.now(UTC) - timedelta(seconds=20)

    def fake_run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        if command[0] == "squeue":
            return subprocess.CompletedProcess(command, 0, "", "")
        return subprocess.CompletedProcess(command, 1, "", "accounting disabled")

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)

    with pytest.raises(RelayError, match="accounting history is required"):
        SlurmSchedulerProvider().find_job_ids_by_marker(
            marker,
            submitted_after=submitted_after,
            scheduler_user=scheduler_user,
        )


def test_scheduler_provider_registry_supports_site_extensions() -> None:
    class SiteSchedulerProvider(ExternalSchedulerProvider):
        name = "site-batch"

    register_scheduler_provider("site-batch", SiteSchedulerProvider)
    assert provider_for_scheduler("site_batch").name == "site-batch"


def test_slurm_execution_adapter_is_separate_from_observation_provider(
    tmp_path: Path,
) -> None:
    provider = SlurmSchedulerProvider()
    command = scheduled_jarvis_command(
        "slurm",
        python_bin="python",
        pipeline_path=tmp_path / "pipeline.yaml",
    )

    assert not hasattr(provider, "pipeline_command")
    assert command[:4] == ["python", "-I", "-S", "-c"]
    source = command[4]
    assert "CLIO_RELAY_RUNTIME_METADATA_FILE" not in source
    assert "CLIO_RELAY_RUNTIME_METADATA_TOKEN" not in source
    assert "CLIO_RELAY_BROKER_CREDENTIAL_FD" in source
    assert "CLIO_RELAY_BROKER_READY_FD" in source
    assert "from clio_relay.jarvis_execution import run_native_jarvis_broker" in source
    assert "run_native_jarvis_broker(" in source
    assert "provider_for_scheduler" not in source
    assert "provider.poll(" not in source
    assert "SchedulerPhase" not in source
    compile(source, "<jarvis-slurm-adapter>", "exec")


def test_custom_observation_provider_does_not_authorize_jarvis_submission(
    tmp_path: Path,
) -> None:
    provider_name = f"site-{tmp_path.name}".lower().replace("_", "-")

    class SiteSchedulerProvider(ExternalSchedulerProvider):
        name = provider_name

    register_scheduler_provider(provider_name, SiteSchedulerProvider)

    assert provider_for_scheduler(provider_name).name == provider_name
    with pytest.raises(ConfigurationError, match="only through slurm"):
        scheduled_jarvis_command(
            provider_name,
            python_bin="python",
            pipeline_path=tmp_path / "pipeline.yaml",
        )


@pytest.mark.parametrize(
    ("scheduler_name", "replace"),
    [("site-batch", False), ("slurm", True)],
)
def test_scheduled_adapter_registration_cannot_bypass_or_replace_slurm_broker(
    tmp_path: Path,
    scheduler_name: str,
    replace: bool,
) -> None:
    def factory(python_bin: str, pipeline_path: Path) -> list[str]:
        return [python_bin, str(pipeline_path)]

    with pytest.raises(ConfigurationError, match="not supported"):
        register_scheduled_jarvis_command(
            scheduler_name,
            factory,
            consumes_runtime_credential=True,
            replace=replace,
        )


def test_slurm_execution_adapter_emits_owned_identity_before_terminal(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runtime_path = tmp_path / "runtime.jsonl"
    pipeline = _ScheduledNativePipeline(name="scheduled-test", job_id="24680")
    _install_yaml_pipeline_module(monkeypatch, pipeline)
    ready_read_fd = _install_runtime_credential_fd(
        monkeypatch,
        runtime_path=runtime_path,
        runtime_token="owned-token",
    )
    monkeypatch.setattr(sys, "argv", ["adapter", "yaml", str(tmp_path / "pipeline.yaml")])
    command = scheduled_jarvis_command(
        "slurm",
        python_bin="python",
        pipeline_path=tmp_path / "pipeline.yaml",
    )

    _execute_test_wrapper(monkeypatch, command=command, ready_read_fd=ready_read_fd)

    records = [loads(line) for line in runtime_path.read_text(encoding="utf-8").splitlines()]
    verified = [
        runtime_metadata_from_sidecar_record(
            record,
            expected_key="owned-token",
            expected_sequence=index,
        )
        for index, record in enumerate(records, start=1)
    ]
    assert pipeline.submit_calls == [(True, False, "jarvis_test_execution")]
    assert [metadata.scheduler_phase for metadata in verified] == [
        "submitted",
        "running",
        "completed",
    ]
    assert all(metadata.scheduler_job_id == "24680" for metadata in verified)
    assert records[0]["runtime_metadata"]["execution_handle"]["scheduler_native_id"] == "24680"
    assert records[-1]["runtime_metadata"]["execution_record"]["terminal"] is True


def test_slurm_execution_adapter_coalesces_more_than_4096_elapsed_only_polls(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runtime_path = tmp_path / "runtime.jsonl"
    pipeline = _ScheduledNativePipeline(
        name="long-running-test",
        job_id="86420",
        duplicate_running_reads=4_096,
    )
    _install_yaml_pipeline_module(monkeypatch, pipeline)
    ready_read_fd = _install_runtime_credential_fd(
        monkeypatch,
        runtime_path=runtime_path,
        runtime_token="long-running-token",
    )
    monkeypatch.setattr(sys, "argv", ["adapter", "yaml", str(tmp_path / "pipeline.yaml")])
    defaults = cast(dict[str, object], run_native_jarvis_broker.__kwdefaults__)
    original_sleep = defaults["sleep"]
    defaults["sleep"] = _fast_test_sleep
    command = scheduled_jarvis_command(
        "slurm",
        python_bin="python",
        pipeline_path=tmp_path / "pipeline.yaml",
    )

    try:
        _execute_test_wrapper(monkeypatch, command=command, ready_read_fd=ready_read_fd)
    finally:
        defaults["sleep"] = original_sleep

    records = [loads(line) for line in runtime_path.read_text(encoding="utf-8").splitlines()]
    assert pipeline.read_count >= 4_099
    assert len(records) == 3
    assert [record["runtime_metadata"]["execution_record"]["state"] for record in records] == [
        "submitted",
        "running",
        "completed",
    ]
    assert runtime_path.stat().st_size < 5 * 1024 * 1024


def test_named_direct_wrapper_emits_authenticated_mode_before_and_after_run(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runtime_path = tmp_path / "direct-runtime.jsonl"
    observations: list[tuple[str, bool] | str] = []

    class DirectPipeline:
        scheduler = None

        def __init__(self, name: str) -> None:
            self.name = name
            self.record: _NativeRecord | None = None

        def load(self) -> None:
            observations.append("loaded")

        def run(self, *, execution_id: str, wait: bool) -> _NativeHandle:
            observations.append((execution_id, wait))
            assert wait is True
            assert os.environ.get("CLIO_RELAY_RUNTIME_METADATA_FILE") is None
            assert os.environ.get("CLIO_RELAY_RUNTIME_METADATA_TOKEN") is None
            assert os.environ.get("CLIO_RELAY_RUNTIME_DIRECT_PROOF") is None
            assert os.environ.get("CLIO_RELAY_BROKER_CREDENTIAL_FD") is None
            assert os.environ.get("CLIO_RELAY_BROKER_READY_FD") is None
            self.record = _NativeRecord(
                execution_id=execution_id,
                pipeline_id=self.name,
                mode="direct",
                state="running",
                submitted=False,
                terminal=False,
            )
            time.sleep(0.3)
            self.record = replace(
                self.record,
                state="completed",
                terminal=True,
                return_code=0,
                updated_at="2026-07-12T00:00:02Z",
            )
            return self.record.handle

        def get_execution(self, execution_id: str) -> _NativeRecord:
            if self.record is None:
                raise FileNotFoundError(execution_id)
            return self.record

        def get_execution_progress(self, execution_id: str) -> _NativeProgress:
            return _NativeProgress(self.get_execution(execution_id))

    _install_named_pipeline_module(monkeypatch, DirectPipeline)
    ready_read_fd = _install_runtime_credential_fd(
        monkeypatch,
        runtime_path=runtime_path,
        runtime_token="direct-token",
        scheduler_expected="unknown",
    )
    monkeypatch.setattr(sys, "argv", ["adapter", "named", "direct-pipeline"])
    command = named_jarvis_command(python_bin="python", pipeline_name="direct-pipeline")

    _execute_test_wrapper(monkeypatch, command=command, ready_read_fd=ready_read_fd)

    records = [loads(line) for line in runtime_path.read_text(encoding="utf-8").splitlines()]
    assert observations == ["loaded", ("jarvis_test_execution", True)]
    assert records[0]["runtime_metadata"]["details"]["direct_execution_proof"] == (
        "test-direct-execution-proof"
    )
    native_records = [
        record for record in records if "execution_record" in record["runtime_metadata"]
    ]
    assert [
        record["runtime_metadata"]["execution_record"]["state"] for record in native_records
    ] == ["running", "completed"]
    for sequence, record in enumerate(records, start=1):
        runtime_metadata_from_sidecar_record(
            record,
            expected_key="direct-token",
            expected_sequence=sequence,
        )


def test_named_slurm_pipeline_on_external_worker_is_refused_before_submit(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runtime_path = tmp_path / "named-scheduler-refusal.jsonl"
    instances: list[ScheduledPipeline] = []

    class ScheduledPipeline:
        def __init__(self, name: str) -> None:
            self.name = name
            self.scheduler: dict[str, object] = {"name": "slurm", "job_name": "operator"}
            self.submit_calls: list[tuple[bool, bool, str]] = []
            instances.append(self)

        def load(self) -> None:
            return None

        def submit(self, *, submit: bool, wait: bool, execution_id: str) -> object:
            self.submit_calls.append((submit, wait, execution_id))
            raise AssertionError("scheduler submission must not be reached")

    _install_named_pipeline_module(monkeypatch, ScheduledPipeline)
    ready_read_fd = _install_runtime_credential_fd(
        monkeypatch,
        runtime_path=runtime_path,
        runtime_token="named-refusal-token",
        scheduler_expected="unknown",
        scheduler_provider="external",
    )
    monkeypatch.setattr(sys, "argv", ["adapter", "named", "scheduled-pipeline"])
    command = named_jarvis_command(python_bin="python", pipeline_name="scheduled-pipeline")

    with pytest.raises(RuntimeError, match="slurm != external"):
        _execute_test_wrapper(monkeypatch, command=command, ready_read_fd=ready_read_fd)

    assert len(instances) == 1
    assert instances[0].submit_calls == []
    assert instances[0].scheduler["job_name"] == "operator"
    records = [loads(line) for line in runtime_path.read_text(encoding="utf-8").splitlines()]
    assert len(records) == 1
    verified = runtime_metadata_from_sidecar_record(
        records[0],
        expected_key="named-refusal-token",
        expected_sequence=1,
    )
    assert verified.scheduler_phase == "launch_refused"
    assert verified.scheduler_job_id is None
    details = cast(dict[str, object], verified.details["details"])
    assert details["scheduler_submission_attempted"] is False
    assert details["scheduler_launch_refused"] is True


def test_named_direct_wrapper_records_failure_after_mode_observation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runtime_path = tmp_path / "direct-crash-runtime.jsonl"

    class CrashingPipeline:
        scheduler = None

        def __init__(self, name: str) -> None:
            self.name = name
            self.record: _NativeRecord | None = None

        def load(self) -> None:
            return None

        def run(self, *, execution_id: str, wait: bool) -> _NativeHandle:
            assert wait is True
            self.record = _NativeRecord(
                execution_id=execution_id,
                pipeline_id=self.name,
                mode="direct",
                state="failed",
                submitted=False,
                terminal=True,
                return_code=1,
                error="direct workload crashed",
            )
            raise RuntimeError("direct workload crashed")

        def get_execution(self, execution_id: str) -> _NativeRecord:
            if self.record is None:
                raise FileNotFoundError(execution_id)
            return self.record

        def get_execution_progress(self, execution_id: str) -> _NativeProgress:
            return _NativeProgress(self.get_execution(execution_id))

    _install_named_pipeline_module(monkeypatch, CrashingPipeline)
    ready_read_fd = _install_runtime_credential_fd(
        monkeypatch,
        runtime_path=runtime_path,
        runtime_token="direct-crash-token",
        scheduler_expected="unknown",
    )
    monkeypatch.setattr(sys, "argv", ["adapter", "named", "crashing-pipeline"])
    command = named_jarvis_command(python_bin="python", pipeline_name="crashing-pipeline")

    with pytest.raises(RuntimeError, match="direct workload crashed"):
        _execute_test_wrapper(monkeypatch, command=command, ready_read_fd=ready_read_fd)

    records = [loads(line) for line in runtime_path.read_text(encoding="utf-8").splitlines()]
    assert records[0]["runtime_metadata"]["details"]["execution_mode"] == "direct"
    assert records[-1]["runtime_metadata"]["execution_record"]["state"] == "failed"
    assert records[-1]["runtime_metadata"]["progress"]["terminal"] is True


def test_isolated_named_wrapper_loads_plain_module_roots_without_python_hooks(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    if os.name == "nt":
        runtime_path = tmp_path / "windows-gated-runtime.jsonl"
        ready_read_fd = _install_runtime_credential_fd(
            monkeypatch,
            runtime_path=runtime_path,
            runtime_token="windows-gated-token",
            scheduler_expected="unknown",
        )
        command = scheduled_jarvis_command(
            "slurm",
            python_bin=sys.executable,
            pipeline_path=tmp_path / "unused.yaml",
        )
        try:
            with pytest.raises(
                RuntimeError,
                match="secure JARVIS runtime signing requires Linux PR_SET_DUMPABLE",
            ):
                exec(
                    compile(command[4], "<jarvis-windows-platform-gate>", "exec"),
                    {"__name__": "__main__"},
                )
        finally:
            os.close(ready_read_fd)
        return

    environment = tmp_path / "adapter-venv"
    subprocess.run(
        [sys.executable, "-m", "venv", "--without-pip", str(environment)],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    python_bin = environment / "bin" / "python"
    python_version = f"python{sys.version_info.major}.{sys.version_info.minor}"
    site_packages = environment / "lib" / python_version / "site-packages"
    jarvis_core = site_packages / "jarvis_cd" / "core"
    relay_package = site_packages / "clio_relay"
    bounded_package = relay_package / "bounded_command"
    jarvis_core.mkdir(parents=True)
    bounded_package.mkdir(parents=True)
    for package_init in (
        site_packages / "jarvis_cd" / "__init__.py",
        jarvis_core / "__init__.py",
        relay_package / "__init__.py",
        bounded_package / "__init__.py",
    ):
        package_init.write_text("", encoding="utf-8")
    containment_filename = process_containment.__file__
    assert containment_filename is not None
    containment_source = Path(containment_filename)
    (relay_package / "process_containment.py").write_bytes(containment_source.read_bytes())
    jarvis_execution_filename = sys.modules[run_native_jarvis_broker.__module__].__file__
    assert isinstance(jarvis_execution_filename, str)
    (relay_package / "jarvis_execution.py").write_bytes(
        Path(jarvis_execution_filename).read_bytes()
    )
    errors_filename = sys.modules[ConfigurationError.__module__].__file__
    assert isinstance(errors_filename, str)
    (relay_package / "errors.py").write_bytes(Path(errors_filename).read_bytes())
    (relay_package / "runtime_metadata.py").write_text(
        "from types import SimpleNamespace\n"
        "class JarvisExecutionHandleDocument:\n"
        "    @classmethod\n"
        "    def model_validate(cls, document): return SimpleNamespace(**document)\n"
        "def native_execution_documents(envelope):\n"
        "    return SimpleNamespace(\n"
        "        execution_handle=SimpleNamespace(**envelope['execution_handle']),\n"
        "        execution_record=SimpleNamespace(**envelope['execution_record']),\n"
        "    )\n",
        encoding="utf-8",
    )
    progress_source = (
        Path(__file__).parents[1]
        / "jarvis-packages"
        / "clio_relay"
        / "clio_relay"
        / "bounded_command"
        / "progress.py"
    )
    (bounded_package / "progress.py").write_bytes(progress_source.read_bytes())
    (jarvis_core / "pipeline.py").write_text(
        "import json\n"
        "import os\n"
        "import subprocess\n"
        "import sys\n"
        "from pathlib import Path\n"
        "from clio_relay.bounded_command.progress import append_progress_record\n"
        "try:\n"
        "    initial_environ = Path('/proc/self/environ').read_bytes()\n"
        "except PermissionError:\n"
        "    initial_environ = b''\n"
        "    initial_environ_denied = True\n"
        "else:\n"
        "    initial_environ_denied = False\n"
        "initial_cmdline = Path('/proc/self/cmdline').read_bytes()\n"
        "progress_file = os.environ['CLIO_RELAY_PROGRESS_FILE'].encode()\n"
        "progress_token = os.environ['CLIO_RELAY_PROGRESS_TOKEN'].encode()\n"
        "Path(os.environ['WRAPPER_INITIAL_MARKER']).write_text(json.dumps({\n"
        "    'environ_denied': initial_environ_denied,\n"
        "    'environ_has_progress_file': progress_file in initial_environ,\n"
        "    'environ_has_progress_token': progress_token in initial_environ,\n"
        "    'cmdline_has_progress_file': progress_file in initial_cmdline,\n"
        "    'cmdline_has_progress_token': progress_token in initial_cmdline,\n"
        "}), encoding='utf-8')\n"
        "class Handle:\n"
        "    def __init__(self, record): self.record = record\n"
        "    def to_dict(self):\n"
        "        return {\n"
        "            'schema_version': 'jarvis.execution.handle.v1',\n"
        "            'execution_id': self.record.execution_id,\n"
        "            'pipeline_id': self.record.pipeline_id,\n"
        "            'mode': 'direct',\n"
        "            'scheduler_provider': None,\n"
        "            'scheduler_native_id': None,\n"
        "            'cluster': None,\n"
        "        }\n"
        "class Record:\n"
        "    def __init__(self, execution_id, pipeline_id, state, terminal, return_code):\n"
        "        self.execution_id = execution_id\n"
        "        self.pipeline_id = pipeline_id\n"
        "        self.mode = 'direct'\n"
        "        self.state = state\n"
        "        self.submitted = False\n"
        "        self.terminal = terminal\n"
        "        self.scheduler_native_id = None\n"
        "        self.error = None\n"
        "        self.return_code = return_code\n"
        "    @property\n"
        "    def handle(self): return Handle(self)\n"
        "    def to_dict(self):\n"
        "        return {\n"
        "            **self.handle.to_dict(),\n"
        "            'schema_version': 'jarvis.execution.record.v1',\n"
        "            'pipeline_name': self.pipeline_id,\n"
        "            'state': self.state,\n"
        "            'submitted': False,\n"
        "            'terminal': self.terminal,\n"
        "            'created_at': '2026-07-12T00:00:00Z',\n"
        "            'updated_at': '2026-07-12T00:00:01Z',\n"
        "            'return_code': self.return_code,\n"
        "            'error': self.error,\n"
        "            'metadata': {},\n"
        "        }\n"
        "class Progress:\n"
        "    def __init__(self, record): self.record = record\n"
        "    def to_dict(self):\n"
        "        return {\n"
        "            'schema_version': 'jarvis.execution.progress.v1',\n"
        "            'execution_id': self.record.execution_id,\n"
        "            'pipeline_id': self.record.pipeline_id,\n"
        "            'execution_state': self.record.state,\n"
        "            'terminal': self.record.terminal,\n"
        "            'packages': [],\n"
        "        }\n"
        "class Pipeline:\n"
        "    scheduler = None\n"
        "    def __init__(self, name): self.name, self.record = name, None\n"
        "    def load(self): return None\n"
        "    def get_execution(self, execution_id):\n"
        "        if self.record is None: raise FileNotFoundError(execution_id)\n"
        "        return self.record\n"
        "    def get_execution_progress(self, execution_id):\n"
        "        return Progress(self.get_execution(execution_id))\n"
        "    def run(self, *, execution_id, wait):\n"
        "        if wait is not True: raise RuntimeError('direct execution detached')\n"
        "        self.record = Record(execution_id, self.name, 'running', False, None)\n"
        "        append_progress_record({'label': 'live', 'current': 1, 'total': 1})\n"
        "        result = subprocess.run([sys.executable, '-I', '-S', "
        "os.environ['APP_PROBE'], os.environ['APP_MARKER']], check=False)\n"
        "        if result.returncode != 0: raise RuntimeError('application probe failed')\n"
        "        self.record = Record(execution_id, self.name, 'completed', True, 0)\n"
        "        return self.record.handle\n",
        encoding="utf-8",
    )
    (jarvis_core / "pipeline_test.py").write_text(
        "def load_yaml_auto(path): raise AssertionError(path)\n",
        encoding="utf-8",
    )
    site_marker = tmp_path / "sitecustomize-ran"
    user_marker = tmp_path / "usercustomize-ran"
    pth_marker = tmp_path / "pth-ran"
    (site_packages / "sitecustomize.py").write_text(
        f"from pathlib import Path\nPath({str(site_marker)!r}).write_text('ran')\n",
        encoding="utf-8",
    )
    (site_packages / "usercustomize.py").write_text(
        f"from pathlib import Path\nPath({str(user_marker)!r}).write_text('ran')\n",
        encoding="utf-8",
    )
    (site_packages / "hostile.pth").write_text(
        f"import pathlib; pathlib.Path({str(pth_marker)!r}).write_text('ran')\n",
        encoding="utf-8",
    )
    application_probe = tmp_path / "application-probe.py"
    application_probe.write_text(
        "import ctypes\n"
        "import json\n"
        "import os\n"
        "import sys\n"
        "from pathlib import Path\n"
        "parent_pid = os.getppid()\n"
        "limits = Path(f'/proc/{parent_pid}/limits').read_text(encoding='utf-8')\n"
        "core_line = next(line for line in limits.splitlines() "
        "if line.startswith('Max core file size'))\n"
        "try:\n"
        "    Path(f'/proc/{parent_pid}/environ').read_bytes()\n"
        "except PermissionError:\n"
        "    environ_denied = True\n"
        "else:\n"
        "    environ_denied = False\n"
        "try:\n"
        "    parent_mem = open(f'/proc/{parent_pid}/mem', 'rb', buffering=0)\n"
        "except OSError:\n"
        "    mem_denied = True\n"
        "else:\n"
        "    parent_mem.close()\n"
        "    mem_denied = False\n"
        "libc = ctypes.CDLL(None, use_errno=True)\n"
        "libc.ptrace.restype = ctypes.c_long\n"
        "ptrace_denied = libc.ptrace(16, parent_pid, None, None) == -1\n"
        "Path(sys.argv[1]).write_text(json.dumps({\n"
        "    'core_disabled': core_line.split()[-3:-1] == ['0', '0'],\n"
        "    'environ_denied': environ_denied,\n"
        "    'mem_denied': mem_denied,\n"
        "    'ptrace_denied': ptrace_denied,\n"
        "}), encoding='utf-8')\n",
        encoding="utf-8",
    )
    runtime_path = tmp_path / "isolated-runtime.jsonl"
    descriptor = os.open(runtime_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(descriptor)
    runtime_path.chmod(0o600)
    progress_path = tmp_path / "isolated-progress.jsonl"
    progress_descriptor = os.open(
        progress_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    os.close(progress_descriptor)
    progress_path.chmod(0o600)
    runtime_stat = runtime_path.stat()
    direct_proof = "isolated-direct-proof"
    runtime_key = "isolated-runtime-key"
    progress_key = "isolated-progress-key"
    runtime_environment = dict(os.environ)
    runtime_environment.update(
        {
            "PYTHONPATH": str(site_packages),
            "APP_MARKER": str(tmp_path / "application-ran"),
            "APP_PROBE": str(application_probe),
            "WRAPPER_INITIAL_MARKER": str(tmp_path / "wrapper-initial.json"),
            "CLIO_RELAY_PROGRESS_FILE": str(progress_path),
            "CLIO_RELAY_PROGRESS_TOKEN": progress_key,
            "CLIO_RELAY_RUNTIME_METADATA_FILE": str(runtime_path),
            "CLIO_RELAY_RUNTIME_METADATA_TOKEN": runtime_key,
            "CLIO_RELAY_RUNTIME_METADATA_ANCHOR": dumps(
                {
                    "device": int(runtime_stat.st_dev),
                    "inode": int(runtime_stat.st_ino),
                    "owner": int(runtime_stat.st_uid),
                    "link_count": int(runtime_stat.st_nlink),
                    "mode": runtime_stat.st_mode & 0o7777,
                }
            ),
            "CLIO_RELAY_RUNTIME_SUBMISSION_INTENT": dumps(
                {
                    "schema_version": "clio-relay.scheduler-submission-intent.v1",
                    "execution_id": "jarvis_isolated_execution",
                    "marker": "clio-relay-0123456789abcdef",
                    "created_at": datetime.now(UTC).isoformat(),
                    "scheduler_user": getpass.getuser(),
                    "scheduler_expected": "unknown",
                    "direct_proof_sha256": hashlib.sha256(direct_proof.encode("utf-8")).hexdigest(),
                }
            ),
            "CLIO_RELAY_RUNTIME_DIRECT_PROOF": direct_proof,
            "CLIO_RELAY_RUNTIME_SCHEDULER_PROVIDER": "external",
        }
    )
    launch_environment, credential = scheduled_runtime_credential_channel(runtime_environment)
    command = named_jarvis_command(
        python_bin=str(python_bin),
        pipeline_name="isolated-direct",
    )
    process = process_containment.spawn_owned_process(
        command,
        credential_payload=credential,
        env=process_containment.owner_environment(launch_environment),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        stdout, stderr = process.communicate(timeout=30)
        assert process.returncode == 0, f"{stdout}\n{stderr}"
    finally:
        if process.poll() is None:
            process_containment.terminate_owned_process(process)
        process_containment.release_owned_process(process)

    application_result = loads(Path(runtime_environment["APP_MARKER"]).read_text(encoding="utf-8"))
    assert application_result == {
        "core_disabled": True,
        "environ_denied": True,
        "mem_denied": True,
        "ptrace_denied": True,
    }
    wrapper_initial = loads(
        Path(runtime_environment["WRAPPER_INITIAL_MARKER"]).read_text(encoding="utf-8")
    )
    assert wrapper_initial == {
        "environ_denied": True,
        "environ_has_progress_file": False,
        "environ_has_progress_token": False,
        "cmdline_has_progress_file": False,
        "cmdline_has_progress_token": False,
    }
    assert not site_marker.exists()
    assert not user_marker.exists()
    assert not pth_marker.exists()
    records = [loads(line) for line in runtime_path.read_text(encoding="utf-8").splitlines()]
    assert records[0]["runtime_metadata"]["details"]["direct_execution_proof"] == direct_proof
    native_records = [
        record for record in records if "execution_record" in record["runtime_metadata"]
    ]
    assert native_records
    assert native_records[-1]["runtime_metadata"]["execution_record"]["state"] == "completed"
    for sequence, record in enumerate(records, start=1):
        runtime_metadata_from_sidecar_record(
            record,
            expected_key=runtime_key,
            expected_sequence=sequence,
        )
    progress_record = loads(progress_path.read_text(encoding="utf-8"))
    signed_progress = {
        "schema_version": "clio-relay.progress-sidecar-record.v1",
        "sequence": 1,
        "progress": {"label": "live", "current": 1, "total": 1},
    }
    expected_progress_hmac = hmac.new(
        progress_key.encode("utf-8"),
        dumps(
            signed_progress,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    assert progress_record == {
        **signed_progress,
        "progress_hmac": expected_progress_hmac,
    }


def test_slurm_broker_scrubs_sidecar_credentials_before_package_and_child_context(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runtime_path = tmp_path / "runtime.jsonl"
    observations: dict[str, tuple[str | None, ...]] = {}

    def observe_environment(label: str) -> None:
        observations[label] = tuple(
            os.environ.get(name)
            for name in (
                "CLIO_RELAY_PROGRESS_FILE",
                "CLIO_RELAY_PROGRESS_TOKEN",
                "CLIO_RELAY_RUNTIME_METADATA_FILE",
                "CLIO_RELAY_RUNTIME_METADATA_TOKEN",
                "CLIO_RELAY_BROKER_CREDENTIAL_FD",
                "CLIO_RELAY_BROKER_READY_FD",
            )
        )

    def observe_submit_and_child() -> None:
        observe_environment("submit")
        child = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import json,os; print(json.dumps(["
                    "os.environ.get('CLIO_RELAY_PROGRESS_FILE'),"
                    "os.environ.get('CLIO_RELAY_PROGRESS_TOKEN'),"
                    "os.environ.get('CLIO_RELAY_RUNTIME_METADATA_FILE'),"
                    "os.environ.get('CLIO_RELAY_RUNTIME_METADATA_TOKEN'),"
                    "os.environ.get('CLIO_RELAY_BROKER_CREDENTIAL_FD'),"
                    "os.environ.get('CLIO_RELAY_BROKER_READY_FD')]))"
                ),
            ],
            text=True,
            capture_output=True,
            check=True,
        )
        child_values = loads(child.stdout)
        assert isinstance(child_values, list)
        observations["child"] = tuple(cast(list[str | None], child_values))

    pipeline = _ScheduledNativePipeline(
        name="credential-boundary",
        job_id="13579",
        on_submit=observe_submit_and_child,
    )
    _install_yaml_pipeline_module(
        monkeypatch,
        pipeline,
        on_load=lambda: observe_environment("package_load"),
    )
    ready_read_fd = _install_runtime_credential_fd(
        monkeypatch,
        runtime_path=runtime_path,
        runtime_token="broker-only-token",
    )
    monkeypatch.setattr(sys, "argv", ["adapter", "yaml", str(tmp_path / "pipeline.yaml")])
    command = scheduled_jarvis_command(
        "slurm",
        python_bin="python",
        pipeline_path=tmp_path / "pipeline.yaml",
    )

    _execute_test_wrapper(monkeypatch, command=command, ready_read_fd=ready_read_fd)

    assert observations == {
        "package_load": (
            str(runtime_path.with_name(f"{runtime_path.name}.progress")),
            "test-progress-token",
            None,
            None,
            None,
            None,
        ),
        "submit": (None, None, None, None, None, None),
        "child": (None, None, None, None, None, None),
    }
    records = [loads(line) for line in runtime_path.read_text(encoding="utf-8").splitlines()]
    assert all("broker-only-token" not in dumps(record) for record in records)
    assert records[0]["runtime_metadata"]["execution_record"]["scheduler_native_id"] == "13579"


def test_slurm_execution_adapter_uses_bounded_secure_sidecar_append(
    tmp_path: Path,
) -> None:
    command = scheduled_jarvis_command(
        "slurm",
        python_bin="python",
        pipeline_path=tmp_path / "pipeline.yaml",
    )
    source = command[4]

    assert "CLIO_RELAY_RUNTIME_METADATA_FILE" not in source
    assert "CLIO_RELAY_RUNTIME_METADATA_TOKEN" not in source
    assert source.index("enforce_linux_secret_memory_gate()") < source.index(
        'os.environ.pop("CLIO_RELAY_BROKER_CREDENTIAL_FD"'
    )
    assert source.index('os.environ.pop("CLIO_RELAY_BROKER_CREDENTIAL_FD"') < source.index(
        "from jarvis_cd.core.pipeline_test import load_yaml_auto"
    )
    assert source.index('os.environ["CLIO_RELAY_PROGRESS_FILE"] = progress_file') < source.index(
        "from jarvis_cd.core.pipeline_test import load_yaml_auto"
    )
    assert source.index('os.environ["CLIO_RELAY_PROGRESS_TOKEN"] = progress_token') < source.index(
        "from jarvis_cd.core.pipeline_test import load_yaml_auto"
    )
    assert "os.set_inheritable(credential_fd, False)" in source
    assert "os.close(credential_fd)" in source
    assert "RUNTIME_METADATA_MAX_RECORD_BYTES = 5 * 1024 * 1024" in source
    assert "RUNTIME_METADATA_MAX_TOTAL_BYTES = 64 * 1024 * 1024" in source
    assert "os.O_APPEND" in source
    assert 'getattr(os, "O_NOFOLLOW", 0)' in source
    assert "os.set_inheritable(descriptor, False)" in source
    assert "opened = os.fstat(descriptor)" in source
    assert "stat.S_ISREG(opened.st_mode)" in source
    assert source.count("os.write(descriptor, payload)") == 1
    assert "written != len(payload)" in source
    assert 'open(runtime_file, "a"' not in source
    compile(source, "<jarvis-slurm-secure-sidecar>", "exec")


def test_jarvis_gate_failure_reads_no_credentials_and_imports_no_package(
    monkeypatch: MonkeyPatch,
) -> None:
    credential_read_fd, credential_write_fd = os.pipe()
    ready_read_fd, ready_write_fd = os.pipe()
    credential_payload = b"must-remain-unread"
    os.write(credential_write_fd, credential_payload)
    os.close(credential_write_fd)
    monkeypatch.setenv("CLIO_RELAY_BROKER_CREDENTIAL_FD", str(credential_read_fd))
    monkeypatch.setenv("CLIO_RELAY_BROKER_READY_FD", str(ready_write_fd))
    monkeypatch.setattr(sys, "argv", ["adapter", "yaml", "unused.yaml"])
    monkeypatch.delitem(sys.modules, "jarvis_cd.core.pipeline_test", raising=False)

    def reject_gate() -> None:
        raise RuntimeError("injected secret-memory gate failure")

    monkeypatch.setattr(
        process_containment,
        "enforce_linux_secret_memory_gate",
        reject_gate,
    )
    command = scheduled_jarvis_command(
        "slurm",
        python_bin="python",
        pipeline_path=Path("unused.yaml"),
    )
    try:
        with pytest.raises(RuntimeError, match="injected secret-memory gate failure"):
            exec(
                compile(command[4], "<jarvis-gate-failure>", "exec"),
                {"__name__": "__main__"},
            )
        assert os.read(credential_read_fd, len(credential_payload) + 1) == credential_payload
        assert os.environ["CLIO_RELAY_BROKER_CREDENTIAL_FD"] == str(credential_read_fd)
        assert os.environ["CLIO_RELAY_BROKER_READY_FD"] == str(ready_write_fd)
        assert "jarvis_cd.core.pipeline_test" not in sys.modules
    finally:
        os.close(credential_read_fd)
        os.close(ready_read_fd)
        os.close(ready_write_fd)


def test_slurm_validation_job_is_held_bounded_and_released_exactly(
    monkeypatch: MonkeyPatch,
) -> None:
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check, timeout
        commands.append(command)
        if command[0] == "sbatch":
            return subprocess.CompletedProcess(command, 0, "validation-789\n", "")
        if command[:2] == ["scontrol", "release"]:
            return subprocess.CompletedProcess(command, 0, "", "")
        raise AssertionError(command)

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)
    provider = SlurmSchedulerProvider()

    scheduler_job_id = provider.submit_held_validation_job(
        job_name="clio-relay-validation-test",
        run_seconds=30,
    )
    released = provider.release_validation_job(scheduler_job_id)

    assert scheduler_job_id == "validation-789"
    assert commands[0] == [
        "sbatch",
        "--parsable",
        "--hold",
        "--job-name",
        "clio-relay-validation-test",
        "--time",
        "00:05:00",
        "--wrap",
        "sleep 30",
    ]
    assert commands[1] == ["scontrol", "release", "validation-789"]
    assert released.returncode == 0


def test_cluster_scheduler_provider_defaults_to_external_and_normalizes_aliases() -> None:
    assert ClusterDefinition(name="local", ssh_host="localhost").scheduler_provider == "external"
    assert (
        ClusterDefinition(
            name="cluster", ssh_host="cluster", scheduler_provider="SLURM"
        ).scheduler_provider
        == "slurm"
    )
    assert (
        ClusterDefinition(
            name="custom", ssh_host="custom", scheduler_provider="none"
        ).scheduler_provider
        == "external"
    )
    assert (
        ClusterDefinition(
            name="site", ssh_host="site", scheduler_provider="site_batch"
        ).scheduler_provider
        == "site-batch"
    )


def test_slurm_provider_rejects_option_like_job_ids(monkeypatch: MonkeyPatch) -> None:
    def fail_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del command, text, capture_output, check, timeout
        pytest.fail("scheduler command must not run")

    monkeypatch.setattr(
        "clio_relay.scheduler_providers.subprocess.run",
        fail_run,
    )
    with pytest.raises(ConfigurationError, match="invalid scheduler job id"):
        SlurmSchedulerProvider().cancel("--all")


def test_slurm_provider_surfaces_command_failures(monkeypatch: MonkeyPatch) -> None:
    def fail_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check, timeout
        return subprocess.CompletedProcess(command, 1, "", "scheduler unavailable")

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fail_run)
    with pytest.raises(RelayError, match="scheduler unavailable"):
        SlurmSchedulerProvider().poll("100")


def test_slurm_provider_bounds_scheduler_commands(monkeypatch: MonkeyPatch) -> None:
    def timeout_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check
        raise subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", timeout_run)

    with pytest.raises(RelayError, match="timed out after 15s: squeue"):
        SlurmSchedulerProvider().poll("100")


@pytest.mark.parametrize(
    ("raw_state", "phase"),
    [
        ("REQUEUE_HOLD", SchedulerPhase.PENDING),
        ("SUSPENDED", SchedulerPhase.RUNNING),
        ("CANCELLED+", SchedulerPhase.CANCELED),
        ("PREEMPTED", SchedulerPhase.FAILED),
    ],
)
def test_slurm_status_normalizes_provider_states(
    monkeypatch: MonkeyPatch,
    raw_state: str,
    phase: SchedulerPhase,
) -> None:
    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check, timeout
        if command[:4] == ["squeue", "-h", "-j", "100"]:
            row = f"100|{raw_state}|reason|compute|normal|alice|1|4|4G|time|N/A|0:00|1:00\n"
            return subprocess.CompletedProcess(command, 0, row, "")
        if command[:4] == ["squeue", "-h", "-t", "PD"]:
            return subprocess.CompletedProcess(command, 0, "", "")
        raise AssertionError(command)

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)
    assert SlurmSchedulerProvider().poll("100").phase == phase


def test_job_status_includes_relay_queue_and_scheduler_metadata(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: queued\npkgs: []\n"),
            idempotency_key="status-job",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="jarvis.execution"))
    queue.update_task_metadata(
        task.task_id,
        {
            "scheduler_status": {
                "scheduler": "slurm",
                "scheduler_job_id": "100",
                "phase": "pending",
            }
        },
    )

    status = job_status(queue, job.job_id)

    assert status["relay_queue"] == {"state": "queued", "jobs_ahead": 0, "position": 1}
    scheduler = cast(list[dict[str, Any]], status["scheduler"])
    assert scheduler[0]["status"]["scheduler_job_id"] == "100"
