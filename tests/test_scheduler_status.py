from __future__ import annotations

import getpass
import hashlib
import hmac
import os
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from json import dumps, loads
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest
from pytest import MonkeyPatch

from clio_relay import process_containment
from clio_relay.adapters.worker.core_queue import ClioCoreQueue
from clio_relay.adapters.worker.jarvis_execution import (
    named_jarvis_command,
    register_scheduled_jarvis_command,
    run_native_jarvis_broker,
    scheduled_jarvis_command,
    scheduled_runtime_credential_channel,
)
from clio_relay.adapters.worker.scheduler import (
    ExternalSchedulerProvider,
    PsijSchedulerProvider,
    SlurmSchedulerProvider,
    provider_for_scheduler,
)
from clio_relay.adapters.worker.scheduler_status import relay_queue_status
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import ConfigurationError
from clio_relay.models import (
    JarvisRunSpec,
    JobKind,
    JobState,
    RelayJob,
    RelayTask,
    TransformRef,
)
from clio_relay.relay_ops import job_status
from clio_relay.runtime_metadata import runtime_metadata_from_sidecar_record


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


def _install_managed_jarvis_config(
    monkeypatch: MonkeyPatch,
    *,
    home: Path,
) -> None:
    """Install a fake JARVIS config backed by bootstrap-shaped relay state."""
    home.mkdir(parents=True, exist_ok=True)
    managed_repository, _registered_package = _managed_relay_repository(home)

    class _FixtureJarvisState:
        repos = {"repos": [str(managed_repository)]}

    class _FixtureJarvis:
        @classmethod
        def get_instance(cls) -> _FixtureJarvisState:
            return _FixtureJarvisState()

    config_module = ModuleType("jarvis_cd.core.config")
    config_module.Jarvis = _FixtureJarvis  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "jarvis_cd.core.config", config_module)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))


def _install_yaml_pipeline_module(
    monkeypatch: MonkeyPatch,
    pipeline: object,
    *,
    home: Path,
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
    _install_managed_jarvis_config(monkeypatch, home=home)


def _install_named_pipeline_module(
    monkeypatch: MonkeyPatch,
    pipeline_class: type[object],
    *,
    home: Path,
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
    _install_managed_jarvis_config(monkeypatch, home=home)


def _execute_test_wrapper(
    monkeypatch: MonkeyPatch,
    *,
    command: list[str],
    ready_read_fd: int,
) -> None:
    """Execute a credential-bound wrapper after replacing the platform gate in tests."""
    monkeypatch.setattr(process_containment, "enforce_linux_secret_memory_gate", lambda: None)
    relay_namespace = cast(Any, sys.modules["clio_relay"])
    original_namespace_paths = list(relay_namespace.__path__)
    source = command[4]
    if os.name == "nt":
        symbolic_link_guard = "if not stat.S_ISLNK(registered_link_before.st_mode):"
        if source.count(symbolic_link_guard) != 1:
            raise AssertionError("production symbolic-link guard changed")
        source = source.replace(
            symbolic_link_guard,
            (
                "if not (stat.S_ISLNK(registered_link_before.st_mode) "
                "or stat.S_ISDIR(registered_link_before.st_mode)):"
            ),
        )
    try:
        exec(compile(source, "<jarvis-native-adapter>", "exec"), {"__name__": "__main__"})
        assert os.read(ready_read_fd, 1) == b"1"
    finally:
        relay_namespace.__path__ = original_namespace_paths
        os.close(ready_read_fd)


def _fast_test_sleep(_seconds: float) -> None:
    """Yield the GIL without adding wall-clock delay to polling tests."""
    time.sleep(0)


@pytest.fixture
def managed_jarvis_home(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Return a short isolated home for bootstrap-shaped JARVIS state."""
    return tmp_path_factory.mktemp("jwh")


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


def test_relay_queue_status_reflects_live_state_despite_a_stale_snapshot(
    tmp_path: Path,
) -> None:
    """#290: a job that left QUEUED after the caller's snapshot was taken must
    not be reported as absent from the bounded active-job index.

    ``relay_queue_status`` is handed a job snapshot the caller may have read
    before this call; a legitimate concurrent transition can make that
    snapshot's state disagree with the live record by the time the active-job
    scan actually runs. Deterministic without any thread or timing: passing a
    snapshot that says QUEUED for a job the durable record now reports
    terminal reproduces the exact stale-state window without a race.
    """
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: stale\npkgs: []\n"),
            idempotency_key="stale-snapshot",
        )
    )
    stale = queue.get_job(job.job_id)
    assert stale.state == JobState.QUEUED
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)

    assert relay_queue_status(queue, stale) == {
        "state": "succeeded",
        "jobs_ahead": None,
        "position": None,
    }


def test_job_status_survives_a_state_transition_racing_the_queue_position_read(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """#290: force the submit-vs-observe interleaving deterministically.

    ``job_status`` reads a job snapshot (``queue.get_job``), then a second
    collaborator (``queue.get_transform_ref``), then calls
    ``relay_queue_status`` with that snapshot. A barrier seam on
    ``get_transform_ref`` -- a real collaborator that already sits between the
    two reads, not a fake hook -- lets a second thread complete
    ``update_job_state`` (transitioning the job out of QUEUED and out of the
    active-job index) before the observer's ``relay_queue_status`` call runs,
    reproducing the observed intermittent flake
    (``test_relay_observe_until_pattern_returns_terminal_without_match``)
    without depending on timing.
    """
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: observe-race\npkgs: []\n"),
            idempotency_key="observe-race",
        )
    )
    barrier = threading.Barrier(2)
    original_get_transform_ref = queue.get_transform_ref

    def blocking_get_transform_ref(job_id: str) -> TransformRef | None:
        result = original_get_transform_ref(job_id)
        barrier.wait(timeout=5)
        return result

    monkeypatch.setattr(queue, "get_transform_ref", blocking_get_transform_ref)

    results: list[dict[str, object]] = []
    errors: list[BaseException] = []

    def observe() -> None:
        try:
            results.append(job_status(queue, job.job_id))
        except BaseException as exc:  # pragma: no cover - asserted in the parent thread
            errors.append(exc)

    thread = threading.Thread(target=observe)
    thread.start()
    # This transition and de-index complete on the main thread strictly before
    # its own barrier.wait() call below; the observer thread cannot resume
    # past blocking_get_transform_ref (and therefore cannot call
    # relay_queue_status) until that same barrier releases.
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)
    barrier.wait(timeout=5)
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert errors == []
    assert len(results) == 1
    status = results[0]
    # ``status["job"]``/``status["terminal"]`` legitimately reflect the point-in-
    # time snapshot job_status read before the barrier (still QUEUED) -- that is
    # ordinary read-time semantics, not the defect. The defect under test is
    # specifically relay_queue_status raising QueueConflictError instead of
    # reasoning from the job's actual live state, which by now (after
    # update_job_state, before the barrier released) is terminal and no longer
    # in the active-job index.
    assert isinstance(status["job"], dict)
    assert status["job"]["state"] == "queued"
    assert status["relay_queue"] == {"state": "succeeded", "jobs_ahead": None, "position": None}


def test_scheduler_provider_selection_is_explicit() -> None:
    assert isinstance(provider_for_scheduler("slurm"), SlurmSchedulerProvider)
    assert isinstance(provider_for_scheduler("external"), ExternalSchedulerProvider)
    assert isinstance(provider_for_scheduler("unmanaged"), ExternalSchedulerProvider)
    assert isinstance(provider_for_scheduler("pbs"), PsijSchedulerProvider)
    with pytest.raises(ConfigurationError, match="must be explicit"):
        provider_for_scheduler(None)
    with pytest.raises(ConfigurationError, match="unsupported scheduler provider"):
        provider_for_scheduler("batch-system")


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
    assert (
        "from clio_relay.adapters.worker.jarvis_execution import run_native_jarvis_broker" in source
    )
    assert "run_native_jarvis_broker(" in source
    assert "provider_for_scheduler" not in source
    assert "provider.poll(" not in source
    assert "SchedulerPhase" not in source
    compile(source, "<jarvis-slurm-adapter>", "exec")


def test_non_slurm_observation_provider_does_not_authorize_jarvis_submission(
    tmp_path: Path,
) -> None:
    assert provider_for_scheduler("pbs").name == "pbs"
    with pytest.raises(ConfigurationError, match="only through slurm"):
        scheduled_jarvis_command(
            "pbs",
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
    managed_jarvis_home: Path,
) -> None:
    runtime_path = tmp_path / "runtime.jsonl"
    pipeline = _ScheduledNativePipeline(name="scheduled-test", job_id="24680")
    _install_yaml_pipeline_module(monkeypatch, pipeline, home=managed_jarvis_home)
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
    managed_jarvis_home: Path,
) -> None:
    runtime_path = tmp_path / "runtime.jsonl"
    pipeline = _ScheduledNativePipeline(
        name="long-running-test",
        job_id="86420",
        duplicate_running_reads=4_096,
    )
    _install_yaml_pipeline_module(monkeypatch, pipeline, home=managed_jarvis_home)
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
    managed_jarvis_home: Path,
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

    _install_named_pipeline_module(monkeypatch, DirectPipeline, home=managed_jarvis_home)
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
    managed_jarvis_home: Path,
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

    _install_named_pipeline_module(monkeypatch, ScheduledPipeline, home=managed_jarvis_home)
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
    managed_jarvis_home: Path,
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

    _install_named_pipeline_module(monkeypatch, CrashingPipeline, home=managed_jarvis_home)
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
    managed_jarvis_home: Path,
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
    home = managed_jarvis_home
    managed_repository, _registered_package = _managed_relay_repository(home)
    (jarvis_core / "config.py").write_text(
        "class _Jarvis:\n"
        f"    repos = {{'repos': [{str(managed_repository)!r}]}}\n"
        "class Jarvis:\n"
        "    @classmethod\n"
        "    def get_instance(cls):\n"
        "        return _Jarvis()\n",
        encoding="utf-8",
    )
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
            "HOME": str(home),
            "USERPROFILE": str(home),
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
    managed_jarvis_home: Path,
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
        home=managed_jarvis_home,
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


def _directory_link(link: Path, target: Path) -> None:
    """Create a test directory link without requiring Windows symlink privilege."""
    if os.name != "nt":
        link.symlink_to(target, target_is_directory=True)
        return
    completed = subprocess.run(
        [
            os.environ.get("COMSPEC", "cmd.exe"),
            "/d",
            "/c",
            "mklink",
            "/J",
            str(link),
            str(target.resolve(strict=True)),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    if completed.returncode != 0:
        raise OSError(
            "could not create Windows test directory junction: "
            f"{completed.stderr.strip() or completed.stdout.strip()}"
        )


def _managed_relay_repository(home: Path) -> tuple[Path, Path]:
    """Create the exact stable-to-generation binding produced by bootstrap."""
    fingerprint = "a" * 64
    generation = home / ".local/share/clio-relay/generations" / fingerprint
    registered_repository = generation / "source/jarvis-packages/clio_relay"
    registered_package = registered_repository / "clio_relay"
    registered_bounded_command = registered_package / "bounded_command"
    registered_bounded_command.mkdir(parents=True)
    (registered_package / "__init__.py").write_text("", encoding="utf-8")
    (registered_bounded_command / "__init__.py").write_text("", encoding="utf-8")
    (registered_bounded_command / "runner.py").write_text(
        "IDENTITY = 'registered-package'\n",
        encoding="utf-8",
    )
    share = home / ".local/share/clio-relay"
    _directory_link(share / "current", generation)
    managed = share / "clio_relay"
    _directory_link(
        managed,
        share / "current/source/jarvis-packages/clio_relay",
    )
    return managed, registered_package


def _relay_repository_binding_source(tmp_path: Path) -> str:
    source = scheduled_jarvis_command(
        "slurm",
        python_bin=sys.executable,
        pipeline_path=tmp_path / "pipeline.yaml",
    )[4]
    start_marker = "# BEGIN REGISTERED RELAY PACKAGE REPOSITORY BINDING"
    end_marker = "# END REGISTERED RELAY PACKAGE REPOSITORY BINDING"
    binding_start = source.index(start_marker)
    binding_end = source.index(end_marker) + len(end_marker)
    binding = source[binding_start:binding_end]
    if os.name == "nt":
        # Bootstrap uses a symbolic link in production, and this assertion
        # locks that guard in the generated source. Windows CI may not have
        # symbolic-link privilege, so the fixture supplies a directory
        # junction and substitutes only its link-type predicate. The exact
        # resolve, active-generation, shape, and stability checks still run.
        symbolic_link_guard = "if not stat.S_ISLNK(registered_link_before.st_mode):"
        if binding.count(symbolic_link_guard) != 1:
            raise AssertionError("production symbolic-link guard changed")
        binding = binding.replace(
            symbolic_link_guard,
            (
                "if not (stat.S_ISLNK(registered_link_before.st_mode) "
                "or stat.S_ISDIR(registered_link_before.st_mode)):"
            ),
        )
    # The extracted boundary normally executes inside the complete wrapper,
    # whose prelude has already imported these standard-library names. Keep
    # the focused subprocess probe faithful to those production globals.
    return "import os\nimport stat\nfrom pathlib import Path\n" + binding


def test_wrapper_resolves_only_bootstrap_managed_relay_repository_after_core_import(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Bind the exact managed generation and preserve unrelated JARVIS repositories."""
    # A factory-owned short base keeps the exact 64-character generation
    # fingerprint below legacy Windows MAX_PATH while retaining pytest cleanup.
    tmp_path = tmp_path_factory.mktemp("jrw")
    installed_root = tmp_path / "installed"
    installed_package = installed_root / "clio_relay"
    installed_package.mkdir(parents=True)
    (installed_package / "__init__.py").write_text("", encoding="utf-8")
    (installed_package / "process_containment.py").write_text(
        "IDENTITY = 'trusted-core'\n",
        encoding="utf-8",
    )
    home = tmp_path / "home"
    home.mkdir()
    managed_repository, registered_package = _managed_relay_repository(home)
    registered_bounded_command = registered_package / "bounded_command"
    same_basename_operator_repository = tmp_path / "operator/clio_relay"
    same_basename_operator_repository.mkdir(parents=True)
    unrelated_repository = tmp_path / "operator/science_packages"
    unrelated_repository.mkdir(parents=True)
    repositories = [
        str(same_basename_operator_repository),
        str(managed_repository),
        str(unrelated_repository),
    ]

    jarvis_root = tmp_path / "jarvis"
    jarvis_core = jarvis_root / "jarvis_cd" / "core"
    jarvis_core.mkdir(parents=True)
    (jarvis_root / "jarvis_cd" / "__init__.py").write_text("", encoding="utf-8")
    (jarvis_core / "__init__.py").write_text("", encoding="utf-8")
    (jarvis_core / "config.py").write_text(
        "class _Jarvis:\n"
        f"    repos = {{'repos': {repositories!r}}}\n"
        "class Jarvis:\n"
        "    @classmethod\n"
        "    def get_instance(cls):\n"
        "        return _Jarvis()\n",
        encoding="utf-8",
    )

    probe = (
        "import json,sys\n"
        "sys.path[:0] = [sys.argv[1], sys.argv[2]]\n"
        "import clio_relay\n"
        "import clio_relay.process_containment as containment\n"
        "exec(sys.argv[3], globals(), globals())\n"
        "import clio_relay.bounded_command.runner as runner\n"
        "print(json.dumps({\n"
        "    'containment': containment.__file__,\n"
        "    'namespace_paths': list(clio_relay.__path__),\n"
        "    'runner': runner.__file__,\n"
        "    'identity': runner.IDENTITY,\n"
        "    'repositories': registered_repositories,\n"
        "}))\n"
    )
    environment = {**os.environ, "HOME": str(home), "USERPROFILE": str(home)}
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            "-c",
            probe,
            str(installed_root),
            str(jarvis_root),
            _relay_repository_binding_source(tmp_path),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
        env=environment,
    )
    observed = loads(completed.stdout)

    assert observed["containment"] == str(installed_package / "process_containment.py")
    assert observed["namespace_paths"] == [
        str(registered_package.resolve()),
        str(installed_package),
    ]
    assert observed["runner"] == str(registered_bounded_command / "runner.py")
    assert observed["identity"] == "registered-package"
    assert observed["repositories"] == repositories


def test_wrapper_rejects_basename_only_relay_repository(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """An operator repository named clio_relay is not relay bootstrap authority."""
    tmp_path = tmp_path_factory.mktemp("jrb")
    installed_root = tmp_path / "installed"
    installed_package = installed_root / "clio_relay"
    installed_package.mkdir(parents=True)
    (installed_package / "__init__.py").write_text("", encoding="utf-8")

    home = tmp_path / "home"
    home.mkdir()
    operator_repository = tmp_path / "operator/clio_relay"
    operator_repository.mkdir(parents=True)

    jarvis_root = tmp_path / "jarvis"
    jarvis_core = jarvis_root / "jarvis_cd" / "core"
    jarvis_core.mkdir(parents=True)
    (jarvis_root / "jarvis_cd" / "__init__.py").write_text("", encoding="utf-8")
    (jarvis_core / "__init__.py").write_text("", encoding="utf-8")
    (jarvis_core / "config.py").write_text(
        "class _Jarvis:\n"
        f"    repos = {{'repos': [{str(operator_repository)!r}]}}\n"
        "class Jarvis:\n"
        "    @classmethod\n"
        "    def get_instance(cls):\n"
        "        return _Jarvis()\n",
        encoding="utf-8",
    )

    probe = (
        "import sys\n"
        "sys.path[:0] = [sys.argv[1], sys.argv[2]]\n"
        "import clio_relay\n"
        "exec(sys.argv[3], globals(), globals())\n"
    )
    environment = {**os.environ, "HOME": str(home), "USERPROFILE": str(home)}
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            "-c",
            probe,
            str(installed_root),
            str(jarvis_root),
            _relay_repository_binding_source(tmp_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
        env=environment,
    )

    assert completed.returncode != 0
    assert "exactly one exact bootstrap-managed clio_relay repository" in completed.stderr


def test_wrapper_rejects_ambiguous_managed_relay_repository_entries(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Duplicate or aliased stable entries cannot select package authority."""
    tmp_path = tmp_path_factory.mktemp("jra")
    installed_root = tmp_path / "installed"
    installed_package = installed_root / "clio_relay"
    installed_package.mkdir(parents=True)
    (installed_package / "__init__.py").write_text("", encoding="utf-8")
    home = tmp_path / "home"
    home.mkdir()
    managed_repository, _registered_package = _managed_relay_repository(home)

    jarvis_root = tmp_path / "jarvis"
    jarvis_core = jarvis_root / "jarvis_cd" / "core"
    jarvis_core.mkdir(parents=True)
    (jarvis_root / "jarvis_cd" / "__init__.py").write_text("", encoding="utf-8")
    (jarvis_core / "__init__.py").write_text("", encoding="utf-8")
    (jarvis_core / "config.py").write_text(
        "class _Jarvis:\n"
        f"    repos = {{'repos': [{str(managed_repository)!r}, "
        f"{str(managed_repository.parent / '.' / managed_repository.name)!r}]}}\n"
        "class Jarvis:\n"
        "    @classmethod\n"
        "    def get_instance(cls):\n"
        "        return _Jarvis()\n",
        encoding="utf-8",
    )
    probe = (
        "import sys\n"
        "sys.path[:0] = [sys.argv[1], sys.argv[2]]\n"
        "import clio_relay\n"
        "exec(sys.argv[3], globals(), globals())\n"
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            "-c",
            probe,
            str(installed_root),
            str(jarvis_root),
            _relay_repository_binding_source(tmp_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
        env={**os.environ, "HOME": str(home), "USERPROFILE": str(home)},
    )

    assert completed.returncode != 0
    assert "exactly one exact bootstrap-managed clio_relay repository" in completed.stderr


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
