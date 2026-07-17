from __future__ import annotations

import hashlib
import hmac
import json
import os
import signal
import stat
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from threading import Barrier, Lock
from typing import Any, cast

import pytest
from pytest import MonkeyPatch

from clio_relay import endpoint as endpoint_module
from clio_relay import process_containment
from clio_relay.config import RelaySettings
from clio_relay.core_queue import DEFAULT_EXACT_RECORD_LIMIT, ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.filesystem_paths import internal_filesystem_path, logical_filesystem_path
from clio_relay.jarvis_execution import run_native_jarvis_broker
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import (
    Cursor,
    EndpointRegistration,
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    Lease,
    McpCallSpec,
    RelayEvent,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
    SchedulerPhase,
    SchedulerStatus,
    utc_now,
)
from clio_relay.progress_adapters import package_progress_adapter_from_pipeline
from clio_relay.queue_management import cleanup_stale_jobs, diagnose_job
from clio_relay.relay_ops import cancel_job
from clio_relay.remote_mcp import remote_mcp_server_artifact_digest
from clio_relay.runtime_metadata import runtime_sidecar_record
from tests.jarvis_mcp_fakes import verified_jarvis_server_artifact
from tests.plugin_fakes import SiteSimulationProgressAdapter, install_site_progress_plugin


def _write_anchored_sidecar(path: Path, payload: str = "owned\n") -> dict[str, int]:
    """Create a relay-owned sidecar and return its durable anchor metadata."""
    anchor = cast(Any, endpoint_module)._precreate_runtime_sidecar(path)
    path.write_text(payload, encoding="utf-8")
    metadata = cast(dict[str, int], anchor.as_metadata())
    if anchor.descriptor is not None:
        os.close(anchor.descriptor)
    return metadata


def test_runtime_sidecar_supports_long_operator_configured_spool_root(
    tmp_path: Path,
) -> None:
    """Endpoint-owned sidecars work past MAX_PATH without leaking the prefix."""
    spool = tmp_path.joinpath(*(f"operator-sidecar-{index}-{'x' * 72}" for index in range(3)))
    internal_filesystem_path(spool, force_extended=True).mkdir(parents=True)
    sidecar = spool / ".runtime-metadata-owned.jsonl"
    private = cast(Any, endpoint_module)
    anchor = private._precreate_runtime_sidecar(sidecar)
    try:
        internal_filesystem_path(sidecar).write_text(
            "owned\n",
            encoding="utf-8",
            newline="\n",
        )
        summary = private._file_summary(sidecar)
    finally:
        if anchor.descriptor is not None:
            os.close(anchor.descriptor)

    assert summary["path"] == str(sidecar)
    assert summary["exists"] is True
    assert summary["size_bytes"] == len(b"owned\n")
    assert "\\\\?\\" not in str(summary)


def test_native_jarvis_cwd_rejects_long_and_unc_windows_paths(tmp_path: Path) -> None:
    """Native JARVIS cannot silently launch from a different Windows directory."""
    private = cast(Any, endpoint_module)
    assert private._validated_native_subprocess_cwd(tmp_path) == tmp_path
    long_cwd = tmp_path.joinpath(*(f"native-{index}-{'x' * 72}" for index in range(3)))
    unc_cwd = Path(r"\\storage.example\relay\checkout")
    if os.name != "nt":
        assert private._validated_native_subprocess_cwd(long_cwd) == long_cwd
        assert private._validated_native_subprocess_cwd(unc_cwd) == unc_cwd
        return

    with pytest.raises(ConfigurationError, match="path bound"):
        private._validated_native_subprocess_cwd(long_cwd)
    with pytest.raises(ConfigurationError, match="must not use UNC"):
        private._validated_native_subprocess_cwd(unc_cwd)


def test_worker_failure_persists_only_logical_windows_paths(tmp_path: Path) -> None:
    """Worker failure state and events must not persist private path namespaces."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="configured-target",
        queue=queue,
    )
    job = queue.submit_job(
        RelayJob(
            cluster="configured-target",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["workload"]),
            idempotency_key="logical-worker-error",
        )
    )
    logical_failure_path = tmp_path / "spool" / job.job_id / "stdout.log"
    internal_failure_path = internal_filesystem_path(
        logical_failure_path,
        force_extended=True,
    )

    cast(Any, worker)._record_unhandled_job_failure(
        job,
        RuntimeError(f"failed to read {internal_failure_path}"),
    )

    failed = queue.get_job(job.job_id)
    assert failed.last_error is not None
    assert "\\\\?\\" not in failed.last_error
    assert str(logical_failure_path) in failed.last_error


def test_replacement_worker_reconciles_owned_process_before_cancel_acknowledgment(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=[sys.executable, "-c", "import time;time.sleep(60)"]),
            idempotency_key="restart-cancellation-reconciliation",
        )
    )
    old_worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
    )
    old_endpoint = old_worker.register()
    lease = queue.acquire_next_job(
        old_endpoint.endpoint_id,
        cluster="ares",
        ttl_seconds=-1,
    )
    assert lease is not None
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
    progress_name = ".progress-restart.jsonl"
    runtime_name = ".runtime-restart.jsonl"
    progress_anchor = _write_anchored_sidecar(restart_spool / progress_name, "")
    runtime_anchor = _write_anchored_sidecar(restart_spool / runtime_name, "")
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
    try:
        old_worker._append_execution_start(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            job,
            task,
            process.pid,
        )
        canceled = cancel_job(queue, job.job_id)
        assert canceled.state is JobState.RUNNING
        assert isinstance(canceled.metadata.get("cancellation_request"), dict)

        replacement = EndpointWorker(
            role=EndpointRole.WORKER,
            settings=settings,
            cluster="ares",
            queue=queue,
        )
        result = replacement.run_once()

        assert result is None
        final_job = queue.get_job(job.job_id)
        assert final_job.state is JobState.CANCELED
        assert queue.get_task(task.task_id).state is JobState.CANCELED
        deadline = time.monotonic() + 5
        while process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.05)
        assert process.poll() is not None
        events, _ = queue.read_event_page(job.job_id, limit=100)
        assert "execution.restart_reconciled" in {event.event_type for event in events}
        request = cast(dict[str, object], final_job.metadata["cancellation_request"])
        assert request["cleanup_acknowledged"] is True
        _assert_no_execution_sidecars(settings, job.job_id)
    finally:
        if process.poll() is None:
            process_containment.terminate_owned_process(process)
        process_containment.release_owned_process(process)


def test_hard_crashed_worker_is_reconciled_before_cancellation_acknowledgment(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "hard-crash.json"
    repository = Path(__file__).parents[1]
    helper = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.hard_crash_worker_fixture",
            str(tmp_path),
            str(marker),
        ],
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert helper.returncode == 0, helper.stderr
    identity = cast(dict[str, object], json.loads(marker.read_text(encoding="utf-8")))
    job_id = cast(str, identity["job_id"])
    task_id = cast(str, identity["task_id"])
    process_id = cast(int, identity["process_id"])
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    task = queue.get_task(task_id)
    ownership = cast(dict[str, object], task.metadata["execution_ownership"])
    expected_start = cast(str, ownership["process_start_identity"])
    try:
        replacement = EndpointWorker(
            role=EndpointRole.WORKER,
            settings=settings,
            cluster="ares",
            queue=queue,
        )
        assert replacement.run_once() is None
        result = queue.get_job(job_id)

        assert result.job_id == job_id
        assert result.state is JobState.CANCELED
        assert queue.get_task(task_id).state is JobState.CANCELED
        assert process_containment.process_start_identity(process_id) is None
        request = cast(dict[str, object], result.metadata["cancellation_request"])
        assert request["cleanup_acknowledged"] is True
        _assert_no_execution_sidecars(settings, job_id)
    finally:
        observed_start = process_containment.process_start_identity(process_id)
        if observed_start == expected_start:
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/PID", str(process_id), "/T", "/F"],
                    check=False,
                    capture_output=True,
                    timeout=10,
                )
            else:
                os.killpg(process_id, signal.SIGKILL)


def test_hard_crash_before_broker_release_never_starts_workload_and_reconciles(
    tmp_path: Path,
) -> None:
    crash_marker = tmp_path / "pre-release-crash.json"
    workload_marker = tmp_path / "workload-started.txt"
    repository = Path(__file__).parents[1]
    helper = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.hard_crash_pre_release_fixture",
            str(tmp_path),
            str(crash_marker),
            str(workload_marker),
        ],
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert helper.returncode == 0, helper.stderr
    assert workload_marker.exists() is False
    identity = cast(dict[str, object], json.loads(crash_marker.read_text(encoding="utf-8")))
    job_id = cast(str, identity["job_id"])
    task_id = cast(str, identity["task_id"])
    process_id = cast(int, identity["process_id"])
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    replacement = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
    )
    assert replacement.run_once() is None
    result = queue.get_job(job_id)

    assert result.job_id == job_id
    assert result.state is JobState.CANCELED
    task = queue.get_task(task_id)
    assert task.state is JobState.CANCELED
    assert task.metadata["execution_sidecars_removed"] is True
    assert workload_marker.exists() is False
    assert process_containment.process_start_identity(process_id) is None
    sidecars = cast(dict[str, object], identity["execution_sidecars"])
    for role in ("progress", "runtime"):
        assert not (settings.spool_dir / job_id / cast(str, sidecars[role])).exists()


def test_hard_crashed_non_canceled_attempt_blocks_requeue_until_cleanup_retry(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    crash_marker = tmp_path / "non-cancel-crash.json"
    workload_marker = tmp_path / "workload-started.txt"
    repository = Path(__file__).parents[1]
    helper = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.hard_crash_pre_release_fixture",
            str(tmp_path),
            str(crash_marker),
            str(workload_marker),
            "no-cancel",
        ],
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert helper.returncode == 0, helper.stderr
    assert workload_marker.exists() is False
    identity = cast(dict[str, object], json.loads(crash_marker.read_text(encoding="utf-8")))
    job_id = cast(str, identity["job_id"])
    task_id = cast(str, identity["task_id"])
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    recovered = queue.recover_stale_jobs(cluster="ares")
    assert [job.job_id for job in recovered] == [job_id]
    assert queue.get_job(job_id).state is JobState.QUEUED

    pending, truncated = queue.scan_execution_cleanup(cluster="ares", limit=10)
    assert truncated is False
    assert [task.task_id for task in pending] == [task_id]
    endpoint = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
    )
    registration = endpoint.register()
    assert queue.acquire_job(job_id, registration.endpoint_id, cluster="ares") is None

    original_cleanup = cast(Any, endpoint)._remove_recorded_execution_sidecars

    def fail_cleanup(job: RelayJob, task: RelayTask) -> None:
        del job, task
        raise ConfigurationError("injected restart sidecar cleanup fault")

    monkeypatch.setattr(endpoint, "_remove_recorded_execution_sidecars", fail_cleanup)
    with pytest.raises(RelayError, match="injected restart sidecar cleanup fault"):
        cast(Any, endpoint)._reconcile_pending_execution_cleanup()
    pending, _ = queue.scan_execution_cleanup(cluster="ares", limit=10)
    assert [task.task_id for task in pending] == [task_id]
    assert queue.acquire_job(job_id, registration.endpoint_id, cluster="ares") is None

    monkeypatch.setattr(endpoint, "_remove_recorded_execution_sidecars", original_cleanup)
    cast(Any, endpoint)._reconcile_pending_execution_cleanup()

    pending, truncated = queue.scan_execution_cleanup(cluster="ares", limit=10)
    assert pending == []
    assert truncated is False
    assert queue.get_task(task_id).state is JobState.FAILED
    sidecars = cast(dict[str, object], identity["execution_sidecars"])
    for role in ("progress", "runtime"):
        assert not (settings.spool_dir / job_id / cast(str, sidecars[role])).exists()
    retry_lease = queue.acquire_job(job_id, registration.endpoint_id, cluster="ares")
    assert retry_lease is not None
    events, _ = queue.read_event_page(job_id, limit=100)
    assert "execution.restart_reconciled" in {event.event_type for event in events}


def test_pending_execution_cleanup_processes_truncated_batches_automatically(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["controlled"]),
            idempotency_key="bounded-restart-cleanup",
        )
    )
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    task_ids: list[str] = []
    for index in range(3):
        task = queue.append_task(
            RelayTask(
                job_id=job.job_id,
                name=f"jarvis.execution.{index}",
                metadata={"cluster": "ares"},
            )
        )
        queue.update_task_state(task.task_id, JobState.RUNNING)
        progress_name = f".progress-batch-{index}.jsonl"
        runtime_name = f".runtime-batch-{index}.jsonl"
        progress_anchor = _write_anchored_sidecar(spool / progress_name)
        runtime_anchor = _write_anchored_sidecar(spool / runtime_name)
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
        task_ids.append(task.task_id)

    monkeypatch.setattr(endpoint_module, "EXECUTION_CLEANUP_SCAN_LIMIT", 2)
    endpoint = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
    )
    endpoint.register()
    cast(Any, endpoint)._reconcile_pending_execution_cleanup()

    pending, has_more = queue.scan_execution_cleanup(cluster="ares", limit=10)
    assert len(pending) == 1
    assert has_more is False
    assert endpoint.endpoint is not None
    scan = cast(dict[str, object], endpoint.endpoint.metadata["execution_cleanup_scan"])
    assert scan["batch_limit"] == 2
    assert scan["batch_size"] == 2
    assert scan["completed"] == 2
    assert scan["has_more"] is True

    cast(Any, endpoint)._reconcile_pending_execution_cleanup()
    pending, has_more = queue.scan_execution_cleanup(cluster="ares", limit=10)
    assert pending == []
    assert has_more is False
    assert {queue.get_task(task_id).state for task_id in task_ids} == {JobState.FAILED}


@pytest.mark.parametrize("crash_boundary", ["after_quarantine", "inside_ack"])
def test_execution_cleanup_crash_boundaries_converge_from_exact_quarantine(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    crash_boundary: str,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["controlled"]),
            idempotency_key=f"quarantine-crash-{crash_boundary}",
        )
    )
    task = queue.append_task(
        RelayTask(
            job_id=job.job_id,
            name="jarvis.execution",
            metadata={"cluster": "ares"},
        )
    )
    queue.update_task_state(task.task_id, JobState.RUNNING)
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    progress = spool / ".progress-crash-boundary.jsonl"
    runtime = spool / ".runtime-crash-boundary.jsonl"
    progress_anchor = _write_anchored_sidecar(progress)
    runtime_anchor = _write_anchored_sidecar(runtime)
    queue.register_execution_cleanup(
        task.task_id,
        {
            "execution_sidecars": {
                "schema_version": "clio-relay.execution-sidecars.v1",
                "progress": progress.name,
                "progress_anchor": progress_anchor,
                "runtime": runtime.name,
                "runtime_anchor": runtime_anchor,
            },
            # Exercise the marker-first migration used for existing v0.9 tasks.
            "execution_cleanup": {
                "schema_version": "clio-relay.execution-cleanup.v1",
                "launch_protocol": "broker-release-after-ownership-v1",
            },
        },
    )
    endpoint = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
    )

    cleanup_metadata = cast(Any, endpoint)._remove_recorded_execution_sidecars(
        job,
        queue.get_task(task.task_id),
    )
    assert progress.exists() is False
    assert runtime.exists() is False
    staged = queue.get_task(task.task_id)
    staged_cleanup = cast(dict[str, object], staged.metadata["execution_cleanup"])
    assert staged_cleanup["acknowledgment_stage"] == "quarantining"

    if crash_boundary == "inside_ack":

        def crash_after_canonical_ack(_task: RelayTask) -> None:
            raise OSError("injected crash after canonical cleanup acknowledgment")

        with monkeypatch.context() as patch:
            patch.setattr(
                queue,
                "_after_execution_cleanup_canonical_ack",
                crash_after_canonical_ack,
            )
            with pytest.raises(OSError, match="after canonical cleanup acknowledgment"):
                queue.acknowledge_execution_cleanup(
                    job.job_id,
                    task.task_id,
                    metadata=cleanup_metadata,
                )
        acknowledged = queue.get_task(task.task_id)
        acknowledged_cleanup = cast(
            dict[str, object],
            acknowledged.metadata["execution_cleanup"],
        )
        assert acknowledged_cleanup["acknowledgment_stage"] == "acknowledged"

    pending, truncated = queue.scan_execution_cleanup(cluster="ares", limit=10)
    assert [record.task_id for record in pending] == [task.task_id]
    assert truncated is False

    cast(Any, endpoint)._reconcile_pending_execution_cleanup()

    pending, truncated = queue.scan_execution_cleanup(cluster="ares", limit=10)
    assert pending == []
    assert truncated is False
    recovered = queue.get_task(task.task_id)
    assert recovered.metadata["execution_sidecars_quarantined"] is True
    cleanup = cast(dict[str, object], recovered.metadata["execution_cleanup"])
    assert cleanup["acknowledgment_stage"] == "acknowledged"
    evidence = cast(dict[str, object], recovered.metadata["execution_sidecar_quarantines"])
    entries = cast(dict[str, str], evidence["entries"])
    assert set(entries) == {progress.name, runtime.name}
    assert all((spool / name).is_file() for name in entries.values())


def test_cleanup_batch_reaches_expired_marker_after_live_lease_markers(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    assert endpoint_module.EXECUTION_CLEANUP_SCAN_LIMIT == DEFAULT_EXACT_RECORD_LIMIT + 1
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    markers: list[RelayTask] = []
    jobs: list[RelayJob] = []
    for index in range(4):
        job = queue.submit_job(
            RelayJob(
                cluster="ares",
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(command=["controlled"]),
                idempotency_key=f"cleanup-live-prefix-{index}",
            )
        )
        if index < 2:
            assert (
                queue.acquire_job(
                    job.job_id,
                    f"live-endpoint-{index}",
                    cluster="ares",
                    ttl_seconds=60,
                )
                is not None
            )
        task = queue.append_task(
            RelayTask(
                job_id=job.job_id,
                name="jarvis.execution",
                metadata={"cluster": "ares"},
            )
        )
        queue.update_task_state(task.task_id, JobState.RUNNING)
        spool = settings.spool_dir / job.job_id
        spool.mkdir(parents=True)
        progress_name = f".progress-live-prefix-{index}.jsonl"
        runtime_name = f".runtime-live-prefix-{index}.jsonl"
        progress_anchor = _write_anchored_sidecar(spool / progress_name)
        runtime_anchor = _write_anchored_sidecar(spool / runtime_name)
        marker = queue.register_execution_cleanup(
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
        jobs.append(job)
        markers.append(marker)

    def live_prefix_batch(*, cluster: str, limit: int) -> tuple[list[RelayTask], bool]:
        assert cluster == "ares"
        assert limit == endpoint_module.EXECUTION_CLEANUP_SCAN_LIMIT
        return markers[:3], True

    monkeypatch.setattr(queue, "scan_execution_cleanup", live_prefix_batch)
    endpoint = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
    )
    registration = endpoint.register()
    cast(Any, endpoint)._reconcile_pending_execution_cleanup()

    assert queue.get_task(markers[0].task_id).state is JobState.RUNNING
    assert queue.get_task(markers[1].task_id).state is JobState.RUNNING
    assert queue.get_task(markers[2].task_id).state is JobState.FAILED
    assert queue.get_task(markers[3].task_id).state is JobState.RUNNING
    assert (
        queue.acquire_job(
            jobs[2].job_id,
            registration.endpoint_id,
            cluster="ares",
        )
        is not None
    )
    assert endpoint.endpoint is not None
    scan = cast(dict[str, object], endpoint.endpoint.metadata["execution_cleanup_scan"])
    assert scan["batch_size"] == 3
    assert scan["eligible"] == 1
    assert scan["completed"] == 1
    assert scan["has_more"] is True


def test_execution_cleanup_marker_is_durable_before_task_metadata_update(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["controlled"]),
            idempotency_key="cleanup-marker-first",
        )
    )
    task = queue.append_task(
        RelayTask(
            job_id=job.job_id,
            name="jarvis.execution",
            metadata={"cluster": "ares"},
        )
    )
    queue.update_task_state(task.task_id, JobState.RUNNING)
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    progress_name = ".progress-marker-first.jsonl"
    runtime_name = ".runtime-marker-first.jsonl"
    progress_anchor = _write_anchored_sidecar(spool / progress_name)
    runtime_anchor = _write_anchored_sidecar(spool / runtime_name)
    original_write = cast(Any, queue)._write
    task_path = settings.core_dir / "tasks" / f"{task.task_id}.json"

    def fail_task_write(path: Path, record: object) -> None:
        if logical_filesystem_path(path) == task_path:
            raise OSError("injected post-marker task write crash")
        original_write(path, record)

    with monkeypatch.context() as patch:
        patch.setattr(queue, "_write", fail_task_write)
        with pytest.raises(OSError, match="post-marker task write crash"):
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

    assert "execution_sidecars" not in queue.get_task(task.task_id).metadata
    pending, has_more = queue.scan_execution_cleanup(cluster="ares", limit=10)
    assert [marker.task_id for marker in pending] == [task.task_id]
    assert has_more is False
    endpoint = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
    )
    cast(Any, endpoint)._reconcile_pending_execution_cleanup()
    assert queue.get_task(task.task_id).metadata["execution_sidecars_removed"] is True
    assert list(spool.glob(".*-marker-first.jsonl")) == []


def test_execution_cleanup_empty_directory_crash_boundaries_fail_closed(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["controlled"]),
            idempotency_key="cleanup-empty-directory-boundaries",
        )
    )
    task = queue.append_task(
        RelayTask(
            job_id=job.job_id,
            name="jarvis.execution",
            metadata={"cluster": "ares"},
        )
    )
    pending_job_path = cast(Any, queue)._execution_cleanup_job_path(
        "ares",
        job.job_id,
    )
    pending_job_path.mkdir(parents=True)
    assert cast(Any, queue)._job_has_pending_execution_cleanup_unlocked("ares", job.job_id)
    pending, _ = queue.scan_execution_cleanup(cluster="ares", limit=10)
    assert pending == []
    assert pending_job_path.exists() is False
    assert not cast(Any, queue)._job_has_pending_execution_cleanup_unlocked("ares", job.job_id)

    queue.register_execution_cleanup(
        task.task_id,
        {
            "execution_sidecars": {
                "schema_version": "clio-relay.execution-sidecars.v1",
                "progress": ".progress-empty-boundary.jsonl",
                "runtime": ".runtime-empty-boundary.jsonl",
            },
            "execution_cleanup": {
                "schema_version": "clio-relay.execution-cleanup.v1",
                "launch_protocol": "broker-release-after-ownership-v1",
            },
        },
    )
    marker_path = cast(Any, queue)._execution_cleanup_path(
        "ares",
        job.job_id,
        task.task_id,
    )
    marker_path.unlink()
    assert pending_job_path.is_dir()
    assert cast(Any, queue)._job_has_pending_execution_cleanup_unlocked("ares", job.job_id)
    pending, _ = queue.scan_execution_cleanup(cluster="ares", limit=10)
    assert pending == []
    assert pending_job_path.exists() is False
    assert not cast(Any, queue)._job_has_pending_execution_cleanup_unlocked("ares", job.job_id)


def test_execution_cleanup_legacy_flat_markers_migrate_in_bounded_batches(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["controlled"]),
            idempotency_key="cleanup-flat-migration",
        )
    )
    markers: list[RelayTask] = []
    for index in range(2):
        markers.append(
            queue.append_task(
                RelayTask(
                    job_id=job.job_id,
                    name=f"jarvis.execution.{index}",
                    metadata={"cluster": "ares"},
                )
            )
        )
    shard = cast(Any, queue)._execution_cleanup_shard(job.job_id)
    shard_path = cast(Any, queue)._execution_cleanup_shard_path("ares", shard)
    for marker in markers:
        legacy_path = shard_path / f"{job.job_id}--{marker.task_id}.json"
        cast(Any, queue)._write(legacy_path, marker)
    receipt_path = cast(Any, queue)._execution_cleanup_migration_receipt_path(
        "ares",
        shard,
    )
    receipt_path.unlink()

    assert not cast(Any, queue)._migrate_execution_cleanup_shard_unlocked(
        "ares",
        shard,
        limit=1,
    )
    assert receipt_path.exists() is False
    assert len(list(shard_path.glob("*.json"))) == 1
    assert cast(Any, queue)._job_has_pending_execution_cleanup_unlocked("ares", job.job_id)
    assert receipt_path.exists() is False
    assert cast(Any, queue)._migrate_execution_cleanup_shard_unlocked(
        "ares",
        shard,
        limit=1,
    )
    assert receipt_path.is_file()
    pending_job_path = cast(Any, queue)._execution_cleanup_job_path("ares", job.job_id)
    assert len(list(pending_job_path.glob("*.json"))) == 2


def test_retry_exhausted_hard_crash_cleans_sidecars_without_requeue(tmp_path: Path) -> None:
    crash_marker = tmp_path / "failed-crash.json"
    workload_marker = tmp_path / "workload-started.txt"
    repository = Path(__file__).parents[1]
    helper = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.hard_crash_pre_release_fixture",
            str(tmp_path),
            str(crash_marker),
            str(workload_marker),
            "no-cancel",
        ],
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert helper.returncode == 0, helper.stderr
    identity = cast(dict[str, object], json.loads(crash_marker.read_text(encoding="utf-8")))
    job_id = cast(str, identity["job_id"])
    task_id = cast(str, identity["task_id"])
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    recovered = queue.recover_stale_jobs(cluster="ares", max_attempts=1)
    assert [job.job_id for job in recovered] == [job_id]
    assert queue.get_job(job_id).state is JobState.FAILED

    endpoint = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
    )
    cast(Any, endpoint)._reconcile_pending_execution_cleanup()

    assert queue.get_job(job_id).state is JobState.FAILED
    assert queue.get_task(task_id).state is JobState.FAILED
    pending, truncated = queue.scan_execution_cleanup(cluster="ares", limit=10)
    assert pending == []
    assert truncated is False
    sidecars = cast(dict[str, object], identity["execution_sidecars"])
    for role in ("progress", "runtime"):
        assert not (settings.spool_dir / job_id / cast(str, sidecars[role])).exists()


def test_sidecar_reader_preserves_incomplete_record_until_newline() -> None:
    handle = BytesIO(b'{"value":')

    line, status = cast(Any, endpoint_module)._read_bounded_sidecar_record(
        handle,
        max_bytes=64,
        allow_final_record=False,
    )

    assert line is None
    assert status == "incomplete"
    assert handle.tell() == 0
    handle.seek(0, os.SEEK_END)
    handle.write(b"1}\n")
    handle.seek(0)
    line, status = cast(Any, endpoint_module)._read_bounded_sidecar_record(
        handle,
        max_bytes=64,
        allow_final_record=False,
    )
    assert status == "record"
    assert line == b'{"value":1}\n'


def test_sidecar_reader_discards_oversized_record_in_bounded_chunks() -> None:
    handle = BytesIO(b"x" * 100 + b'\n{"value":2}\n')

    line, status = cast(Any, endpoint_module)._read_bounded_sidecar_record(
        handle,
        max_bytes=32,
        allow_final_record=False,
    )

    assert line is None
    assert status == "oversized"
    line, status = cast(Any, endpoint_module)._read_bounded_sidecar_record(
        handle,
        max_bytes=32,
        allow_final_record=False,
    )
    assert status == "record"
    assert line == b'{"value":2}\n'


def test_progress_sidecar_hmac_rejects_tampering_and_replay_without_serializing_key() -> None:
    private = cast(Any, endpoint_module)
    key = "progress-secret-that-must-not-be-serialized"
    progress = {
        "label": "iteration",
        "current": 1,
        "total": 10,
        "metadata": {"adapter": "regex"},
    }
    record = _signed_progress_sidecar_record(progress, key=key)

    assert key not in json.dumps(record)
    assert (
        private._progress_from_sidecar_record(
            record,
            expected_key=key,
            expected_sequence=1,
        )
        == progress
    )
    tampered = json.loads(json.dumps(record))
    cast(dict[str, object], tampered["progress"])["current"] = 9
    with pytest.raises(ValueError, match="HMAC did not match"):
        private._progress_from_sidecar_record(
            tampered,
            expected_key=key,
            expected_sequence=1,
        )
    with pytest.raises(ValueError, match="sequence did not match"):
        private._progress_from_sidecar_record(
            record,
            expected_key=key,
            expected_sequence=2,
        )


def test_execution_sidecar_cleanup_removes_only_owned_non_directory_entries(
    tmp_path: Path,
) -> None:
    spool = tmp_path / "spool" / "job"
    spool.mkdir(parents=True)
    progress = spool / ".progress-owned.jsonl"
    runtime = spool / ".runtime-owned.jsonl"
    progress_anchor_metadata = _write_anchored_sidecar(progress, "progress")
    runtime_anchor_metadata = _write_anchored_sidecar(runtime, "runtime")
    private = cast(Any, endpoint_module)
    anchors = {
        progress: private._runtime_sidecar_anchor_from_metadata(
            progress_anchor_metadata,
            task_id="test-progress",
        ),
        runtime: private._runtime_sidecar_anchor_from_metadata(
            runtime_anchor_metadata,
            task_id="test-runtime",
        ),
    }

    quarantined = private._remove_execution_sidecars(  # noqa: SLF001
        [progress, runtime],
        spool_path=spool,
        expected_anchors=anchors,
    )

    assert progress.exists() is False
    assert runtime.exists() is False
    assert all(path.is_file() for path in quarantined.values())

    hostile = spool / ".progress-hostile.jsonl"
    hostile.mkdir()
    with pytest.raises(ConfigurationError, match="became a directory"):
        private._remove_execution_sidecars(  # noqa: SLF001
            [hostile],
            spool_path=spool,
            expected_anchors={
                hostile: private._runtime_sidecar_anchor(os.stat(hostile, follow_symlinks=False))
            },
        )
    assert hostile.is_dir()


def test_execution_sidecar_quarantine_name_is_bounded_on_long_spool_paths(
    tmp_path: Path,
) -> None:
    private = cast(Any, endpoint_module)
    source_name = f".runtime-{'a' * 32}.jsonl"
    spool = tmp_path / "spool"
    source = spool / source_name
    while len(str(source)) < 235:
        remaining = 235 - len(str(source))
        spool /= "d" * min(40, max(1, remaining - 1))
        source = spool / source_name
    spool.mkdir(parents=True)
    anchor_metadata = _write_anchored_sidecar(source, "long-path-evidence")
    anchor = private._runtime_sidecar_anchor_from_metadata(
        anchor_metadata,
        task_id="long-path-sidecar",
    )
    quarantine = spool / private._execution_sidecar_quarantine_name(anchor)

    assert len(quarantine.name) == 47
    assert len(quarantine.name) <= len(source.name)
    assert quarantine.name.startswith(".q1-")
    assert len(str(quarantine)) <= len(str(source))

    quarantined = private._remove_execution_sidecars(
        [source],
        spool_path=spool,
        expected_anchors={source: anchor},
        expected_quarantines={source: quarantine},
    )

    assert quarantined == {source: quarantine}
    assert source.exists() is False
    assert quarantine.read_text(encoding="utf-8") == "long-path-evidence"


def test_execution_sidecar_quarantine_restarts_beyond_windows_max_path(
    tmp_path: Path,
) -> None:
    """A real deep sidecar quarantine is durable and idempotent after restart."""
    private = cast(Any, endpoint_module)
    spool = tmp_path.joinpath(*(f"quarantine-{index}-{'x' * 72}" for index in range(3)))
    assert len(str(spool / ".runtime-owned.jsonl")) > 260
    internal_filesystem_path(spool, force_extended=True).mkdir(parents=True)
    source = spool / ".runtime-owned.jsonl"
    anchor = private._precreate_runtime_sidecar(source)
    internal_filesystem_path(source).write_text(
        "restart-evidence",
        encoding="utf-8",
    )
    quarantine = spool / private._execution_sidecar_quarantine_name(anchor)

    first = private._remove_execution_sidecars(
        [source],
        spool_path=spool,
        expected_anchors={source: anchor},
        expected_quarantines={source: quarantine},
    )
    restarted_anchor = private._runtime_sidecar_anchor(
        os.stat(internal_filesystem_path(quarantine), follow_symlinks=False)
    )
    second = private._remove_execution_sidecars(
        [source],
        spool_path=spool,
        expected_anchors={source: restarted_anchor},
        expected_quarantines={source: quarantine},
    )

    assert first == second == {source: quarantine}
    assert internal_filesystem_path(source).exists() is False
    assert internal_filesystem_path(quarantine).read_text(encoding="utf-8") == ("restart-evidence")
    assert "\\\\?\\" not in str(first)


@pytest.mark.parametrize("replace_spool", [False, True])
def test_anchored_sidecar_rename_fails_closed_and_closes_live_anchor(
    tmp_path: Path,
    replace_spool: bool,
) -> None:
    private = cast(Any, endpoint_module)
    spool = tmp_path / "spool" / "job"
    spool.mkdir(parents=True)
    sidecar = spool / ".runtime-owned.jsonl"
    anchor = private._precreate_runtime_sidecar(sidecar)
    descriptor = anchor.descriptor
    moved_spool = spool.with_name("job-moved")
    moved_sidecar = spool / ".runtime-owned.moved.jsonl"
    if replace_spool:
        spool.rename(moved_spool)
    else:
        sidecar.rename(moved_sidecar)
        sidecar.write_text("replacement", encoding="utf-8")
        if os.name != "nt":
            sidecar.chmod(0o600)

    with pytest.raises(
        ConfigurationError,
        match="anchored execution|identity or permissions|file identity changed",
    ):
        private._remove_execution_sidecars(
            [sidecar],
            spool_path=spool,
            expected_anchors={sidecar: anchor},
        )

    if descriptor is not None:
        with pytest.raises(OSError):
            os.fstat(descriptor)
    if replace_spool:
        assert (moved_spool / sidecar.name).is_file()
    else:
        assert moved_sidecar.is_file()
        assert sidecar.read_text(encoding="utf-8") == "replacement"


def test_sidecar_quarantine_never_replaces_existing_evidence(tmp_path: Path) -> None:
    private = cast(Any, endpoint_module)
    spool = tmp_path / "spool" / "job"
    spool.mkdir(parents=True)
    sidecar = spool / ".runtime-no-replace.jsonl"
    anchor = private._precreate_runtime_sidecar(sidecar)
    sidecar.write_text("owned", encoding="utf-8")
    quarantine = spool / private._execution_sidecar_quarantine_name(anchor)
    quarantine.write_text("hostile", encoding="utf-8")
    if os.name != "nt":
        quarantine.chmod(0o600)

    with pytest.raises(ConfigurationError, match="identity|permissions|quarantine"):
        private._remove_execution_sidecars(
            [sidecar],
            spool_path=spool,
            expected_anchors={sidecar: anchor},
            expected_quarantines={sidecar: quarantine},
        )

    assert sidecar.read_text(encoding="utf-8") == "owned"
    assert quarantine.read_text(encoding="utf-8") == "hostile"


def test_posix_source_swap_during_quarantine_retains_every_inode(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    private = cast(Any, endpoint_module)
    if os.name == "nt":
        assert private._WINDOWS_FILE_RENAME_INFO == 3
        return
    spool = tmp_path / "spool" / "job"
    spool.mkdir(parents=True)
    sidecar = spool / ".runtime-race.jsonl"
    anchor = private._precreate_runtime_sidecar(sidecar)
    sidecar.write_text("anchored", encoding="utf-8")
    moved_anchor = spool / ".runtime-race.anchored"
    original_rename = private._rename_noreplace_at

    def swap_before_rename(directory_fd: int, source_name: str, quarantine_name: str) -> None:
        sidecar.rename(moved_anchor)
        sidecar.write_text("replacement", encoding="utf-8")
        sidecar.chmod(0o600)
        original_rename(directory_fd, source_name, quarantine_name)

    monkeypatch.setattr(endpoint_module, "_rename_noreplace_at", swap_before_rename)
    quarantine = spool / private._execution_sidecar_quarantine_name(anchor)

    with pytest.raises(ConfigurationError, match="identity or permissions changed"):
        private._remove_execution_sidecars(
            [sidecar],
            spool_path=spool,
            expected_anchors={sidecar: anchor},
            expected_quarantines={sidecar: quarantine},
        )

    assert moved_anchor.read_text(encoding="utf-8") == "anchored"
    assert quarantine.read_text(encoding="utf-8") == "replacement"


def test_windows_sidecar_cleanup_anchors_parent_and_rejects_reparse_points(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    spool = tmp_path / "spool" / "job"
    spool.mkdir(parents=True)
    progress = spool / ".progress-owned.jsonl"
    progress_anchor_metadata = _write_anchored_sidecar(progress, "progress")
    private = cast(Any, endpoint_module)
    progress_anchor = private._runtime_sidecar_anchor_from_metadata(
        progress_anchor_metadata,
        task_id="windows-cleanup-progress",
    )

    if os.name != "nt":
        calls: list[tuple[str, int]] = []

        def fake_open_windows_cleanup_handle(
            path: Path,
            *,
            desired_access: int,
            share_mode: int,
            flags: int,
            missing_ok: bool,
        ) -> int:
            del path, desired_access, share_mode, flags, missing_ok
            return 41

        def fake_windows_handle_information(handle: int, path: Path) -> tuple[int, int]:
            del handle, path
            return (
                cast(Any, endpoint_module)._WINDOWS_FILE_ATTRIBUTE_DIRECTORY,
                os.stat(spool).st_ino,
            )

        def fake_quarantine_windows_sidecar_by_handle(
            path: Path,
            *,
            quarantine: Path,
            anchored_directory_handle: int,
            expected_anchor: object,
        ) -> None:
            del quarantine, expected_anchor
            calls.append((path.name, anchored_directory_handle))

        def fake_close_windows_cleanup_handle(handle: int) -> None:
            calls.append(("closed", handle))

        monkeypatch.setattr(
            endpoint_module,
            "_open_windows_cleanup_handle",
            fake_open_windows_cleanup_handle,
        )
        monkeypatch.setattr(
            endpoint_module,
            "_windows_handle_information",
            fake_windows_handle_information,
        )
        monkeypatch.setattr(
            endpoint_module,
            "_quarantine_windows_sidecar_by_handle",
            fake_quarantine_windows_sidecar_by_handle,
        )
        monkeypatch.setattr(
            endpoint_module,
            "_close_windows_cleanup_handle",
            fake_close_windows_cleanup_handle,
        )
        cast(Any, endpoint_module)._remove_execution_sidecars_windows(
            [progress],
            spool_path=spool,
            expected_spool_identity=(os.stat(spool).st_dev, os.stat(spool).st_ino),
            expected_anchors={progress: progress_anchor},
        )
        assert calls == [(progress.name, 41), ("closed", 41)]
        return

    original_quarantine = private._quarantine_windows_sidecar_by_handle
    moved_spool = spool.with_name("job-replaced")
    replacement_attempts: list[OSError] = []

    def adversarial_quarantine(
        path: Path,
        *,
        quarantine: Path,
        anchored_directory_handle: int,
        expected_anchor: object,
    ) -> None:
        try:
            spool.rename(moved_spool)
        except OSError as exc:
            replacement_attempts.append(exc)
        else:
            pytest.fail("Windows cleanup allowed its anchored spool to be replaced")
        original_quarantine(
            path,
            quarantine=quarantine,
            anchored_directory_handle=anchored_directory_handle,
            expected_anchor=expected_anchor,
        )

    monkeypatch.setattr(
        endpoint_module,
        "_quarantine_windows_sidecar_by_handle",
        adversarial_quarantine,
    )
    quarantined = private._remove_execution_sidecars(  # noqa: SLF001
        [progress],
        spool_path=spool,
        expected_anchors={progress: progress_anchor},
    )
    assert len(replacement_attempts) == 1
    assert spool.is_dir()
    assert progress.exists() is False
    assert quarantined[progress].is_file()

    target = tmp_path / "junction-target"
    target.mkdir()
    junction = spool / ".runtime-hostile.jsonl"
    subprocess.run(
        ["cmd.exe", "/d", "/c", "mklink", "/J", str(junction), str(target)],
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        with pytest.raises(ConfigurationError, match="became a reparse point"):
            private._remove_execution_sidecars(  # noqa: SLF001
                [junction],
                spool_path=spool,
                expected_anchors={
                    junction: private._runtime_sidecar_anchor(
                        os.stat(junction, follow_symlinks=False)
                    )
                },
            )
    finally:
        junction.rmdir()


def test_windows_repeated_quarantine_creates_only_exact_directory_entries(
    tmp_path: Path,
) -> None:
    private = cast(Any, endpoint_module)
    if os.name != "nt":
        assert private._WINDOWS_FILE_RENAME_INFO == 3
        return
    spool = tmp_path / "spool"
    spool.mkdir()
    sentinel = spool / "sentinel.txt"
    sentinel.write_text("keep", encoding="utf-8")
    expected_entries = {sentinel.name}

    for iteration in range(32):
        source = spool / f".runtime-repeat-{iteration:02d}.jsonl"
        anchor_metadata = _write_anchored_sidecar(source, f"record-{iteration}")
        anchor = private._runtime_sidecar_anchor_from_metadata(
            anchor_metadata,
            task_id=f"windows-repeat-{iteration}",
        )
        quarantine = spool / private._execution_sidecar_quarantine_name(anchor)

        result = private._remove_execution_sidecars(
            [source],
            spool_path=spool,
            expected_anchors={source: anchor},
            expected_quarantines={source: quarantine},
        )

        expected_entries.add(quarantine.name)
        assert result == {source: quarantine}
        assert source.exists() is False
        assert quarantine.read_text(encoding="utf-8") == f"record-{iteration}"
        assert {entry.name for entry in spool.iterdir()} == expected_entries


def test_windows_repeated_quarantine_restart_acknowledgment_is_exact(
    tmp_path: Path,
) -> None:
    if os.name != "nt":
        assert os.name == "posix"
        return
    private = cast(Any, endpoint_module)

    for iteration in range(8):
        root = tmp_path / f"attempt-{iteration}"
        settings = RelaySettings(core_dir=root / "core", spool_dir=root / "spool")
        queue = ClioCoreQueue(settings.core_dir)
        job = queue.submit_job(
            RelayJob(
                cluster="ares",
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(command=["controlled"]),
                idempotency_key=f"windows-quarantine-restart-{iteration}",
            )
        )
        task = queue.append_task(
            RelayTask(
                job_id=job.job_id,
                name="jarvis.execution",
                metadata={"cluster": "ares"},
            )
        )
        queue.update_task_state(task.task_id, JobState.RUNNING)
        spool = settings.spool_dir / job.job_id
        spool.mkdir(parents=True)
        sentinel = spool / "sentinel.txt"
        sentinel.write_text("keep", encoding="utf-8")
        progress = spool / f".progress-restart-{iteration}.jsonl"
        runtime = spool / f".runtime-restart-{iteration}.jsonl"
        progress_anchor_metadata = _write_anchored_sidecar(progress, "progress")
        runtime_anchor_metadata = _write_anchored_sidecar(runtime, "runtime")
        progress_anchor = private._runtime_sidecar_anchor_from_metadata(
            progress_anchor_metadata,
            task_id=task.task_id,
        )
        runtime_anchor = private._runtime_sidecar_anchor_from_metadata(
            runtime_anchor_metadata,
            task_id=task.task_id,
        )
        progress_quarantine = spool / private._execution_sidecar_quarantine_name(progress_anchor)
        runtime_quarantine = spool / private._execution_sidecar_quarantine_name(runtime_anchor)
        queue.register_execution_cleanup(
            task.task_id,
            {
                "execution_sidecars": {
                    "schema_version": "clio-relay.execution-sidecars.v1",
                    "progress": progress.name,
                    "progress_anchor": progress_anchor_metadata,
                    "runtime": runtime.name,
                    "runtime_anchor": runtime_anchor_metadata,
                },
                "execution_cleanup": {
                    "schema_version": "clio-relay.execution-cleanup.v1",
                    "launch_protocol": "broker-release-after-ownership-v1",
                },
            },
        )
        worker = EndpointWorker(
            role=EndpointRole.WORKER,
            settings=settings,
            cluster="ares",
            queue=queue,
        )

        cast(Any, worker)._remove_recorded_execution_sidecars(
            job,
            queue.get_task(task.task_id),
        )
        assert {entry.name for entry in spool.iterdir()} == {
            sentinel.name,
            progress_quarantine.name,
            runtime_quarantine.name,
        }

        cast(Any, worker)._reconcile_pending_execution_cleanup()

        pending, truncated = queue.scan_execution_cleanup(cluster="ares", limit=10)
        assert pending == []
        assert truncated is False
        recovered = queue.get_task(task.task_id)
        assert recovered.metadata["execution_sidecars_quarantined"] is True
        assert recovered.metadata["restart_cleanup_acknowledged"] is True
        assert {entry.name for entry in spool.iterdir()} == {
            sentinel.name,
            progress_quarantine.name,
            runtime_quarantine.name,
        }


def _write_runtime_sidecar(
    env: dict[str, str] | None,
    *,
    scheduler_job_id: str,
    scheduler_provider: str = "test-scheduler",
    schema_version: str | None = "jarvis.runtime.v1",
) -> None:
    assert env is not None
    runtime_metadata: dict[str, object] = {
        "execution_id": f"execution-{scheduler_job_id}",
        "scheduler_provider": scheduler_provider,
        "scheduler_job_id": scheduler_job_id,
        "scheduler_phase": "submitted",
        "details": {
            "scheduler_submission": {
                "schema_version": "jarvis.scheduler.submission.v1",
                "provider": scheduler_provider,
                "scheduler_job_id": scheduler_job_id,
                "identity_source": "scheduler_submit_api",
                "submitted": True,
            }
        },
    }
    if schema_version is not None:
        runtime_metadata["schema_version"] = schema_version
    Path(env["CLIO_RELAY_RUNTIME_METADATA_FILE"]).write_text(
        json.dumps(
            runtime_sidecar_record(
                runtime_metadata,
                key=env["CLIO_RELAY_RUNTIME_METADATA_TOKEN"],
                sequence=1,
            )
        )
        + "\n",
        encoding="utf-8",
    )


def _write_direct_runtime_sidecar(env: dict[str, str] | None) -> None:
    assert env is not None
    intent = cast(
        dict[str, object],
        json.loads(env["CLIO_RELAY_RUNTIME_SUBMISSION_INTENT"]),
    )
    direct_proof = env["CLIO_RELAY_RUNTIME_DIRECT_PROOF"]
    runtime_metadata: dict[str, object] = {
        "schema_version": "jarvis.runtime.v1",
        "execution_id": intent["execution_id"],
        "scheduler_phase": "direct_completed",
        "terminal": {"state": "direct_completed", "terminal": True},
        "details": {
            "execution_mode": "direct",
            "scheduler_expected": False,
            "direct_execution_proof": direct_proof,
        },
    }
    Path(env["CLIO_RELAY_RUNTIME_METADATA_FILE"]).write_text(
        json.dumps(
            runtime_sidecar_record(
                runtime_metadata,
                key=env["CLIO_RELAY_RUNTIME_METADATA_TOKEN"],
                sequence=1,
            )
        )
        + "\n",
        encoding="utf-8",
    )


def _write_native_mcp_transport_runtime_sidecar(env: dict[str, str] | None) -> None:
    """Write the native direct JARVIS record used to transport one MCP call."""
    assert env is not None
    intent = cast(
        dict[str, object],
        json.loads(env["CLIO_RELAY_RUNTIME_SUBMISSION_INTENT"]),
    )
    execution_id = cast(str, intent["execution_id"])
    pipeline_id = "clio-relay-mcp-call"
    runtime_metadata: dict[str, object] = {
        "execution_handle": {
            "schema_version": "jarvis.execution.handle.v1",
            "execution_id": execution_id,
            "pipeline_id": pipeline_id,
            "mode": "direct",
            "scheduler_provider": None,
            "scheduler_native_id": None,
            "cluster": None,
        },
        "execution_record": {
            "schema_version": "jarvis.execution.record.v1",
            "execution_id": execution_id,
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_id,
            "mode": "direct",
            "scheduler_provider": None,
            "scheduler_native_id": None,
            "cluster": None,
            "state": "completed",
            "submitted": False,
            "terminal": True,
            "created_at": "2026-07-16T14:00:00Z",
            "updated_at": "2026-07-16T14:00:01Z",
            "return_code": 0,
            "error": None,
            "metadata": {},
        },
        "progress": {
            "schema_version": "jarvis.execution.progress.v1",
            "execution_id": execution_id,
            "pipeline_id": pipeline_id,
            "execution_state": "completed",
            "terminal": True,
            "packages": [],
        },
    }
    Path(env["CLIO_RELAY_RUNTIME_METADATA_FILE"]).write_text(
        json.dumps(
            runtime_sidecar_record(
                runtime_metadata,
                key=env["CLIO_RELAY_RUNTIME_METADATA_TOKEN"],
                sequence=1,
            )
        )
        + "\n",
        encoding="utf-8",
    )


def _signed_progress_sidecar_record(
    progress: Mapping[str, object],
    *,
    key: str,
    sequence: int = 1,
) -> dict[str, object]:
    signed: dict[str, object] = {
        "schema_version": "clio-relay.progress-sidecar-record.v1",
        "sequence": sequence,
        "progress": dict(progress),
    }
    canonical = json.dumps(
        signed,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return {
        **signed,
        "progress_hmac": hmac.new(
            key.encode("utf-8"),
            canonical,
            hashlib.sha256,
        ).hexdigest(),
    }


def _owned_scheduler_metadata(
    *,
    relay_job_id: str,
    task_id: str,
    scheduler_job_id: str,
    scheduler_provider: str = "test-scheduler",
) -> dict[str, object]:
    return {
        "scheduler": scheduler_provider,
        "scheduler_job_ids": [scheduler_job_id],
        "scheduler_job_ownership": [
            {
                "scheduler_job_id": scheduler_job_id,
                "scheduler_provider": scheduler_provider,
                "relay_job_id": relay_job_id,
                "task_id": task_id,
                "execution_id": f"execution-{scheduler_job_id}",
                "runtime_metadata_source": "jarvis_sidecar",
                "ownership_verified": True,
                "proof": "authenticated_runtime_sidecar",
            }
        ],
    }


def _assert_no_execution_sidecars(settings: RelaySettings, job_id: str) -> None:
    spool = settings.spool_dir / job_id
    assert list(spool.glob(".progress-*.jsonl")) == []
    assert list(spool.glob(".runtime-*.jsonl")) == []


class FakeSchedulerProvider:
    name = "test-scheduler"

    def __init__(self, status: SchedulerStatus | None = None) -> None:
        self.status = status or SchedulerStatus(
            scheduler=self.name,
            scheduler_job_id="unknown",
            phase=SchedulerPhase.UNKNOWN,
        )
        self.canceled: list[str] = []
        self.polled: list[str] = []
        self.reconciliation_matches: list[str] = []
        self.reconciliation_markers: list[str] = []

    def scheduler_cluster_name(self) -> str | None:
        """Return the scheduler-native cluster identity used by this test provider."""
        return "test-cluster"

    def poll(self, scheduler_job_id: str) -> SchedulerStatus:
        self.polled.append(scheduler_job_id)
        return self.status.model_copy(
            update={"scheduler": self.name, "scheduler_job_id": scheduler_job_id}
        )

    def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        self.canceled.append(scheduler_job_id)
        return subprocess.CompletedProcess(["cancel", scheduler_job_id], 0, "", "")

    def find_job_ids_by_marker(
        self,
        marker: str,
        *,
        submitted_after: datetime,
        scheduler_user: str,
    ) -> list[str]:
        """Return exact marker matches configured by an interrupted-submit test."""
        assert submitted_after.tzinfo is not None
        assert scheduler_user
        self.reconciliation_markers.append(marker)
        return list(self.reconciliation_matches)


class RecordingProvider(JarvisCdProvider):
    def __init__(self) -> None:
        super().__init__(jarvis_bin="jarvis")
        self.runs: list[Path] = []
        self.named_runs: list[str] = []

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
        env: dict[str, str] | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        on_start: Callable[[int], None] | None = None,
        should_cancel: Callable[[], bool] | None = None,
        on_poll: Callable[[], None] | None = None,
        timeout_seconds: int | None = None,
        on_timeout: Callable[[], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.runs.append(pipeline_path)
        if env is not None:
            Path(env["CLIO_RELAY_PROGRESS_FILE"]).write_text("", encoding="utf-8")
            Path(env["CLIO_RELAY_RUNTIME_METADATA_FILE"]).write_text("", encoding="utf-8")
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

    def run_named_pipeline_streaming(
        self,
        pipeline_name: str,
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
        del cwd, should_cancel, timeout_seconds, on_timeout
        self.named_runs.append(pipeline_name)
        if env is not None:
            Path(env["CLIO_RELAY_PROGRESS_FILE"]).write_text("", encoding="utf-8")
            _write_direct_runtime_sidecar(env)
        if on_start is not None:
            on_start(456)
        if on_stdout is not None:
            on_stdout("named ok\n")
        if on_stderr is not None:
            on_stderr("named warn\n")
        if on_poll is not None:
            on_poll()
        return subprocess.CompletedProcess(
            args=["jarvis", "ppl", "run"],
            returncode=0,
            stdout="named ok\n",
            stderr="named warn\n",
        )


def test_worker_applies_linux_secret_gate_before_first_runtime_key(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["controlled"]),
            idempotency_key="secret-gate-before-key",
        )
    )
    gate_applied = [False]
    original_token_urlsafe = endpoint_module.secrets.token_urlsafe

    def apply_gate() -> None:
        gate_applied[0] = True

    def guarded_token_urlsafe(byte_count: int | None = None) -> str:
        assert gate_applied[0] is True
        return original_token_urlsafe(byte_count)

    monkeypatch.setattr(endpoint_module.sys, "platform", "linux")
    monkeypatch.setattr(
        process_containment,
        "enforce_linux_secret_memory_gate",
        apply_gate,
    )
    monkeypatch.setattr(endpoint_module.secrets, "token_urlsafe", guarded_token_urlsafe)
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=RecordingProvider(),
    )
    worker.register()

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.SUCCEEDED
    assert gate_applied == [True]


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
    endpoint = worker.register()

    assert endpoint.metadata["scheduler_provider"] == "external"

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
        "log_capture",
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
    _assert_no_execution_sidecars(settings, job.job_id)


def test_worker_bounds_durable_output_events_and_records_truncation_provenance(
    tmp_path: Path,
) -> None:
    class OversizedOutputProvider(RecordingProvider):
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
            del cwd, env, on_stderr, should_cancel, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            if on_start is not None:
                on_start(321)
            if on_stdout is not None:
                on_stdout("é" * 100_000 + "\n")
            if on_poll is not None:
                on_poll()
            return subprocess.CompletedProcess(["jarvis"], 0, "", "")

    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        spool_max_log_bytes_per_stream=100_000,
        spool_max_log_bytes_per_job=100_000,
    )
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["large-output"]),
            idempotency_key="bounded-worker-output",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="test-cluster",
        queue=queue,
        provider=OversizedOutputProvider(),
    )
    worker.register()

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.SUCCEEDED
    stdout_path = settings.spool_dir / job.job_id / "stdout.log"
    assert stdout_path.stat().st_size == 100_000
    assert stdout_path.read_text(encoding="utf-8") == "é" * 50_000
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    deltas = [event for event in events if event.event_type == "stdout.delta"]
    truncations = [event for event in events if event.event_type == "stdout.truncated"]
    assert len(deltas) == 2
    assert all(len(str(event.payload["text"]).encode()) <= 64 * 1024 for event in deltas)
    assert len(truncations) == 1
    assert truncations[0].payload["dropped_chunk_bytes"] == 100_001
    provenance = json.loads(
        (settings.spool_dir / job.job_id / "provenance.json").read_text(encoding="utf-8")
    )
    capture = provenance["spool"]["log_capture"]
    assert capture["observed_bytes"] == 200_001
    assert capture["persisted_bytes"] == 100_000
    assert capture["dropped_bytes"] == 100_001
    assert capture["truncated"] is True
    assert provenance["artifacts"]["log_capture"]["sha256"] is not None


def test_concurrent_jobs_receive_isolated_sidecar_environments(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    sidecar_keys = {
        "CLIO_RELAY_PROGRESS_FILE",
        "CLIO_RELAY_PROGRESS_TOKEN",
        "CLIO_RELAY_RUNTIME_METADATA_FILE",
        "CLIO_RELAY_RUNTIME_METADATA_TOKEN",
    }
    for key in sidecar_keys:
        monkeypatch.delenv(key, raising=False)

    class BarrierProvider(RecordingProvider):
        def __init__(self) -> None:
            super().__init__()
            self.barrier = Barrier(2)
            self.lock = Lock()
            self.job_envs: list[dict[str, str]] = []

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
            del pipeline_path, cwd, on_stdout, on_stderr, on_start
            del should_cancel, on_poll, timeout_seconds, on_timeout
            assert env is not None
            with self.lock:
                self.job_envs.append({key: env[key] for key in sidecar_keys})
            self.barrier.wait(timeout=5)
            return subprocess.CompletedProcess(["jarvis"], 0, "", "")

    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    provider = BarrierProvider()
    for index in range(2):
        queue.submit_job(
            RelayJob(
                cluster="ares",
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(command=["echo", str(index)]),
                idempotency_key=f"concurrent-{index}",
            )
        )
    workers = [
        EndpointWorker(
            role=EndpointRole.WORKER,
            settings=settings,
            cluster="ares",
            queue=queue,
            provider=provider,
        )
        for _ in range(2)
    ]
    endpoints = [worker.register() for worker in workers]
    leases = [
        queue.acquire_next_job(endpoint.endpoint_id, cluster="ares", ttl_seconds=30)
        for endpoint in endpoints
    ]
    assert all(lease is not None for lease in leases)
    resolved_leases = [lease for lease in leases if lease is not None]

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    worker._run_job,  # pyright: ignore[reportPrivateUsage]
                    queue.get_job(lease.job_id),
                    lease,
                )
                for worker, lease in zip(workers, resolved_leases, strict=True)
            ]
            for future in futures:
                # The provider's five-second barrier is the semantic overlap proof.
                # This outer bound only detects a hung durable setup/cleanup path and
                # must accommodate the queue's bounded Windows filesystem latency.
                future.result(timeout=60)
    finally:
        for lease in resolved_leases:
            queue.release_lease(lease.lease_id)

    assert len(provider.job_envs) == 2
    for key in sidecar_keys:
        assert len({env[key] for env in provider.job_envs}) == 2
        assert key not in os.environ
    assert all(job.state == JobState.SUCCEEDED for job in queue.list_jobs())


def test_worker_runs_named_jarvis_pipeline(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    provider = RecordingProvider()
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_name="site_simulation_4node"),
            idempotency_key="named-pipeline-worker",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=provider,
    )
    endpoint = worker.register()

    assert endpoint.metadata["scheduler_provider"] == "external"

    result = worker.run_once()

    assert result is not None
    assert result.state == JobState.SUCCEEDED
    assert provider.named_runs == ["site_simulation_4node"]
    artifacts = queue.list_artifacts(job.job_id)
    assert {artifact.kind for artifact in artifacts} == {
        "jarvis_pipeline_reference",
        "runtime_metadata",
        "stdout",
        "stderr",
        "log_capture",
        "provenance",
    }
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=50)
    assert "jarvis.named_pipeline" in [event.event_type for event in events]


@pytest.mark.parametrize("failure_mode", ["corrupt", "missing", "renamed"])
def test_unresolved_runtime_sidecar_failure_retains_recovery_evidence(
    tmp_path: Path,
    failure_mode: str,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    scheduler = FakeSchedulerProvider()
    renamed_path: list[Path] = []

    class FailedRuntimeEvidenceProvider(RecordingProvider):
        def run_named_pipeline_streaming(
            self,
            pipeline_name: str,
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
            del cwd, on_stdout, on_stderr, should_cancel, timeout_seconds, on_timeout
            self.named_runs.append(pipeline_name)
            assert env is not None
            runtime_path = Path(env["CLIO_RELAY_RUNTIME_METADATA_FILE"])
            _write_direct_runtime_sidecar(env)
            if failure_mode == "corrupt":
                document = cast(
                    dict[str, object],
                    json.loads(runtime_path.read_text(encoding="utf-8")),
                )
                document["runtime_metadata_hmac"] = "0" * 64
                runtime_path.write_text(json.dumps(document) + "\n", encoding="utf-8")
            elif failure_mode == "missing":
                runtime_path.unlink()
            else:
                renamed = runtime_path.with_name(f"{runtime_path.name}.renamed")
                runtime_path.rename(renamed)
                renamed_path.append(renamed)
            if on_start is not None:
                on_start(919)
            if on_poll is not None:
                on_poll()
            return subprocess.CompletedProcess(["jarvis", "ppl", "run"], 0, "", "")

    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_name="unresolved-runtime-evidence"),
            idempotency_key=f"unresolved-runtime-evidence-{failure_mode}",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=FailedRuntimeEvidenceProvider(),
        scheduler_provider=scheduler,
    )
    worker.register()

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.FAILED
    assert result.last_error is not None
    assert "unresolved" in result.last_error
    task = queue.list_tasks(job.job_id)[0]
    assert task.metadata.get("execution_sidecars_removed") is not True
    pending, truncated = queue.scan_execution_cleanup(cluster="ares", limit=10)
    assert truncated is False
    assert [marker.task_id for marker in pending] == [task.task_id]
    sidecars = cast(dict[str, object], task.metadata["execution_sidecars"])
    progress_path = settings.spool_dir / job.job_id / cast(str, sidecars["progress"])
    assert progress_path.is_file()
    if failure_mode == "corrupt":
        assert (settings.spool_dir / job.job_id / cast(str, sidecars["runtime"])).is_file()
    elif failure_mode == "renamed":
        assert len(renamed_path) == 1
        assert renamed_path[0].is_file()
    assert scheduler.reconciliation_markers


def test_runtime_sidecar_failure_latches_until_exact_scheduler_reconciliation(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    scheduler = FakeSchedulerProvider()
    owned_processes: list[subprocess.Popen[str]] = []

    class InvalidThenSignedDirectProvider(RecordingProvider):
        def run_named_pipeline_streaming(
            self,
            pipeline_name: str,
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
            del cwd, on_stdout, on_stderr, should_cancel, timeout_seconds, on_timeout
            self.named_runs.append(pipeline_name)
            assert env is not None
            runtime_path = Path(env["CLIO_RELAY_RUNTIME_METADATA_FILE"])
            _write_direct_runtime_sidecar(env)
            signed_direct = runtime_path.read_text(encoding="utf-8")
            runtime_path.write_text("{}\n" + signed_direct, encoding="utf-8")
            process = process_containment.spawn_owned_process(
                [sys.executable, "-c", "import time;time.sleep(60)"],
                env=process_containment.owner_environment(None),
                text=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            owned_processes.append(process)
            if on_start is not None:
                on_start(process.pid)
            if on_poll is not None:
                on_poll()
            return subprocess.CompletedProcess(["jarvis", "ppl", "run"], 0, "", "")

    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_name="invalid-then-signed-direct"),
            idempotency_key="runtime-sidecar-failure-latch",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=InvalidThenSignedDirectProvider(),
        scheduler_provider=scheduler,
    )
    worker.register()

    try:
        result = worker.run_once()

        assert result is not None
        assert result.state is JobState.FAILED
        task = queue.list_tasks(job.job_id)[0]
        channel = cast(dict[str, object], task.metadata["runtime_sidecar_channel"])
        assert channel["state"] == "failed_closed"
        assert channel["resolution_requirement"] == "exact_scheduler_marker_reconciliation"
        sidecars = cast(dict[str, object], task.metadata["execution_sidecars"])
        assert "scheduler_expected_resolved" not in sidecars
        assert task.metadata["scheduler_job_ids"] == []
        assert task.metadata["scheduler_job_ownership"] == []
        runtime_path = settings.spool_dir / job.job_id / cast(str, sidecars["runtime"])
        assert runtime_path.read_text(encoding="utf-8").count("\n") == 2
        events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
        event_types = {event.event_type for event in events}
        assert "runtime.metadata_channel_failed_closed" in event_types
        assert "scheduler.direct_execution_confirmed" not in event_types

        scheduler.reconciliation_matches = ["exact-42"]
        cast(Any, worker)._reconcile_pending_execution_cleanup()

        recovered = queue.get_task(task.task_id)
        recovered_channel = cast(
            dict[str, object],
            recovered.metadata["runtime_sidecar_channel"],
        )
        assert recovered_channel["state"] == "resolved_by_exact_scheduler_reconciliation"
        assert recovered.metadata["scheduler_job_ids"] == ["exact-42"]
        ownership = cast(list[dict[str, object]], recovered.metadata["scheduler_job_ownership"])
        assert ownership[0]["proof"] == "exact_scheduler_marker_reconciliation"
        pending, truncated = queue.scan_execution_cleanup(cluster="ares", limit=10)
        assert pending == []
        assert truncated is False
        assert len(owned_processes) == 1
        owned_processes[0].wait(timeout=5)
    finally:
        for process in owned_processes:
            if process.poll() is None:
                process_containment.terminate_owned_process(process)
            process_containment.release_owned_process(process)


def test_authenticated_direct_mode_pin_rejects_later_scheduler_identity(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    scheduler = FakeSchedulerProvider()

    class DirectThenSchedulerProvider(RecordingProvider):
        def run_named_pipeline_streaming(
            self,
            pipeline_name: str,
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
            del cwd, on_stdout, on_stderr, should_cancel, timeout_seconds, on_timeout
            self.named_runs.append(pipeline_name)
            assert env is not None
            assert on_poll is not None
            _write_direct_runtime_sidecar(env)
            on_poll()
            intent = cast(
                dict[str, object],
                json.loads(env["CLIO_RELAY_RUNTIME_SUBMISSION_INTENT"]),
            )
            runtime_path = Path(env["CLIO_RELAY_RUNTIME_METADATA_FILE"])
            with runtime_path.open("a", encoding="utf-8") as stream:
                stream.write(
                    json.dumps(
                        runtime_sidecar_record(
                            {
                                "schema_version": "jarvis.runtime.v1",
                                "execution_id": intent["execution_id"],
                                "scheduler_provider": "test-scheduler",
                                "scheduler_job_id": "must-not-be-owned",
                                "scheduler_phase": "submitted",
                                "details": {
                                    "scheduler_submission": {
                                        "schema_version": "jarvis.scheduler.submission.v1",
                                        "provider": "test-scheduler",
                                        "scheduler_job_id": "must-not-be-owned",
                                        "identity_source": "scheduler_submit_api",
                                        "submitted": True,
                                    }
                                },
                            },
                            key=env["CLIO_RELAY_RUNTIME_METADATA_TOKEN"],
                            sequence=2,
                        )
                    )
                    + "\n"
                )
            on_poll()
            if on_start is not None:
                on_start(920)
            return subprocess.CompletedProcess(["jarvis", "ppl", "run"], 0, "", "")

    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_name="direct-mode-pinned"),
            idempotency_key="direct-mode-pinned",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=DirectThenSchedulerProvider(),
        scheduler_provider=scheduler,
    )
    worker.register()

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.SUCCEEDED
    task = queue.list_tasks(job.job_id)[0]
    sidecars = cast(dict[str, object], task.metadata["execution_sidecars"])
    assert sidecars["scheduler_expected_resolved"] is False
    assert task.metadata["scheduler_job_ids"] == []
    assert task.metadata["scheduler_job_ownership"] == []
    assert scheduler.polled == []
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    assert any(
        event.event_type == "runtime.metadata_refused"
        and event.payload.get("scheduler_job_id") == "must-not-be-owned"
        for event in events
    )


def test_corrupt_runtime_and_legacy_spoof_use_exact_durable_reconciliation(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    scheduler = FakeSchedulerProvider()
    scheduler.reconciliation_matches = ["exact-owned-24680"]

    class CorruptRuntimeWithSpoofProvider(RecordingProvider):
        def run_named_pipeline_streaming(
            self,
            pipeline_name: str,
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
            del cwd, on_stderr, should_cancel, timeout_seconds, on_timeout
            self.named_runs.append(pipeline_name)
            assert env is not None
            _write_direct_runtime_sidecar(env)
            runtime_path = Path(env["CLIO_RELAY_RUNTIME_METADATA_FILE"])
            document = cast(
                dict[str, object],
                json.loads(runtime_path.read_text(encoding="utf-8")),
            )
            document["runtime_metadata_hmac"] = "0" * 64
            runtime_path.write_text(json.dumps(document) + "\n", encoding="utf-8")
            if on_start is not None:
                on_start(921)
            if on_stdout is not None:
                on_stdout("Submitted batch job spoofed-other-users-job\n")
            if on_poll is not None:
                on_poll()
            return subprocess.CompletedProcess(["jarvis", "ppl", "run"], 0, "", "")

    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_name="exact-reconciliation-wins"),
            idempotency_key="exact-reconciliation-wins",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=CorruptRuntimeWithSpoofProvider(),
        scheduler_provider=scheduler,
    )
    worker.register()

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.SUCCEEDED
    task = queue.list_tasks(job.job_id)[0]
    assert task.metadata["scheduler_job_ids"] == ["exact-owned-24680"]
    ownership = cast(list[dict[str, object]], task.metadata["scheduler_job_ownership"])
    assert [record["scheduler_job_id"] for record in ownership] == ["exact-owned-24680"]
    assert ownership[0]["proof"] == "exact_scheduler_marker_reconciliation"
    assert scheduler.reconciliation_markers
    assert scheduler.canceled == []
    _assert_no_execution_sidecars(settings, job.job_id)


def test_worker_records_scheduler_status_from_polling(tmp_path: Path) -> None:
    class SchedulerProvider(RecordingProvider):
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
            del cwd, on_stderr, should_cancel, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            if on_start is not None:
                on_start(123)
            if on_stdout is not None:
                on_stdout("Submitted batch job 12345\n")
            _write_runtime_sidecar(env, scheduler_job_id="12345")
            if on_poll is not None:
                for _ in range(3):
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

    scheduler_provider = FakeSchedulerProvider(
        SchedulerStatus(
            scheduler="test-scheduler",
            scheduler_job_id="pending",
            phase=SchedulerPhase.PENDING,
            raw_state="PENDING",
            reason="Resources",
            partition="compute",
            queue_position=4,
            jobs_ahead=3,
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=provider,
        scheduler_provider=scheduler_provider,
    )
    endpoint = worker.register()

    assert endpoint.metadata["scheduler_provider"] == "test-scheduler"

    result = worker.run_once()

    assert result is not None
    task = queue.list_tasks(job.job_id)[0]
    status = task.metadata["scheduler_status"]
    assert isinstance(status, dict)
    assert status["scheduler"] == "test-scheduler"
    assert status["scheduler_job_id"] == "12345"
    assert status["phase"] == "pending"
    assert status["jobs_ahead"] == 3
    assert scheduler_provider.polled == ["12345"]
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=50)
    assert "scheduler.pending" in [event.event_type for event in events]


def test_external_worker_rejects_inline_slurm_before_jarvis_launch(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    provider = RecordingProvider()
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(
                pipeline_yaml="""
name: scheduled
scheduler:
  name: slurm
pkgs: []
"""
            ),
            idempotency_key="external-worker-rejects-inline-slurm",
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
    assert result.state is JobState.FAILED
    assert result.last_error is not None
    assert "slurm != external" in result.last_error
    assert "no JARVIS execution was launched" in result.last_error
    assert provider.runs == []
    assert queue.list_artifacts(job.job_id) == []
    task = queue.list_tasks(job.job_id)[0]
    assert task.state is JobState.FAILED
    assert "scheduler_job_ids" not in task.metadata
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=50)
    assert "execution.started" not in [event.event_type for event in events]
    assert "scheduler.job_detected" not in [event.event_type for event in events]


def test_slurm_worker_rejects_other_inline_provider_before_jarvis_launch(
    tmp_path: Path,
) -> None:
    class SlurmSchedulerProvider(FakeSchedulerProvider):
        name = "slurm"

    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    provider = RecordingProvider()
    scheduler = SlurmSchedulerProvider()
    queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(
                pipeline_yaml="""
name: scheduled
scheduler:
  name: site-batch
pkgs: []
"""
            ),
            idempotency_key="slurm-worker-rejects-other-provider",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=provider,
        scheduler_provider=scheduler,
    )
    worker.register()

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.FAILED
    assert result.last_error is not None
    assert "site-batch != slurm" in result.last_error
    assert provider.runs == []
    assert scheduler.polled == []
    assert scheduler.canceled == []
    assert scheduler.reconciliation_markers == []


def test_external_worker_named_slurm_refusal_proves_zero_submission(
    tmp_path: Path,
) -> None:
    class ExternalSchedulerProvider(FakeSchedulerProvider):
        name = "external"

    class NamedScheduledPipeline:
        name = "site-scheduled-pipeline"

        def __init__(self) -> None:
            self.scheduler: dict[str, object] = {"name": "slurm", "job_name": "operator"}
            self.submit_calls: list[tuple[bool, bool, str]] = []

        def submit(self, *, submit: bool, wait: bool, execution_id: str) -> object:
            self.submit_calls.append((submit, wait, execution_id))
            raise AssertionError("scheduler submission must not be reached")

    scheduled_pipeline = NamedScheduledPipeline()

    class NamedScheduledProvider(RecordingProvider):
        def run_named_pipeline_streaming(
            self,
            pipeline_name: str,
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
            del cwd, on_stdout, on_stderr, on_start, should_cancel, on_poll
            del timeout_seconds, on_timeout
            self.named_runs.append(pipeline_name)
            assert env is not None
            intent = cast(
                dict[str, object],
                json.loads(env["CLIO_RELAY_RUNTIME_SUBMISSION_INTENT"]),
            )
            sequence = 0

            def append_runtime_record(record: dict[str, Any]) -> None:
                nonlocal sequence
                sequence += 1
                with Path(env["CLIO_RELAY_RUNTIME_METADATA_FILE"]).open(
                    "a", encoding="utf-8", newline="\n"
                ) as handle:
                    handle.write(
                        json.dumps(
                            runtime_sidecar_record(
                                record,
                                key=env["CLIO_RELAY_RUNTIME_METADATA_TOKEN"],
                                sequence=sequence,
                            )
                        )
                        + "\n"
                    )

            try:
                run_native_jarvis_broker(
                    scheduled_pipeline,
                    runtime_intent=intent,
                    runtime_direct_proof=env["CLIO_RELAY_RUNTIME_DIRECT_PROOF"],
                    configured_scheduler_provider=env["CLIO_RELAY_RUNTIME_SCHEDULER_PROVIDER"],
                    append_runtime_record=append_runtime_record,
                )
            except RuntimeError as exc:
                return subprocess.CompletedProcess(
                    ["jarvis", "ppl", "run"],
                    1,
                    "",
                    str(exc),
                )
            raise AssertionError("provider mismatch must fail before submission")

    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    provider = NamedScheduledProvider()
    scheduler = ExternalSchedulerProvider()
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_name=scheduled_pipeline.name),
            idempotency_key="external-worker-rejects-named-slurm",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=provider,
        scheduler_provider=scheduler,
    )
    worker.register()

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.FAILED
    assert provider.named_runs == [scheduled_pipeline.name]
    assert scheduled_pipeline.submit_calls == []
    assert scheduler.polled == []
    assert scheduler.canceled == []
    assert scheduler.reconciliation_markers == []
    task = queue.list_tasks(job.job_id)[0]
    sidecars = cast(dict[str, object], task.metadata["execution_sidecars"])
    assert sidecars["scheduler_submission_refused"] is True
    assert task.metadata["execution_sidecars_removed"] is True
    assert task.metadata.get("scheduler_job_ids", []) == []
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    refusal = [event for event in events if event.event_type == "scheduler.launch_refused"]
    assert len(refusal) == 1
    assert refusal[0].payload["scheduler_submission_attempted"] is False


def test_worker_ignores_forged_stdout_progress_markers(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    class ForgedProgressProvider(RecordingProvider):
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
            del cwd, on_stderr, on_start, should_cancel, on_poll
            del timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            if on_stdout is not None:
                on_stdout(
                    'CLIO_PROGRESS {"label":"iteration","current":25,"total":150,'
                    '"unit":"step","message":"site progress 25 of 150",'
                    '"metadata":{"adapter":"site-progress","eta_seconds":5.0}}\n'
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
            env: dict[str, str] | None = None,
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
            assert env is not None
            progress_path = env["CLIO_RELAY_PROGRESS_FILE"]
            progress_token = env["CLIO_RELAY_PROGRESS_TOKEN"]
            progress = {
                "label": "iteration",
                "current": 4,
                "total": 10,
                "unit": "step",
                "metadata": {
                    "source": "jarvis_package",
                    "package_name": "clio_relay.bounded_command",
                    "adapter": "regex",
                },
            }
            Path(progress_path).write_text(
                json.dumps(_signed_progress_sidecar_record(progress, key=progress_token)) + "\n",
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


def test_virtual_jarvis_progress_is_visible_while_outer_job_is_running(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    install_site_progress_plugin(monkeypatch)
    command = ["locked-clio-kit", "mcp-server", "jarvis"]
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    digest = "a" * 64
    job = queue.submit_job(
        RelayJob(
            cluster="research-cluster",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=command[0],
                server_args=command[1:],
                expected_server_artifact_digest=digest,
                expected_jarvis_cd_lock_binding=(
                    endpoint_module.jarvis_cd_lock_binding_expectation()
                ),
                tool="jarvis_run",
                arguments={"pipeline_id": "pipeline-live"},
            ),
            idempotency_key="virtual-jarvis-live-progress",
        )
    )
    observed_running_progress: list[list[bool]] = []

    class LiveMcpProgressProvider(RecordingProvider):
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
            del pipeline_path, cwd, on_stdout, on_stderr, should_cancel
            del timeout_seconds, on_timeout
            assert env is not None
            assert on_poll is not None
            if on_start is not None:
                on_start(910)
            progress_path = Path(env["CLIO_RELAY_PROGRESS_FILE"])
            token = env["CLIO_RELAY_PROGRESS_TOKEN"]
            initial = _virtual_mcp_progress_record(
                digest=digest,
                execution_validated=False,
            )
            progress_path.write_text(
                json.dumps(_signed_progress_sidecar_record(initial, key=token)) + "\n",
                encoding="utf-8",
            )
            on_poll()
            assert queue.get_job(job.job_id).state is JobState.RUNNING
            observed_running_progress.append(
                [
                    bool(item.metadata["acceptance_validated"])
                    for item in queue.list_progress(job.job_id)
                ]
            )
            final = _virtual_mcp_progress_record(
                digest=digest,
                execution_validated=True,
            )
            with progress_path.open("a", encoding="utf-8") as stream:
                stream.write(
                    json.dumps(_signed_progress_sidecar_record(final, key=token, sequence=2)) + "\n"
                )
            on_poll()
            return subprocess.CompletedProcess(["jarvis"], 0, "", "")

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="research-cluster",
        queue=queue,
        provider=LiveMcpProgressProvider(),
    )

    result = worker.run_once()
    progress = queue.list_progress(job.job_id)

    assert result is not None
    assert result.state is JobState.SUCCEEDED
    assert observed_running_progress == [[False]]
    assert [item.metadata["acceptance_validated"] for item in progress] == [False, True]
    assert all(item.metadata["provider_validated"] is True for item in progress)
    assert progress[-1].metadata["provider_execution_id"] == "jarvis-execution-live"
    assert progress[-1].metadata["provider_server_artifact_digest"] == digest
    assert progress[-1].metadata["provider_source_authority"] == "mcp_progress_notification"


def test_virtual_jarvis_progress_rejects_provider_identity_mismatch(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    install_site_progress_plugin(monkeypatch)
    command = ["locked-clio-kit", "mcp-server", "jarvis"]
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    digest = "b" * 64
    job = queue.submit_job(
        RelayJob(
            cluster="research-cluster",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=command[0],
                server_args=command[1:],
                expected_server_artifact_digest=digest,
                expected_jarvis_cd_lock_binding=(
                    endpoint_module.jarvis_cd_lock_binding_expectation()
                ),
                tool="jarvis_run",
                arguments={"pipeline_id": "pipeline-live"},
            ),
            idempotency_key="virtual-jarvis-provider-mismatch",
        )
    )

    class MismatchedMcpProgressProvider(RecordingProvider):
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
            del pipeline_path, cwd, on_stdout, on_stderr, should_cancel
            del timeout_seconds, on_timeout
            assert env is not None
            record = _virtual_mcp_progress_record(
                digest=digest,
                execution_validated=True,
            )
            bridge = cast(dict[str, Any], record["metadata"])["mcp_progress_bridge"]
            assert isinstance(bridge, dict)
            raw_provider = cast(dict[str, Any], bridge)["provider"]
            assert isinstance(raw_provider, dict)
            provider = cast(dict[str, Any], raw_provider)
            provider["distribution_version"] = "malicious-version"
            Path(env["CLIO_RELAY_PROGRESS_FILE"]).write_text(
                json.dumps(
                    _signed_progress_sidecar_record(
                        record,
                        key=env["CLIO_RELAY_PROGRESS_TOKEN"],
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            if on_poll is not None:
                on_poll()
            return subprocess.CompletedProcess(["jarvis"], 0, "", "")

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="research-cluster",
        queue=queue,
        provider=MismatchedMcpProgressProvider(),
    )

    result = worker.run_once()
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)

    assert result is not None
    assert result.state is JobState.FAILED
    assert result.last_error is not None
    assert "provider identity did not match" in result.last_error
    assert queue.list_progress(job.job_id) == []
    assert any(
        event.event_type == "progress.parse_failed"
        and "provider identity did not match" in event.message
        for event in events
    )


def test_virtual_jarvis_native_progress_accepts_indeterminate_event_without_adapter(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    command = ["locked-clio-kit", "mcp-server", "jarvis"]
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    digest = "c" * 64
    job = queue.submit_job(
        RelayJob(
            cluster="research-cluster",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=command[0],
                server_args=command[1:],
                expected_server_artifact_digest=digest,
                expected_jarvis_cd_lock_binding=(
                    endpoint_module.jarvis_cd_lock_binding_expectation()
                ),
                tool="jarvis_run",
                arguments={"pipeline_id": "pipeline-live"},
            ),
            idempotency_key="virtual-jarvis-native-progress",
        )
    )

    class NativeMcpProgressProvider(RecordingProvider):
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
            del pipeline_path, cwd, on_stdout, on_stderr, should_cancel
            del timeout_seconds, on_timeout
            assert env is not None
            if on_start is not None:
                on_start(912)
            record = _virtual_native_mcp_progress_record(digest=digest)
            Path(env["CLIO_RELAY_PROGRESS_FILE"]).write_text(
                json.dumps(
                    _signed_progress_sidecar_record(
                        record,
                        key=env["CLIO_RELAY_PROGRESS_TOKEN"],
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            if on_poll is not None:
                on_poll()
            return subprocess.CompletedProcess(["jarvis"], 0, "", "")

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="research-cluster",
        queue=queue,
        provider=NativeMcpProgressProvider(),
    )

    result = worker.run_once()
    progress = queue.list_progress(job.job_id)

    assert result is not None
    assert result.state is JobState.SUCCEEDED
    assert len(progress) == 1
    assert progress[0].current is None
    assert progress[0].total is None
    assert progress[0].source == "jarvis_execution"
    assert progress[0].metadata["relay_job_id"] == job.job_id
    assert progress[0].metadata["execution_id"] == "native-execution-live"
    assert progress[0].metadata["pipeline_id"] == "pipeline-live"
    assert progress[0].metadata["package_name"] == "builtin.paraview"
    assert progress[0].metadata["package_id"] == "server"
    assert progress[0].metadata["progress_state"] == "ready"
    assert progress[0].metadata["progress_determinate"] is False
    assert progress[0].metadata["execution_binding_validated"] is True


def _virtual_mcp_progress_record(
    *,
    digest: str,
    execution_validated: bool,
) -> dict[str, object]:
    return {
        "label": "iteration",
        "current": 5.0,
        "total": 10.0,
        "unit": "step",
        "message": "iteration 5",
        "metadata": {
            "adapter": "site-progress",
            "package_name": "site.simulation",
            "package_version": "test-plugin",
            "run_id": "jarvis-execution-live",
            "execution_id": "jarvis-execution-live",
            "prediction_status": "observed",
            "eta_seconds": 5.0,
            "mcp_progress_bridge": {
                "schema_version": "clio-relay.mcp-package-progress-bridge.v1",
                "execution_id": "jarvis-execution-live",
                "pipeline_id": "pipeline-live",
                "notification_sequence": 2 if execution_validated else 1,
                "source_authority": "package_log",
                "provider": {
                    "entry_point": "site-progress",
                    "entry_point_value": ("tests.plugin_fakes:site_progress_adapter_from_package"),
                    "distribution": "site-progress-plugin",
                    "distribution_version": "3.4.5",
                    "adapter": "site-progress",
                    "package_name": "site.simulation",
                    "package_version": "test-plugin",
                    "application_profile": "site-stack",
                },
                "provider_acceptance_validated": True,
                "expected_server_artifact_digest": digest,
                "observed_server_artifact_digest": digest,
                "execution_validated": execution_validated,
            },
        },
    }


def _virtual_native_mcp_progress_record(*, digest: str) -> dict[str, object]:
    return {
        "label": "server readiness",
        "message": "ParaView server is ready",
        "metadata": {
            "mode": "server",
            "mcp_native_progress_bridge": {
                "schema_version": "clio-relay.mcp-jarvis-progress-bridge.v1",
                "execution_id": "native-execution-live",
                "pipeline_id": "pipeline-live",
                "execution_state": "running",
                "terminal": False,
                "transport_sequence": 1,
                "package_name": "builtin.paraview",
                "package_id": "server",
                "event_count": 1,
                "event_schema_version": "jarvis.progress.v1",
                "event_sequence": 0,
                "event_state": "ready",
                "observed_at_epoch": 1_789_000_000.0,
                "determinate": False,
                "skipped_event_count": 0,
                "expected_server_artifact_digest": digest,
                "observed_server_artifact_digest": digest,
                "execution_validated": True,
            },
        },
    }


def _native_mcp_result_document(
    *,
    command: list[str],
    digest: str,
    pipeline_id: str,
    server_artifact: dict[str, Any],
) -> dict[str, object]:
    execution_id = f"{pipeline_id}-execution"
    handle: dict[str, object] = {
        "schema_version": "jarvis.execution.handle.v1",
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "mode": "direct",
        "scheduler_provider": None,
        "scheduler_native_id": None,
        "cluster": None,
    }
    record: dict[str, object] = {
        "schema_version": "jarvis.execution.record.v1",
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "pipeline_name": pipeline_id,
        "mode": "direct",
        "scheduler_provider": None,
        "scheduler_native_id": None,
        "cluster": None,
        "state": "completed",
        "submitted": False,
        "terminal": True,
        "created_at": "2026-07-12T10:00:00Z",
        "updated_at": "2026-07-12T10:00:01Z",
        "return_code": 0,
        "error": None,
        "metadata": {},
    }
    progress: dict[str, object] = {
        "schema_version": "jarvis.execution.progress.v1",
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "execution_state": "completed",
        "terminal": True,
        "packages": [],
    }
    structured: dict[str, object] = {
        "execution_handle": handle,
        "execution_record": record,
        "progress": progress,
        "runtime_metadata": {
            "schema_version": "jarvis.runtime.v1",
            "source": "jarvis_mcp",
            "execution_id": execution_id,
            "pipeline_id": pipeline_id,
            "mode": "direct",
            "scheduler_provider": None,
            "scheduler_native_id": None,
            "cluster": None,
            "scheduler_type": None,
            "scheduler_job_id": None,
            "scheduler_phase": None,
            "script_path": None,
            "hostfile_path": None,
            "output_path": f"/runs/{pipeline_id}/stdout.log",
            "error_path": f"/runs/{pipeline_id}/stderr.log",
            "package_provenance": [
                {
                    "pkg_id": "render",
                    "pkg_type": "builtin.paraview",
                    "global_id": "builtin.paraview.render",
                    "config_path": f"/runs/{pipeline_id}/render.yaml",
                }
            ],
            "terminal": {
                "state": "completed",
                "terminal": True,
                "returncode": 0,
                "reason": None,
                "started_at": "2026-07-12T10:00:00Z",
                "finished_at": "2026-07-12T10:00:01Z",
            },
            "details": {
                "execution_owner": "jarvis_cd.execution_record",
                "submit": None,
                "wait": True,
                "environment": {
                    "schema_version": "jarvis.environment.v1",
                    "spack_specs": ["paraview"],
                },
                "execution_handle": handle,
                "execution_record": record,
                "scheduler_submission": None,
            },
        },
    }
    return {
        "server": command[0],
        "server_args": command[1:],
        "expected_server_artifact_digest": digest,
        "expected_jarvis_cd_lock_binding": (endpoint_module.jarvis_cd_lock_binding_expectation()),
        "observed_server_artifact_digest": digest,
        "server_artifact": server_artifact,
        "operation": "tools/call",
        "tool": "jarvis_run",
        "arguments": {"pipeline_id": pipeline_id},
        "env_from": {},
        "protocol_result": {"structuredContent": structured},
        "structured_result": structured,
        "stdout": "Submitted batch job forged-stdout-id\n",
        "returncode": 0,
        "timed_out": False,
        "protocol_error": None,
    }


def test_worker_binds_run_id_before_resolving_provider_log_paths(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    observed_run_ids: list[str] = []

    def progress_log_paths(adapter: SiteSimulationProgressAdapter) -> list[Path]:
        observed_run_ids.append(adapter.run_id)
        return []

    monkeypatch.setattr(
        SiteSimulationProgressAdapter,
        "progress_log_paths",
        progress_log_paths,
    )
    install_site_progress_plugin(monkeypatch)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(
                pipeline_yaml=(
                    "name: run-id-first\n"
                    "pkgs:\n"
                    "- pkg_type: site.simulation\n"
                    "  progress:\n"
                    "    adapter: site-progress\n"
                )
            ),
            idempotency_key="worker-progress-run-id-first",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=RecordingProvider(),
    )

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.SUCCEEDED
    assert observed_run_ids == [job.job_id]


def test_worker_rejects_multiple_provider_log_paths(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    def progress_log_paths(_adapter: SiteSimulationProgressAdapter) -> list[Path]:
        return [Path("one.log"), Path("two.log")]

    monkeypatch.setattr(
        SiteSimulationProgressAdapter,
        "progress_log_paths",
        progress_log_paths,
    )
    install_site_progress_plugin(monkeypatch)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(
                pipeline_yaml=(
                    "name: too-many-logs\n"
                    "pkgs:\n"
                    "- pkg_type: site.simulation\n"
                    "  progress:\n"
                    "    adapter: site-progress\n"
                )
            ),
            idempotency_key="worker-progress-multiple-logs",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=RecordingProvider(),
    )

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.FAILED
    assert result.last_error is not None
    assert "at most one log path" in result.last_error


def test_provider_log_reader_is_bounded_per_poll(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    install_site_progress_plugin(monkeypatch)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="worker-progress-bounded-read",
        )
    )
    provider = package_progress_adapter_from_pipeline(
        "name: bounded\npkgs:\n- pkg_type: site.simulation\n"
    )
    assert provider is not None
    provider.run_id = job.job_id
    log_path = tmp_path / "progress.log"
    log_path.write_text("PROGRESS 3 10\n" * 10, encoding="utf-8")
    state = endpoint_module._PackageProgressLogState(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        path=log_path,
        offset=0,
        identity=None,
        checkpoint_offset=0,
        checkpoint_sha256=None,
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
    )

    consumed, at_eof = worker._ingest_package_progress_logs(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        job,
        provider,
        {log_path: state},
        max_bytes_per_path=8,
    )

    assert consumed == 8
    assert at_eof is False
    assert state.offset == 8


def test_provider_log_reader_rejects_non_regular_files(tmp_path: Path) -> None:
    directory = tmp_path / "not-a-log"
    directory.mkdir()

    with pytest.raises(ConfigurationError, match="not a regular file"):
        endpoint_module._open_package_progress_log(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            directory
        )


def test_provider_log_reader_rejects_symlinks_without_opening(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    path = tmp_path / "provider.log"
    path.write_text("progress\n", encoding="utf-8")
    real_stat = os.stat
    observed = real_stat(path)

    def symlink_stat(
        candidate: str | os.PathLike[str],
        *,
        follow_symlinks: bool = True,
    ) -> os.stat_result:
        if Path(candidate) == path and follow_symlinks is False:
            return os.stat_result(
                (
                    stat.S_IFLNK | 0o777,
                    observed.st_ino,
                    observed.st_dev,
                    observed.st_nlink,
                    observed.st_uid,
                    observed.st_gid,
                    observed.st_size,
                    observed.st_atime,
                    observed.st_mtime,
                    observed.st_mtime,
                )
            )
        return real_stat(candidate, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(endpoint_module.os, "stat", symlink_stat)

    with pytest.raises(ConfigurationError, match="symlinks are not allowed"):
        endpoint_module._open_package_progress_log(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            path
        )


def test_provider_log_reader_rejects_path_replacement_race(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    path = tmp_path / "provider.log"
    replacement = tmp_path / "replacement.log"
    path.write_text("first\n", encoding="utf-8")
    replacement.write_text("second\n", encoding="utf-8")
    real_open = os.open
    replaced = False

    def racing_open(candidate: str | os.PathLike[str], flags: int) -> int:
        nonlocal replaced
        if Path(candidate) == path and not replaced:
            replaced = True
            os.replace(replacement, path)
        return real_open(candidate, flags)

    monkeypatch.setattr(endpoint_module.os, "open", racing_open)

    with pytest.raises(ConfigurationError, match="changed while it was opened"):
        endpoint_module._open_package_progress_log(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            path
        )


def test_worker_rejects_side_channel_progress_without_token(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    class ForgedSideChannelProvider(RecordingProvider):
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
            del pipeline_path, cwd, on_stdout, on_stderr, on_start, should_cancel
            del on_poll, timeout_seconds, on_timeout
            assert env is not None
            progress_path = env["CLIO_RELAY_PROGRESS_FILE"]
            Path(progress_path).write_text(
                json.dumps(
                    {
                        "label": "timestep",
                        "current": 100,
                        "total": 100,
                        "metadata": {
                            "source": "jarvis_package",
                            "package_name": "site.simulation",
                            "package_version": "2.1",
                            "adapter": "site-progress",
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
            env: dict[str, str] | None = None,
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
            assert env is not None
            progress_path = env["CLIO_RELAY_PROGRESS_FILE"]
            progress_token = env["CLIO_RELAY_PROGRESS_TOKEN"]
            progress = {
                "label": "timestep",
                "current": 100,
                "total": 100,
                "metadata": {
                    "source": "jarvis_package",
                    "package_name": "site.simulation",
                    "package_version": "2.1",
                    "adapter": "site-progress",
                },
            }
            Path(progress_path).write_text(
                json.dumps(_signed_progress_sidecar_record(progress, key=progress_token)) + "\n",
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


def test_worker_uses_jarvis_stdout_fallback_and_only_its_eof_finalizer(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    finalize_calls = {"jarvis_stdout": 0, "package_log": 0}
    original_record = SiteSimulationProgressAdapter._record  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def spoofed_record(
        adapter: SiteSimulationProgressAdapter,
        line: str,
    ) -> dict[str, object] | None:
        record = original_record(adapter, line)
        if record is None:
            return None
        metadata_value = record["metadata"]
        assert isinstance(metadata_value, dict)
        metadata = cast(dict[str, object], metadata_value)
        metadata.update(
            {
                "adapter": "spoofed-adapter",
                "package_name": "spoofed.package",
                "package_version": "spoofed-version",
                "provider_entry_point": "spoofed-entry-point",
                "provider_distribution": "spoofed-distribution",
                "provider_distribution_version": "999",
                "provider_source_authority": "spoofed-source",
                "application_profile": "spoofed-profile",
                "provider_validated": False,
                "acceptance_validated": True,
            }
        )
        return record

    def finalize_jarvis_stdout(
        adapter: SiteSimulationProgressAdapter,
    ) -> list[dict[str, object]]:
        finalize_calls["jarvis_stdout"] += 1
        record = spoofed_record(adapter, "PROGRESS 75 100")
        assert record is not None
        return [record]

    def finalize_stdout(
        adapter: SiteSimulationProgressAdapter,
    ) -> list[dict[str, object]]:
        finalize_calls["package_log"] += 1
        record = spoofed_record(adapter, "PROGRESS 100 100")
        assert record is not None
        return [record]

    monkeypatch.setattr(SiteSimulationProgressAdapter, "_record", spoofed_record)
    monkeypatch.setattr(
        SiteSimulationProgressAdapter,
        "finalize_jarvis_stdout",
        finalize_jarvis_stdout,
    )
    monkeypatch.setattr(SiteSimulationProgressAdapter, "finalize_stdout", finalize_stdout)
    install_site_progress_plugin(monkeypatch)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    class SiteSimulationProvider(RecordingProvider):
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
            del cwd, on_stderr, on_start, should_cancel, on_poll
            del timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            if on_stdout is not None:
                on_stdout(
                    "[site.simulation] [START] BEGIN\n"
                    "PROGRESS 0 100\nPROGRESS 25 100\n"
                    "[site.simulation] [START] END\n"
                )
            return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="", stderr="")

    pipeline_yaml = "name: external\npkgs:\n- pkg_type: site.simulation\n"
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml=pipeline_yaml),
            idempotency_key="worker-external-progress",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=SiteSimulationProvider(),
    )
    worker.register()

    worker.run_once()

    progress = queue.list_progress(job.job_id)
    assert [item.current for item in progress] == [0, 25, 75]
    assert finalize_calls == {"jarvis_stdout": 1, "package_log": 0}
    assert progress[-1].metadata["source"] == "jarvis_package"
    assert progress[-1].metadata["package_name"] == "site.simulation"
    assert progress[-1].metadata["package_version"] == "test-plugin"
    assert progress[-1].metadata["run_id"] == job.job_id
    assert progress[-1].metadata["execution_id"] == job.job_id
    assert progress[-1].metadata["adapter"] == "site-progress"
    assert progress[-1].metadata["provider_entry_point"] == "site-progress"
    assert progress[-1].metadata["provider_distribution"] == "site-progress-plugin"
    assert progress[-1].metadata["provider_distribution_version"] == "3.4.5"
    assert progress[-1].metadata["provider_source_authority"] == "jarvis_stdout_fallback"
    assert progress[-1].metadata["application_profile"] == "site-stack"
    assert progress[-1].metadata["provider_validated"] is True
    assert progress[-1].metadata["acceptance_validated"] is True


def test_worker_persists_provider_valid_warming_progress_without_acceptance(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    install_site_progress_plugin(monkeypatch)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="worker-rejected-provider-progress",
        )
    )
    provider = package_progress_adapter_from_pipeline(
        "name: external\n"
        "pkgs:\n"
        "- pkg_type: site.simulation\n"
        "  progress:\n"
        "    adapter: site-progress\n"
    )
    assert provider is not None
    provider.run_id = job.job_id
    candidates = provider.observe_jarvis_stdout(
        "[site.simulation] [START] BEGIN\nPROGRESS 3 10\n[site.simulation] [START] END\n"
    )
    metadata = candidates[0]["metadata"]
    assert isinstance(metadata, dict)
    metadata["prediction_status"] = "claimed"
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
    )

    worker._append_package_progress_records(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        job,
        candidates,
        source_event_seq=None,
        package_progress_provider=provider,
        source_authority=endpoint_module.PackageProgressSourceAuthority.JARVIS_STDOUT_FALLBACK,
    )

    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=50)
    progress = queue.list_progress(job.job_id)
    assert len(progress) == 1
    assert progress[0].current == 3
    assert progress[0].metadata["provider_validated"] is True
    assert progress[0].metadata["acceptance_validated"] is False
    assert "progress.candidate_not_acceptance_validated" in {event.event_type for event in events}


@pytest.mark.parametrize(
    "pipeline_yaml",
    [
        (
            "name: inherited-container\n"
            "base_deploy_mode: container\n"
            "pkgs:\n"
            "- pkg_type: site.simulation\n"
            "  out: container-private-output\n"
            "  progress:\n"
            "    adapter: site-progress\n"
        ),
        (
            "name: node-local-host-log\n"
            "pkgs:\n"
            "- pkg_type: site.simulation\n"
            "  out: /tmp/node-local-output\n"
            "  progress:\n"
            "    adapter: site-progress\n"
        ),
    ],
    ids=["inherited-container", "node-local-tmp"],
)
def test_worker_uses_stdout_authority_when_provider_exposes_no_shared_log(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    pipeline_yaml: str,
) -> None:
    finalize_calls = {"jarvis_stdout": 0, "package_log": 0}

    def finalize_jarvis_stdout(
        _adapter: SiteSimulationProgressAdapter,
    ) -> list[dict[str, object]]:
        finalize_calls["jarvis_stdout"] += 1
        return []

    def finalize_stdout(
        _adapter: SiteSimulationProgressAdapter,
    ) -> list[dict[str, object]]:
        finalize_calls["package_log"] += 1
        return []

    monkeypatch.setattr(
        SiteSimulationProgressAdapter,
        "finalize_jarvis_stdout",
        finalize_jarvis_stdout,
    )
    monkeypatch.setattr(SiteSimulationProgressAdapter, "finalize_stdout", finalize_stdout)
    install_site_progress_plugin(monkeypatch)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    class ContainerProvider(RecordingProvider):
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
            del pipeline_path, cwd, env, on_stderr, on_start, should_cancel, on_poll
            del timeout_seconds, on_timeout
            if on_stdout is not None:
                on_stdout(
                    "[site.simulation] [START] BEGIN\n"
                    "PROGRESS 4 10\n"
                    "[site.simulation] [START] END\n"
                )
            return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="", stderr="")

    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml=pipeline_yaml),
            idempotency_key="worker-private-log-progress",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=ContainerProvider(),
    )
    worker.register()

    worker.run_once()

    progress = queue.list_progress(job.job_id)
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    bound_event = next(event for event in events if event.event_type == "progress.provider_bound")
    assert [item.current for item in progress] == [4]
    assert progress[0].metadata["provider_source_authority"] == "jarvis_stdout_fallback"
    assert bound_event.payload["provider_source_authority"] == "jarvis_stdout_fallback"
    assert finalize_calls == {"jarvis_stdout": 1, "package_log": 0}


def test_worker_uses_package_log_authority_without_dual_source_replay(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    finalize_calls = {"jarvis_stdout": 0, "package_log": 0}
    buffered_fragments: dict[int, str] = {}
    original_record = SiteSimulationProgressAdapter._record  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def observe_buffered_log(
        adapter: SiteSimulationProgressAdapter,
        text: str,
    ) -> list[dict[str, object]]:
        combined = buffered_fragments.pop(id(adapter), "") + text
        final_newline = combined.rfind("\n")
        if final_newline < 0:
            buffered_fragments[id(adapter)] = combined
            return []
        complete = combined[: final_newline + 1]
        buffered_fragments[id(adapter)] = combined[final_newline + 1 :]
        return [
            record
            for line in complete.splitlines()
            if (record := original_record(adapter, line)) is not None
        ]

    def finalize_jarvis_stdout(
        adapter: SiteSimulationProgressAdapter,
    ) -> list[dict[str, object]]:
        finalize_calls["jarvis_stdout"] += 1
        record = original_record(adapter, "PROGRESS 999 1000")
        assert record is not None
        return [record]

    def finalize_stdout(
        adapter: SiteSimulationProgressAdapter,
    ) -> list[dict[str, object]]:
        finalize_calls["package_log"] += 1
        fragment = buffered_fragments.pop(id(adapter), "")
        record = original_record(adapter, fragment)
        return [] if record is None else [record]

    monkeypatch.setattr(SiteSimulationProgressAdapter, "observe_stdout", observe_buffered_log)
    monkeypatch.setattr(
        SiteSimulationProgressAdapter,
        "finalize_jarvis_stdout",
        finalize_jarvis_stdout,
    )
    monkeypatch.setattr(SiteSimulationProgressAdapter, "finalize_stdout", finalize_stdout)
    install_site_progress_plugin(monkeypatch)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    output_dir = tmp_path / "site-output"

    class SiteLogProvider(RecordingProvider):
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
            del cwd, on_stderr, on_start, should_cancel, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            output_dir.mkdir(parents=True)
            if on_stdout is not None:
                on_stdout("[site.simulation] [START] BEGIN\nPROGRESS 0 100\nPROGRESS 50 100\n")
            (output_dir / "progress.log").write_text(
                "PROGRESS 0 100\nPROGRESS 50",
                encoding="utf-8",
            )
            if on_poll is not None:
                on_poll()
            if on_stdout is not None:
                on_stdout("PROGRESS 100 100\n[site.simulation] [START] END\n")
            with (output_dir / "progress.log").open("a", encoding="utf-8") as stream:
                stream.write(" 100\nPROGRESS 100 100")
            if on_poll is not None:
                on_poll()
            return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="", stderr="")

    pipeline_yaml = (
        f"name: external\npkgs:\n- pkg_type: site.simulation\n"
        f"  out: {output_dir.as_posix()}\n"
        "  progress:\n"
        "    adapter: site-progress\n"
        "    log_visibility: shared\n"
    )
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml=pipeline_yaml),
            idempotency_key="worker-external-log-progress",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=SiteLogProvider(),
    )
    worker.register()

    worker.run_once()

    progress = queue.list_progress(job.job_id)
    assert [item.current for item in progress] == [0, 50, 100]
    assert finalize_calls == {"jarvis_stdout": 0, "package_log": 1}
    assert progress[-1].source_event_seq is None
    assert progress[-1].metadata["adapter"] == "site-progress"
    assert progress[-1].metadata["prediction_status"] == "observed"
    assert progress[-1].metadata["provider_source_authority"] == "package_log"


@pytest.mark.parametrize(
    ("rewrite_mode", "expected_reason"),
    [("truncate", "truncated"), ("replace", "replaced")],
)
def test_worker_baselines_reused_relative_provider_log_and_detects_reset(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    rewrite_mode: str,
    expected_reason: str,
) -> None:
    reset_calls = 0
    original_reset = SiteSimulationProgressAdapter.reset_stdout

    def reset_stdout(adapter: SiteSimulationProgressAdapter) -> None:
        nonlocal reset_calls
        reset_calls += 1
        original_reset(adapter)

    monkeypatch.setattr(SiteSimulationProgressAdapter, "reset_stdout", reset_stdout)
    install_site_progress_plugin(monkeypatch)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    pipeline_yaml = (
        "name: reused-output\n"
        "pkgs:\n"
        "- pkg_type: site.simulation\n"
        "  out: relative-output\n"
        "  progress:\n"
        "    adapter: site-progress\n"
        "    log_visibility: shared\n"
    )
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml=pipeline_yaml),
            idempotency_key=f"worker-reused-progress-{rewrite_mode}",
        )
    )
    child_cwd = settings.spool_dir / job.job_id
    log_path = child_cwd / "relative-output" / "progress.log"
    log_path.parent.mkdir(parents=True)
    log_path.write_text("PROGRESS 900 1000\n" * 20, encoding="utf-8")

    class ReusedOutputProvider(RecordingProvider):
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
            del pipeline_path, env, on_stderr, on_start, should_cancel, timeout_seconds, on_timeout
            assert cwd is not None
            assert cwd.resolve() == child_cwd.resolve()
            if on_stdout is not None:
                on_stdout(
                    "[site.simulation] [START] BEGIN\n"
                    "PROGRESS 900 1000\n"
                    "[site.simulation] [START] END\n"
                )
            new_content = "PROGRESS 3 10\n"
            if rewrite_mode == "truncate":
                log_path.write_text(new_content, encoding="utf-8")
            else:
                replacement = log_path.with_suffix(".replacement")
                replacement.write_text(new_content, encoding="utf-8")
                os.replace(replacement, log_path)
            if on_poll is not None:
                on_poll()
            return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="", stderr="")

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=ReusedOutputProvider(),
    )
    worker.register()

    worker.run_once()

    progress = queue.list_progress(job.job_id)
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    reset_events = [event for event in events if event.event_type == "progress.provider_log_reset"]
    baseline_event = next(
        event for event in events if event.event_type == "progress.provider_log_baselined"
    )
    assert [item.current for item in progress] == [3]
    assert progress[0].metadata["provider_source_authority"] == "package_log"
    assert reset_calls == 1
    assert len(reset_events) == 1
    assert reset_events[0].payload["reason"] == expected_reason
    assert int(baseline_event.payload["prelaunch_size"]) > len("PROGRESS 3 10\n")


def test_worker_ignores_external_progress_outside_plugin_scope(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    install_site_progress_plugin(monkeypatch)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    class UnscopedSiteProvider(RecordingProvider):
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
            del cwd, on_stderr, on_start, should_cancel, on_poll, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            if on_stdout is not None:
                on_stdout("[clio_relay.remote_agent] [START] BEGIN\n")
                on_stdout("PROGRESS 0 100\nPROGRESS 25 100\n")
                on_stdout("[clio_relay.remote_agent] [START] END\n")
            return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="", stderr="")

    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(
                pipeline_yaml="name: external\npkgs:\n- pkg_type: site.simulation\n"
            ),
            idempotency_key="worker-unscoped-external-progress",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=UnscopedSiteProvider(),
    )
    worker.register()

    worker.run_once()

    assert queue.list_progress(job.job_id) == []


def test_worker_ignores_plugin_scope_from_mixed_pipeline(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    install_site_progress_plugin(monkeypatch)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    class FakeScopedSiteProvider(RecordingProvider):
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
            del cwd, on_stderr, on_start, should_cancel, on_poll, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            if on_stdout is not None:
                on_stdout(
                    "[site.simulation] [START] BEGIN\n"
                    "PROGRESS 0 100\nPROGRESS 25 100\n"
                    "[site.simulation] [START] END\n"
                )
            return subprocess.CompletedProcess(args=["jarvis"], returncode=0, stdout="", stderr="")

    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(
                pipeline_yaml=(
                    "name: mixed\npkgs:\n"
                    "- pkg_type: site.simulation\n"
                    "- pkg_type: clio_relay.bounded_command\n"
                    "  command: [echo, fake]\n"
                )
            ),
            idempotency_key="worker-fake-plugin-scope",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=FakeScopedSiteProvider(),
    )
    worker.register()

    worker.run_once()

    assert queue.list_progress(job.job_id) == []


def test_worker_preserves_canceled_state_and_scancels_when_requested(
    tmp_path: Path,
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
    scheduler_provider = FakeSchedulerProvider()

    class CancelingProvider(RecordingProvider):
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
            del cwd, on_stderr, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            if on_start is not None:
                on_start(456)
            if on_stdout is not None:
                on_stdout("Submitted batch job 12345\nstarted\n")
            _write_runtime_sidecar(env, scheduler_job_id="12345")
            assert env is not None
            Path(env["CLIO_RELAY_PROGRESS_FILE"]).write_text("", encoding="utf-8")
            if on_poll is not None:
                on_poll()
            cancel_job(queue, job.job_id, cancel_scheduler=True)
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
        scheduler_provider=scheduler_provider,
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
    assert scheduler_provider.canceled == ["12345"]
    tasks = queue.list_tasks(job.job_id)
    assert tasks
    assert tasks[0].metadata["scheduler"] == "test-scheduler"
    assert tasks[0].metadata["scheduler_job_ids"] == ["12345"]
    assert tasks[0].metadata["scheduler_job_ownership"][0]["ownership_verified"] is True
    assert tasks[0].metadata["scheduler_status"]["phase"] == "unknown"
    _assert_no_execution_sidecars(settings, job.job_id)


def test_worker_never_cancels_scheduler_identity_without_producer_schema(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="cancel-untrusted-runtime-identity",
        )
    )
    scheduler_provider = FakeSchedulerProvider()

    class UntrustedIdentityProvider(RecordingProvider):
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
            del cwd, on_stdout, on_stderr, on_start, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            _write_runtime_sidecar(
                env,
                scheduler_job_id="another-users-job",
                schema_version=None,
            )
            if on_poll is not None:
                on_poll()
            cancel_job(queue, job.job_id, cancel_scheduler=True)
            assert should_cancel is not None
            assert should_cancel() is True
            return subprocess.CompletedProcess(["jarvis"], -15, "", "")

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=UntrustedIdentityProvider(),
        scheduler_provider=scheduler_provider,
    )
    worker.register()

    result = worker.run_once()

    assert result is not None
    assert result.state == JobState.CANCELED
    assert scheduler_provider.canceled == []
    task = queue.list_tasks(job.job_id)[0]
    assert task.metadata["runtime_metadata_source"] == "untrusted_compatibility"
    assert task.metadata["scheduler_job_ids"] == []
    assert task.metadata["scheduler_job_ownership"] == []
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    assert "runtime.metadata_untrusted" in {event.event_type for event in events}
    assert "scheduler.cancel_requested" not in {event.event_type for event in events}


def test_conflicting_authoritative_sidecar_cannot_replace_cancellation_identity(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="conflicting-authoritative-sidecar",
        )
    )
    scheduler_provider = FakeSchedulerProvider()

    class ConflictingSidecarProvider(RecordingProvider):
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
            del cwd, on_stdout, on_stderr, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            if on_start is not None:
                on_start(456)
            _write_runtime_sidecar(env, scheduler_job_id="owned-123")
            assert env is not None
            assert on_poll is not None
            on_poll()
            sidecar = Path(env["CLIO_RELAY_RUNTIME_METADATA_FILE"])
            with sidecar.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        runtime_sidecar_record(
                            {
                                "schema_version": "jarvis.runtime.v1",
                                "execution_id": "execution-unrelated-999",
                                "scheduler_provider": "test-scheduler",
                                "scheduler_job_id": "unrelated-999",
                                "scheduler_phase": "running",
                                "details": {
                                    "scheduler_submission": {
                                        "schema_version": "jarvis.scheduler.submission.v1",
                                        "provider": "test-scheduler",
                                        "scheduler_job_id": "unrelated-999",
                                        "identity_source": "scheduler_submit_api",
                                        "submitted": True,
                                    }
                                },
                            },
                            key=env["CLIO_RELAY_RUNTIME_METADATA_TOKEN"],
                            sequence=2,
                        )
                    )
                    + "\n"
                )
            on_poll()
            cancel_job(queue, job.job_id, cancel_scheduler=True)
            assert should_cancel is not None
            assert should_cancel() is True
            return subprocess.CompletedProcess(["jarvis"], -15, "", "")

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=ConflictingSidecarProvider(),
        scheduler_provider=scheduler_provider,
    )
    worker.register()

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.CANCELED
    assert scheduler_provider.canceled == ["owned-123"]
    task = queue.list_tasks(job.job_id)[0]
    runtime = cast(dict[str, object], task.metadata["runtime_metadata"])
    assert runtime["execution_id"] == "execution-owned-123"
    assert runtime["scheduler_job_id"] == "owned-123"
    assert task.metadata["scheduler_job_ids"] == ["owned-123"]
    ownership = cast(list[dict[str, object]], task.metadata["scheduler_job_ownership"])
    assert [record["scheduler_job_id"] for record in ownership] == ["owned-123"]
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    refused = [event for event in events if event.event_type == "runtime.metadata_refused"]
    assert refused
    assert refused[-1].payload["scheduler_job_id"] == "unrelated-999"
    assert refused[-1].payload["ownership_verified"] is False


def test_worker_timeout_scancels_scheduler_job(
    tmp_path: Path,
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
    scheduler_provider = FakeSchedulerProvider(
        SchedulerStatus(
            scheduler="test-scheduler",
            scheduler_job_id="98765",
            phase=SchedulerPhase.CANCELED,
            raw_state="CANCELLED",
        )
    )

    class TimeoutProvider(RecordingProvider):
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
            del cwd, on_stderr, should_cancel
            self.runs.append(pipeline_path)
            if on_start is not None:
                on_start(789)
            if on_stdout is not None:
                on_stdout("Submitted batch job 98765\nstarted\n")
            _write_runtime_sidecar(env, scheduler_job_id="98765")
            assert env is not None
            Path(env["CLIO_RELAY_PROGRESS_FILE"]).write_text("", encoding="utf-8")
            if on_poll is not None:
                on_poll()
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
        scheduler_provider=scheduler_provider,
    )
    worker.register()

    result = worker.run_once()
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=50)

    assert result is not None
    assert result.state == JobState.FAILED
    assert "execution.timeout" in [event.event_type for event in events]
    assert scheduler_provider.canceled == ["98765"]
    _assert_no_execution_sidecars(settings, job.job_id)
    tasks = queue.list_tasks(job.job_id)
    assert tasks
    assert tasks[0].metadata["scheduler"] == "test-scheduler"
    assert tasks[0].metadata["scheduler_job_ids"] == ["98765"]
    assert tasks[0].metadata["scheduler_status"]["phase"] == "canceled"


def test_worker_reconciles_scheduler_cancel_request_after_restart(
    tmp_path: Path,
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
    task = RelayTask(
        job_id=job.job_id,
        name="jarvis.execution",
        state=JobState.RUNNING,
    )
    task = task.model_copy(
        update={
            "metadata": _owned_scheduler_metadata(
                relay_job_id=job.job_id,
                task_id=task.task_id,
                scheduler_job_id="24680",
            )
        }
    )
    queue.append_task(task)
    cancel_job(queue, job.job_id, cancel_scheduler=True)
    scheduler_provider = FakeSchedulerProvider(
        SchedulerStatus(
            scheduler="test-scheduler",
            scheduler_job_id="24680",
            phase=SchedulerPhase.CANCELED,
            raw_state="CANCELLED",
        )
    )

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=RecordingProvider(),
        scheduler_provider=scheduler_provider,
    )
    worker.register()

    assert worker.run_once() is None
    assert scheduler_provider.canceled == ["24680"]
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=50)
    event_types = [event.event_type for event in events]
    assert "scheduler.cancel_requested" in event_types
    assert "scheduler.canceled" in event_types


def test_worker_refuses_unowned_scheduler_cancel_after_restart(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="restart-unowned-cancel",
        )
    )
    queue.append_task(
        RelayTask(
            job_id=job.job_id,
            name="jarvis.execution",
            state=JobState.RUNNING,
            metadata={"scheduler": "slurm", "scheduler_job_ids": ["not-owned-24680"]},
        )
    )
    cancel_job(queue, job.job_id, cancel_scheduler=True)
    scheduler_provider = FakeSchedulerProvider()
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=RecordingProvider(),
        scheduler_provider=scheduler_provider,
    )
    worker.register()

    assert worker.run_once() is None
    assert scheduler_provider.canceled == []
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=50)
    refused = [event for event in events if event.event_type == "scheduler.cancel_refused"]
    assert refused
    assert refused[-1].payload == {
        "scheduler_job_id": "not-owned-24680",
        "metadata_source": "unverified_durable_metadata",
        "ownership_verified": False,
    }


def test_worker_retries_transient_scheduler_cancel_failure_with_backoff(
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
            idempotency_key="retry-transient-scheduler-cancel",
        )
    )
    task = RelayTask(
        job_id=job.job_id,
        name="jarvis.execution",
        state=JobState.RUNNING,
    )
    queue.append_task(
        task.model_copy(
            update={
                "metadata": _owned_scheduler_metadata(
                    relay_job_id=job.job_id,
                    task_id=task.task_id,
                    scheduler_job_id="retry-123",
                )
            }
        )
    )
    cancel_job(queue, job.job_id, cancel_scheduler=True)

    class FlakyScheduler(FakeSchedulerProvider):
        def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
            self.canceled.append(scheduler_job_id)
            if len(self.canceled) == 1:
                return subprocess.CompletedProcess(
                    ["scancel", scheduler_job_id],
                    1,
                    "",
                    "temporary scheduler outage",
                )
            return subprocess.CompletedProcess(["scancel", scheduler_job_id], 0, "", "")

    scheduler = FlakyScheduler(
        SchedulerStatus(
            scheduler="test-scheduler",
            scheduler_job_id="retry-123",
            phase=SchedulerPhase.CANCELED,
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=RecordingProvider(),
        scheduler_provider=scheduler,
    )
    worker.register()

    assert worker.run_once() is None
    assert scheduler.canceled == ["retry-123"]
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    failed = [event for event in events if event.event_type == "scheduler.cancel_failed"]
    assert failed[-1].payload["attempt"] == 1
    assert failed[-1].payload["retryable"] is True
    assert failed[-1].payload["retry_delay_seconds"] == 2.0

    assert worker.run_once() is None
    assert scheduler.canceled == ["retry-123"]

    monkeypatch.setattr(
        "clio_relay.endpoint.utc_now",
        lambda: failed[-1].created_at + timedelta(seconds=3),
    )
    assert worker.run_once() is None
    assert scheduler.canceled == ["retry-123", "retry-123"]
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    assert "scheduler.cancel_requested" in [event.event_type for event in events]


def test_worker_bounds_persistent_scheduler_cancel_failures(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="bounded-scheduler-cancel-retries",
        )
    )
    task = RelayTask(
        job_id=job.job_id,
        name="jarvis.execution",
        state=JobState.RUNNING,
    )
    queue.append_task(
        task.model_copy(
            update={
                "metadata": _owned_scheduler_metadata(
                    relay_job_id=job.job_id,
                    task_id=task.task_id,
                    scheduler_job_id="retry-456",
                )
            }
        )
    )
    cancel_job(queue, job.job_id, cancel_scheduler=True)

    class FailingScheduler(FakeSchedulerProvider):
        def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
            self.canceled.append(scheduler_job_id)
            return subprocess.CompletedProcess(
                ["scancel", scheduler_job_id],
                1,
                "",
                "persistent scheduler outage",
            )

    scheduler = FailingScheduler()
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=RecordingProvider(),
        scheduler_provider=scheduler,
    )
    worker.scheduler_cancel_retry_base_seconds = 0
    worker.register()

    for _ in range(10):
        assert worker.run_once() is None

    assert scheduler.canceled == ["retry-456"] * worker.scheduler_cancel_max_attempts
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    failed = [event for event in events if event.event_type == "scheduler.cancel_failed"]
    assert [event.payload["attempt"] for event in failed] == [1, 2, 3, 4, 5]
    assert failed[-1].payload["retryable"] is False


def test_scheduler_cancel_acceptance_remains_pending_until_terminal_confirmation(
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
            idempotency_key="scheduler-confirmation-pending",
        )
    )
    task = RelayTask(job_id=job.job_id, name="jarvis.execution", state=JobState.RUNNING)
    queue.append_task(
        task.model_copy(
            update={
                "metadata": _owned_scheduler_metadata(
                    relay_job_id=job.job_id,
                    task_id=task.task_id,
                    scheduler_job_id="confirm-123",
                )
            }
        )
    )
    cancel_job(queue, job.job_id, cancel_scheduler=True)
    scheduler = FakeSchedulerProvider(
        SchedulerStatus(
            scheduler="test-scheduler",
            scheduler_job_id="confirm-123",
            phase=SchedulerPhase.UNKNOWN,
            reason="cancellation is still propagating",
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=RecordingProvider(),
        scheduler_provider=scheduler,
    )
    worker.register()

    assert worker.run_once() is None
    pending = queue.get_scheduler_cancel_pending(job.job_id, cluster="ares")
    assert pending is not None
    assert pending.dispositions[0].state.value == "cancel_requested"
    assert pending.dispositions[0].next_attempt_at is not None

    scheduler.status = SchedulerStatus(
        scheduler="test-scheduler",
        scheduler_job_id="confirm-123",
        phase=SchedulerPhase.CANCELED,
    )
    monkeypatch.setattr(
        "clio_relay.endpoint.utc_now",
        lambda: cast(datetime, pending.dispositions[0].next_attempt_at) + timedelta(seconds=1),
    )
    assert worker.run_once() is None

    assert queue.get_scheduler_cancel_pending(job.job_id, cluster="ares") is None
    disposition = queue.get_scheduler_cancel_disposition(job.job_id, cluster="ares")
    assert disposition is not None
    assert disposition.dispositions[0].state.value == "canceled"


def test_failed_relay_wrapper_does_not_supersede_owned_scheduler_cancel(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="failed-wrapper-live-scheduler",
        )
    )
    queue.update_job_state(job.job_id, JobState.RUNNING)
    task = RelayTask(job_id=job.job_id, name="jarvis.execution", state=JobState.RUNNING)
    queue.append_task(
        task.model_copy(
            update={
                "metadata": _owned_scheduler_metadata(
                    relay_job_id=job.job_id,
                    task_id=task.task_id,
                    scheduler_job_id="orphan-456",
                )
            }
        )
    )
    cancel_job(queue, job.job_id, cancel_scheduler=True)
    queue.update_job_state(job.job_id, JobState.FAILED, error="wrapper failed")
    scheduler = FakeSchedulerProvider(
        SchedulerStatus(
            scheduler="test-scheduler",
            scheduler_job_id="orphan-456",
            phase=SchedulerPhase.CANCELED,
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=RecordingProvider(),
        scheduler_provider=scheduler,
    )
    worker.register()

    assert worker.run_once() is None
    assert scheduler.canceled == ["orphan-456"]
    assert queue.get_scheduler_cancel_disposition(job.job_id, cluster="ares") is not None


def test_scheduler_cancel_reconciliation_never_reconstructs_event_history(
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
            idempotency_key="long-stream-scheduler-cancel",
        )
    )
    task = RelayTask(
        job_id=job.job_id,
        name="jarvis.execution",
        state=JobState.RUNNING,
    )
    queue.append_task(
        task.model_copy(
            update={
                "metadata": _owned_scheduler_metadata(
                    relay_job_id=job.job_id,
                    task_id=task.task_id,
                    scheduler_job_id="long-stream-123",
                )
            }
        )
    )
    cancel_job(queue, job.job_id, cancel_scheduler=True)
    read_real_event_page = queue.read_event_page

    def fail_event_history_read(
        requested_job_id: str,
        *,
        next_seq: int = 1,
        limit: int = 100,
    ) -> tuple[list[RelayEvent], int]:
        del requested_job_id, next_seq, limit
        raise AssertionError("scheduler cancellation reconstructed event history")

    monkeypatch.setattr(queue, "read_event_page", fail_event_history_read)

    class FailingScheduler(FakeSchedulerProvider):
        def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
            self.canceled.append(scheduler_job_id)
            return subprocess.CompletedProcess(
                ["scancel", scheduler_job_id],
                1,
                "",
                "temporary scheduler outage",
            )

    scheduler = FailingScheduler(
        SchedulerStatus(
            scheduler="test-scheduler",
            scheduler_job_id="long-stream-123",
            phase=SchedulerPhase.CANCELED,
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=RecordingProvider(),
        scheduler_provider=scheduler,
    )
    worker.scheduler_cancel_retry_base_seconds = 0
    worker.register()

    assert worker.run_once() is None
    assert scheduler.canceled == ["long-stream-123"]
    real_events, _ = read_real_event_page(job.job_id, next_seq=1, limit=20)
    latest_failure = next(
        event for event in reversed(real_events) if event.event_type == "scheduler.cancel_failed"
    )
    assert latest_failure.payload["attempt"] == 1

    assert worker.run_once() is None
    assert scheduler.canceled == ["long-stream-123", "long-stream-123"]
    request = cast(
        dict[str, object],
        queue.get_job(job.job_id).metadata["cancellation_request"],
    )
    assert request["cancel_scheduler"] is True


def test_worker_does_not_scancel_relay_only_canceled_job_after_restart(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="restart-relay-only-cancel",
        )
    )
    task = queue.append_task(
        RelayTask(
            job_id=job.job_id,
            name="jarvis.execution",
            state=JobState.RUNNING,
            metadata={"scheduler": "slurm", "scheduler_job_ids": ["13579"]},
        )
    )
    del task
    cancel_job(queue, job.job_id)
    scheduler_provider = FakeSchedulerProvider()

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=RecordingProvider(),
        scheduler_provider=scheduler_provider,
    )
    worker.register()

    assert worker.run_once() is None
    assert scheduler_provider.canceled == []
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=50)
    event_types = [event.event_type for event in events]
    assert "job.cancel_requested" in event_types
    assert "scheduler.cancel_requested" not in event_types


def test_worker_run_once_heartbeats_existing_endpoint(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=RecordingProvider(),
    )
    endpoint = worker.register()
    stale_endpoint = EndpointRegistration(
        endpoint_id=endpoint.endpoint_id,
        role=endpoint.role,
        cluster=endpoint.cluster,
        hostname=endpoint.hostname,
        pid=endpoint.pid,
        registered_at=endpoint.registered_at,
        last_seen_at=utc_now() - timedelta(seconds=120),
        metadata=endpoint.metadata,
    )
    endpoint_path = queue.root / "endpoints" / f"{endpoint.endpoint_id}.json"
    endpoint_path.write_text(stale_endpoint.model_dump_json(indent=2), encoding="utf-8")

    assert worker.run_once() is None

    refreshed = queue.list_endpoints(cluster="ares")[0]
    assert refreshed.endpoint_id == endpoint.endpoint_id
    assert utc_now() - refreshed.last_seen_at < timedelta(seconds=5)


def test_worker_persists_unhandled_job_failure_and_runs_next_job(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    bad_job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["bad-validation-command"]),
            idempotency_key="worker-unhandled-bad-job",
        )
    )
    good_job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["good-validation-command"]),
            idempotency_key="worker-after-unhandled-good-job",
        )
    )

    class SelectiveFailureProvider(RecordingProvider):
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
            del cwd, on_stderr, should_cancel, on_poll, timeout_seconds, on_timeout
            assert env is not None
            Path(env["CLIO_RELAY_PROGRESS_FILE"]).write_text("", encoding="utf-8")
            Path(env["CLIO_RELAY_RUNTIME_METADATA_FILE"]).write_text("", encoding="utf-8")
            pipeline = pipeline_path.read_text(encoding="utf-8")
            if "bad-validation-command" in pipeline:
                raise ValueError("malformed validation pipeline")
            if on_start is not None:
                on_start(900)
            if on_stdout is not None:
                on_stdout("good job ran\n")
            return subprocess.CompletedProcess(["jarvis"], 0, "good job ran\n", "")

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=SelectiveFailureProvider(),
    )
    worker.register()

    failed = worker.run_once()
    succeeded = worker.run_once()

    assert failed is not None
    assert failed.job_id == bad_job.job_id
    assert failed.state is JobState.FAILED
    assert failed.last_error == "ValueError: malformed validation pipeline"
    failed_tasks = queue.list_tasks(bad_job.job_id)
    assert failed_tasks[0].state is JobState.FAILED
    assert failed_tasks[0].metadata["worker_error"] == ("ValueError: malformed validation pipeline")
    _assert_no_execution_sidecars(settings, bad_job.job_id)
    failed_events, _ = queue.drain_events(Cursor(job_id=bad_job.job_id), limit=100)
    assert "job.failed" in [event.event_type for event in failed_events]
    assert queue.list_leases(cluster="ares") == []
    assert succeeded is not None
    assert succeeded.job_id == good_job.job_id
    assert succeeded.state is JobState.SUCCEEDED


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
            env: dict[str, str] | None = None,
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


def test_worker_poll_heartbeats_live_slot_before_stale_cleanup(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["long-running"]),
            idempotency_key="long-running-slot-heartbeat",
        )
    )
    observations: dict[str, object] = {}
    worker: EndpointWorker

    class LongRunningProvider(RecordingProvider):
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
            del cwd, env, on_stdout, on_stderr, should_cancel, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            assert worker.endpoint is not None
            if on_start is not None:
                on_start(789)
            endpoint_path = queue.root / "endpoints" / f"{worker.endpoint.endpoint_id}.json"
            stale_endpoint = worker.endpoint.model_copy(
                update={"last_seen_at": utc_now() - timedelta(seconds=120)}
            )
            endpoint_path.write_text(
                stale_endpoint.model_dump_json(indent=2),
                encoding="utf-8",
            )
            observations["before"] = diagnose_job(
                queue,
                job.job_id,
                cluster="ares",
                stale_after_seconds=60,
            )["reason"]
            assert on_poll is not None
            on_poll()
            observations["cleanup"] = cleanup_stale_jobs(
                queue,
                cluster="ares",
                older_than_seconds=60,
                dry_run=False,
            )
            return subprocess.CompletedProcess(["jarvis"], 0, "", "")

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=LongRunningProvider(),
    )
    worker.lease_renew_seconds = 0
    endpoint = worker.register()

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.SUCCEEDED
    assert observations["before"] == "stale_ownership"
    cleanup = cast(dict[str, object], observations["cleanup"])
    assert cleanup["planned"] == []
    assert cleanup["canceled_count"] == 0
    assert "cancellation_request" not in queue.get_job(job.job_id).metadata
    refreshed = queue.get_endpoint(endpoint.endpoint_id)
    assert refreshed is not None
    assert utc_now() - refreshed.last_seen_at < timedelta(seconds=5)


def test_worker_poll_rejects_replaced_endpoint_generation(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["long-running"]),
            idempotency_key="replaced-slot-generation",
        )
    )
    worker: EndpointWorker

    class ReplacedGenerationProvider(RecordingProvider):
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
            del cwd, env, on_stdout, on_stderr, should_cancel, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            assert worker.endpoint is not None
            if on_start is not None:
                on_start(790)
            endpoint_path = queue.root / "endpoints" / f"{worker.endpoint.endpoint_id}.json"
            replacement = worker.endpoint.model_copy(
                update={"registered_at": utc_now() + timedelta(seconds=1)}
            )
            endpoint_path.write_text(replacement.model_dump_json(indent=2), encoding="utf-8")
            assert on_poll is not None
            on_poll()
            return subprocess.CompletedProcess(["jarvis"], 0, "", "")

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        provider=ReplacedGenerationProvider(),
    )
    worker.lease_renew_seconds = 0
    endpoint = worker.register()

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.FAILED
    assert result.last_error is not None
    assert "endpoint identity or registration generation changed" in result.last_error
    preserved = queue.get_endpoint(endpoint.endpoint_id)
    assert preserved is not None
    assert preserved.registered_at != endpoint.registered_at


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
            env: dict[str, str] | None = None,
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


def test_worker_prefers_structured_jarvis_mcp_runtime_metadata(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    command = ["clio-kit", "mcp-server", "jarvis"]
    server_args = command[1:]
    server_artifact = verified_jarvis_server_artifact()
    digest = remote_mcp_server_artifact_digest(server_artifact)
    monkeypatch.setattr(
        endpoint_module,
        "jarvis_mcp_command",
        lambda: command,
    )
    job = queue.submit_job(
        RelayJob(
            cluster="research-cluster",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=command[0],
                server_args=server_args,
                expected_server_artifact_digest=digest,
                expected_jarvis_cd_lock_binding=(
                    endpoint_module.jarvis_cd_lock_binding_expectation()
                ),
                tool="jarvis_run",
                arguments={"pipeline_id": "runtime-test"},
            ),
            idempotency_key="structured-jarvis-runtime",
        )
    )

    class RuntimeProvider(RecordingProvider):
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
            del on_stderr, should_cancel, on_poll, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            assert cwd is not None
            _write_native_mcp_transport_runtime_sidecar(env)
            if on_start is not None:
                on_start(700)
            if on_stdout is not None:
                on_stdout("Submitted batch job stdout-wrong\n")
            (cwd / "mcp-result.json").write_text(
                json.dumps(
                    {
                        "server": command[0],
                        "server_args": server_args,
                        "expected_server_artifact_digest": digest,
                        "expected_jarvis_cd_lock_binding": (
                            endpoint_module.jarvis_cd_lock_binding_expectation()
                        ),
                        "observed_server_artifact_digest": digest,
                        "server_artifact": server_artifact,
                        "operation": "tools/call",
                        "tool": "jarvis_run",
                        "arguments": {"pipeline_id": "runtime-test"},
                        "env_from": {},
                        "protocol_result": {
                            "structuredContent": {
                                "runtime_metadata": {
                                    "schema_version": "jarvis.runtime.v1",
                                    "execution_id": "jarvis-execution-9",
                                    "pipeline_id": "runtime-test",
                                    "scheduler": {
                                        "provider": "test-scheduler",
                                        "type": "batch",
                                        "job_id": "structured-42",
                                        "phase": "allocated",
                                        "allocated_nodes": ["compute-09"],
                                    },
                                    "script_path": "/runs/runtime-test/submit.sh",
                                    "hostfile_path": "/runs/runtime-test/hosts",
                                    "output_path": "/runs/runtime-test/job.out",
                                    "error_path": "/runs/runtime-test/job.err",
                                    "package_provenance": [
                                        {
                                            "package_name": "builtin.echo",
                                            "package_version": "2.2.6",
                                        }
                                    ],
                                    "terminal": {
                                        "state": "completed",
                                        "terminal": True,
                                        "returncode": 0,
                                    },
                                    "details": {
                                        "scheduler_submission": {
                                            "schema_version": ("jarvis.scheduler.submission.v1"),
                                            "provider": "test-scheduler",
                                            "scheduler_job_id": "structured-42",
                                            "identity_source": "scheduler_submit_api",
                                            "submitted": True,
                                        }
                                    },
                                }
                            }
                        },
                        "stdout": "Submitted batch job stdout-wrong\n",
                        "returncode": 0,
                        "timed_out": False,
                        "protocol_error": None,
                    }
                ),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(["jarvis"], 0, "", "")

    scheduler = FakeSchedulerProvider(
        SchedulerStatus(
            scheduler="test-scheduler",
            scheduler_job_id="structured-42",
            phase=SchedulerPhase.ALLOCATED,
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="research-cluster",
        queue=queue,
        provider=RuntimeProvider(),
        scheduler_provider=scheduler,
    )
    worker.register()

    result = worker.run_once()

    assert result is not None
    assert result.state == JobState.SUCCEEDED
    durable_job = queue.get_job(job.job_id)
    task = queue.list_tasks(job.job_id)[0]
    runtime = task.metadata["runtime_metadata"]
    assert isinstance(runtime, dict)
    assert durable_job.metadata["runtime_metadata_source"] == "jarvis_mcp"
    assert task.metadata["runtime_metadata_source"] == "jarvis_mcp"
    assert task.metadata["scheduler_job_ids"] == ["structured-42"]
    assert runtime["scheduler_job_id"] == "structured-42"
    assert runtime["allocated_nodes"] == ["compute-09"]
    assert runtime["packages"][0]["name"] == "builtin.echo"
    assert runtime["terminal"]["state"] == "completed"
    artifact_kinds = {artifact.kind for artifact in queue.list_artifacts(job.job_id)}
    assert "runtime_metadata" in artifact_kinds
    provenance = json.loads(
        (settings.spool_dir / job.job_id / "provenance.json").read_text(encoding="utf-8")
    )
    assert provenance["runtime_metadata"]["scheduler_job_id"] == "structured-42"
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    detected = [event for event in events if event.event_type == "scheduler.job_detected"]
    assert [event.payload["metadata_source"] for event in detected] == ["jarvis_mcp"]
    assert task.metadata["scheduler_job_ownership"][0]["ownership_verified"] is True
    transport_runtime = cast(
        dict[str, object],
        task.metadata["mcp_transport_runtime_metadata"],
    )
    assert transport_runtime["source"] == "jarvis_sidecar"
    assert transport_runtime["scheduler_job_id"] is None
    assert transport_runtime["execution_id"] != runtime["execution_id"]
    event_types = {event.event_type for event in events}
    assert "runtime.transport_metadata_superseded" in event_types
    assert "runtime.metadata_refused" not in event_types


def test_worker_native_direct_execution_discards_stdout_scheduler_fallback(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    command = ["locked-clio-kit", "mcp-server", "jarvis"]
    server_artifact = verified_jarvis_server_artifact()
    digest = remote_mcp_server_artifact_digest(server_artifact)
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    job = queue.submit_job(
        RelayJob(
            cluster="research-cluster",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=command[0],
                server_args=command[1:],
                expected_server_artifact_digest=digest,
                expected_jarvis_cd_lock_binding=(
                    endpoint_module.jarvis_cd_lock_binding_expectation()
                ),
                tool="jarvis_run",
                arguments={"pipeline_id": "native-direct"},
            ),
            idempotency_key="native-direct-runtime",
        )
    )

    class NativeRuntimeProvider(RecordingProvider):
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
            del pipeline_path, env, on_stderr, should_cancel, on_poll
            del timeout_seconds, on_timeout
            assert cwd is not None
            if on_start is not None:
                on_start(701)
            if on_stdout is not None:
                on_stdout("Submitted batch job forged-stdout-id\n")
            (cwd / "mcp-result.json").write_text(
                json.dumps(
                    _native_mcp_result_document(
                        command=command,
                        digest=digest,
                        pipeline_id="native-direct",
                        server_artifact=server_artifact,
                    )
                ),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(["jarvis"], 0, "", "")

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="research-cluster",
        queue=queue,
        provider=NativeRuntimeProvider(),
    )

    result = worker.run_once()
    task = queue.list_tasks(job.job_id)[0]
    runtime = cast(dict[str, Any], task.metadata["runtime_metadata"])

    assert result is not None
    assert result.state is JobState.SUCCEEDED
    assert runtime["execution_id"] == "native-direct-execution"
    assert runtime["scheduler_provider"] is None
    assert runtime["scheduler_job_id"] is None
    assert runtime["scheduler_phase"] is None
    assert runtime["output_path"] == "/runs/native-direct/stdout.log"
    assert runtime["error_path"] == "/runs/native-direct/stderr.log"
    assert runtime["packages"] == [
        {
            "name": "builtin.paraview",
            "version": None,
            "package_type": "builtin.paraview",
            "package_id": "render",
            "source": None,
            "path": "/runs/native-direct/render.yaml",
            "metadata": {"global_id": "builtin.paraview.render"},
        }
    ]
    assert runtime["details"]["environment"] == {
        "schema_version": "jarvis.environment.v1",
        "spack_specs": ["paraview"],
    }
    assert runtime["details"]["producer_contract"]["runtime_projection_merged"] is True
    assert task.metadata["scheduler"] is None
    assert task.metadata["scheduler_job_ids"] == []
    assert task.metadata["scheduler_job_ownership"] == []
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    event_types = {event.event_type for event in events}
    assert event_types.isdisjoint(
        {
            "scheduler.pending",
            "scheduler.allocated",
            "scheduler.running",
            "scheduler.completed",
        }
    )
    assert task.metadata["jarvis_execution_handle"]["schema_version"] == (
        "jarvis.execution.handle.v1"
    )
    assert task.metadata["jarvis_execution_record"]["state"] == "completed"
    assert task.metadata["jarvis_execution_progress"]["terminal"] is True
    provenance = json.loads(
        (settings.spool_dir / job.job_id / "provenance.json").read_text(encoding="utf-8")
    )
    assert provenance["runtime_metadata"]["output_path"] == "/runs/native-direct/stdout.log"
    assert provenance["runtime_metadata"]["packages"][0]["package_id"] == "render"


def test_worker_refuses_runtime_identity_from_unconfigured_jarvis_named_tool() -> None:
    job = RelayJob(
        cluster="research-cluster",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server="untrusted-mcp",
            tool="jarvis_run",
            arguments={"pipeline_id": "forged"},
        ),
        idempotency_key="untrusted-jarvis-named-tool",
    )
    trusted, reason = cast(Any, endpoint_module)._trusted_jarvis_mcp_result(
        job,
        {
            "server": "untrusted-mcp",
            "server_args": [],
            "operation": "tools/call",
            "tool": "jarvis_run",
            "returncode": 0,
            "timed_out": False,
            "protocol_error": None,
            "protocol_result": {
                "structuredContent": {
                    "runtime_metadata": {
                        "scheduler_provider": "slurm",
                        "scheduler_job_id": "another-users-job",
                    }
                }
            },
        },
    )

    assert trusted is False
    assert reason == "MCP command does not match the configured JARVIS server"


def test_generic_jarvis_named_mcp_result_is_ignored_before_native_validation(
    tmp_path: Path,
) -> None:
    """Operator MCP tools do not acquire built-in JARVIS parsing semantics by name."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="research-cluster",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server="operator-mcp",
                tool="jarvis_run",
                arguments={"pipeline_id": "operator-pipeline"},
            ),
            idempotency_key="generic-jarvis-shaped-result",
        )
    )

    class GenericProvider(RecordingProvider):
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
            del pipeline_path, env, on_stdout, on_stderr, should_cancel, on_poll
            del timeout_seconds, on_timeout
            assert cwd is not None
            if on_start is not None:
                on_start(702)
            (cwd / "mcp-result.json").write_text(
                json.dumps(
                    {
                        "server": "operator-mcp",
                        "server_args": [],
                        "operation": "tools/call",
                        "tool": "jarvis_run",
                        "arguments": {"pipeline_id": "operator-pipeline"},
                        "structured_result": {
                            "runtime_metadata": {"schema_version": "not-a-jarvis-runtime"}
                        },
                        "returncode": 0,
                        "timed_out": False,
                        "protocol_error": None,
                    }
                ),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(["operator-mcp"], 0, "", "")

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="research-cluster",
        queue=queue,
        provider=GenericProvider(),
    )

    result = worker.run_once()
    task = queue.list_tasks(job.job_id)[0]
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)

    assert result is not None
    assert result.state is JobState.SUCCEEDED
    assert "runtime_metadata" not in task.metadata
    assert "runtime.metadata_refused" not in {event.event_type for event in events}


def test_trusted_jarvis_mcp_result_requires_content_derived_server_digest(
    monkeypatch: MonkeyPatch,
) -> None:
    """Self-reported digests cannot substitute for the persisted artifact document."""
    command = ["clio-kit", "mcp-server", "jarvis"]
    claimed_digest = "f" * 64
    expected_lock = endpoint_module.jarvis_cd_lock_binding_expectation()
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    job = RelayJob(
        cluster="research-cluster",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server=command[0],
            server_args=command[1:],
            expected_server_artifact_digest=claimed_digest,
            expected_jarvis_cd_lock_binding=expected_lock,
            tool="jarvis_run",
            arguments={"pipeline_id": "owned"},
        ),
        idempotency_key="self-reported-server-digest",
    )
    document: dict[str, object] = {
        "server": command[0],
        "server_args": command[1:],
        "env_from": {},
        "expected_jarvis_cd_lock_binding": expected_lock,
        "expected_server_artifact_digest": claimed_digest,
        "observed_server_artifact_digest": claimed_digest,
        "server_artifact": verified_jarvis_server_artifact(),
        "operation": "tools/call",
        "tool": "jarvis_run",
        "arguments": {"pipeline_id": "owned"},
        "returncode": 0,
        "timed_out": False,
        "protocol_error": None,
        "protocol_result": {"structuredContent": {"runtime_metadata": {}}},
    }

    trusted, reason = cast(Any, endpoint_module)._trusted_jarvis_mcp_result(job, document)

    assert trusted is False
    assert reason == "MCP result server artifact identity is not the exact relay release pin"


def test_worker_refuses_builtin_semantics_without_jarvis_lock_marker(
    monkeypatch: MonkeyPatch,
) -> None:
    """A generic JARVIS call cannot unlock built-in progress/runtime handling."""
    command = ["clio-kit", "mcp-server", "jarvis"]
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    job = RelayJob(
        cluster="research-cluster",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server=command[0],
            server_args=command[1:],
            expected_server_artifact_digest="a" * 64,
            tool="jarvis_run",
            arguments={"pipeline_id": "operator-pipeline"},
        ),
        idempotency_key="generic-jarvis-route",
    )

    trusted, reason = cast(Any, endpoint_module)._trusted_jarvis_mcp_route(job)

    assert trusted is False
    assert reason == "MCP call did not enforce the relay JARVIS-CD lock pin"


def test_worker_refuses_result_with_mismatched_jarvis_lock_marker(
    monkeypatch: MonkeyPatch,
) -> None:
    """Built-in result evidence must carry the same lock marker as its job."""
    command = ["clio-kit", "mcp-server", "jarvis"]
    expected = endpoint_module.jarvis_cd_lock_binding_expectation()
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    job = RelayJob(
        cluster="research-cluster",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server=command[0],
            server_args=command[1:],
            expected_server_artifact_digest="a" * 64,
            expected_jarvis_cd_lock_binding=expected,
            tool="jarvis_run",
            arguments={"pipeline_id": "owned"},
        ),
        idempotency_key="mismatched-jarvis-lock-result",
    )

    trusted, reason = cast(Any, endpoint_module)._trusted_jarvis_mcp_result(
        job,
        {
            "server": command[0],
            "server_args": command[1:],
            "env_from": {},
            "expected_jarvis_cd_lock_binding": None,
            "operation": "tools/call",
            "tool": "jarvis_run",
            "arguments": {"pipeline_id": "owned"},
        },
    )

    assert trusted is False
    assert reason == "MCP result JARVIS-CD lock pin does not match the durable job spec"


def test_worker_refuses_runtime_identity_when_result_arguments_do_not_match(
    monkeypatch: MonkeyPatch,
) -> None:
    server_args = [
        "--from",
        "clio-kit==2.3.1",
        "clio-kit",
        "mcp-server",
        "jarvis",
    ]
    digest = "d" * 64
    monkeypatch.setattr(
        endpoint_module,
        "jarvis_mcp_command",
        lambda: ["uvx", *server_args],
    )
    job = RelayJob(
        cluster="research-cluster",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server="uvx",
            server_args=server_args,
            expected_server_artifact_digest=digest,
            expected_jarvis_cd_lock_binding=(endpoint_module.jarvis_cd_lock_binding_expectation()),
            tool="jarvis_run",
            arguments={"pipeline_id": "owned"},
        ),
        idempotency_key="mismatched-jarvis-result",
    )

    trusted, reason = cast(Any, endpoint_module)._trusted_jarvis_mcp_result(
        job,
        cast(
            object,
            {
                "server": "uvx",
                "server_args": server_args,
                "env_from": {},
                "expected_server_artifact_digest": digest,
                "expected_jarvis_cd_lock_binding": (
                    endpoint_module.jarvis_cd_lock_binding_expectation()
                ),
                "observed_server_artifact_digest": digest,
                "operation": "tools/call",
                "tool": "jarvis_run",
                "arguments": {"pipeline_id": "different"},
                "returncode": 0,
                "timed_out": False,
                "protocol_error": None,
                "protocol_result": {"structuredContent": {"runtime_metadata": {}}},
            },
        ),
    )

    assert trusted is False
    assert reason == "MCP result arguments do not match the durable job spec"


def test_worker_refuses_stdout_only_jarvis_mcp_runtime_identity(
    monkeypatch: MonkeyPatch,
) -> None:
    server_args = [
        "--from",
        "clio-kit==2.3.1",
        "clio-kit",
        "mcp-server",
        "jarvis",
    ]
    digest = "e" * 64
    monkeypatch.setattr(
        endpoint_module,
        "jarvis_mcp_command",
        lambda: ["uvx", *server_args],
    )
    arguments = {"pipeline_id": "owned"}
    job = RelayJob(
        cluster="research-cluster",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server="uvx",
            server_args=server_args,
            expected_server_artifact_digest=digest,
            expected_jarvis_cd_lock_binding=(endpoint_module.jarvis_cd_lock_binding_expectation()),
            tool="jarvis_run",
            arguments=arguments,
        ),
        idempotency_key="stdout-only-jarvis-result",
    )

    trusted, reason = cast(Any, endpoint_module)._trusted_jarvis_mcp_result(
        job,
        {
            "server": "uvx",
            "server_args": server_args,
            "env_from": {},
            "expected_server_artifact_digest": digest,
            "expected_jarvis_cd_lock_binding": (
                endpoint_module.jarvis_cd_lock_binding_expectation()
            ),
            "observed_server_artifact_digest": digest,
            "operation": "tools/call",
            "tool": "jarvis_run",
            "arguments": arguments,
            "returncode": 0,
            "timed_out": False,
            "protocol_error": None,
            "stdout": (
                '{"jsonrpc":"2.0","id":"clio-relay-mcp-call","result":'
                '{"structuredContent":{"runtime_metadata":{"scheduler_job_id":"forged"}}}}'
            ),
        },
    )

    assert trusted is False
    assert reason == "MCP result has no persisted structured protocol result"


def test_worker_ingests_authenticated_runtime_sidecar_over_stdout_fallback(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="research-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="jarvis-runtime-sidecar",
        )
    )

    class SidecarProvider(RecordingProvider):
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
            del cwd, on_stderr, on_start, should_cancel, timeout_seconds, on_timeout
            self.runs.append(pipeline_path)
            if on_stdout is not None:
                on_stdout("scheduler_job_id=legacy-id\n")
            assert env is not None
            sidecar = Path(env["CLIO_RELAY_RUNTIME_METADATA_FILE"])
            sidecar.write_text("", encoding="utf-8")
            for sequence, phase in enumerate(
                ("pending", "allocated", "running", "completed"),
                start=1,
            ):
                with sidecar.open("a", encoding="utf-8") as handle:
                    handle.write(
                        json.dumps(
                            runtime_sidecar_record(
                                {
                                    "schema_version": "jarvis.runtime.v1",
                                    "execution_id": "execution-sidecar-id",
                                    "scheduler_provider": "test-scheduler",
                                    "scheduler_job_id": "sidecar-id",
                                    "scheduler_phase": phase,
                                    "allocated_nodes": (
                                        ["compute-1"]
                                        if phase in {"allocated", "running", "completed"}
                                        else []
                                    ),
                                    "script_path": "/runtime/script.sh",
                                    "details": {
                                        "scheduler_submission": {
                                            "schema_version": ("jarvis.scheduler.submission.v1"),
                                            "provider": "test-scheduler",
                                            "scheduler_job_id": "sidecar-id",
                                            "identity_source": "scheduler_submit_api",
                                            "submitted": True,
                                        }
                                    },
                                },
                                key=env["CLIO_RELAY_RUNTIME_METADATA_TOKEN"],
                                sequence=sequence,
                            )
                        )
                        + "\n"
                    )
                if on_poll is not None:
                    on_poll()
            return subprocess.CompletedProcess(["jarvis"], 0, "", "")

    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="research-cluster",
        queue=queue,
        provider=SidecarProvider(),
        scheduler_provider=FakeSchedulerProvider(),
    )
    worker.register()

    result = worker.run_once()

    assert result is not None
    task = queue.list_tasks(job.job_id)[0]
    assert task.metadata["runtime_metadata_source"] == "jarvis_sidecar"
    assert task.metadata["scheduler_job_ids"] == ["sidecar-id"]
    runtime = task.metadata["runtime_metadata"]
    assert isinstance(runtime, dict)
    assert runtime["script_path"] == "/runtime/script.sh"
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    structured_phases = [
        event.event_type
        for event in events
        if event.event_type
        in {
            "scheduler.pending",
            "scheduler.allocated",
            "scheduler.running",
            "scheduler.completed",
        }
        and event.payload.get("structured") is True
    ]
    assert structured_phases == [
        "scheduler.pending",
        "scheduler.allocated",
        "scheduler.running",
        "scheduler.completed",
    ]


def test_worker_reconciles_interrupted_submission_by_one_exact_marker_without_canceling(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="research-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(
                pipeline_yaml=("name: interrupted-submit\nscheduler:\n  name: slurm\npkgs: []\n")
            ),
            idempotency_key="jarvis-interrupted-submit-reconciliation",
        )
    )

    class InterruptedSubmitProvider(RecordingProvider):
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
            del (
                cwd,
                on_stdout,
                on_stderr,
                on_start,
                should_cancel,
                on_poll,
                timeout_seconds,
                on_timeout,
            )
            self.runs.append(pipeline_path)
            assert env is not None
            intent = cast(
                dict[str, object],
                json.loads(env["CLIO_RELAY_RUNTIME_SUBMISSION_INTENT"]),
            )
            sidecar = Path(env["CLIO_RELAY_RUNTIME_METADATA_FILE"])
            sidecar.write_text(
                json.dumps(
                    runtime_sidecar_record(
                        {
                            "schema_version": "jarvis.runtime.v1",
                            "execution_id": intent["execution_id"],
                            "scheduler_provider": "slurm",
                            "scheduler_type": "slurm",
                            "scheduler_phase": "submission_intent",
                            "terminal": {"state": "submission_intent", "terminal": False},
                            "details": {
                                "scheduler_submission_intent": {
                                    **intent,
                                    "provider": "slurm",
                                }
                            },
                        },
                        key=env["CLIO_RELAY_RUNTIME_METADATA_TOKEN"],
                        sequence=1,
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(["jarvis"], 70, "", "submit adapter crashed")

    class SlurmSchedulerProvider(FakeSchedulerProvider):
        name = "slurm"

    scheduler = SlurmSchedulerProvider()
    scheduler.reconciliation_matches = ["24680"]
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="research-cluster",
        queue=queue,
        provider=InterruptedSubmitProvider(),
        scheduler_provider=scheduler,
    )
    worker.register()

    result = worker.run_once()

    assert result is not None
    assert result.state is JobState.FAILED
    assert len(scheduler.reconciliation_markers) == 1
    marker = scheduler.reconciliation_markers[0]
    assert marker.startswith("clio-relay-")
    assert scheduler.canceled == []
    task = queue.list_tasks(job.job_id)[0]
    assert task.metadata["scheduler_job_ids"] == ["24680"]
    ownership = cast(list[dict[str, object]], task.metadata["scheduler_job_ownership"])
    assert ownership[0]["proof"] == "exact_scheduler_marker_reconciliation"
    assert ownership[0]["reconciliation_marker"] == marker
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    assert "scheduler.reconciled" in {event.event_type for event in events}


@pytest.mark.parametrize("matches", [[], ["111", "222"]])
def test_durable_scheduler_intent_zero_or_multiple_matches_remains_unresolved(
    tmp_path: Path,
    matches: list[str],
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="research-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(
                pipeline_yaml=(
                    "name: unresolved-submit\nscheduler:\n  name: test-scheduler\npkgs: []\n"
                )
            ),
            idempotency_key=f"unresolved-submit-{len(matches)}",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="jarvis.execution"))
    marker = "clio-relay-0123456789abcdef"
    task = queue.update_task_metadata(
        task.task_id,
        {
            "execution_sidecars": {
                "schema_version": "clio-relay.execution-sidecars.v1",
                "scheduler_submission_intent": {
                    "schema_version": "clio-relay.scheduler-submission-intent.v1",
                    "execution_id": "jarvis_0123456789abcdef",
                    "marker": marker,
                    "created_at": utc_now().isoformat(),
                    "scheduler_user": "alice",
                    "scheduler_expected": True,
                    "direct_proof_sha256": hashlib.sha256(b"unused-direct-proof").hexdigest(),
                },
            }
        },
    )
    scheduler = FakeSchedulerProvider()
    scheduler.reconciliation_matches = matches
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="research-cluster",
        queue=queue,
        scheduler_provider=scheduler,
    )

    with pytest.raises(RelayError, match="remains unresolved"):
        cast(Any, worker)._reconcile_recorded_scheduler_submission(job, task)

    refreshed = queue.get_task(task.task_id)
    scheduler_job_ids = refreshed.metadata.get("scheduler_job_ids")
    assert scheduler_job_ids is None or scheduler_job_ids == []
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    assert "scheduler.reconciliation_unresolved" in {event.event_type for event in events}


def test_restart_cleanup_uses_direct_mode_proof_without_scheduler_query(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="research-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_name="named-direct"),
            idempotency_key="named-direct-restart-proof",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="jarvis.execution"))
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    runtime_path = spool / ".runtime-direct-proof.jsonl"
    private = cast(Any, endpoint_module)
    anchor = private._precreate_runtime_sidecar(runtime_path)
    direct_proof = "one-use-direct-execution-proof"
    execution_id = "jarvis_0123456789abcdef"
    runtime_path.write_text(
        json.dumps(
            runtime_sidecar_record(
                {
                    "schema_version": "jarvis.runtime.v1",
                    "execution_id": execution_id,
                    "scheduler_phase": "direct_running",
                    "terminal": {"state": "direct_running", "terminal": False},
                    "details": {
                        "execution_mode": "direct",
                        "scheduler_expected": False,
                        "direct_execution_proof": direct_proof,
                    },
                },
                key="discarded-after-worker-crash",
                sequence=1,
            )
        )
        + "\n",
        encoding="utf-8",
    )
    task = queue.update_task_metadata(
        task.task_id,
        {
            "execution_sidecars": {
                "schema_version": "clio-relay.execution-sidecars.v1",
                "progress": ".progress-direct-proof.jsonl",
                "runtime": runtime_path.name,
                "runtime_anchor": anchor.as_metadata(),
                "scheduler_submission_intent": {
                    "schema_version": "clio-relay.scheduler-submission-intent.v1",
                    "execution_id": execution_id,
                    "marker": "clio-relay-0123456789abcdef",
                    "created_at": utc_now().isoformat(),
                    "scheduler_user": "alice",
                    "scheduler_expected": "unknown",
                    "direct_proof_sha256": hashlib.sha256(direct_proof.encode("utf-8")).hexdigest(),
                },
            }
        },
    )
    scheduler = FakeSchedulerProvider()
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="research-cluster",
        queue=queue,
        scheduler_provider=scheduler,
    )

    reconciled = cast(Any, worker)._reconcile_recorded_scheduler_submission(job, task)

    assert reconciled is False
    assert scheduler.reconciliation_markers == []
    refreshed = queue.get_task(task.task_id)
    sidecars = cast(dict[str, object], refreshed.metadata["execution_sidecars"])
    assert sidecars["scheduler_expected_resolved"] is False
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    assert "scheduler.direct_execution_recovered" in {event.event_type for event in events}


def test_restart_cleanup_uses_scheduler_refusal_proof_without_scheduler_query(
    tmp_path: Path,
) -> None:
    class ExternalSchedulerProvider(FakeSchedulerProvider):
        name = "external"

    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="research-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_name="named-scheduled"),
            idempotency_key="named-scheduler-refusal-restart-proof",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="jarvis.execution"))
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    runtime_path = spool / ".runtime-scheduler-refusal.jsonl"
    private = cast(Any, endpoint_module)
    anchor = private._precreate_runtime_sidecar(runtime_path)
    refusal_proof = "one-use-scheduler-refusal-proof"
    execution_id = "jarvis_0123456789abcdef"
    runtime_path.write_text(
        json.dumps(
            runtime_sidecar_record(
                {
                    "schema_version": "jarvis.runtime.v1",
                    "execution_id": execution_id,
                    "pipeline_id": "named-scheduled",
                    "scheduler_provider": "slurm",
                    "scheduler_phase": "launch_refused",
                    "terminal": {
                        "state": "launch_refused",
                        "terminal": True,
                        "returncode": 2,
                        "reason": "slurm != external",
                    },
                    "details": {
                        "execution_owner": "jarvis_cd.pipeline.preflight",
                        "execution_mode": "scheduler",
                        "scheduler_expected": "unknown",
                        "scheduler_submission_attempted": False,
                        "scheduler_launch_refused": True,
                        "scheduler_provider": "slurm",
                        "configured_scheduler_provider": "external",
                        "scheduler_launch_refusal_proof": refusal_proof,
                    },
                },
                key="discarded-after-worker-crash",
                sequence=1,
            )
        )
        + "\n",
        encoding="utf-8",
    )
    task = queue.update_task_metadata(
        task.task_id,
        {
            "execution_sidecars": {
                "schema_version": "clio-relay.execution-sidecars.v1",
                "progress": ".progress-scheduler-refusal.jsonl",
                "runtime": runtime_path.name,
                "runtime_anchor": anchor.as_metadata(),
                "scheduler_submission_intent": {
                    "schema_version": "clio-relay.scheduler-submission-intent.v1",
                    "execution_id": execution_id,
                    "marker": "clio-relay-0123456789abcdef",
                    "created_at": utc_now().isoformat(),
                    "scheduler_user": "alice",
                    "scheduler_expected": "unknown",
                    "direct_proof_sha256": hashlib.sha256(
                        refusal_proof.encode("utf-8")
                    ).hexdigest(),
                },
            }
        },
    )
    scheduler = ExternalSchedulerProvider()
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="research-cluster",
        queue=queue,
        scheduler_provider=scheduler,
    )

    reconciled = cast(Any, worker)._reconcile_recorded_scheduler_submission(job, task)

    assert reconciled is False
    assert scheduler.reconciliation_markers == []
    refreshed = queue.get_task(task.task_id)
    sidecars = cast(dict[str, object], refreshed.metadata["execution_sidecars"])
    assert sidecars["scheduler_submission_refused"] is True
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    assert "scheduler.launch_refusal_recovered" in {event.event_type for event in events}


def test_runtime_sidecar_anchor_refuses_replacement_and_cleanup_leaves_it_in_place(
    tmp_path: Path,
) -> None:
    private = cast(Any, endpoint_module)
    path = tmp_path / ".runtime-replaced.jsonl"
    anchor = private._precreate_runtime_sidecar(path)
    path.unlink()
    path.write_text("replacement\n", encoding="utf-8")
    if os.name != "nt":
        path.chmod(0o600)

    with pytest.raises(ConfigurationError, match="identity or permissions changed"):
        private._open_owned_sidecar(
            path,
            label="runtime metadata sidecar",
            expected_anchor=anchor,
        )
    with pytest.raises(
        ConfigurationError,
        match="identity or permissions changed|file identity changed",
    ):
        private._remove_execution_sidecars(
            [path],
            spool_path=tmp_path,
            expected_anchors={path: anchor},
        )

    assert path.read_text(encoding="utf-8") == "replacement\n"


def test_runtime_sidecar_anchor_refuses_added_hardlink(tmp_path: Path) -> None:
    private = cast(Any, endpoint_module)
    path = tmp_path / ".runtime-hardlinked.jsonl"
    anchor = private._precreate_runtime_sidecar(path)
    hardlink = tmp_path / "runtime-hardlink"
    try:
        os.link(path, hardlink)
    except OSError as exc:
        pytest.fail(f"filesystem does not support required hard-link test semantics: {exc}")

    with pytest.raises(ConfigurationError, match="identity or permissions changed"):
        private._open_owned_sidecar(
            path,
            label="runtime metadata sidecar",
            expected_anchor=anchor,
        )


def test_runtime_sidecar_anchor_refuses_mode_change(tmp_path: Path) -> None:
    private = cast(Any, endpoint_module)
    path = tmp_path / ".runtime-mode.jsonl"
    anchor = private._precreate_runtime_sidecar(path)
    path.chmod(0o444 if os.name == "nt" else 0o640)

    with pytest.raises(ConfigurationError, match="identity or permissions changed"):
        private._open_owned_sidecar(
            path,
            label="runtime metadata sidecar",
            expected_anchor=anchor,
        )
