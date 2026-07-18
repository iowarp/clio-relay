"""Focused tests for artifact-pinned JARVIS execution-recovery guards."""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from pytest import MonkeyPatch

from clio_relay import endpoint as endpoint_module
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import (
    MCP_RUNNER_BASE_ENV_NAMES,
    EndpointWorker,
    _close_recovery_directory_anchor,  # pyright: ignore[reportPrivateUsage]
    _durable_jarvis_execution_recovery,  # pyright: ignore[reportPrivateUsage]
    _jarvis_execution_recovery_intent,  # pyright: ignore[reportPrivateUsage]
    _jarvis_execution_recovery_retry_due,  # pyright: ignore[reportPrivateUsage]
    _minimal_mcp_runner_environment,  # pyright: ignore[reportPrivateUsage]
    _open_or_create_recovery_directory,  # pyright: ignore[reportPrivateUsage]
    _read_owned_recovery_result,  # pyright: ignore[reportPrivateUsage]
)
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import (
    EndpointRole,
    JobKind,
    McpCallSpec,
    RelayJob,
    RelayTask,
)
from clio_relay.remote_mcp import remote_mcp_server_artifact_digest
from tests.jarvis_mcp_fakes import verified_jarvis_server_artifact


def _trusted_recovery_record(
    monkeypatch: MonkeyPatch,
) -> tuple[RelayJob, RelayTask, dict[str, Any]]:
    """Build one valid prepared recovery record without dispatching a process."""
    command = ["locked-clio-kit", "mcp-server", "jarvis"]
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    digest = remote_mcp_server_artifact_digest(verified_jarvis_server_artifact())
    job = RelayJob(
        cluster="research-cluster",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server=command[0],
            server_args=command[1:],
            expected_server_artifact_digest=digest,
            expected_jarvis_cd_lock_binding=(endpoint_module.jarvis_cd_lock_binding_expectation()),
            tool="jarvis_run",
            arguments={
                "pipeline_id": "durable-science",
                "execution_id": f"jarvis_{'a' * 32}",
            },
        ),
        idempotency_key="recovery-state-validation",
    )
    intent = _jarvis_execution_recovery_intent(
        job,
        created_at=datetime(2026, 7, 18, 12, 0, tzinfo=UTC),
    )
    assert intent is not None
    intent["recovery_directory_anchor"] = {
        "device": 1,
        "inode": 2,
        "owner": 3,
        "mode": 0o700,
    }
    task = RelayTask(
        job_id=job.job_id,
        name="mcp.call",
        metadata={"jarvis_execution_recovery": intent},
    )
    return job, task, cast(dict[str, Any], intent)


def _task_with_recovery(task: RelayTask, intent: dict[str, Any]) -> RelayTask:
    """Copy a task with one candidate durable recovery record."""
    return task.model_copy(update={"metadata": {"jarvis_execution_recovery": intent}})


def test_recovery_retry_without_deadline_is_due() -> None:
    """A newly durable recovery without backoff is immediately eligible."""
    assert _jarvis_execution_recovery_retry_due({"next_retry_at": None})
    assert _jarvis_execution_recovery_retry_due({})


def test_recovery_retry_respects_past_and_future_deadlines() -> None:
    """Recovery eligibility changes only when its durable deadline is reached."""
    now = datetime(2026, 7, 18, 12, 0, tzinfo=UTC)

    assert _jarvis_execution_recovery_retry_due(
        {"next_retry_at": (now - timedelta(microseconds=1)).isoformat()},
        now=now,
    )
    assert _jarvis_execution_recovery_retry_due(
        {"next_retry_at": now.isoformat()},
        now=now,
    )
    assert not _jarvis_execution_recovery_retry_due(
        {"next_retry_at": (now + timedelta(microseconds=1)).isoformat()},
        now=now,
    )


def test_recovery_record_validates_lifecycle_and_timestamp_coherence(
    monkeypatch: MonkeyPatch,
) -> None:
    """Only coherent prepared, failed-pending, and resolved states are durable."""
    job, task, prepared = _trusted_recovery_record(monkeypatch)
    assert _durable_jarvis_execution_recovery(job, task) == prepared

    started_at = "2026-07-18T12:01:00+00:00"
    attempted_at = "2026-07-18T12:02:00+00:00"
    retry_at = "2026-07-18T12:03:00+00:00"
    failure_hash = hashlib.sha256(b"untrusted-query-result").hexdigest()
    failed_pending = {
        **prepared,
        "dispatch_state": "started",
        "dispatch_started_at": started_at,
        "attempts": 1,
        "last_attempt_at": attempted_at,
        "next_retry_at": retry_at,
        "last_error": "scheduler identity is not available yet",
        "result_sha256": failure_hash,
    }
    assert (
        _durable_jarvis_execution_recovery(
            job,
            _task_with_recovery(task, failed_pending),
        )
        == failed_pending
    )

    resolved = {
        **failed_pending,
        "state": "resolved",
        "next_retry_at": None,
        "last_error": None,
        "result_sha256": hashlib.sha256(b"trusted-query-result").hexdigest(),
        "resolved_at": "2026-07-18T12:04:00+00:00",
        "resolution": "execution_query",
        "scheduler_provider": "slurm",
        "scheduler_job_id": "21999",
    }
    assert (
        _durable_jarvis_execution_recovery(
            job,
            _task_with_recovery(task, resolved),
        )
        == resolved
    )


def test_recovery_record_rejects_impossible_lifecycle_states(
    monkeypatch: MonkeyPatch,
) -> None:
    """Corrupt cross-field states cannot authorize adoption or skip recovery."""
    job, task, prepared = _trusted_recovery_record(monkeypatch)
    started = {
        **prepared,
        "dispatch_state": "started",
        "dispatch_started_at": "2026-07-18T12:01:00+00:00",
    }
    attempted = {
        **started,
        "attempts": 1,
        "last_attempt_at": "2026-07-18T12:02:00+00:00",
    }
    resolved = {
        **attempted,
        "state": "resolved",
        "result_sha256": hashlib.sha256(b"trusted-result").hexdigest(),
        "resolved_at": "2026-07-18T12:03:00+00:00",
        "resolution": "execution_query",
        "scheduler_provider": "slurm",
        "scheduler_job_id": "21999",
    }
    invalid_records = [
        {**prepared, "created_at": "2026-07-18T12:00:00"},
        {
            **prepared,
            "dispatch_state": "started",
            "dispatch_started_at": "2026-07-18T11:59:00+00:00",
        },
        {**started, "attempts": 1},
        {
            **started,
            "last_attempt_at": "2026-07-18T12:02:00+00:00",
        },
        {
            **attempted,
            "next_retry_at": "2026-07-18T12:01:59+00:00",
        },
        {**attempted, "last_error": "failed", "next_retry_at": None},
        {**started, "result_sha256": "not-a-sha256"},
        {**started, "resolution": "execution_query"},
        {**started, "scheduler_provider": "slurm"},
        {
            **resolved,
            "resolved_at": "2026-07-18T12:01:59+00:00",
        },
        {**resolved, "result_sha256": None},
        {**resolved, "scheduler_job_id": None},
        {**resolved, "attempts": 0, "last_attempt_at": None},
        {
            **attempted,
            "query_process": {
                "schema_version": "clio-relay.execution-ownership.v1",
                "pid": 17,
            },
        },
    ]
    for invalid in invalid_records:
        with pytest.raises(RelayError, match="recovery intent is invalid"):
            _durable_jarvis_execution_recovery(
                job,
                _task_with_recovery(task, invalid),
            )


def test_execution_start_persists_process_and_dispatch_release_atomically(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """No durable write can expose a released process as an unreleased dispatch."""
    command = ["locked-clio-kit", "mcp-server", "jarvis"]
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    digest = remote_mcp_server_artifact_digest(verified_jarvis_server_artifact())
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
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
                arguments={"pipeline_id": "durable-science"},
            ),
            idempotency_key="atomic-dispatch-release",
        )
    )
    intent = _jarvis_execution_recovery_intent(
        job,
        created_at=datetime(2026, 7, 18, 12, 0, tzinfo=UTC),
    )
    assert intent is not None
    intent["recovery_directory_anchor"] = {
        "device": 1,
        "inode": 2,
        "owner": 3,
        "mode": 0o700,
    }
    task = queue.append_task(
        RelayTask(
            job_id=job.job_id,
            name="mcp.call",
            metadata={"jarvis_execution_recovery": intent},
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster=job.cluster,
        queue=queue,
    )
    metadata_writes: list[dict[str, object]] = []
    update_task_metadata = queue.update_task_metadata

    def record_metadata_write(
        task_id: str,
        metadata: dict[str, object],
    ) -> RelayTask:
        metadata_writes.append(metadata)
        return update_task_metadata(task_id, metadata)

    monkeypatch.setattr(queue, "update_task_metadata", record_metadata_write)
    cast(Any, worker)._append_execution_start(job, task, os.getpid())

    start_writes = [update for update in metadata_writes if "execution_ownership" in update]
    assert len(start_writes) == 1
    start_write = start_writes[0]
    ownership = cast(dict[str, object], start_write["execution_ownership"])
    recovery = cast(dict[str, object], start_write["jarvis_execution_recovery"])
    assert recovery["dispatch_state"] == "started"
    assert recovery["dispatch_started_at"] == ownership["started_at"]
    durable_task = queue.get_task(task.task_id)
    assert _durable_jarvis_execution_recovery(job, durable_task) == recovery


def test_minimal_recovery_environment_exposes_only_base_and_referenced_sources(
    monkeypatch: MonkeyPatch,
) -> None:
    """Recovery subprocesses do not inherit relay credentials or ambient secrets."""
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "must-not-leak")
    monkeypatch.setenv("UNRELATED_RECOVERY_SECRET", "must-not-leak")
    monkeypatch.setenv("EXPLICIT_RECOVERY_TOKEN", "allowed")

    environment = _minimal_mcp_runner_environment({"REMOTE_TOKEN": "EXPLICIT_RECOVERY_TOKEN"})
    expected_names = {name for name in MCP_RUNNER_BASE_ENV_NAMES if name in os.environ} | {
        "EXPLICIT_RECOVERY_TOKEN"
    }

    assert set(environment) == expected_names
    assert environment["EXPLICIT_RECOVERY_TOKEN"] == "allowed"
    assert "CLIO_RELAY_API_TOKEN" not in environment
    assert "UNRELATED_RECOVERY_SECRET" not in environment
    assert "REMOTE_TOKEN" not in environment


def test_recovery_directory_rejects_an_existing_unowned_path(tmp_path: Path) -> None:
    """A recovery attempt cannot adopt a directory it did not create and pin."""
    recovery_directory = tmp_path / "recovery"
    recovery_directory.mkdir()

    with pytest.raises(ConfigurationError, match="unowned JARVIS recovery path already exists"):
        _open_or_create_recovery_directory(
            recovery_directory,
            expected_metadata=None,
        )


def test_recovery_directory_rejects_changed_durable_identity(tmp_path: Path) -> None:
    """Reopening a recovery directory requires its exact durable identity."""
    recovery_directory = tmp_path / "recovery"
    anchor, created = _open_or_create_recovery_directory(
        recovery_directory,
        expected_metadata=None,
    )
    try:
        assert created
        changed_identity = {**anchor.as_metadata(), "inode": anchor.inode + 1}
    finally:
        _close_recovery_directory_anchor(anchor)

    with pytest.raises(ConfigurationError, match="recovery directory identity changed"):
        _open_or_create_recovery_directory(
            recovery_directory,
            expected_metadata=changed_identity,
        )


def test_recovery_result_rejects_a_hard_link_on_windows_and_posix(tmp_path: Path) -> None:
    """Only a single-link result owned inside the pinned directory may be read."""
    recovery_directory = tmp_path / "recovery"
    anchor, created = _open_or_create_recovery_directory(
        recovery_directory,
        expected_metadata=None,
    )
    try:
        assert created
        result_path = recovery_directory / "mcp-result.json"
        alias_path = recovery_directory / "mcp-result-alias.json"
        result_path.write_bytes(b'{"ok":true}')
        os.link(result_path, alias_path)

        with pytest.raises(ConfigurationError, match="exactly one hard link"):
            _read_owned_recovery_result(result_path, directory_anchor=anchor)
    finally:
        _close_recovery_directory_anchor(anchor)


def test_recovery_result_reads_a_single_link_regular_file(tmp_path: Path) -> None:
    """The secure reader accepts the bounded ordinary result produced by the runner."""
    recovery_directory = tmp_path / "recovery"
    anchor, created = _open_or_create_recovery_directory(
        recovery_directory,
        expected_metadata=None,
    )
    try:
        assert created
        result_path = recovery_directory / "mcp-result.json"
        payload = b'{"ok":true}'
        result_path.write_bytes(payload)

        assert _read_owned_recovery_result(result_path, directory_anchor=anchor) == payload
    finally:
        _close_recovery_directory_anchor(anchor)
