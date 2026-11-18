from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from clio_relay.adapters.worker.cli import worker_app as app
from clio_relay.adapters.worker.core_queue import ClioCoreQueue
from clio_relay.adapters.worker.http_api import create_app
from clio_relay.config import RelaySettings
from clio_relay.models import (
    ArtifactRef,
    JarvisRunSpec,
    JobKind,
    JobState,
    ProgressRecord,
    RelayJob,
    RelayTask,
)

RECORD_COUNT = 502


@dataclass(frozen=True)
class _PaginationFixture:
    root: Path
    settings: RelaySettings
    queue: ClioCoreQueue
    job_id: str


@pytest.fixture(scope="module")
def pagination_fixture(tmp_path_factory: pytest.TempPathFactory) -> _PaginationFixture:
    """Build one real >501-record queue shared by every public surface test."""
    root = tmp_path_factory.mktemp("surface-pagination")
    settings = RelaySettings(core_dir=root / "core", spool_dir=root / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    artifact_path = root / "artifact.bin"
    artifact_path.write_bytes(b"surface pagination\n")
    target = queue.submit_job(
        RelayJob(
            cluster="sparse-match",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: pagination\npkgs: []\n"),
            idempotency_key="surface-pagination-target",
        )
    )
    for index in range(RECORD_COUNT):
        queue.append_task(RelayTask(job_id=target.job_id, name=f"pagination.task.{index:04d}"))
        queue.append_artifact(
            ArtifactRef(
                job_id=target.job_id,
                uri=artifact_path.as_uri(),
                kind=f"page-{index:04d}",
            )
        )
        queue.append_progress(
            ProgressRecord(
                job_id=target.job_id,
                label="page",
                current=float(index),
                total=float(RECORD_COUNT),
            )
        )
    queue.update_job_state(target.job_id, JobState.SUCCEEDED)
    for index in range(2, RECORD_COUNT):
        job = queue.submit_job(
            RelayJob(
                cluster="sparse-other",
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(pipeline_yaml="name: sparse\npkgs: []\n"),
                idempotency_key=f"surface-pagination-other-{index:04d}",
            )
        )
        queue.update_job_state(job.job_id, JobState.SUCCEEDED)
    final = queue.submit_job(
        RelayJob(
            cluster="sparse-match",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: sparse-final\npkgs: []\n"),
            idempotency_key="surface-pagination-final",
        )
    )
    queue.update_job_state(final.job_id, JobState.SUCCEEDED)
    return _PaginationFixture(root=root, settings=settings, queue=queue, job_id=target.job_id)


def _assert_exact_page(page: dict[str, Any], record_key: str) -> None:
    assert len(page[record_key]) == 2
    assert page["cursor"] == 501
    assert page["limit"] == 2
    assert page["next_cursor"] is None
    assert page["total"] == RECORD_COUNT


def test_cli_pages_more_than_501_job_records(
    pagination_fixture: _PaginationFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(pagination_fixture.settings.core_dir))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(pagination_fixture.settings.spool_dir))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    runner = CliRunner()
    commands = {
        "tasks": ["job", "tasks", pagination_fixture.job_id],
        "artifacts": ["job", "list-artifacts", pagination_fixture.job_id],
        "progress": ["job", "progress", pagination_fixture.job_id],
    }
    for record_key, command in commands.items():
        result = runner.invoke(app, [*command, "--cursor", "501", "--limit", "2"])
        assert result.exit_code == 0, result.output
        _assert_exact_page(json.loads(result.output), record_key)


def test_http_pages_more_than_501_job_records(
    pagination_fixture: _PaginationFixture,
) -> None:
    client = cast(Any, TestClient(create_app(pagination_fixture.settings)))
    for record_key in ("tasks", "artifacts", "progress"):
        response = client.get(
            f"/jobs/{pagination_fixture.job_id}/{record_key}",
            params={"cursor": 501, "limit": 2},
        )
        assert response.status_code == 200
        _assert_exact_page(response.json(), record_key)
