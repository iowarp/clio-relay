from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from clio_relay.cli import app
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.http_api import create_app
from clio_relay.mcp_server import handle_request
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


def _mcp_call(
    fixture: _PaginationFixture,
    *,
    tool: str,
    arguments: dict[str, object],
) -> dict[str, Any]:
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": tool,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        },
        queue=fixture.queue,
        settings=fixture.settings,
        profile="admin",
    )
    assert response is not None
    return cast(dict[str, Any], response["result"]["structuredContent"])


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


def test_mcp_pages_more_than_501_job_records(
    pagination_fixture: _PaginationFixture,
) -> None:
    for record_key, tool in (
        ("tasks", "relay_list_tasks"),
        ("artifacts", "relay_list_artifacts"),
        ("progress", "relay_list_progress"),
    ):
        page = _mcp_call(
            pagination_fixture,
            tool=tool,
            arguments={
                "job_id": pagination_fixture.job_id,
                "cursor": 501,
                "limit": 2,
            },
        )
        _assert_exact_page(page, record_key)


def test_sparse_job_filters_return_empty_source_page_with_next_cursor(
    pagination_fixture: _PaginationFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected: dict[str, object] = {
        "jobs": [],
        "source_cursor": 2,
        "source_limit": 500,
        "source_next_cursor": 502,
        "source_total": RECORD_COUNT,
    }
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(pagination_fixture.settings.core_dir))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(pagination_fixture.settings.spool_dir))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    cli_result = CliRunner().invoke(
        app,
        [
            "queue",
            "list",
            "--cluster",
            "sparse-match",
            "--state",
            "succeeded",
            "--cursor",
            "2",
            "--limit",
            "500",
        ],
    )
    assert cli_result.exit_code == 0, cli_result.output
    cli_page = json.loads(cli_result.output)
    assert {key: cli_page[key] for key in expected} == expected

    client = cast(Any, TestClient(create_app(pagination_fixture.settings)))
    http_response = client.get(
        "/queue",
        params={
            "cluster": "sparse-match",
            "state": "succeeded",
            "cursor": 2,
            "limit": 500,
        },
    )
    assert http_response.status_code == 200
    http_page = http_response.json()
    assert {key: http_page[key] for key in expected} == expected

    mcp_page = _mcp_call(
        pagination_fixture,
        tool="relay_queue_list",
        arguments={
            "cluster": "sparse-match",
            "state": "succeeded",
            "cursor": 2,
            "limit": 500,
        },
    )
    assert {key: mcp_page[key] for key in expected} == expected
