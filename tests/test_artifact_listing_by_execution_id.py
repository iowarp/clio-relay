"""clio-relay#278: ``execution_id`` as an alternative to ``job_id`` on every
artifact-listing surface -- the door route, the MCP tool, and the CLI verb.

The server already had the resolution (``resolve_jarvis_run_owner``,
extended here to ``resolve_jarvis_run_owner_by_execution_id`` for callers
with no incumbent query job); the gap was purely that the listing surfaces
never accepted the id. These tests prove, per surface: listing by
execution_id returns the SAME page listing by job_id does (for both a
deferred run that carries an ``artifact_page`` and a sync-shaped run that
never does -- resolution is by the ``jarvis_run`` job's own admitted
arguments, never by ``artifact_page`` presence), the typed refusals (unknown
execution id, both ids, neither id), and that an artifact returned by an
execution_id-scoped listing reads back exactly like any other.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from clio_relay.cli import app
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.http_api import create_app
from clio_relay.jarvis_execution_artifacts import ingest_jarvis_execution_outputs
from clio_relay.mcp_server import handle_request
from clio_relay.models import ArtifactRef, JobKind, McpCallSpec, RelayJob


def _call_job(*, tool: str, execution_id: str, key: str) -> RelayJob:
    return RelayJob(
        cluster="test-cluster",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(server="jarvis-mcp", tool=tool, arguments={"execution_id": execution_id}),
        idempotency_key=key,
    )


def _register_deferred_execution_output(
    queue: ClioCoreQueue,
    *,
    execution_id: str,
    execution_root: Path,
) -> RelayJob:
    """Register one execution-output artifact through the real #252 ingest path."""
    owner = queue.submit_job(_call_job(tool="jarvis_run", execution_id=execution_id, key="run"))
    query = queue.submit_job(
        _call_job(tool="jarvis_get_execution", execution_id=execution_id, key="query")
    )
    execution_root.mkdir(parents=True, exist_ok=True)
    payload = b"deferred run output\n"
    (execution_root / "stdout.log").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    result: dict[str, Any] = {
        "structured_result": {
            "execution_id": execution_id,
            "execution_record": {
                "terminal": True,
                "metadata": {"pipeline_snapshot_path": str(execution_root / "submit.sh")},
            },
            "artifact_page": {
                "terminal": True,
                "artifacts": [
                    {
                        "package_id": "jarvis.execution",
                        "kind": "execution-file",
                        "role": "log",
                        "location": {"kind": "execution_path", "value": "stdout.log"},
                        "size_bytes": len(payload),
                        "checksum": f"sha256:{digest}",
                    }
                ],
            },
        }
    }
    indexed, truncation, outputs_missing = ingest_jarvis_execution_outputs(queue, query, result)
    assert truncation is None
    assert outputs_missing is None
    assert len(indexed) == 1
    return owner


def _mcp_result(response: dict[str, Any] | None) -> dict[str, Any]:
    assert response is not None
    assert "error" not in response, response
    return cast(dict[str, Any], response["result"]["structuredContent"])


def _mcp_error_message(response: dict[str, Any] | None) -> str:
    assert response is not None
    assert "error" in response, response
    return cast(str, response["error"]["message"])


def _list_by(
    queue: ClioCoreQueue,
    settings: RelaySettings,
    *,
    job_id: str | None = None,
    execution_id: str | None = None,
) -> dict[str, Any]:
    arguments: dict[str, Any] = {}
    if job_id is not None:
        arguments["job_id"] = job_id
    if execution_id is not None:
        arguments["execution_id"] = execution_id
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "relay_list_artifacts", "arguments": arguments},
        },
        queue=queue,
        settings=settings,
        profile="user",
    )
    return _mcp_result(response)


# --------------------------------------------------------------------------- #
# MCP surface (relay_list_artifacts / relay_read_artifact)
# --------------------------------------------------------------------------- #


def test_mcp_list_by_execution_id_matches_list_by_job_id_for_a_deferred_run(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    execution_id = "execution-278-deferred"
    owner = _register_deferred_execution_output(
        queue, execution_id=execution_id, execution_root=tmp_path / "execution"
    )

    by_job = _list_by(queue, settings, job_id=owner.job_id)
    by_execution = _list_by(queue, settings, execution_id=execution_id)

    assert by_execution["total"] == by_job["total"] == 1
    assert {a["artifact_id"] for a in by_execution["artifacts"]} == {
        a["artifact_id"] for a in by_job["artifacts"]
    }


def test_mcp_list_by_execution_id_works_for_a_sync_run_with_no_artifact_page(
    tmp_path: Path,
) -> None:
    """clio-relay#278 design point 3: resolution is by execution record, not
    by ``artifact_page`` presence -- a synchronous ``jarvis_run`` dispatch
    never carries one at all (clio-relay#266), so this never runs
    ``ingest_jarvis_execution_outputs``; the artifact is appended directly,
    exactly as the generic per-job spool registration
    (``endpoint_result_finalization.py``) does for any job regardless of
    sync/deferred shape.
    """
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    execution_id = "execution-278-sync"
    owner = queue.submit_job(_call_job(tool="jarvis_run", execution_id=execution_id, key="run"))
    artifact_path = tmp_path / "sync-stdout.log"
    artifact_path.write_bytes(b"sync run output\n")
    artifact = queue.append_artifact(
        ArtifactRef(job_id=owner.job_id, uri=artifact_path.as_uri(), kind="stdout")
    )

    by_execution = _list_by(queue, settings, execution_id=execution_id)

    assert by_execution["total"] == 1
    assert by_execution["artifacts"][0]["artifact_id"] == artifact.artifact_id


def test_mcp_list_artifacts_rejects_an_unknown_execution_id(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    queue.submit_job(_call_job(tool="jarvis_run", execution_id="execution-known", key="run"))

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "relay_list_artifacts",
                "arguments": {"execution_id": "execution-never-admitted"},
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
    )

    assert "execution_not_found" in _mcp_error_message(response)


def test_mcp_list_artifacts_rejects_both_job_id_and_execution_id(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    owner = queue.submit_job(_call_job(tool="jarvis_run", execution_id="execution-x", key="run"))

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "relay_list_artifacts",
                "arguments": {"job_id": owner.job_id, "execution_id": "execution-x"},
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
    )

    assert "artifact_scope_ambiguous" in _mcp_error_message(response)


def test_mcp_list_artifacts_rejects_neither_job_id_nor_execution_id(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "relay_list_artifacts", "arguments": {}},
        },
        queue=queue,
        settings=settings,
        profile="user",
    )

    assert "artifact_scope_ambiguous" in _mcp_error_message(response)


def test_mcp_read_artifact_after_listing_by_execution_id(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    execution_id = "execution-278-read-after-list"
    _register_deferred_execution_output(
        queue, execution_id=execution_id, execution_root=tmp_path / "execution"
    )

    listed = _list_by(queue, settings, execution_id=execution_id)
    artifact_id = listed["artifacts"][0]["artifact_id"]

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "relay_read_artifact", "arguments": {"artifact_id": artifact_id}},
        },
        queue=queue,
        settings=settings,
        profile="user",
    )
    document = _mcp_result(response)
    assert document["encoding"] == "base64"


# --------------------------------------------------------------------------- #
# Door HTTP surface (GET /executions/{execution_id}/artifacts)
# --------------------------------------------------------------------------- #


def test_http_get_artifacts_by_execution_matches_get_artifacts_by_job(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    execution_id = "execution-278-http"
    owner = _register_deferred_execution_output(
        queue, execution_id=execution_id, execution_root=tmp_path / "execution"
    )
    client = cast(Any, TestClient(create_app(settings)))

    by_job = client.get(f"/jobs/{owner.job_id}/artifacts")
    by_execution = client.get(f"/executions/{execution_id}/artifacts")

    assert by_job.status_code == 200
    assert by_execution.status_code == 200
    assert by_execution.json()["total"] == by_job.json()["total"] == 1
    assert {a["artifact_id"] for a in by_execution.json()["artifacts"]} == {
        a["artifact_id"] for a in by_job.json()["artifacts"]
    }


def test_http_get_artifacts_by_execution_rejects_unknown_execution_id(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    queue.submit_job(_call_job(tool="jarvis_run", execution_id="execution-known", key="run"))
    client = cast(Any, TestClient(create_app(settings)))

    response = client.get("/executions/execution-never-admitted/artifacts")

    assert response.status_code == 404
    document = response.json()
    assert document["reason"] == "execution_not_found"


# --------------------------------------------------------------------------- #
# CLI surface (job list-artifacts --execution-id)
# --------------------------------------------------------------------------- #


def test_cli_list_artifacts_by_execution_id_matches_by_job_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    execution_id = "execution-278-cli"
    owner = _register_deferred_execution_output(
        queue, execution_id=execution_id, execution_root=tmp_path / "execution"
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(settings.core_dir))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(settings.spool_dir))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    runner = CliRunner()

    by_job = runner.invoke(app, ["job", "list-artifacts", owner.job_id])
    by_execution = runner.invoke(app, ["job", "list-artifacts", "--execution-id", execution_id])

    assert by_job.exit_code == 0, by_job.output
    assert by_execution.exit_code == 0, by_execution.output
    job_page = json.loads(by_job.output)
    execution_page = json.loads(by_execution.output)
    assert execution_page["total"] == job_page["total"] == 1
    assert {a["artifact_id"] for a in execution_page["artifacts"]} == {
        a["artifact_id"] for a in job_page["artifacts"]
    }


def test_cli_list_artifacts_rejects_both_job_id_and_execution_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    ClioCoreQueue(settings.core_dir)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(settings.core_dir))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(settings.spool_dir))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    runner = CliRunner()

    result = runner.invoke(
        app, ["job", "list-artifacts", "job_placeholder", "--execution-id", "execution-x"]
    )

    assert result.exit_code != 0
    assert "artifact_scope_ambiguous" in result.output


def test_cli_list_artifacts_rejects_neither_job_id_nor_execution_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    ClioCoreQueue(settings.core_dir)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(settings.core_dir))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(settings.spool_dir))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    runner = CliRunner()

    result = runner.invoke(app, ["job", "list-artifacts"])

    assert result.exit_code != 0
    assert "artifact_scope_ambiguous" in result.output
