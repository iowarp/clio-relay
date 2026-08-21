"""Failing-first regression coverage for clio-relay#264.

Three consecutive live runs on ares showed ``relay_list_artifacts`` and
``relay_read_artifact`` answering not-found for every id shape a jarvis
execution legitimately surfaces to an agent (job ids from spack_locate,
create_pipeline, add_step, jarvis_run, get_execution, the scheduler task
id -- and even an ``artifact_id`` copied verbatim from a job's own inline
result). #252's execution-output registration
(``jarvis_execution_artifacts.ingest_jarvis_execution_outputs``) does fire
and does index the artifact -- but it indexes it against the *cluster*
that ran the job, e.g. ares, not the door process serving the MCP tools.
Every other job/artifact-scoped tool (``relay_status``, ``relay_cancel``,
``relay_artifact_lineage``, ``relay_wait``) accepts an explicit
``cluster``/``route_revision`` handle and routes to the cluster that
actually holds the record; ``relay_list_artifacts``/``relay_read_artifact``
never gained that routing, so they always queried the door's local core
regardless of the caller's cluster/route_revision arguments.

These tests simulate the two-core topology directly: a "remote" queue
(standing in for ares, reached by faking ``run_remote_clio``/the owned
session client) holds the real registered artifact; a completely separate,
empty "local" (door) queue is the one ``handle_request`` is called
against. Before the fix, both tests fail with a not-found error even
though the caller supplies the same cluster/route_revision handle its own
job result already carried.
"""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from typing import Any, cast

import pytest

from clio_relay.cluster_config import (
    CLUSTER_REGISTRY_ENV,
    ClusterDefinition,
    ClusterRegistry,
    cluster_route_revision,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.jarvis_execution_artifacts import ingest_jarvis_execution_outputs
from clio_relay.mcp_server import handle_request
from clio_relay.models import ArtifactRef, JobKind, McpCallSpec, RelayJob
from clio_relay.relay_ops import read_artifact_bytes


def _call_job(*, cluster: str, tool: str, execution_id: str, key: str) -> RelayJob:
    return RelayJob(
        cluster=cluster,
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(server="jarvis-mcp", tool=tool, arguments={"execution_id": execution_id}),
        idempotency_key=key,
    )


def _register_remote_execution_output(
    remote_queue: ClioCoreQueue,
    *,
    cluster: str,
    execution_root: Path,
) -> tuple[RelayJob, ArtifactRef]:
    """Register one execution-output artifact exactly as #252 does, on the remote core."""
    execution_id = "execution-ares-264"
    owner = remote_queue.submit_job(
        _call_job(cluster=cluster, tool="jarvis_run", execution_id=execution_id, key="run")
    )
    query = remote_queue.submit_job(
        _call_job(cluster=cluster, tool="jarvis_get_execution", execution_id=execution_id, key="q")
    )
    execution_root.mkdir(parents=True, exist_ok=True)
    payload = b"Step Temp E_pair\n0 42.0 -1.25\n"
    (execution_root / "stdout.log").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    result_document: dict[str, Any] = {
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
    indexed, truncation, outputs_missing = ingest_jarvis_execution_outputs(
        remote_queue, query, result_document
    )
    assert truncation is None
    assert outputs_missing is None
    assert len(indexed) == 1
    return owner, indexed[0]


def _bind_remote_cluster(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    cluster: str,
) -> ClusterDefinition:
    definition = ClusterDefinition(name=cluster, ssh_host=f"{cluster}-login")
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={cluster: definition}).save(registry_path)
    monkeypatch.setenv(CLUSTER_REGISTRY_ENV, str(registry_path))
    return definition


def _fake_remote_cli(remote_queue: ClioCoreQueue) -> Any:
    """Return a ``run_remote_clio`` stand-in that answers from the remote core.

    Mirrors exactly what the real SSH-connected ``clio-relay`` CLI on the
    remote cluster returns for ``job list-artifacts``/``job read-artifact``
    (``cli.py``'s ``job_list_artifacts``/``job_read_artifact`` commands),
    without needing a live SSH transport.
    """

    def run_remote_clio(_definition: ClusterDefinition, args: list[str]) -> str:
        assert args[0] == "job"
        if args[1] == "list-artifacts":
            job_id = args[2]
            cursor = int(args[args.index("--cursor") + 1])
            limit = int(args[args.index("--limit") + 1])
            artifacts, next_cursor, total = remote_queue.list_artifacts_page(
                job_id, cursor=cursor, limit=limit
            )
            return json.dumps(
                {
                    "artifacts": [a.model_dump(mode="json") for a in artifacts],
                    "cursor": cursor,
                    "limit": limit,
                    "next_cursor": next_cursor,
                    "total": total,
                }
            )
        if args[1] == "read-artifact":
            artifact_id = args[2]
            return json.dumps(read_artifact_bytes(remote_queue, artifact_id))
        raise AssertionError(f"unexpected remote CLI invocation: {args}")

    return run_remote_clio


def test_relay_list_artifacts_routes_to_the_cluster_that_ran_a_jarvis_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote_queue = ClioCoreQueue(tmp_path / "ares_core")
    definition = _bind_remote_cluster(monkeypatch, tmp_path, cluster="ares")
    owner, artifact = _register_remote_execution_output(
        remote_queue, cluster="ares", execution_root=tmp_path / "execution"
    )

    # The door process serving MCP tools has its own, completely separate,
    # EMPTY core -- it has never heard of `owner.job_id`.
    door_settings = RelaySettings(
        core_dir=tmp_path / "door_core", spool_dir=tmp_path / "door_spool"
    )
    door_queue = ClioCoreQueue(door_settings.core_dir)

    monkeypatch.setattr(
        "clio_relay.artifact_routing.run_remote_clio",
        _fake_remote_cli(remote_queue),
    )

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "relay_list_artifacts",
                "arguments": {
                    "job_id": owner.job_id,
                    "cluster": "ares",
                    "route_revision": cluster_route_revision(definition),
                },
            },
        },
        queue=door_queue,
        settings=door_settings,
        profile="user",
    )

    assert response is not None
    assert "error" not in response, response
    structured = response["result"]["structuredContent"]
    assert structured["total"] == 1
    assert structured["cluster"] == "ares"
    returned_ids = {item["artifact_id"] for item in structured["artifacts"]}
    assert returned_ids == {artifact.artifact_id}


def test_relay_read_artifact_routes_to_the_cluster_that_ran_a_jarvis_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote_queue = ClioCoreQueue(tmp_path / "ares_core")
    definition = _bind_remote_cluster(monkeypatch, tmp_path, cluster="ares")
    _owner, artifact = _register_remote_execution_output(
        remote_queue, cluster="ares", execution_root=tmp_path / "execution"
    )

    door_settings = RelaySettings(
        core_dir=tmp_path / "door_core", spool_dir=tmp_path / "door_spool"
    )
    door_queue = ClioCoreQueue(door_settings.core_dir)

    monkeypatch.setattr(
        "clio_relay.artifact_routing.run_remote_clio",
        _fake_remote_cli(remote_queue),
    )

    # This is exactly the reported second-order symptom: the artifact_id was
    # copied VERBATIM from the job's own inline result.
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "relay_read_artifact",
                "arguments": {
                    "artifact_id": artifact.artifact_id,
                    "cluster": "ares",
                    "route_revision": cluster_route_revision(definition),
                },
            },
        },
        queue=door_queue,
        settings=door_settings,
        profile="user",
    )

    assert response is not None
    assert "error" not in response, response
    structured = response["result"]["structuredContent"]
    assert structured["encoding"] == "base64"
    assert base64.b64decode(cast(str, structured["data"])) == b"Step Temp E_pair\n0 42.0 -1.25\n"


def test_relay_read_artifact_still_refuses_internal_mcp_result_kind_when_routed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The model-readable gate must survive cluster routing, not be bypassed by it."""
    remote_queue = ClioCoreQueue(tmp_path / "ares_core")
    definition = _bind_remote_cluster(monkeypatch, tmp_path, cluster="ares")
    owner = remote_queue.submit_job(
        _call_job(cluster="ares", tool="jarvis_run", execution_id="exec-gate", key="run-gate")
    )
    payload = b'{"internal": true}'
    # mcp_result artifacts are owned by their job's spool directory (the
    # convention read_artifact_bytes falls back to when no explicit
    # owned_root_uri/ownership_schema is recorded -- matches how endpoint.py
    # actually writes one).
    spool_dir = remote_queue.root.parent / "spool" / owner.job_id
    spool_dir.mkdir(parents=True, exist_ok=True)
    result_path = spool_dir / "mcp_result.json"
    result_path.write_bytes(payload)
    protocol_artifact = remote_queue.append_artifact(
        ArtifactRef(
            job_id=owner.job_id,
            uri=result_path.absolute().as_uri(),
            kind="mcp_result",
            size_bytes=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
        )
    )

    door_settings = RelaySettings(
        core_dir=tmp_path / "door_core", spool_dir=tmp_path / "door_spool"
    )
    door_queue = ClioCoreQueue(door_settings.core_dir)
    monkeypatch.setattr(
        "clio_relay.artifact_routing.run_remote_clio",
        _fake_remote_cli(remote_queue),
    )

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "relay_read_artifact",
                "arguments": {
                    "artifact_id": protocol_artifact.artifact_id,
                    "cluster": "ares",
                    "route_revision": cluster_route_revision(definition),
                },
            },
        },
        queue=door_queue,
        settings=door_settings,
        profile="user",
    )

    assert response is not None
    assert "error" in response
    assert "not model-readable" in json.dumps(response["error"])
