from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from pytest import MonkeyPatch

from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    JARVIS_MCP_CACHE_SERVER_NAME,
    jarvis_user_contract,
)
from clio_relay.mcp_stdio_validation import run_packaged_mcp_stdio_session
from clio_relay.models import McpCallSpec
from clio_relay.remote_mcp import (
    RemoteMcpDiscoveryProvenance,
    RemoteMcpSchemaCache,
    RemoteMcpSchemaCacheEntry,
    RemoteMcpToolSchema,
    remote_mcp_server_artifact_digest,
)
from tests.jarvis_mcp_fakes import verified_jarvis_server_artifact


def test_packaged_stdio_session_initializes_lists_and_calls_virtual_jarvis(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    cache_path = tmp_path / ".clio-relay" / "remote-mcp-cache.json"
    monkeypatch.setenv("CLIO_RELAY_REMOTE_MCP_CACHE", str(cache_path))
    ClusterRegistry(clusters={"alpha": ClusterDefinition(name="alpha", ssh_host="localhost")}).save(
        tmp_path / ".clio-relay" / "clusters.json"
    )
    server_artifact = verified_jarvis_server_artifact()
    contract = jarvis_user_contract()
    now = datetime.now(UTC)
    RemoteMcpSchemaCache.update_entry(
        cache_path,
        RemoteMcpSchemaCacheEntry(
            cluster="alpha",
            server_name=JARVIS_MCP_CACHE_SERVER_NAME,
            execution_fingerprint="fixture",
            discovered_at=now,
            expires_at=now + timedelta(hours=1),
            schema_digest=CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
            tools=[
                RemoteMcpToolSchema(
                    name=name,
                    description=str(definition["description"]),
                    input_schema=definition["inputSchema"],
                    output_schema=definition["outputSchema"],
                    annotations=definition["annotations"],
                )
                for name, definition in contract.items()
            ],
            provenance=RemoteMcpDiscoveryProvenance(
                discovery_job_id="job-discovery",
                artifact_id="artifact-discovery",
                artifact_sha256="b" * 64,
                server_artifact=server_artifact,
            ),
        ),
    )

    session = run_packaged_mcp_stdio_session(
        profile="user",
        tool="jarvis_run",
        arguments={"cluster": "alpha", "pipeline_id": "stdio-acceptance"},
    )

    initialize = session.initialize_response["result"]
    listed = session.tools_list_response["result"]["tools"]
    assert "result" in session.tools_call_response, session.evidence()
    call = session.tools_call_response["result"]["structuredContent"]
    job = ClioCoreQueue(tmp_path / "core").get_job(call["job_id"])
    assert initialize["serverInfo"]["name"] == "clio-relay"
    assert "jarvis_run" in {tool["name"] for tool in listed}
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.expected_server_artifact_digest == remote_mcp_server_artifact_digest(
        server_artifact
    )
    assert job.spec.tool == "jarvis_run"
    assert job.spec.arguments == {"pipeline_id": "stdio-acceptance"}
    assert session.evidence()["boundary"] == "packaged_clio_relay_mcp_server_stdio"
    assert session.transcript_sha256
