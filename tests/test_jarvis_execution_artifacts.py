from __future__ import annotations

import base64
import hashlib
from pathlib import Path
from typing import Any, cast

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.jarvis_execution_artifacts import ingest_jarvis_execution_outputs
from clio_relay.mcp_server import handle_request
from clio_relay.models import Cursor, JobKind, McpCallSpec, RelayJob
from clio_relay.relay_ops import read_artifact_bytes


def _call_job(*, tool: str, execution_id: str, key: str) -> RelayJob:
    return RelayJob(
        cluster="test-cluster",
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server="jarvis-mcp",
            tool=tool,
            arguments={"execution_id": execution_id},
        ),
        idempotency_key=key,
    )


def test_terminal_outputs_are_referenced_fetched_and_produced_by_lineage(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    execution_id = "execution-known-bytes"
    owner = queue.submit_job(_call_job(tool="jarvis_run", execution_id=execution_id, key="run"))
    query = queue.submit_job(
        _call_job(tool="jarvis_get_execution", execution_id=execution_id, key="query")
    )
    execution_root = tmp_path / "execution"
    execution_root.mkdir()
    payload = b"Step Temp E_pair\n0 42.0 -1.25\n"
    output_path = execution_root / "stdout.log"
    output_path.write_bytes(payload)
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

    indexed, truncation = ingest_jarvis_execution_outputs(queue, query, result)

    assert truncation is None
    assert len(indexed) == 1
    artifact = indexed[0]
    assert artifact.job_id == owner.job_id
    assert artifact.kind == "execution_output"
    fetched = read_artifact_bytes(queue, artifact.artifact_id)
    assert base64.b64decode(cast(str, fetched["data"])) == payload
    lineage = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "relay_artifact_lineage",
                "arguments": {
                    "artifact_id": artifact.artifact_id,
                },
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
    )
    assert lineage is not None
    produced_by = lineage["result"]["structuredContent"]["produced_by"]
    assert produced_by["job_id"] == owner.job_id
    assert produced_by["execution_id"] == execution_id


def test_declared_truncation_is_typed_and_emitted_as_a_relay_event(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    execution_id = "execution-truncated"
    owner = queue.submit_job(_call_job(tool="jarvis_run", execution_id=execution_id, key="run"))
    query = queue.submit_job(
        _call_job(tool="jarvis_get_execution", execution_id=execution_id, key="query")
    )
    execution_root = tmp_path / "execution"
    execution_root.mkdir()
    result: dict[str, Any] = {
        "structured_result": {
            "execution_id": execution_id,
            "execution_record": {
                "terminal": True,
                "metadata": {"script_path": str(execution_root / "submit.sh")},
            },
            "artifact_page": {
                "terminal": True,
                "artifacts": [
                    {
                        "package_id": "jarvis.execution",
                        "kind": "execution-output-truncation",
                        "metadata": {
                            "schema_version": "jarvis.execution-output-truncation.v1",
                            "limit": 64,
                            "observed_count": 65,
                            "omitted_count": 1,
                        },
                    }
                ],
            },
        }
    }

    indexed, truncation = ingest_jarvis_execution_outputs(queue, query, result)

    assert indexed == []
    assert truncation == {
        "schema_version": "jarvis.execution-output-truncation.v1",
        "limit": 64,
        "observed_count": 65,
        "omitted_count": 1,
    }
    events, _ = queue.drain_events(Cursor(job_id=owner.job_id), limit=100)
    assert any(
        event.event_type == "jarvis.execution_outputs_truncated"
        and event.payload["omitted_count"] == 1
        for event in events
    )
