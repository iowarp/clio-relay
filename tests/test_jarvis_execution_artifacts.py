from __future__ import annotations

import base64
import hashlib
from pathlib import Path
from typing import Any, cast

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.jarvis_execution_artifacts import (
    EXECUTION_OUTPUTS_MISSING_SCHEMA,
    ingest_jarvis_execution_outputs,
)
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

    indexed, truncation, outputs_missing = ingest_jarvis_execution_outputs(queue, query, result)

    assert truncation is None
    assert outputs_missing is None
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
    """Original fixture shape restored (adversarial review): this isolates
    the truncation-detection code path with ONLY the truncation marker
    declared, deliberately zero real ``execution-file`` entries. D1's own
    zero-declared check is unaffected by this fixture's isolated shape --
    it legitimately reports ``no_outputs_declared`` here too (asserted
    below), exactly as it would for any page declaring no execution-file
    entries; Ruling B is what keeps that SIGNAL from failing the job, a
    concern one layer up (``resolve_execution_outcome``), not this
    function's.
    """
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

    indexed, truncation, outputs_missing = ingest_jarvis_execution_outputs(queue, query, result)

    assert indexed == []
    assert truncation == {
        "schema_version": "jarvis.execution-output-truncation.v1",
        "limit": 64,
        "observed_count": 65,
        "omitted_count": 1,
    }
    assert outputs_missing is not None
    assert outputs_missing["reason"] == "no_outputs_declared"
    events, _ = queue.drain_events(Cursor(job_id=owner.job_id), limit=100)
    assert any(
        event.event_type == "jarvis.execution_outputs_truncated"
        and event.payload["omitted_count"] == 1
        for event in events
    )


def _terminal_result(
    execution_id: str,
    execution_root: Path,
    *,
    artifacts: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "structured_result": {
            "execution_id": execution_id,
            "execution_record": {
                "terminal": True,
                "metadata": {"pipeline_snapshot_path": str(execution_root / "submit.sh")},
            },
            "artifact_page": {
                "terminal": True,
                "artifacts": artifacts,
            },
        }
    }


def test_declared_and_present_output_is_not_outputs_missing(tmp_path: Path) -> None:
    """clio-relay#265: declared + present + non-empty -> no outputs_missing verdict."""
    queue = ClioCoreQueue(tmp_path / "core")
    execution_id = "execution-present"
    queue.submit_job(_call_job(tool="jarvis_run", execution_id=execution_id, key="run"))
    query = queue.submit_job(
        _call_job(tool="jarvis_get_execution", execution_id=execution_id, key="query")
    )
    execution_root = tmp_path / "execution"
    execution_root.mkdir()
    payload = b"log line one\n"
    (execution_root / "stdout.log").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    result = _terminal_result(
        execution_id,
        execution_root,
        artifacts=[
            {
                "package_id": "jarvis.execution",
                "kind": "execution-file",
                "role": "log",
                "location": {"kind": "execution_path", "value": "stdout.log"},
                "size_bytes": len(payload),
                "checksum": f"sha256:{digest}",
            }
        ],
    )

    indexed, _truncation, outputs_missing = ingest_jarvis_execution_outputs(queue, query, result)

    assert len(indexed) == 1
    assert outputs_missing is None


def test_declared_and_missing_output_is_typed_outputs_missing(tmp_path: Path) -> None:
    """clio-relay#265: declared but absent on disk -> typed outputs_missing, no crash."""
    queue = ClioCoreQueue(tmp_path / "core")
    execution_id = "execution-missing"
    owner = queue.submit_job(_call_job(tool="jarvis_run", execution_id=execution_id, key="run"))
    query = queue.submit_job(
        _call_job(tool="jarvis_get_execution", execution_id=execution_id, key="query")
    )
    execution_root = tmp_path / "execution"
    execution_root.mkdir()
    # dump.h5 is declared but was never actually written to disk.
    result = _terminal_result(
        execution_id,
        execution_root,
        artifacts=[
            {
                "package_id": "jarvis.execution",
                "kind": "execution-file",
                "role": "output",
                "location": {"kind": "execution_path", "value": "dump.h5"},
                "size_bytes": 4096,
                "checksum": f"sha256:{'a' * 64}",
            }
        ],
    )

    indexed, _truncation, outputs_missing = ingest_jarvis_execution_outputs(queue, query, result)

    assert indexed == []
    assert outputs_missing is not None
    assert outputs_missing["schema_version"] == EXECUTION_OUTPUTS_MISSING_SCHEMA
    assert outputs_missing["reason"] == "declared_outputs_missing"
    assert outputs_missing["execution_id"] == execution_id
    assert outputs_missing["declared_count"] == 1
    assert outputs_missing["missing"] == [
        {
            "relative_path": "dump.h5",
            "role": "output",
            "reason": "absent",
            "declared_size_bytes": 4096,
        }
    ]
    events, _ = queue.drain_events(Cursor(job_id=owner.job_id), limit=100)
    assert any(event.event_type == "jarvis.execution_output_missing" for event in events)
    assert any(event.event_type == "jarvis.execution_outputs_missing" for event in events)


def test_declared_and_empty_output_is_typed_outputs_missing(tmp_path: Path) -> None:
    """clio-relay#265: declared but 0 bytes -> typed outputs_missing, still indexed."""
    queue = ClioCoreQueue(tmp_path / "core")
    execution_id = "execution-empty"
    owner = queue.submit_job(_call_job(tool="jarvis_run", execution_id=execution_id, key="run"))
    query = queue.submit_job(
        _call_job(tool="jarvis_get_execution", execution_id=execution_id, key="query")
    )
    execution_root = tmp_path / "execution"
    execution_root.mkdir()
    (execution_root / "stdout.log").write_bytes(b"")
    empty_digest = hashlib.sha256(b"").hexdigest()
    result = _terminal_result(
        execution_id,
        execution_root,
        artifacts=[
            {
                "package_id": "jarvis.execution",
                "kind": "execution-file",
                "role": "log",
                "location": {"kind": "execution_path", "value": "stdout.log"},
                "size_bytes": 0,
                "checksum": f"sha256:{empty_digest}",
            }
        ],
    )

    indexed, _truncation, outputs_missing = ingest_jarvis_execution_outputs(queue, query, result)

    # Still indexed -- a verified, genuinely 0-byte file is fetchable -- but
    # also typed as an outputs-missing reason (#265: empty counts too).
    assert len(indexed) == 1
    assert outputs_missing is not None
    assert outputs_missing["reason"] == "declared_outputs_missing"
    assert outputs_missing["missing"] == [
        {
            "relative_path": "stdout.log",
            "role": "log",
            "reason": "empty",
            "declared_size_bytes": 0,
        }
    ]
    events, _ = queue.drain_events(Cursor(job_id=owner.job_id), limit=100)
    assert any(event.event_type == "jarvis.execution_output_empty" for event in events)


def test_zero_declared_outputs_is_typed_outputs_missing(tmp_path: Path) -> None:
    """clio-relay#265 D1: a completed run declaring ZERO outputs is not silently clean.

    Revises the pre-D1 "nothing declared keeps current semantics" ruling: a
    terminal artifact page that IS present but declares no execution-file
    entries at all (a 0-step/empty-output run) is exactly the false-green
    shape #265's own issue text names, distinct from "some declared outputs
    were found missing/empty" (reason=declared_outputs_missing, covered by
    the two tests above).
    """
    queue = ClioCoreQueue(tmp_path / "core")
    execution_id = "execution-nothing-declared"
    query = queue.submit_job(
        _call_job(tool="jarvis_get_execution", execution_id=execution_id, key="query")
    )
    owner = queue.submit_job(_call_job(tool="jarvis_run", execution_id=execution_id, key="run"))
    execution_root = tmp_path / "execution"
    execution_root.mkdir()
    result = _terminal_result(execution_id, execution_root, artifacts=[])

    indexed, truncation, outputs_missing = ingest_jarvis_execution_outputs(queue, query, result)

    assert indexed == []
    assert truncation is None
    assert outputs_missing is not None
    assert outputs_missing["schema_version"] == EXECUTION_OUTPUTS_MISSING_SCHEMA
    assert outputs_missing["reason"] == "no_outputs_declared"
    assert outputs_missing["execution_id"] == execution_id
    assert outputs_missing["declared_count"] == 0
    assert outputs_missing["missing"] == []
    events, _ = queue.drain_events(Cursor(job_id=owner.job_id), limit=100)
    assert any(
        event.event_type == "jarvis.execution_outputs_missing"
        and event.payload.get("reason") == "no_outputs_declared"
        for event in events
    )


def test_no_artifact_page_keeps_current_semantics(tmp_path: Path) -> None:
    """clio-relay#265: a synchronous dispatch with no artifact_page at all is untouched.

    Distinct from zero DECLARED outputs (above): here the terminal record
    carries no artifact_page key whatsoever (jarvis_run's own outputSchema
    never declares one on a synchronous/non-#266-watched dispatch), so the
    pre-existing early return applies unchanged -- this is not a case #265
    can verify either way.
    """
    queue = ClioCoreQueue(tmp_path / "core")
    execution_id = "execution-no-artifact-page"
    query = queue.submit_job(
        _call_job(tool="jarvis_get_execution", execution_id=execution_id, key="query")
    )
    queue.submit_job(_call_job(tool="jarvis_run", execution_id=execution_id, key="run"))
    result: dict[str, Any] = {
        "structured_result": {
            "execution_id": execution_id,
            "execution_record": {"terminal": True, "metadata": {}},
        }
    }

    indexed, truncation, outputs_missing = ingest_jarvis_execution_outputs(queue, query, result)

    assert indexed == []
    assert truncation is None
    assert outputs_missing is None
