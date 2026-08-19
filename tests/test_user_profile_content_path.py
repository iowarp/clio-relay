"""User-profile produced-artifact content path (2026-08-19 ares L3 clause-(d)).

The grounded L3 run twice located a completed LAMMPS execution's real
``log.lammps`` thermo output and could not read it: ``relay_read_artifact``
exists (declared AND dispatched) but was absent from ``USER_MCP_TOOL_NAMES``,
and the user profile exposed no produced-outputs discovery either
(``relay_artifact_lineage``'s job direction lists a job's CONSUMED inputs --
the retry proved every job_id returns ``used_artifacts: []`` for outputs).
The user surface must carry the whole produced-content path: discovery
(``relay_list_artifacts`` by job_id) plus bounded fetch
(``relay_read_artifact`` by artifact_id). Evidence:
``D:\\relay-p5local\\evidence\\l3-run-20260819T073640.json``.
"""

from __future__ import annotations

import base64
import hashlib
from pathlib import Path

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.mcp_server import handle_request
from clio_relay.models import ArtifactRef, JarvisRunSpec, JobKind, RelayJob


def _produced_artifact(tmp_path: Path, queue: ClioCoreQueue) -> tuple[RelayJob, ArtifactRef, bytes]:
    job = queue.submit_job(
        RelayJob(
            cluster="desktop",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "thermo"]),
            idempotency_key="produced-content-path",
        )
    )
    payload = b"Step Temp E_pair\n100 0.7128 -5.6021\n"
    owned_dir = tmp_path / "spool" / job.job_id
    owned_dir.mkdir(parents=True)
    output = owned_dir / "log.lammps"
    output.write_bytes(payload)
    artifact = queue.append_artifact(
        ArtifactRef(
            job_id=job.job_id,
            uri=output.absolute().as_uri(),
            kind="execution_output",
            size_bytes=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
        )
    )
    return job, artifact, payload


def test_user_profile_advertises_both_content_tools(tmp_path: Path) -> None:
    """tools/list under ``--profile user`` carries discovery AND fetch."""
    queue = ClioCoreQueue(tmp_path / "core")
    listed = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        profile="user",
    )
    assert listed is not None
    names = {tool["name"] for tool in listed["result"]["tools"]}
    assert "relay_list_artifacts" in names, "discovery half absent from the user profile"
    assert "relay_read_artifact" in names, "fetch half absent from the user profile"


def test_user_profile_content_path_round_trips(tmp_path: Path) -> None:
    """List a job's produced artifacts, then read one's bytes, all as user."""
    queue = ClioCoreQueue(tmp_path / "core")
    job, artifact, payload = _produced_artifact(tmp_path, queue)

    page = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "relay_list_artifacts", "arguments": {"job_id": job.job_id}},
        },
        queue=queue,
        profile="user",
    )
    assert page is not None
    records = page["result"]["structuredContent"]["artifacts"]
    assert [record["artifact_id"] for record in records] == [artifact.artifact_id]

    read = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "relay_read_artifact",
                "arguments": {"artifact_id": artifact.artifact_id},
            },
        },
        queue=queue,
        profile="user",
    )
    assert read is not None
    envelope = read["result"]["structuredContent"]
    assert envelope["encoding"] == "base64"
    assert base64.b64decode(envelope["data"]) == payload
    assert envelope["artifact"]["artifact_id"] == artifact.artifact_id
