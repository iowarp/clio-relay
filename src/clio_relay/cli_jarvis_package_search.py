"""JARVIS package-search query execution (iowarp/clio-relay#231
continuation): the acceptance type and runner for ``jarvis-mcp-
validate``'s package-search phase."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import clio_relay.cli_jarvis_artifact_io as cli_jarvis_artifact_io
import clio_relay.cli_remote_collection_pagination as cli_remote_collection_pagination
import clio_relay.core_queue as core_queue
import clio_relay.mcp_stdio_validation as mcp_stdio_validation
import clio_relay.remote_cli as remote_cli
from clio_relay.cluster_config import (
    ClusterDefinition,
)


@dataclass(frozen=True)
class _JarvisPackageSearchAcceptance:
    """Durable evidence from one bounded JARVIS package-discovery query."""

    tools_list_response: dict[str, Any]
    call_response: dict[str, Any]
    call_job_id: str
    call_status: dict[str, Any]
    artifacts: list[dict[str, Any]]
    mcp_result: dict[str, Any] | None
    provenance: dict[str, Any] | None
    initialize_response: dict[str, Any]
    stdio_evidence: dict[str, Any]


def _run_jarvis_package_search_query(
    *,
    cluster: str,
    definition: ClusterDefinition,
    queue: core_queue.ClioCoreQueue,
    profile: str,
    query: str,
    wait_timeout_seconds: float,
    poll_seconds: float,
) -> _JarvisPackageSearchAcceptance:
    """Exercise bounded package discovery through the local virtual MCP surface."""
    session = mcp_stdio_validation.run_packaged_mcp_stdio_session(
        profile=profile,
        tool="jarvis_describe",
        arguments={
            "cluster": cluster,
            "target": "package_search",
            "query": query,
            "page_size": 5,
        },
    )
    call_job_id = cli_jarvis_artifact_io._mcp_response_job_id(session.tools_call_response)
    if remote_cli.should_execute_on_cluster(definition):
        call_status = cli_remote_collection_pagination._wait_for_remote_job_terminal(
            definition,
            call_job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        artifacts = cli_jarvis_artifact_io._remote_artifact_records(definition, call_job_id)
        mcp_result = cli_jarvis_artifact_io._read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="mcp_result",
        )
        provenance = cli_jarvis_artifact_io._read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="provenance",
        )
    else:
        call_status = cli_remote_collection_pagination._wait_for_local_job_terminal(
            queue,
            call_job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        artifacts = cli_remote_collection_pagination._complete_local_artifact_records(
            queue, call_job_id
        )
        mcp_result = cli_jarvis_artifact_io._read_local_json_artifact_kind(
            queue, artifacts, kind="mcp_result"
        )
        provenance = cli_jarvis_artifact_io._read_local_json_artifact_kind(
            queue, artifacts, kind="provenance"
        )
    return _JarvisPackageSearchAcceptance(
        tools_list_response=session.tools_list_response,
        call_response=session.tools_call_response,
        call_job_id=call_job_id,
        call_status=cast(dict[str, Any], call_status),
        artifacts=artifacts,
        mcp_result=mcp_result,
        provenance=provenance,
        initialize_response=session.initialize_response,
        stdio_evidence=session.evidence(),
    )
