"""Job status and artifact-lineage MCP tools: reading one job's state and
its used-artifact/used-by lineage, each branching between a local
queue.py-backed path and a remote (SSH or owned-session) path via the
shared cluster-target resolver.

Split out of mcp_job_lifecycle.py (iowarp/clio-relay#231) -- that module
landed at 813 lines as a single file, over the 800-line ratchet cap, so it
splits along its own seam: this is the read-only status/lineage half;
mcp_job_lifecycle.py (unchanged name) keeps the mutating/bounded-
reconciliation half (cancel/observe/wait). `_job_target` and
`_require_local_job_cluster` are the shared cluster-target resolver both
halves use; neither is monkeypatched, so mcp_job_lifecycle.py's plain
cross-module import of them back is a normal one-directional leaf
dependency, not a back-reference.

Each of `_status_job`/`_used_artifacts_tool`/`_used_by_tool` calls three
names tests monkeypatch at `mcp_server_module.<name>`
(`should_execute_on_cluster`, `OwnedSessionApiClient`, `_remote_json`)
through the function-scope `_mcp_server.<name>(...)` back-reference
established in slices 3-7.
"""

from __future__ import annotations

import hmac
from typing import Any

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.mcp_arguments import (
    _optional_durable_record_id,
    _required_durable_record_id,
    _response_page_limit,
)
from clio_relay.mcp_remote_catalog import _route_revision, _validated_route_revision
from clio_relay.mcp_remote_transport import (
    _owned_json,
    _validate_owned_job_status,
)
from clio_relay.relay_ops import job_status

JSON = dict[str, Any]


def _job_target(arguments: JSON) -> ClusterDefinition | None:
    """Resolve and verify an optional self-routing cluster job handle."""
    from clio_relay import mcp_server as _mcp_server

    raw_cluster = arguments.get("cluster")
    raw_revision = arguments.get("route_revision")
    if raw_cluster is None:
        if raw_revision is not None:
            raise ValueError("route_revision requires cluster")
        return None
    if not isinstance(raw_cluster, str) or not raw_cluster:
        raise ValueError("cluster must be a non-empty string")
    if raw_revision is None:
        raise ValueError("route_revision is required when cluster routes an existing job handle")
    revision = _validated_route_revision(raw_revision)
    definition = _mcp_server._remote_cluster_definition(raw_cluster)
    expected_revision = _route_revision(definition)
    if not hmac.compare_digest(revision, expected_revision):
        raise ValueError(
            f"cluster route changed for {raw_cluster}; refuse to route an existing job handle"
        )
    return definition


def _require_local_job_cluster(
    queue: ClioCoreQueue,
    job_id: str,
    target: ClusterDefinition | None,
) -> None:
    if target is None:
        return
    job = queue.get_job(job_id)
    if job.cluster != target.name:
        raise ValueError(
            f"job {job_id} belongs to cluster {job.cluster}, not requested cluster {target.name}"
        )


def _status_job(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    from clio_relay import mcp_server as _mcp_server

    job_id = _required_durable_record_id(arguments, "job_id")
    target = _job_target(arguments)
    if target is not None and _mcp_server.should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            with _mcp_server.OwnedSessionApiClient(definition=target, settings=settings) as client:
                result = _owned_json(
                    client,
                    method="GET",
                    path=f"/jobs/{job_id}/status",
                    label="owned remote job status",
                )
            _validate_owned_job_status(result, job_id=job_id, cluster=target.name)
        else:
            result = _mcp_server._remote_json(
                target, ["job", "status", job_id], "remote job status"
            )
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
        return result
    _require_local_job_cluster(queue, job_id, target)
    result = job_status(queue, job_id)
    if target is not None:
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
    return result


def _used_artifacts_tool(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """Query a job's immutable artifact dependencies through its bound route."""
    from clio_relay import mcp_server as _mcp_server

    job_id = _required_durable_record_id(arguments, "job_id")
    cursor = _optional_durable_record_id(arguments, "cursor")
    limit = _response_page_limit(arguments)
    target = _job_target(arguments)
    if target is not None and _mcp_server.should_execute_on_cluster(target):
        query: dict[str, object] = {"limit": limit}
        command = ["job", "used-artifacts", job_id, "--limit", str(limit)]
        if cursor is not None:
            query["cursor"] = cursor
            command.extend(["--cursor", cursor])
        if settings.owner_session_id is not None:
            with _mcp_server.OwnedSessionApiClient(definition=target, settings=settings) as client:
                result = _owned_json(
                    client,
                    method="GET",
                    path=f"/jobs/{job_id}/used-artifacts",
                    query=query,
                    label="owned remote used-artifact query",
                )
        else:
            result = _mcp_server._remote_json(target, command, "remote used-artifact query")
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
        return result
    _require_local_job_cluster(queue, job_id, target)
    records, next_cursor, total = queue.list_used_artifacts_page(
        job_id,
        cursor=cursor,
        limit=limit,
    )
    result: JSON = {
        "used_artifacts": [record.model_dump(mode="json") for record in records],
        "cursor": cursor,
        "limit": limit,
        "next_cursor": next_cursor,
        "total": total,
    }
    if target is not None:
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
    return result


def _used_by_tool(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """Query downstream consumers of an artifact through its bound route."""
    from clio_relay import mcp_server as _mcp_server

    artifact_id = _required_durable_record_id(arguments, "artifact_id")
    cursor = _optional_durable_record_id(arguments, "cursor")
    limit = _response_page_limit(arguments)
    target = _job_target(arguments)
    if target is not None and _mcp_server.should_execute_on_cluster(target):
        query: dict[str, object] = {"limit": limit}
        command = ["job", "used-by", artifact_id, "--limit", str(limit)]
        if cursor is not None:
            query["cursor"] = cursor
            command.extend(["--cursor", cursor])
        if settings.owner_session_id is not None:
            with _mcp_server.OwnedSessionApiClient(definition=target, settings=settings) as client:
                result = _owned_json(
                    client,
                    method="GET",
                    path=f"/artifacts/{artifact_id}/used-by",
                    query=query,
                    label="owned remote artifact-consumer query",
                )
        else:
            result = _mcp_server._remote_json(target, command, "remote artifact-consumer query")
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
        return result
    artifact = queue.get_artifact(artifact_id)
    _require_local_job_cluster(queue, artifact.job_id, target)
    records, next_cursor, total = queue.list_artifact_users_page(
        artifact_id,
        cursor=cursor,
        limit=limit,
    )
    result = {
        "used_by": [record.model_dump(mode="json") for record in records],
        "produced_by": artifact.metadata.get("produced_by"),
        "cursor": cursor,
        "limit": limit,
        "next_cursor": next_cursor,
        "total": total,
    }
    if target is not None:
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
    return result
