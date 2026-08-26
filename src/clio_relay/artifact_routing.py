"""Cluster-routed artifact list/read for the MCP tool handlers (clio-relay#264).

``relay_list_artifacts`` and ``relay_read_artifact`` were the only two
artifact-facing MCP tools that never gained the ``cluster``/``route_revision``
routing every sibling job/artifact tool already has (``relay_status``,
``relay_cancel``, ``relay_artifact_lineage``, ``relay_wait``). A jarvis
execution dispatched to a configured remote cluster registers its
execution-output artifacts
(``jarvis_execution_artifacts.ingest_jarvis_execution_outputs``) against the
job that ran it -- durable in *that cluster's* core, not the door process's
local one. Without cluster routing, ``relay_list_artifacts``/
``relay_read_artifact`` always queried the door's local core regardless of
where the job actually ran, so every artifact an agent legitimately held
from a remote jarvis run answered not-found even though registration
succeeded.

This module owns the routing decision (local core vs. the SSH/owned-session
route to the asserted cluster) and the fetch mechanics for both tools.
``mcp_server.py`` resolves the caller's asserted ``ClusterDefinition`` via
its existing ``_job_target`` and forwards it here, so the security-relevant
route-revision verification stays in one place.
"""

from __future__ import annotations

import json
from typing import Any, cast

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.jarvis_execution_artifacts import resolve_jarvis_run_owner_by_execution_id
from clio_relay.pagination import (
    DEFAULT_RESPONSE_PAGE_RECORDS,
    validate_record_cursor,
    validate_response_page_limit,
)
from clio_relay.relay_ops import read_artifact_bytes
from clio_relay.remote_cli import run_remote_clio, should_execute_on_cluster
from clio_relay.session_api import OwnedSessionApiClient

JSON = dict[str, Any]

INTERNAL_PROTOCOL_ARTIFACT_MESSAGE = (
    "artifact is internal protocol evidence and is not model-readable; use relay_wait "
    "for its bounded public result"
)


def list_artifacts(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
    target: ClusterDefinition | None,
) -> JSON:
    """Return one artifact page for a job, routed to the cluster that ran it.

    clio-relay#278: ``execution_id`` is an alternative to ``job_id`` -- the
    id ``jarvis_run``/``jarvis_get_execution`` hand back, which the old
    cluster-operator pack's prompt taught the model to reverse-engineer into
    a job id. Exactly one of the two must be given; both or neither is a
    typed ``artifact_scope_ambiguous`` refusal, mirroring
    ``relay_artifact_lineage``'s established job_id/artifact_id idiom (its
    definition sits immediately below ``relay_list_artifacts`` in the same
    tool catalog). A bare ``execution_id`` resolves through the same
    ``jarvis_execution_artifacts.resolve_jarvis_run_owner_by_execution_id``
    the door's ``GET /executions/{execution_id}/artifacts`` route and the
    CLI's ``--execution-id`` flag both use -- never a second, invented
    resolution path.

    Remote-cluster routing forwards whichever id the caller gave rather than
    resolving locally first: a job dispatched to a configured remote cluster
    is durable only in *that cluster's* core (this module's own docstring),
    so a bare execution_id can only be resolved against the remote door's
    own queue, exactly as a job_id-keyed call already only resolves there.
    """
    job_id_argument = _optional_str(arguments, "job_id")
    execution_id_argument = _optional_str(arguments, "execution_id")
    if (job_id_argument is None) == (execution_id_argument is None):
        raise ValueError("artifact_scope_ambiguous: pass exactly one of job_id or execution_id")
    cursor = validate_record_cursor(arguments.get("cursor", 1))
    limit = validate_response_page_limit(arguments.get("limit", DEFAULT_RESPONSE_PAGE_RECORDS))
    if target is not None and should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            with OwnedSessionApiClient(definition=target, settings=settings) as client:
                path = (
                    f"/jobs/{validate_durable_record_id(job_id_argument)}/artifacts"
                    if job_id_argument is not None
                    else f"/executions/{execution_id_argument}/artifacts"
                )
                payload = client.request_json(
                    method="GET",
                    path=path,
                    query={"cursor": cursor, "limit": limit},
                )
        else:
            remote_args = ["job", "list-artifacts"]
            if job_id_argument is not None:
                remote_args.append(validate_durable_record_id(job_id_argument))
            else:
                remote_args.extend(["--execution-id", cast(str, execution_id_argument)])
            remote_args.extend(["--cursor", str(cursor), "--limit", str(limit)])
            payload = _remote_json(target, remote_args, "remote artifact list")
        return _require_json_object(payload, "remote artifact list")
    if job_id_argument is not None:
        job_id = validate_durable_record_id(job_id_argument)
    else:
        owner = resolve_jarvis_run_owner_by_execution_id(
            queue,
            cast(str, execution_id_argument),
            cluster=target.name if target is not None else None,
        )
        job_id = owner.job_id
    _require_local_job_cluster(queue, job_id, target)
    artifacts, next_cursor, total = queue.list_artifacts_page(job_id, cursor=cursor, limit=limit)
    return {
        "artifacts": [artifact.model_dump(mode="json") for artifact in artifacts],
        "cursor": cursor,
        "limit": limit,
        "next_cursor": next_cursor,
        "total": total,
    }


def read_artifact(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
    target: ClusterDefinition | None,
) -> JSON:
    """Read one model-readable artifact payload, routed to its owning cluster."""
    artifact_id = validate_durable_record_id(_required_str(arguments, "artifact_id"))
    if target is not None and should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            with OwnedSessionApiClient(definition=target, settings=settings) as client:
                payload = client.request_json(
                    method="GET",
                    path=f"/artifacts/{artifact_id}/content",
                )
        else:
            payload = _remote_json(
                target,
                ["job", "read-artifact", artifact_id],
                "remote artifact content",
            )
        envelope = _require_json_object(payload, "remote artifact content")
        raw_artifact = envelope.get("artifact")
        if isinstance(raw_artifact, dict):
            _require_model_readable(cast(JSON, raw_artifact))
        return envelope
    artifact = queue.get_artifact(artifact_id)
    _require_local_job_cluster(queue, artifact.job_id, target)
    if artifact.kind == "mcp_result" or artifact.metadata.get("model_readable") is False:
        raise ValueError(INTERNAL_PROTOCOL_ARTIFACT_MESSAGE)
    return read_artifact_bytes(queue, artifact_id)


def _require_model_readable(artifact: JSON) -> None:
    raw_metadata = artifact.get("metadata")
    metadata = cast(JSON, raw_metadata) if isinstance(raw_metadata, dict) else {}
    if artifact.get("kind") == "mcp_result" or metadata.get("model_readable") is False:
        raise ValueError(INTERNAL_PROTOCOL_ARTIFACT_MESSAGE)


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


def _required_str(arguments: JSON, key: str) -> str:
    value = arguments.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} is required")
    return value


def _optional_str(arguments: JSON, key: str) -> str | None:
    value = arguments.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _remote_json(definition: ClusterDefinition, args: list[str], label: str) -> object:
    output = run_remote_clio(definition, args)
    try:
        return json.loads(output)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} returned invalid JSON") from exc


def _require_json_object(value: object, label: str) -> JSON:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must return a JSON object")
    return cast(JSON, value)
