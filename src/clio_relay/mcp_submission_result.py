"""Submission result-assembly helpers shared by every job/MCP-call
submission tool: resolving a named cluster (required or optional),
building a remote-CLI submission receipt, the MCP-call-specific remote
receipt shape, the owned-session submission receipt (including its own
verified-result attachment), and the shared local/remote/owned-session
bounded-wait finalizer every submission tool calls before returning.

Split out of mcp_server.py (iowarp/clio-relay#231) as one of three seams
the job/MCP-call submission cluster split into (a single module would
have measured well over 800 lines). `_remote_cluster_definition`,
`_optional_cluster_definition`, and `_owned_session_submission_result` are
directly monkeypatched by tests at `mcp_server_module.<name>`; several
more names these functions call bare (`OwnedSessionApiClient`,
`_complete_owned_collection`, `_verified_owned_mcp_result`,
`_complete_remote_collection`, `_verified_mcp_result`, `_remote_json`,
`run_remote_clio`, `_complete_local_artifacts`, `_job_logs`,
`wait_for_terminal`) are monkeypatched too. Every one of those call sites
goes through the function-scope `_mcp_server.<name>(...)` back-reference
established in slices 3-8 -- found and rewritten by the same AST-based
extraction script slice 8 introduced (exact line/column spans, not a
hand-written per-function list or a regex over the source text).
"""

from __future__ import annotations

from typing import Any

from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry, default_registry_path
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ObservationTimeoutError
from clio_relay.mcp_arguments import (
    _attach_wait_observation,
    _log_limit,
    _object,
    _observation_timeout_seconds,
    _required_durable_record_id,
)
from clio_relay.mcp_job_lifecycle import (
    REMOTE_WAIT_STATUS_TIMEOUT_SECONDS,
    _relay_job_from_wait_document,
)
from clio_relay.mcp_remote_catalog import _route_revision
from clio_relay.mcp_remote_transport import (
    _owned_job_logs,
    _owned_json,
    _remote_job_logs,
    _validate_owned_job_status,
)
from clio_relay.mcp_result_verification import (
    _attach_terminal_mcp_evidence,
    _owned_mcp_result_is_required,
    _VerifiedMcpResult,
)
from clio_relay.mcp_tool_catalog_job_lifecycle import MAX_AGENT_LOG_READ_BYTES
from clio_relay.models import (
    TERMINAL_STATES,
    JobKind,
    JobState,
    RelayJob,
)
from clio_relay.remote_cli import (
    remote_command_timeout,
)
from clio_relay.session_api import OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS

JSON = dict[str, Any]


def _remote_cluster_definition(cluster: str) -> ClusterDefinition:
    registry_path = default_registry_path()
    if not registry_path.exists():
        raise ValueError(f"cluster is not configured: {cluster}")
    registry = ClusterRegistry.load(registry_path)
    return registry.require(cluster)


def _optional_cluster_definition(cluster: str) -> ClusterDefinition | None:
    registry_path = default_registry_path()
    if not registry_path.exists():
        return None
    return ClusterRegistry.load(registry_path).clusters.get(cluster)


def _remote_submission_result(
    output: str,
    *,
    kind: JobKind,
    definition: ClusterDefinition,
) -> JSON:
    job_id = output.strip().splitlines()[-1].strip()
    return {
        "cluster": definition.name,
        "job_id": job_id,
        "state": JobState.QUEUED.value,
        "kind": kind.value,
        "terminal": False,
        "remote": True,
        "route_revision": _route_revision(definition),
    }


def _remote_mcp_submission_result(
    output: str,
    *,
    definition: ClusterDefinition,
    arguments: JSON,
) -> JSON:
    """Return a remote MCP receipt and bounded result when the caller waited."""

    from clio_relay import mcp_server as _mcp_server

    result = _remote_submission_result(output, kind=JobKind.MCP_CALL, definition=definition)
    if not bool(arguments.get("wait_for_terminal", False)):
        return result
    job_id = _required_durable_record_id(result, "job_id")
    wait_timeout_seconds = _observation_timeout_seconds(
        arguments,
        "wait_timeout_seconds",
    )
    try:
        with remote_command_timeout(
            wait_timeout_seconds + OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS
        ):
            _mcp_server.run_remote_clio(
                definition,
                [
                    "job",
                    "wait",
                    job_id,
                    "--timeout-seconds",
                    str(wait_timeout_seconds),
                    "--poll-seconds",
                    str(
                        _observation_timeout_seconds(
                            arguments,
                            "poll_seconds",
                            default=2.0,
                        )
                    ),
                ],
            )
    except ObservationTimeoutError:
        pass
    with remote_command_timeout(REMOTE_WAIT_STATUS_TIMEOUT_SECONDS):
        status = _mcp_server._remote_json(
            definition, ["job", "status", job_id], "remote job status"
        )
    job = _object(status.get("job"))
    if job.get("job_id") != job_id or job.get("cluster") != definition.name:
        raise ValueError("remote MCP wait returned a different job")
    source_job = RelayJob.model_validate(job)
    state = source_job.state.value
    terminal = source_job.state in TERMINAL_STATES
    if status.get("terminal") is not terminal:
        raise ValueError("remote MCP wait status disagrees with its durable job state")
    result.update({"state": state, "terminal": terminal})
    _attach_wait_observation(
        result,
        observation_unknown=not terminal,
        timeout_seconds=wait_timeout_seconds,
    )
    if not terminal:
        return result
    artifacts = _mcp_server._complete_remote_collection(
        definition,
        ["job", "list-artifacts", job_id],
        record_key="artifacts",
        label=f"remote artifacts for {job_id}",
    )
    parsed_result = _mcp_server._verified_mcp_result(definition, job_id, artifacts)
    logs: JSON | None = None
    if arguments.get("include_logs", False) is True:
        logs = _remote_job_logs(
            definition,
            job_id,
            limit=_log_limit(arguments),
        )
    last_error = job.get("last_error")
    if last_error is not None and not isinstance(last_error, str):
        raise ValueError("remote MCP job returned an invalid last_error")
    _attach_terminal_mcp_evidence(
        result,
        source_job=source_job,
        last_error=last_error,
        artifacts=artifacts,
        parsed_result=parsed_result,
    )
    if logs is not None:
        result["logs"] = logs
    return result


def _owned_session_submission_result(
    job: RelayJob,
    *,
    definition: ClusterDefinition,
    settings: RelaySettings,
    wait_for_terminal_result: bool,
    wait_timeout_seconds: float,
    poll_seconds: float,
    include_terminal_mcp_result: bool = False,
    include_terminal_logs: bool = False,
    terminal_log_limit: int = MAX_AGENT_LOG_READ_BYTES,
) -> JSON:
    """Return an owned receipt, optionally waiting through the same protected API."""
    from clio_relay import mcp_server as _mcp_server

    artifacts: list[JSON] = []
    parsed_result: _VerifiedMcpResult | None = None
    logs: JSON | None = None
    observation_unknown = False
    if wait_for_terminal_result:
        try:
            with _mcp_server.OwnedSessionApiClient(
                definition=definition, settings=settings
            ) as client:
                document = _owned_json(
                    client,
                    method="POST",
                    path=f"/jobs/{job.job_id}/wait",
                    query={
                        "timeout_seconds": wait_timeout_seconds,
                        "poll_seconds": poll_seconds,
                    },
                    label="owned remote submitted job wait",
                    response_timeout_seconds=(
                        wait_timeout_seconds + OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS
                    ),
                )
                waited = _relay_job_from_wait_document(document)
        except ObservationTimeoutError:
            with _mcp_server.OwnedSessionApiClient(
                definition=definition, settings=settings
            ) as client:
                status = _owned_json(
                    client,
                    method="GET",
                    path=f"/jobs/{job.job_id}/status",
                    label="owned remote submitted job status after bounded wait",
                )
                _validate_owned_job_status(
                    status,
                    job_id=job.job_id,
                    cluster=definition.name,
                )
                waited = RelayJob.model_validate(_object(status.get("job")))
        if (
            waited.job_id != job.job_id
            or waited.cluster != definition.name
            or waited.metadata.get("owner_session_id") != settings.owner_session_id
            or waited.metadata.get("owner_session_generation_id")
            != settings.owner_session_generation_id
        ):
            raise ValueError("owned remote wait returned a different submission receipt")
        observation_unknown = waited.state not in TERMINAL_STATES
        job = waited
        if not observation_unknown and (include_terminal_mcp_result or include_terminal_logs):
            with _mcp_server.OwnedSessionApiClient(
                definition=definition, settings=settings
            ) as client:
                if include_terminal_mcp_result:
                    artifacts = _mcp_server._complete_owned_collection(
                        client,
                        path=f"/jobs/{job.job_id}/artifacts",
                        record_key="artifacts",
                        label=f"owned remote artifacts for {job.job_id}",
                    )
                    parsed_result = _mcp_server._verified_owned_mcp_result(
                        client,
                        job.job_id,
                        artifacts,
                        require_result=_owned_mcp_result_is_required(job),
                    )
                if include_terminal_logs:
                    logs = _owned_job_logs(
                        client,
                        job.job_id,
                        limit=terminal_log_limit,
                    )
    result: JSON = {
        "cluster": definition.name,
        "job_id": job.job_id,
        "state": job.state.value,
        "kind": job.kind.value,
        "terminal": job.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED},
        "remote": True,
        "route_revision": _route_revision(definition),
    }
    if wait_for_terminal_result:
        _attach_wait_observation(
            result,
            observation_unknown=observation_unknown,
            timeout_seconds=wait_timeout_seconds,
        )
    if wait_for_terminal_result and not observation_unknown and include_terminal_mcp_result:
        _attach_terminal_mcp_evidence(
            result,
            source_job=job,
            last_error=job.last_error,
            artifacts=artifacts,
            parsed_result=parsed_result,
        )
    if wait_for_terminal_result and not observation_unknown and logs is not None:
        result["logs"] = logs
    return result


def _submission_result(
    job: RelayJob,
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings | None = None,
    definition: ClusterDefinition | None = None,
    include_terminal_mcp_result: bool = False,
) -> JSON:
    from clio_relay import mcp_server as _mcp_server

    waited = bool(arguments.get("wait_for_terminal", False))
    observation_unknown = False
    wait_timeout_seconds = _observation_timeout_seconds(
        arguments,
        "wait_timeout_seconds",
    )
    if waited:
        try:
            job = _mcp_server.wait_for_terminal(
                queue,
                job.job_id,
                timeout_seconds=wait_timeout_seconds,
                poll_seconds=_observation_timeout_seconds(
                    arguments,
                    "poll_seconds",
                    default=2.0,
                ),
            )
        except TimeoutError:
            job = queue.get_job(job.job_id)
        observation_unknown = job.state not in TERMINAL_STATES
    result: JSON = {
        "cluster": job.cluster,
        "job_id": job.job_id,
        "state": job.state.value,
        "kind": job.kind.value,
        "terminal": job.state.value in {"succeeded", "failed", "canceled"},
    }
    if definition is not None:
        result["route_revision"] = _route_revision(definition)
    if waited:
        _attach_wait_observation(
            result,
            observation_unknown=observation_unknown,
            timeout_seconds=wait_timeout_seconds,
        )
    if waited and not observation_unknown and include_terminal_mcp_result:
        artifacts = _mcp_server._complete_local_artifacts(queue, job.job_id)
        _attach_terminal_mcp_evidence(
            result,
            source_job=job,
            last_error=job.last_error,
            artifacts=artifacts,
            parsed_result=_mcp_server._verified_local_mcp_result(queue, job.job_id),
        )
    if waited and not observation_unknown and arguments.get("include_logs", False) is True:
        if settings is None:
            raise ValueError("local waited log retrieval requires relay settings")
        result["logs"] = _mcp_server._job_logs(
            queue,
            settings,
            job.job_id,
            limit=_log_limit(arguments),
        )
    return result
