"""Relay-queue MCP tools: list/cancel/diagnose/stale-discovery/cleanup-stale
and worker status, each routed to a local queue.py-backed path or a remote
(SSH or owned-session) path depending on the resolved cluster target.

Split out of mcp_server.py (iowarp/clio-relay#231). Two names used on the
remote branch (`should_execute_on_cluster`, `OwnedSessionApiClient`) are
directly monkeypatched by tests at `mcp_server_module.<name>`; three more
(`_owned_json`, `_remote_json`, `_validate_owned_job_status`) stay defined
in mcp_server.py itself. Both cases go through the function-scope
`_mcp_server.<name>(...)` back-reference the slice-3 dispatcher and slice-4
remote-catalog cluster established (`from clio_relay import mcp_server as
_mcp_server`, imported inside each function body, not at module top, to
avoid the load-order cycle a module-level back-reference would create).

`_queue_tool_target`/`_queue_route_result` use neither -- confirmed by grep
before the move -- so the six tool functions' calls into them stay bare,
same-module references.
"""

from __future__ import annotations

import hmac
from typing import Any, cast

from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry, default_registry_path
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.mcp_arguments import (
    _boolean_argument,
    _bounded_integer_limit,
    _optional_durable_record_id,
    _optional_str,
    _positive_integer_argument,
    _required_durable_record_id,
    _required_str,
    _response_page_cursor,
    _response_page_limit,
)
from clio_relay.mcp_remote_catalog import _route_revision, _validated_route_revision
from clio_relay.models import JobKind, JobState
from clio_relay.queue_management import (
    DEFAULT_STALE_SCAN_LIMIT,
    cancel_queue_job,
    cleanup_stale_jobs,
    diagnose_job,
    discover_stale_jobs,
    list_queue_jobs,
    worker_status,
)

JSON = dict[str, Any]


def _queue_cancel_tool(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """Cancel one queue job through the same local-or-SSH route as the CLI."""
    from clio_relay import mcp_server as _mcp_server

    job_id = _required_durable_record_id(arguments, "job_id")
    target = _queue_tool_target(arguments)
    cluster = _optional_str(arguments, "cluster")
    cancel_scheduler = _boolean_argument(arguments, "cancel_scheduler_job", default=False)
    if target is not None and _mcp_server.should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            with _mcp_server.OwnedSessionApiClient(definition=target, settings=settings) as client:
                payload = _mcp_server._owned_json(
                    client,
                    method="POST",
                    path=f"/queue/jobs/{job_id}/cancel",
                    body={
                        "cluster": target.name,
                        "cancel_scheduler_job": cancel_scheduler,
                    },
                    label="owned remote queue cancellation",
                )
            _mcp_server._validate_owned_job_status(payload, job_id=job_id, cluster=target.name)
            return _queue_route_result(payload, target=target, remote=True)
        command = ["queue", "cancel", job_id, "--cluster", target.name]
        command.append("--cancel-scheduler-job" if cancel_scheduler else "--keep-scheduler-job")
        return _queue_route_result(
            _mcp_server._remote_json(target, command, "remote queue cancellation"),
            target=target,
            remote=True,
        )
    result = cast(
        JSON,
        cancel_queue_job(
            queue,
            job_id,
            cluster=cluster,
            scheduler_policy="request-scheduler" if cancel_scheduler else "relay-only",
        ),
    )
    return _queue_route_result(result, target=target, remote=False)


def _queue_list_tool(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """List the selected cluster queue locally or through its configured SSH route."""
    from clio_relay import mcp_server as _mcp_server

    target = _queue_tool_target(arguments)
    cluster = _optional_str(arguments, "cluster")
    raw_state = arguments.get("state")
    if raw_state is not None and not isinstance(raw_state, str):
        raise ValueError("state must be a string")
    state = JobState(raw_state) if isinstance(raw_state, str) else None
    raw_kind = arguments.get("kind")
    if raw_kind is not None and not isinstance(raw_kind, str):
        raise ValueError("kind must be a string")
    kind = JobKind(raw_kind) if isinstance(raw_kind, str) else None
    include_terminal = _boolean_argument(arguments, "include_terminal", default=False)
    cursor = _response_page_cursor(arguments)
    limit = _response_page_limit(arguments)
    scan_limit = _bounded_integer_limit(
        arguments,
        field_name="scan_limit",
        default=1_000,
        maximum=10_000,
    )
    if scan_limit < limit:
        raise ValueError("scan_limit must be greater than or equal to limit")
    if target is not None and _mcp_server.should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            query: dict[str, object] = {
                "cluster": target.name,
                "include_terminal": include_terminal,
                "cursor": cursor,
                "limit": limit,
                "scan_limit": scan_limit,
            }
            if state is not None:
                query["state"] = state.value
            if kind is not None:
                query["kind"] = kind.value
            with _mcp_server.OwnedSessionApiClient(definition=target, settings=settings) as client:
                payload = _mcp_server._owned_json(
                    client,
                    method="GET",
                    path="/queue",
                    query=query,
                    label="owned remote queue listing",
                )
            return _queue_route_result(payload, target=target, remote=True)
        command = [
            "queue",
            "list",
            "--cluster",
            target.name,
            "--cursor",
            str(cursor),
            "--limit",
            str(limit),
            "--scan-limit",
            str(scan_limit),
        ]
        if state is not None:
            command.extend(["--state", state.value])
        if kind is not None:
            command.extend(["--kind", kind.value])
        if include_terminal:
            command.append("--include-terminal")
        return _queue_route_result(
            _mcp_server._remote_json(target, command, "remote queue listing"),
            target=target,
            remote=True,
        )
    result = cast(
        JSON,
        list_queue_jobs(
            queue,
            cluster=cluster,
            state=state,
            kind=kind,
            include_terminal=include_terminal,
            cursor=cursor,
            limit=limit,
            scan_limit=scan_limit,
        ),
    )
    return _queue_route_result(result, target=target, remote=False)


def _queue_diagnose_tool(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """Diagnose one exact queue job on its configured local or SSH route."""
    from clio_relay import mcp_server as _mcp_server

    job_id = _required_durable_record_id(arguments, "job_id")
    target = _queue_tool_target(arguments)
    cluster = _optional_str(arguments, "cluster")
    older_than_seconds = _positive_integer_argument(
        arguments,
        "older_than_seconds",
        default=7_200,
    )
    scan_limit = _bounded_integer_limit(
        arguments,
        field_name="scan_limit",
        default=1_000,
        maximum=10_000,
    )
    if target is not None and _mcp_server.should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            with _mcp_server.OwnedSessionApiClient(definition=target, settings=settings) as client:
                payload = _mcp_server._owned_json(
                    client,
                    method="GET",
                    path=f"/queue/jobs/{job_id}/diagnose",
                    query={
                        "cluster": target.name,
                        "older_than_seconds": older_than_seconds,
                        "scan_limit": scan_limit,
                    },
                    label="owned remote queue diagnosis",
                )
            _mcp_server._validate_owned_job_status(payload, job_id=job_id, cluster=target.name)
            return _queue_route_result(payload, target=target, remote=True)
        return _queue_route_result(
            _mcp_server._remote_json(
                target,
                [
                    "queue",
                    "diagnose",
                    job_id,
                    "--cluster",
                    target.name,
                    "--older-than",
                    f"{older_than_seconds}s",
                    "--scan-limit",
                    str(scan_limit),
                ],
                "remote queue diagnosis",
            ),
            target=target,
            remote=True,
        )
    result = cast(
        JSON,
        diagnose_job(
            queue,
            job_id,
            cluster=cluster,
            stale_after_seconds=older_than_seconds,
            scan_limit=scan_limit,
        ),
    )
    return _queue_route_result(result, target=target, remote=False)


def _queue_stale_tool(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """Discover stale jobs on the selected local or SSH-backed cluster queue."""
    from clio_relay import mcp_server as _mcp_server

    target = _queue_tool_target(arguments)
    cluster = _required_str(arguments, "cluster")
    older_than_seconds = _positive_integer_argument(
        arguments,
        "older_than_seconds",
        required=True,
    )
    job_id = _optional_durable_record_id(arguments, "job_id")
    raw_kind = arguments.get("kind")
    if raw_kind is not None and not isinstance(raw_kind, str):
        raise ValueError("kind must be a string")
    kind = JobKind(raw_kind) if isinstance(raw_kind, str) else None
    limit = _response_page_limit(arguments)
    scan_limit = _bounded_integer_limit(
        arguments,
        field_name="scan_limit",
        default=DEFAULT_STALE_SCAN_LIMIT,
        maximum=10_000,
    )
    if scan_limit < limit:
        raise ValueError("scan_limit must be greater than or equal to limit")
    if target is not None and _mcp_server.should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            raise ValueError(
                "stale discovery is unavailable for an owned relay session because it requires "
                "global queue visibility; diagnose an exact owned job instead"
            )
        command = [
            "queue",
            "stale",
            "--cluster",
            target.name,
            "--older-than",
            f"{older_than_seconds}s",
            "--limit",
            str(limit),
            "--scan-limit",
            str(scan_limit),
        ]
        if job_id is not None:
            command.extend(["--job-id", job_id])
        if kind is not None:
            command.extend(["--kind", kind.value])
        return _queue_route_result(
            _mcp_server._remote_json(target, command, "remote stale queue discovery"),
            target=target,
            remote=True,
        )
    result = cast(
        JSON,
        discover_stale_jobs(
            queue,
            cluster=cluster,
            older_than_seconds=older_than_seconds,
            job_id=job_id,
            kind=kind,
            limit=limit,
            scan_limit=scan_limit,
        ),
    )
    return _queue_route_result(result, target=target, remote=False)


def _queue_cleanup_stale_tool(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """Preview or execute stale cleanup on the selected cluster queue route."""
    from clio_relay import mcp_server as _mcp_server

    target = _queue_tool_target(arguments)
    cluster = _required_str(arguments, "cluster")
    older_than_seconds = _positive_integer_argument(
        arguments,
        "older_than_seconds",
        default=7_200,
    )
    max_attempts = _positive_integer_argument(arguments, "max_attempts", default=3)
    dry_run = _boolean_argument(arguments, "dry_run", default=True)
    cancel_queued = _boolean_argument(arguments, "cancel_queued", default=False)
    job_id = _optional_durable_record_id(arguments, "job_id")
    raw_kind = arguments.get("kind")
    if raw_kind is not None and not isinstance(raw_kind, str):
        raise ValueError("kind must be a string")
    kind = JobKind(raw_kind) if isinstance(raw_kind, str) else None
    limit = _response_page_limit(arguments)
    scan_limit = _bounded_integer_limit(
        arguments,
        field_name="scan_limit",
        default=DEFAULT_STALE_SCAN_LIMIT,
        maximum=10_000,
    )
    if scan_limit < limit:
        raise ValueError("scan_limit must be greater than or equal to limit")
    if target is not None and _mcp_server.should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            raise ValueError(
                "stale cleanup is unavailable for an owned relay session because it requires "
                "global queue mutation authority"
            )
        command = [
            "queue",
            "cleanup-stale",
            "--cluster",
            target.name,
            "--older-than",
            f"{older_than_seconds}s",
            "--max-attempts",
            str(max_attempts),
            "--limit",
            str(limit),
            "--scan-limit",
            str(scan_limit),
            "--dry-run" if dry_run else "--no-dry-run",
        ]
        if job_id is not None:
            command.extend(["--job-id", job_id])
        if kind is not None:
            command.extend(["--kind", kind.value])
        if cancel_queued:
            command.append("--cancel-queued")
        return _queue_route_result(
            _mcp_server._remote_json(target, command, "remote stale queue cleanup"),
            target=target,
            remote=True,
        )
    result = cast(
        JSON,
        cleanup_stale_jobs(
            queue,
            cluster=cluster,
            older_than_seconds=older_than_seconds,
            job_id=job_id,
            kind=kind,
            max_attempts=max_attempts,
            dry_run=dry_run,
            cancel_queued=cancel_queued,
            limit=limit,
            scan_limit=scan_limit,
        ),
    )
    return _queue_route_result(result, target=target, remote=False)


def _worker_status_tool(arguments: JSON, *, queue: ClioCoreQueue) -> JSON:
    """Read worker capacity from the selected local or SSH-backed queue route."""
    from clio_relay import mcp_server as _mcp_server

    target = _queue_tool_target(arguments)
    cluster = _optional_str(arguments, "cluster")
    if target is not None and _mcp_server.should_execute_on_cluster(target):
        return _queue_route_result(
            _mcp_server._remote_json(
                target,
                ["worker", "status", "--cluster", target.name],
                "remote worker status",
            ),
            target=target,
            remote=True,
        )
    result = cast(JSON, worker_status(queue, cluster=cluster))
    return _queue_route_result(result, target=target, remote=False)


def _queue_tool_target(arguments: JSON) -> ClusterDefinition | None:
    """Resolve an optional cluster route while preserving unregistered local queues."""
    raw_cluster = arguments.get("cluster")
    raw_revision = arguments.get("route_revision")
    if raw_cluster is None:
        if raw_revision is not None:
            raise ValueError("route_revision requires cluster")
        return None
    if not isinstance(raw_cluster, str) or not raw_cluster:
        raise ValueError("cluster must be a non-empty string")
    if raw_revision is not None:
        _validated_route_revision(raw_revision)
    registry_path = default_registry_path()
    if not registry_path.exists():
        if raw_revision is not None:
            raise ValueError(f"cluster route is not configured: {raw_cluster}")
        return None
    definition = ClusterRegistry.load(registry_path).clusters.get(raw_cluster)
    if definition is None:
        if raw_revision is not None:
            raise ValueError(f"cluster route is not configured: {raw_cluster}")
        return None
    expected_revision = _route_revision(definition)
    if raw_revision is not None and not hmac.compare_digest(raw_revision, expected_revision):
        raise ValueError(
            f"cluster route changed for {raw_cluster}; refuse to use stale queue routing"
        )
    return definition


def _queue_route_result(
    result: JSON,
    *,
    target: ClusterDefinition | None,
    remote: bool,
) -> JSON:
    """Attach stable route identity to queue results when a target is configured."""
    if target is None:
        return result
    result["cluster"] = target.name
    result["route_revision"] = _route_revision(target)
    result["remote"] = remote
    return result
