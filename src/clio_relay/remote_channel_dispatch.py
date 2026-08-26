"""Route one owned-session CLI operation over the held channel, or take the
typed, visible per-operation ssh fallback (iowarp/clio-relay#179).

The 2FA doctrine (docs/connection-model.md:141-157) makes an unattended
per-operation ssh dial to a 2FA-protected cluster a design violation: the
held channel established once at connection bring-up
(:mod:`clio_relay.remote_connection`) exists precisely so operations ride
it. Precedent: :mod:`clio_relay.session_attach`'s ``_list_owned_jobs_over_
channel`` already rides ``GET /queue`` over the held channel instead of a
per-page ssh exec (iowarp/clio-relay#276). This module generalizes that
pattern into the one place cli_owned_relay_jobs.py / cli_owned_scheduler_
cancel.py / cli_remote_worker_probe.py / cli_remote_mcp.py's dial sites
route their decision through, instead of each repeating an ad hoc
channel-or-ssh branch.

:func:`live_matching_connection` never dials anything -- it is
``RemoteConnectionRegistry.get()``, read-only. A connection held for a
different session/generation/token/port, one that has dropped (``state !=
"connected"``), or no connection at all all return ``None``: each is the
caller's cue to take the explicit ssh fallback rather than silently reusing
a stranger's channel or forcing a reconnect no one authorized. Cross-
generation queries (``owner_session_generation_id=None``, the owned-session
teardown flow's legacy-job discovery) always return ``None`` for the same
reason -- a channel is pinned to one exact generation, by design, and
broadening that scope would be a security-relevant change to the channel
identity contract, not a slice of #179's dial-site burn-down.

:func:`record_per_operation_ssh_fallback` is the one place the typed
``per_op_ssh_fallback`` reason is recorded, in a bounded, queryable,
process-wide ledger -- the same "queryable after the fact" shape
``RemoteConnectionRegistry.event_report`` already establishes for the
channel's own transport-open events (:mod:`clio_relay.
remote_connection_registry`). This is a DIFFERENT ledger: the registry
counts held-channel transport opens; this one counts the ordinary
per-operation ssh dial each of the four modules above still falls back to
when no live channel exists. Never conflate the two in a measurement.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final, cast

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.errors import RelayError
from clio_relay.pagination import MAX_RESPONSE_PAGE_RECORDS
from clio_relay.remote_connection import RemoteConnection, connection_registry

PER_OP_SSH_FALLBACK_REASON: Final = "per_op_ssh_fallback"
MAX_RECORDED_FALLBACKS: Final = 256


@dataclass(frozen=True)
class PerOperationSshFallback:
    """One typed, visible record of an operation that took the ssh fallback.

    ``reason`` is always :data:`PER_OP_SSH_FALLBACK_REASON` -- typed so the
    two-sided ssh-dial measurement (clio-relay#179) can count these
    separately from the held channel's own transport-open events.
    """

    operation: str
    cluster: str
    detail: str
    reason: str = PER_OP_SSH_FALLBACK_REASON
    observed_at: float = 0.0


class _PerOperationFallbackLedger:
    """Process-wide, bounded record of every per-operation ssh fallback."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._records: list[PerOperationSshFallback] = []

    def record(self, *, operation: str, cluster: str, detail: str) -> PerOperationSshFallback:
        entry = PerOperationSshFallback(
            operation=operation, cluster=cluster, detail=detail, observed_at=time.time()
        )
        with self._lock:
            self._records.append(entry)
            if len(self._records) > MAX_RECORDED_FALLBACKS:
                del self._records[: len(self._records) - MAX_RECORDED_FALLBACKS]
        return entry

    def report(self) -> list[PerOperationSshFallback]:
        with self._lock:
            return list(self._records)

    def clear(self) -> None:
        """Test-only: reset the ledger between cases."""
        with self._lock:
            self._records.clear()


_LEDGER = _PerOperationFallbackLedger()


def per_operation_fallback_ledger() -> _PerOperationFallbackLedger:
    """Return the process-wide per-operation ssh-fallback ledger."""
    return _LEDGER


def record_per_operation_ssh_fallback(
    *, operation: str, cluster: str, detail: str
) -> PerOperationSshFallback:
    """Record one typed, visible per-operation ssh fallback (clio-relay#179)."""
    return _LEDGER.record(operation=operation, cluster=cluster, detail=detail)


def live_matching_connection(
    *,
    definition: ClusterDefinition,
    owner_session_id: str,
    owner_session_generation_id: str | None,
) -> RemoteConnection | None:
    """Return an already-held, already-connected channel for this EXACT identity.

    Never establishes or reconnects anything. Returns ``None`` (never
    dials) when no connection is held for this cluster, the held one does
    not match this exact session/generation/token/port, it has dropped, or
    ``owner_session_generation_id`` is ``None`` (a channel is pinned to one
    generation; it cannot serve a cross-generation query).
    """
    if owner_session_generation_id is None:
        return None
    settings = RelaySettings.from_env().model_copy(
        update={
            "owner_session_id": owner_session_id,
            "owner_session_generation_id": owner_session_generation_id,
            "owner_session_cluster": definition.name,
        }
    )
    connection = connection_registry().get(definition.name)
    if connection is None or connection.state != "connected":
        return None
    if not connection.matches(settings=settings):
        return None
    return connection


def _live_matching_connection_from_ambient_settings(
    definition: ClusterDefinition,
) -> RemoteConnection | None:
    """:func:`live_matching_connection` using this process's ambient identity.

    For call sites with no caller-explicit ``owner_session_id``/generation
    id to thread (``cli_remote_worker_probe.py``'s worker-info/target-info
    probes, ``cli_remote_mcp.py``'s discovery-artifact read): reads
    ``CLIO_RELAY_OWNER_SESSION_ID``/``CLIO_RELAY_SESSION_GENERATION_ID``
    from the environment instead. Safe when it does not match --
    :func:`live_matching_connection`'s own identity check still gates
    whether any returned connection is actually usable; an absent or
    mismatched ambient identity just means ``None``, the ssh-fallback cue.
    """
    settings = RelaySettings.from_env()
    if settings.owner_session_id is None or settings.owner_session_generation_id is None:
        return None
    return live_matching_connection(
        definition=definition,
        owner_session_id=settings.owner_session_id,
        owner_session_generation_id=settings.owner_session_generation_id,
    )


def dial_or_route_string_ambient(
    *,
    definition: ClusterDefinition,
    operation: str,
    method: str,
    path: str,
    query: dict[str, object] | None = None,
    body: dict[str, object] | None = None,
    ssh_fallback: Callable[[], str],
) -> str:
    """Ride the held channel (this process's ambient identity), as a JSON string.

    Re-serializes the channel's JSON response to a string so it matches
    ``remote_cli.run_remote_clio``'s return type -- a drop-in replacement
    wherever that call's raw string result feeds an existing ``_json_
    output``-shaped parser unchanged.
    """
    connection = _live_matching_connection_from_ambient_settings(definition)
    if connection is not None:
        return json.dumps(connection.request_json(method=method, path=path, query=query, body=body))
    record_per_operation_ssh_fallback(
        operation=operation,
        cluster=definition.name,
        detail="no live owned-session channel for this process's ambient identity",
    )
    return ssh_fallback()


def dial_or_route(
    *,
    definition: ClusterDefinition,
    owner_session_id: str,
    owner_session_generation_id: str | None,
    operation: str,
    channel: Callable[[RemoteConnection], object],
    ssh_fallback: Callable[[], object],
) -> object:
    """Ride the held channel for one operation, or take the typed ssh fallback."""
    connection = live_matching_connection(
        definition=definition,
        owner_session_id=owner_session_id,
        owner_session_generation_id=owner_session_generation_id,
    )
    if connection is not None:
        return channel(connection)
    record_per_operation_ssh_fallback(
        operation=operation,
        cluster=definition.name,
        detail="no live owned-session channel for this identity",
    )
    return ssh_fallback()


def dial_or_route_json(
    *,
    definition: ClusterDefinition,
    owner_session_id: str,
    owner_session_generation_id: str | None,
    operation: str,
    method: str,
    path: str,
    query: dict[str, object] | None = None,
    body: dict[str, object] | None = None,
    ssh_fallback: Callable[[], object],
) -> object:
    """:func:`dial_or_route` for the common case: one plain JSON request."""
    return dial_or_route(
        definition=definition,
        owner_session_id=owner_session_id,
        owner_session_generation_id=owner_session_generation_id,
        operation=operation,
        channel=lambda connection: connection.request_json(
            method=method, path=path, query=query, body=body
        ),
        ssh_fallback=ssh_fallback,
    )


def channel_backed_json_runner(
    *,
    definition: ClusterDefinition,
    owner_session_id: str,
    owner_session_generation_id: str,
    operation: str,
    method: str,
    path: str,
) -> Callable[[ClusterDefinition, list[str]], str] | None:
    """Build a ``RemoteCliRunner``-shaped callable riding a live channel, or ``None``.

    For a caller (``cli_owned_relay_jobs._owner_session_admission_status``)
    that injects its remote transport as a ``Callable[[ClusterDefinition,
    list[str]], str]`` seam rather than making the request directly:
    re-serializes the channel's JSON response to a string so it flows
    through that seam's existing ``json.loads`` unchanged. Records the
    typed ssh-fallback reason and returns ``None`` when no live channel
    matches -- the caller's cue to keep its own ssh-backed runner.
    """
    connection = live_matching_connection(
        definition=definition,
        owner_session_id=owner_session_id,
        owner_session_generation_id=owner_session_generation_id,
    )
    if connection is None:
        record_per_operation_ssh_fallback(
            operation=operation,
            cluster=definition.name,
            detail="no live owned-session channel for this identity",
        )
        return None

    def runner(_definition: ClusterDefinition, _args: list[str]) -> str:
        return json.dumps(connection.request_json(method=method, path=path))

    return runner


def list_owned_session_jobs_and_tasks_over_channel(
    connection: RemoteConnection,
    *,
    cluster: str,
    include_terminal: bool,
) -> list[tuple[dict[str, object], list[dict[str, object]]]]:
    """Enumerate this held channel's owned jobs, each with its full task list.

    Mirrors ``session_attach._list_owned_jobs_over_channel``'s ``GET
    /queue`` paging (clio-relay#276 precedent) but returns full job + task
    documents instead of the summary row that report needs -- what
    ``cli_owned_relay_jobs._owned_relay_job``'s ownership/scheduler-job-id
    proof chain requires.
    """
    results: list[tuple[dict[str, object], list[dict[str, object]]]] = []
    cursor = 1
    while True:
        document = connection.request_json(
            method="GET",
            path="/queue",
            query={
                "cluster": cluster,
                "include_terminal": include_terminal,
                "cursor": cursor,
                "limit": MAX_RESPONSE_PAGE_RECORDS,
                "scan_limit": MAX_RESPONSE_PAGE_RECORDS,
            },
        )
        if not isinstance(document, dict):
            raise RelayError("owned session queue listing response is not a JSON object")
        page = cast(dict[str, object], document)
        if page.get("visibility_filter") != "exact_owner_session_generation":
            raise RelayError(
                "owned session queue listing was not scoped to the attached owner session"
            )
        raw_jobs = page.get("jobs")
        if not isinstance(raw_jobs, list):
            raise RelayError("owned session queue listing omitted its jobs array")
        for raw_entry in cast(list[object], raw_jobs):
            if not isinstance(raw_entry, dict):
                raise RelayError("owned session queue listing returned a non-object job entry")
            raw_job = cast(dict[str, object], raw_entry).get("job")
            if not isinstance(raw_job, dict):
                raise RelayError("owned session queue listing entry omitted its job")
            job_document = {str(k): v for k, v in cast(dict[object, object], raw_job).items()}
            job_id = job_document.get("job_id")
            if not isinstance(job_id, str):
                raise RelayError("owned session queue listing job omitted its job_id")
            results.append((job_document, _list_job_tasks_over_channel(connection, job_id)))
        next_cursor = page.get("source_next_cursor")
        if next_cursor is None:
            break
        if (
            isinstance(next_cursor, bool)
            or not isinstance(next_cursor, int)
            or next_cursor <= cursor
        ):
            raise RelayError("owned session queue listing returned an invalid page cursor")
        cursor = next_cursor
    return results


def build_owned_jobs_over_channel[T](
    connection: RemoteConnection,
    *,
    cluster: str,
    include_terminal: bool,
    build: Callable[[dict[str, object], list[dict[str, object]]], T],
    needs_inclusion: Callable[[T], bool],
) -> list[T]:
    """Build+filter one caller's owned-job model from the held channel's paging.

    Takes the ``build``/``needs_inclusion`` callbacks instead of importing
    ``cli_owned_relay_jobs._owned_relay_job`` directly, to avoid a circular
    import (that module already imports this one for :func:`dial_or_route`).
    """
    kept: list[T] = []
    for job_document, task_documents in list_owned_session_jobs_and_tasks_over_channel(
        connection, cluster=cluster, include_terminal=include_terminal
    ):
        candidate = build(job_document, task_documents)
        if include_terminal or needs_inclusion(candidate):
            kept.append(candidate)
    return kept


def _list_job_tasks_over_channel(
    connection: RemoteConnection, job_id: str
) -> list[dict[str, object]]:
    """Page ``GET /jobs/{job_id}/tasks`` over the held channel."""
    tasks: list[dict[str, object]] = []
    cursor = 1
    while True:
        document = connection.request_json(
            method="GET",
            path=f"/jobs/{job_id}/tasks",
            query={"cursor": cursor, "limit": MAX_RESPONSE_PAGE_RECORDS},
        )
        if not isinstance(document, dict):
            raise RelayError("owned session task listing response is not a JSON object")
        page = cast(dict[str, object], document)
        raw_tasks = page.get("tasks")
        if not isinstance(raw_tasks, list):
            raise RelayError("owned session task listing omitted its tasks array")
        for raw_task in cast(list[object], raw_tasks):
            if not isinstance(raw_task, dict):
                raise RelayError("owned session task listing returned a non-object task")
            tasks.append({str(k): v for k, v in cast(dict[object, object], raw_task).items()})
        next_cursor = page.get("next_cursor")
        if next_cursor is None:
            break
        if (
            isinstance(next_cursor, bool)
            or not isinstance(next_cursor, int)
            or next_cursor <= cursor
        ):
            raise RelayError("owned session task listing returned an invalid page cursor")
        cursor = next_cursor
    return tasks


__all__ = [
    "MAX_RECORDED_FALLBACKS",
    "PER_OP_SSH_FALLBACK_REASON",
    "PerOperationSshFallback",
    "build_owned_jobs_over_channel",
    "channel_backed_json_runner",
    "dial_or_route",
    "dial_or_route_json",
    "dial_or_route_string_ambient",
    "list_owned_session_jobs_and_tasks_over_channel",
    "live_matching_connection",
    "per_operation_fallback_ledger",
    "record_per_operation_ssh_fallback",
]
