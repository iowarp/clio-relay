"""Paged remote/owned-session JSON fetching and collection completion for
MCP tools: the shared remote-CLI and owned-session-HTTP JSON transport
(`_remote_json`/`_remote_json_value`, `_owned_json`), owned-job-status
shape validation, and the three completion drainers
(`_complete_local_artifacts`, `_complete_remote_collection`,
`_complete_owned_collection`) plus their shared page-continuity validator,
and remote/owned job-log reads.

Split out of mcp_server.py (iowarp/clio-relay#231) as one half of the
result-verification/artifact-completion cluster's real seam split (the
whole cluster measured 830 lines as a single module, over the 800-line
ratchet cap; mcp_result_verification.py is the other half). A clean leaf
with respect to that other half -- confirmed by grep before the move, none
of these functions call anything there.

Two names are directly monkeypatched by tests at
`mcp_server_module.<name>`: `run_remote_clio` (imported from
clio_relay.remote_cli everywhere else, reached here only through
`_remote_json_value`'s function-scope `_mcp_server.run_remote_clio(...)`
back-reference) and `_remote_json` itself, which two of this module's own
functions (`_complete_remote_collection`, `_remote_job_logs`) call through
the same back-reference rather than a same-module bare call -- a bare call
would resolve through this module's own globals instead of mcp_server's
patchable namespace and silently miss the test's patch (the same trap
slice 4 found and fixed for `_tool_definitions_and_remote_catalog`).
"""

from __future__ import annotations

import json
from typing import Any, cast

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.pagination import MAX_RESPONSE_PAGE_RECORDS
from clio_relay.session_api import OwnedSessionApiClient
from clio_relay.spool import LOG_STREAM_NAMES

JSON = dict[str, Any]

MAX_INTERNAL_COLLECTION_RECORDS = 10_000


def _remote_json(
    definition: ClusterDefinition,
    args: list[str],
    label: str,
) -> JSON:
    value = _remote_json_value(definition, args, label)
    if not isinstance(value, dict):
        raise ValueError(f"{label} must return a JSON object")
    return cast(JSON, value)


def _remote_json_value(
    definition: ClusterDefinition,
    args: list[str],
    label: str,
) -> object:
    from clio_relay import mcp_server as _mcp_server

    output = _mcp_server.run_remote_clio(definition, args)
    try:
        return cast(object, json.loads(output))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} returned invalid JSON") from exc


def _owned_json(
    client: OwnedSessionApiClient,
    *,
    method: str,
    path: str,
    label: str,
    query: dict[str, object] | None = None,
    body: dict[str, object] | None = None,
    response_timeout_seconds: float | None = None,
) -> JSON:
    """Read one object from an exact-generation, identity-proven session API."""
    if response_timeout_seconds is None:
        value = client.request_json(method=method, path=path, query=query, body=body)
    else:
        value = client.request_json(
            method=method,
            path=path,
            query=query,
            body=body,
            response_timeout_seconds=response_timeout_seconds,
        )
    if not isinstance(value, dict):
        raise ValueError(f"{label} must return a JSON object")
    return cast(JSON, value)


def _validate_owned_job_status(payload: JSON, *, job_id: str, cluster: str) -> None:
    raw_job = payload.get("job")
    if not isinstance(raw_job, dict):
        raise ValueError("owned session job response is missing its durable job record")
    job = cast(JSON, raw_job)
    if job.get("job_id") != job_id or job.get("cluster") != cluster:
        raise ValueError("owned session job response does not match the requested handle")


def _complete_local_artifacts(queue: ClioCoreQueue, job_id: str) -> list[JSON]:
    """Read all artifacts under an explicit cap or reject incomplete evidence."""
    cursor = 1
    expected_total: int | None = None
    records: list[JSON] = []
    while True:
        page, next_cursor, total = queue.list_artifacts_page(
            job_id,
            cursor=cursor,
            limit=MAX_RESPONSE_PAGE_RECORDS,
        )
        expected_total = _validate_complete_collection_page(
            label=f"artifacts for {job_id}",
            cursor=cursor,
            page_count=len(page),
            next_cursor=next_cursor,
            total=total,
            expected_total=expected_total,
            collected_count=len(records),
        )
        records.extend(artifact.model_dump(mode="json") for artifact in page)
        if next_cursor is None:
            if len(records) != total:
                raise ValueError(f"artifacts for {job_id} changed during bounded discovery")
            return records
        cursor = next_cursor


def _complete_remote_collection(
    definition: ClusterDefinition,
    command: list[str],
    *,
    record_key: str,
    label: str,
) -> list[JSON]:
    """Drain a remote paged CLI collection or reject partial/moving evidence."""
    from clio_relay import mcp_server as _mcp_server

    cursor = 1
    expected_total: int | None = None
    records: list[JSON] = []
    while True:
        payload = _mcp_server._remote_json(
            definition,
            [
                *command,
                "--cursor",
                str(cursor),
                "--limit",
                str(MAX_RESPONSE_PAGE_RECORDS),
            ],
            label,
        )
        raw_records = payload.get(record_key)
        if not isinstance(raw_records, list):
            raise ValueError(f"{label} must contain a {record_key} array")
        page: list[JSON] = []
        for item in cast(list[object], raw_records):
            if not isinstance(item, dict):
                raise ValueError(f"{label} returned a non-object {record_key} entry")
            page.append(cast(JSON, item))
        total = payload.get("total")
        returned_cursor = payload.get("cursor")
        returned_limit = payload.get("limit")
        next_cursor = payload.get("next_cursor")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise ValueError(f"{label} returned an invalid total")
        if returned_cursor != cursor or returned_limit != MAX_RESPONSE_PAGE_RECORDS:
            raise ValueError(f"{label} returned inconsistent page metadata")
        if next_cursor is not None and (
            isinstance(next_cursor, bool) or not isinstance(next_cursor, int)
        ):
            raise ValueError(f"{label} returned an invalid next_cursor")
        expected_total = _validate_complete_collection_page(
            label=label,
            cursor=cursor,
            page_count=len(page),
            next_cursor=next_cursor,
            total=total,
            expected_total=expected_total,
            collected_count=len(records),
        )
        records.extend(page)
        if next_cursor is None:
            if len(records) != total:
                raise ValueError(f"{label} changed during bounded discovery")
            return records
        cursor = next_cursor


def _complete_owned_collection(
    client: OwnedSessionApiClient,
    *,
    path: str,
    record_key: str,
    label: str,
) -> list[JSON]:
    """Drain an owned HTTP collection on one already identity-proven connection."""
    cursor = 1
    expected_total: int | None = None
    records: list[JSON] = []
    while True:
        payload = _owned_json(
            client,
            method="GET",
            path=path,
            query={"cursor": cursor, "limit": MAX_RESPONSE_PAGE_RECORDS},
            label=label,
        )
        raw_records = payload.get(record_key)
        if not isinstance(raw_records, list):
            raise ValueError(f"{label} must contain a {record_key} array")
        page: list[JSON] = []
        for item in cast(list[object], raw_records):
            if not isinstance(item, dict):
                raise ValueError(f"{label} returned a non-object {record_key} entry")
            page.append(cast(JSON, item))
        total = payload.get("total")
        returned_cursor = payload.get("cursor")
        returned_limit = payload.get("limit")
        next_cursor = payload.get("next_cursor")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise ValueError(f"{label} returned an invalid total")
        if returned_cursor != cursor or returned_limit != MAX_RESPONSE_PAGE_RECORDS:
            raise ValueError(f"{label} returned inconsistent page metadata")
        if next_cursor is not None and (
            isinstance(next_cursor, bool) or not isinstance(next_cursor, int)
        ):
            raise ValueError(f"{label} returned an invalid next_cursor")
        expected_total = _validate_complete_collection_page(
            label=label,
            cursor=cursor,
            page_count=len(page),
            next_cursor=next_cursor,
            total=total,
            expected_total=expected_total,
            collected_count=len(records),
        )
        records.extend(page)
        if next_cursor is None:
            # D16: no post-loop completeness check needed here (unlike this
            # function's two siblings above) -- ``_validate_complete_
            # collection_page`` above already enforces the identical
            # ``next_cursor is None and collected_count + page_count != total``
            # invariant on every iteration, before ``records.extend(page)``
            # runs, so a page that understates its own total never reaches
            # this line. Do not re-add a post-loop check here; it would be
            # provably dead code (verified: this function's own regression
            # test still fails on the in-loop message, never a post-loop one).
            return records
        cursor = next_cursor


def _validate_complete_collection_page(
    *,
    label: str,
    cursor: int,
    page_count: int,
    next_cursor: int | None,
    total: int,
    expected_total: int | None,
    collected_count: int,
) -> int:
    """Reject oversized, discontinuous, or moving internal page chains."""
    if total > MAX_INTERNAL_COLLECTION_RECORDS:
        raise ValueError(
            f"{label} exceeds the bounded completeness limit {MAX_INTERNAL_COLLECTION_RECORDS}"
        )
    if expected_total is not None and total != expected_total:
        raise ValueError(f"{label} changed during bounded discovery")
    if collected_count + page_count > total:
        raise ValueError(f"{label} returned more records than its total")
    if next_cursor is not None and (
        page_count == 0 or next_cursor != cursor + page_count or next_cursor > total
    ):
        raise ValueError(f"{label} returned a non-contiguous page cursor")
    if next_cursor is None and collected_count + page_count != total:
        raise ValueError(f"{label} ended before its declared total")
    return total


def _remote_job_logs(
    definition: ClusterDefinition,
    job_id: str,
    *,
    limit: int,
) -> JSON:
    from clio_relay import mcp_server as _mcp_server

    return {
        stream: _mcp_server._remote_json(
            definition,
            [
                "job",
                "read-log",
                job_id,
                "--stream",
                stream,
                "--offset",
                "0",
                "--limit",
                str(limit),
            ],
            f"remote {stream} log",
        )
        for stream in ("stdout", "stderr")
    }


def _owned_job_logs(
    client: OwnedSessionApiClient,
    job_id: str,
    *,
    limit: int,
) -> JSON:
    """Return the current log page for every stream an owned-session job can carry.

    clio-relay#221/#259: includes ``console``/``console_stderr`` alongside
    ``stdout``/``stderr`` -- the owned-session HTTP door already serves all
    four identically (``GET /jobs/{id}/logs/{stream}``), so a
    ``jarvis_run`` mcp_call job dispatched to a remote cluster via an owned
    session is just as visible mid-run as a local one (`_job_logs`,
    mcp_job_lifecycle.py). Deliberately NOT mirrored onto `_remote_job_logs`
    below: that path shells out to the remote ``clio-relay job read-log``
    CLI, whose ``--stream`` option still rejects anything but
    stdout/stderr (cli_job_records.py) -- extending its stream set here
    without first fixing that command would either error loudly for a
    legacy direct-SSH deployment or, worse, silently mislabel data.
    """
    return {
        stream: _owned_json(
            client,
            method="GET",
            path=f"/jobs/{job_id}/logs/{stream}",
            query={"offset": 0, "limit": limit},
            label=f"owned remote {stream} log",
        )
        for stream in LOG_STREAM_NAMES
    }
