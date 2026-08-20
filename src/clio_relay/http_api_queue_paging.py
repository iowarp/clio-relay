"""Owner-session-scoped queue paging for ``GET /queue``.

split/http-api-w3 (iowarp/clio-relay#231): moved verbatim out of
``http_api.py``. Both functions already took ``queue`` as a plain argument
(not a ``create_app()`` closure capture), so this is an unmodified, atomic
move.
"""

from __future__ import annotations

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.models import JobKind, JobState, RelayJob
from clio_relay.pagination import MAX_RESPONSE_PAGE_RECORDS


def _list_owned_session_queue(
    queue: ClioCoreQueue,
    *,
    owner_session_id: str,
    session_generation_id: str,
    cluster: str | None,
    state: JobState | None,
    kind: JobKind | None,
    include_terminal: bool,
    cursor: int,
    limit: int,
    scan_limit: int,
) -> dict[str, object]:
    """List only one exact generation's membership without a global source window."""
    membership_cursor: str | None = None
    source_position = 1
    source_total: int | None = None
    while source_position < cursor:
        skip_limit = min(MAX_RESPONSE_PAGE_RECORDS, cursor - source_position)
        _, next_membership_cursor, source_total, scanned = queue.list_owner_session_jobs_page(
            owner_session_id,
            session_generation_id=session_generation_id,
            cursor=membership_cursor,
            limit=skip_limit,
            include_terminal=True,
        )
        source_position += scanned
        if scanned < skip_limit or next_membership_cursor is None:
            membership_cursor = None
            break
        membership_cursor = next_membership_cursor
    if source_total is not None and (source_position < cursor or cursor > source_total + 1):
        return _owned_queue_page(
            [],
            cluster=cluster,
            state=state,
            kind=kind,
            include_terminal=include_terminal,
            cursor=cursor,
            limit=limit,
            next_cursor=None,
            source_total=source_total,
            scan_limit=scan_limit,
            scanned=0,
        )

    selected: list[RelayJob] = []
    scanned_total = 0
    reached_end = source_total is not None and source_position > source_total
    while not reached_end and scanned_total < scan_limit and len(selected) < limit:
        page_limit = min(MAX_RESPONSE_PAGE_RECORDS, scan_limit - scanned_total)
        jobs, next_membership_cursor, observed_total, scanned = queue.list_owner_session_jobs_page(
            owner_session_id,
            session_generation_id=session_generation_id,
            cursor=membership_cursor,
            limit=page_limit,
            include_terminal=True,
        )
        if source_total is None:
            source_total = observed_total
        elif observed_total != source_total:
            raise QueueConflictError("owner-session membership changed during queue paging")
        consumed = 0
        for job in jobs:
            consumed += 1
            if cluster is not None and job.cluster != cluster:
                continue
            if state is not None and job.state is not state:
                continue
            if kind is not None and job.kind is not kind:
                continue
            if (
                not include_terminal
                and state is None
                and job.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED}
            ):
                continue
            selected.append(job)
            if len(selected) == limit:
                break
        scanned_total += consumed
        source_position += consumed
        if consumed < scanned:
            break
        membership_cursor = next_membership_cursor
        reached_end = membership_cursor is None
        if scanned == 0:
            reached_end = True
    resolved_total = source_total or 0
    next_cursor = source_position if source_position <= resolved_total else None
    return _owned_queue_page(
        selected,
        cluster=cluster,
        state=state,
        kind=kind,
        include_terminal=include_terminal,
        cursor=cursor,
        limit=limit,
        next_cursor=next_cursor,
        source_total=resolved_total,
        scan_limit=scan_limit,
        scanned=scanned_total,
    )


def _owned_queue_page(
    jobs: list[RelayJob],
    *,
    cluster: str | None,
    state: JobState | None,
    kind: JobKind | None,
    include_terminal: bool,
    cursor: int,
    limit: int,
    next_cursor: int | None,
    source_total: int,
    scan_limit: int,
    scanned: int,
) -> dict[str, object]:
    """Render a generation-scoped queue page without cross-session position evidence."""
    return {
        "jobs": [
            {
                "job": job.model_dump(mode="json"),
                "relay_queue": {
                    "state": job.state.value,
                    "jobs_ahead": None,
                    "position": None,
                },
            }
            for job in jobs
        ],
        "count": len(jobs),
        "cluster": cluster,
        "state": None if state is None else state.value,
        "kind": None if kind is None else kind.value,
        "include_terminal": include_terminal,
        "source_cursor": cursor,
        "source_limit": limit,
        "source_next_cursor": next_cursor,
        "source_total": source_total,
        "source_total_semantics": "owner_session_generation_membership",
        "filters_apply_within_source_window": True,
        "visibility_filter": "exact_owner_session_generation",
        "result_truncated": next_cursor is not None,
        "scan_limit": scan_limit,
        "scan_count": scanned,
        "scan_truncated": next_cursor is not None and scanned >= scan_limit,
    }
