"""One place that brings a job's recorded state up to date, and its card as a resource.

A ``serve`` task completes at readiness, and a host stops asking about a task
once it ends, so a service whose job ended later would still read ``running``
on its card, in the site's job list and in a later confirm that names it.
:func:`current` looks at each open job nobody observed lately, with the same
:meth:`RelayService.refresh` a host's ``tasks/get`` makes, and announces
``relay://jobs/{operation_id}`` updated when the card changes. Each surface
that reports a job's state reads it after :func:`current`. The resource holds
the newest card as JSON, the same as ``structuredContent.job``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
from weakref import WeakKeyDictionary

from fastmcp import FastMCP
from fastmcp.exceptions import ResourceError

from clio_relay.jobs.handles import utc_now
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.surface import job_card

if TYPE_CHECKING:
    from clio_relay.surface.changes import ServerChanges
    from clio_relay.surface.service import RelayService

logger = logging.getLogger(__name__)

JOB_WATCH_S = 30.0
"""How often relay itself looks at an open job nobody else is observing."""
WATCHED_KINDS = ("run", "serve", "install")
WATCHED_STATES = (OperationState.SUBMITTING, OperationState.QUEUED, OperationState.RUNNING)
"""``submitting`` too: a submission a crash cut short is reconciled with nobody polling it."""

_ANNOUNCE: WeakKeyDictionary[RelayService, ServerChanges] = WeakKeyDictionary()
_LOOKS: WeakKeyDictionary[RelayService, dict[str, asyncio.Future[bool]]] = WeakKeyDictionary()


def install(server: FastMCP[Any], service: RelayService, changes: ServerChanges) -> None:
    """Add the ``relay://jobs/{operation_id}`` resource template, announced on ``changes``."""
    _ANNOUNCE[service] = changes

    @server.resource(
        job_card.JOB_URI,
        name="job",
        title="Job card",
        description="One job's newest card, the same JSON as structuredContent.job",
        mime_type="application/json",
    )
    def job(operation_id: str) -> str:  # pyright: ignore[reportUnusedFunction]
        record = service.operations.get(operation_id)
        card = job_card.snapshot(service, record) if record is not None else None
        if card is None:
            raise ResourceError(f"there is no job {operation_id!r}")
        return json.dumps(card)


async def watch(service: RelayService, interval_s: float = JOB_WATCH_S) -> None:
    """Bring the open jobs up to date every ``interval_s``, with nobody asking."""
    while True:
        await asyncio.sleep(interval_s)
        try:
            await current(service, stale_s=interval_s)
        except Exception:  # look again next time
            logger.warning("watching open jobs", exc_info=True)


async def current(
    service: RelayService, site_id: str | None = None, *, stale_s: float = JOB_WATCH_S
) -> list[str]:
    """Bring the open jobs (of one site) up to date; return the ids whose card changed.

    Everything that reports a job's state reads it after this: the watcher, a
    site's ``describe`` (its job cards and the Compute list) and the confirm
    text naming the site's running services. A job observed in the last
    ``stale_s``, by a host's ``tasks/get`` say, is not looked at again, and
    callers that ask at once share one look.
    """
    now = utc_now()
    due = [
        record
        for record in service.operations.all()
        if (site_id is None or record.site == site_id) and _due(record, now, stale_s)
    ]
    looks = _LOOKS.setdefault(service, {})
    waiting: list[asyncio.Future[bool]] = []
    for record in due:
        look = looks.get(record.operation_id)
        if look is None:
            look = asyncio.ensure_future(_look(service, record))
            looks[record.operation_id] = look
            look.add_done_callback(_forget(looks, record.operation_id))
        # One caller that gives up must not stop the look the others wait on.
        waiting.append(asyncio.shield(look))
    changed = await asyncio.gather(*waiting)
    return [record.operation_id for record, moved in zip(due, changed, strict=True) if moved]


def _due(record: OperationRecord, now: datetime, stale_s: float) -> bool:
    if record.kind not in WATCHED_KINDS or record.state not in WATCHED_STATES:
        return False
    seen = (record.result or {}).get("observed_at")
    with contextlib.suppress(TypeError, ValueError):
        if now - datetime.fromisoformat(str(seen)) < timedelta(seconds=stale_s):
            return False
    return True


def _forget(looks: dict[str, asyncio.Future[bool]], operation_id: str) -> Callable[..., None]:
    def done(look: asyncio.Future[bool]) -> None:
        if looks.get(operation_id) is look:
            del looks[operation_id]

    return done


async def _look(service: RelayService, record: OperationRecord) -> bool:
    before = job_card.snapshot(service, record)
    try:
        after = await service.refresh(record.operation_id)
    except Exception:
        logger.warning("observing job %s", record.operation_id, exc_info=True)
        return False
    if job_card.snapshot(service, after) == before:
        return False
    changes = _ANNOUNCE.get(service)
    if changes is not None:
        await changes.resource_updated(job_card.job_uri(after.operation_id))
    return True
