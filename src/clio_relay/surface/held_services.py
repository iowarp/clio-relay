"""Relay's ask to end the site's running services when a new job of the site is submitted.

A service keeps its allocation while it runs, and one whose program has
finished may still be up so the person can review its results (a viewer's
linger). A new run or service on the same site then holds nodes beside it: a
pilot's three and a production run's three, say. So when a run or service is
opened while another service of the site's runs, relay asks once, with the
job's other asks: end those services when this job is submitted, or keep them
running. Relay never ends one on its own, and keeping is the answer to a
declined or cancelled ask. The person still has each service's review until
they choose to end it.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, cast

from clio_relay.adapters.hooks import Ask
from clio_relay.jobs.records import OperationKind, OperationRecord, OperationState
from clio_relay.jobs.spec import ServiceSpec
from clio_relay.sites.models import SiteEntry
from clio_relay.surface import job_watch

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

logger = logging.getLogger(__name__)

KEY = "held_services"
FIELD = "running_services"
END = "end them when this job is submitted"
KEEP = "keep them running"


def state_of(record: OperationRecord) -> dict[str, Any]:
    """Return the operation's held-services state, empty when it has none."""
    found = (record.result or {}).get(KEY)
    return cast(dict[str, Any], found) if isinstance(found, dict) else {}


def ask_ids(record: OperationRecord) -> set[str]:
    """Return the held-services ask; declining it keeps the services and never cancels.

    A choice folded into the job's card (``in_card``) is the card's own ask, and
    declining the card does cancel the job, so it is not listed.
    """
    state = state_of(record)
    found = state.get("ask_id")
    return {found} if isinstance(found, str) and not state.get("in_card") else set()


def _nodes(record: OperationRecord) -> int | None:
    try:
        return ServiceSpec.model_validate(record.spec).job.resources.nodes()
    except Exception:  # noqa: BLE001 - a record of an older relay may not read
        return None


async def running_services(service: RelayService, site_id: str) -> list[OperationRecord]:
    """Return the site's services that are up now, oldest first, as just observed."""
    await job_watch.current(service, site_id)
    found = [
        record
        for record in service.operations.all()
        if record.site == site_id
        and record.kind == "serve"
        and record.state == OperationState.RUNNING
    ]
    return sorted(found, key=lambda record: record.created_at)


def _described(record: OperationRecord) -> str:
    job = cast(dict[str, Any], record.spec.get("job") or {})
    steps = [
        str(cast(dict[str, Any], step).get("name"))
        for step in [job.get("package"), *cast(list[Any], job.get("packages") or [])]
        if isinstance(step, dict)
    ]
    what = ", ".join(steps) or str(job.get("executable") or "a service")
    nodes = _nodes(record)
    held = f", holding {nodes} node{'s' if nodes != 1 else ''}" if nodes else ""
    result = record.result or {}
    if str(result.get("observation_failed_at") or "") > str(result.get("observed_at") or ""):
        held += ", though relay could not check it just now"
    return f"{record.operation_id} ({what}{held}, up since {record.created_at:%H:%M} UTC)"


async def question(
    service: RelayService, site: SiteEntry, kind: OperationKind, operation_id: str
) -> tuple[str, list[str]] | None:
    """Return the question for a new run or service while others of the site run, and those."""
    if kind == "install":
        return None
    running = await running_services(service, site.id)
    running = [r for r in running if r.operation_id != operation_id]
    if not running:
        return None
    named = "; ".join(_described(record) for record in running)
    one = len(running) == 1
    message = (
        f"{'A service' if one else 'Services'} of relay's still "
        f"{'runs' if one else 'run'} on {site.title}: {named}. "
        f"{'It keeps its' if one else 'They keep their'} nodes while up, beside this job's. "
        f"End {'it' if one else 'them'} when this job is submitted, or keep "
        f"{'it' if one else 'them'} running, for example to review the results?"
    )
    return message, [r.operation_id for r in running]


async def ask(
    service: RelayService, site: SiteEntry, kind: OperationKind, operation_id: str
) -> tuple[Ask, dict[str, Any]] | None:
    """Build the ask for a new run or service while others of the site run, if any do."""
    found = await question(service, site, kind, operation_id)
    if found is None:
        return None
    message, operations = found
    asked = await service.ask_provider.choose(
        site=site,
        operation_id=operation_id,
        message=message,
        field_name=FIELD,
        options=[END, KEEP],
    )
    return asked, {"ask_id": asked.ask_id, "operations": operations}


async def end_chosen(service: RelayService, operation_id: str) -> None:
    """End the services the person chose to end, as this operation is submitted."""
    record = service.operation(operation_id)
    state = state_of(record)
    if not state or state.get("ended") is not None:
        return
    found = service.asks.get(str(state.get("ask_id")))
    content = cast(dict[str, Any], ((found.answer if found else None) or {}).get("content") or {})
    chosen = found is not None and found.state == "accepted" and content.get(FIELD) == END
    ended: list[str] = []
    for other in cast(list[str], state.get("operations") or []) if chosen else []:
        try:
            if not (await service.cancel(other)).state.final:
                logger.info("ending %s as the person chose: still stopping", other)
            ended.append(other)
        except Exception:  # noqa: BLE001 - one service that does not stop leaves the rest
            logger.warning("ending %s as the person chose", other, exc_info=True)
    with service.operations.locked(operation_id):
        record = service.operation(operation_id)
        record.result = {**(record.result or {}), KEY: {**state_of(record), "ended": ended}}
        service.operations.put(operation_id, record)
