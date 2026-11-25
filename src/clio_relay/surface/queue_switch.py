"""Relay's own ask to move a job that waits long when another partition has room.

The facts are JARVIS's: a pending job's execution record carries
``metadata.queue`` (``jarvis.scheduler.queue.v1``), with the job's partition,
why it waits (``reason_kind``), when the scheduler expects it to start, and
the site's other partitions with whether the job fits there now. A submitted
job (a run's, a service's, or an install's) that is expected to start far off,
while another partition fits it, gets one ask: keep waiting, or move to that
partition. Relay never moves a job on its own. The person's answer is acted on
with the adapter's :meth:`~clio_relay.adapters.base.BaseAdapter.move_queue`,
which is ``jarvis execution move``: JARVIS checks the move and keeps the job's
execution; a later job of the same operation (the run after its install) goes
to the partition the person chose.

The ask is asked once per job and partition: after "keep waiting" relay does
not ask again for that job, and a job that starts before the person answers
withdraws it.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

from clio_relay.adapters.base import BaseAdapter
from clio_relay.adapters.hooks import ChoiceOption
from clio_relay.jobs.handles import JobHandle
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal
from clio_relay.jobs.spec import JobSpec

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

logger = logging.getLogger(__name__)

KEY = "queue_switch"
FIELD = "queue"
DEFAULT_WAIT = timedelta(hours=1)
# A job held by its owner or by a dependency waits on itself, so another partition does not help.
_OWN_REASONS = frozenset({"job_hold"})


def state_of(record: OperationRecord) -> dict[str, Any]:
    """Return the operation's queue-switch state, empty when it has none."""
    found = (record.result or {}).get(KEY)
    return cast(dict[str, Any], found) if isinstance(found, dict) else {}


def asking(record: OperationRecord) -> bool:
    """Return whether the operation waits on the person's queue answer."""
    return record.state == OperationState.AWAITING_INPUT and state_of(record).get("stage") == (
        "asking"
    )


def ask_ids(record: OperationRecord) -> set[str]:
    """Return the queue asks relay raised on the operation; their answers never cancel it."""
    return {str(item) for item in cast(list[Any], state_of(record).get("ask_ids") or [])}


def moved_to(record: OperationRecord) -> str | None:
    """Return the partition the person moved the operation's jobs to, if any."""
    value = state_of(record).get("queue_name")
    return value if isinstance(value, str) and value else None


def applied(record: OperationRecord, job: JobSpec) -> JobSpec:
    """Return ``job`` on the partition the person chose for this operation."""
    queue = moved_to(record)
    if queue is None or job.attributes.queue_name == queue:
        return job
    attributes = job.attributes.model_copy(update={"queue_name": queue})
    return job.model_copy(update={"attributes": attributes})


def can_move(adapter: BaseAdapter[Any]) -> bool:
    """Return whether the adapter defines :meth:`BaseAdapter.move_queue`."""
    base = vars(BaseAdapter)["move_queue"]
    return getattr(type(adapter), "move_queue", None) is not base


def _wait_limit(service: RelayService, site_id: str) -> float:
    wait = service.site(site_id).defaults.queue_wait
    return (wait if wait is not None else DEFAULT_WAIT).total_seconds()


def _hours(seconds: float) -> str:
    hours = seconds / 3600
    return f"{hours:.0f} h" if hours >= 2 else f"{max(1, round(seconds / 60))} min"


def _what(record: OperationRecord, phase: str) -> str:
    if phase == "install":
        return "The install job"
    return "The service's job" if record.kind == "serve" else "The job"


async def consider(
    service: RelayService,
    operation_id: str,
    adapter: BaseAdapter[Any],
    handle: JobHandle | None,
    phase: str,
) -> OperationRecord:
    """Ask the person to keep waiting or move, when the job's wait calls for it."""
    record = service.operation(operation_id)
    if record.state != OperationState.QUEUED or handle is None or not can_move(adapter):
        return record
    facts = cast(dict[str, Any], (record.result or {}).get("queue") or {})
    starts_in = facts.get("starts_in_s")
    partition = facts.get("partition")
    reason = str(facts.get("reason") or "")
    if not isinstance(starts_in, int) or not isinstance(partition, str):
        return record
    if facts.get("move_supported") is False or facts.get("reason_kind") in _OWN_REASONS:
        return record
    if starts_in < _wait_limit(service, record.site):
        return record
    listed = cast(list[Any], facts.get("partitions") or [])
    entries = [cast(dict[str, Any], item) for item in listed if isinstance(item, dict)]
    fitting = [
        item
        for item in entries
        if item.get("fits") and item.get("partition") and item.get("partition") != partition
    ]
    if not fitting:
        return record
    key = f"{handle.native_id}:{partition}"
    state = state_of(record)
    if key in cast(list[str], state.get("asked") or []):
        return record
    keep = f"keep waiting in {partition}"
    options = {keep: ""}
    for item in fitting:
        options[f"move to {item['partition']}"] = str(item["partition"])
    start = facts.get("estimated_start") or "later"
    typed = [
        ChoiceOption(
            value=keep,
            label=f"Keep waiting in {partition}",
            description=(
                f"Nothing changes; the scheduler expects it to start at {start}, "
                f"in about {_hours(starts_in)}."
            ),
        ),
        *(
            ChoiceOption(
                value=f"move to {item['partition']}",
                label=f"Move to {item['partition']}",
                description=(
                    "Has room now"
                    + (
                        f" ({item['idle_nodes']} idle nodes)"
                        if isinstance(item.get("idle_nodes"), int)
                        else ""
                    )
                    + "; the same job keeps its place in relay and starts when "
                    f"{item['partition']} runs it, under that partition's limits."
                ),
            )
            for item in fitting
        ),
    ]
    names = ", ".join(
        f"{item['partition']} ({item['idle_nodes']} idle nodes)"
        if isinstance(item.get("idle_nodes"), int)
        else str(item["partition"])
        for item in fitting
    )
    site = service.site(record.site)
    message = (
        f"{_what(record, phase)} {handle.native_id} waits in the {partition} queue on "
        f"{site.title}{f' ({reason})' if reason else ''}. The scheduler expects it to start "
        f"at {start}, in about {_hours(starts_in)}. {names} "
        f"{'has' if len(fitting) == 1 else 'have'} room for it now. Keep waiting, or move "
        "it there? Moving keeps the job and its place in relay; it starts when that "
        "partition runs it."
    )
    ask = await service.ask_provider.choose(
        site=site,
        operation_id=operation_id,
        message=message,
        field_name=FIELD,
        options=typed,
        title="Where the job waits",
    )
    with service.operations.locked(operation_id):
        record = service.operation(operation_id)
        if record.state != OperationState.QUEUED:
            service.answer(ask.ask_id, "cancel", by="relay")
            return record
        state = state_of(record)
        record.result = {
            **(record.result or {}),
            KEY: {
                **state,
                "stage": "asking",
                "ask_id": ask.ask_id,
                "phase": phase,
                "native_id": handle.native_id,
                "partition": partition,
                "options": options,
                "asked": [*cast(list[str], state.get("asked") or []), key],
                "ask_ids": [*cast(list[str], state.get("ask_ids") or []), ask.ask_id],
            },
        }
        record.asks = [*record.asks, ask.ask_id]
        record.move(OperationState.AWAITING_INPUT, message)
        service.operations.put(operation_id, record)
    return record


def _resume(service: RelayService, operation_id: str, note: str, **changes: Any) -> None:
    with service.operations.locked(operation_id):
        record = service.operation(operation_id)
        if record.state.final:
            return
        state = {**state_of(record), **changes}
        record.result = {**(record.result or {}), KEY: state, "message": note}
        record.move(OperationState.QUEUED, note)
        service.operations.put(operation_id, record)


async def advance(
    service: RelayService, operation_id: str, handle: JobHandle | None
) -> OperationRecord:
    """Act on the person's queue answer, or withdraw the ask once the job starts."""
    record = service.operation(operation_id)
    state = state_of(record)
    ask = service.asks.get(str(state.get("ask_id")))
    site = service.site(record.site)
    adapter = service.adapter_named(site, record.adapter)
    if handle is None:
        _resume(service, operation_id, "the job relay asked about is gone", stage="gone")
        return await service.refresh(operation_id)
    if ask is None or ask.state == "pending":
        try:
            status = await adapter.status(handle)
        except Exception:  # noqa: BLE001 - one failed look keeps the ask open
            logger.warning("observing %s while its queue ask waits", operation_id, exc_info=True)
            return record
        if status.queue is not None and not status.state.final:
            return record
        if ask is not None:
            service.answer(ask.ask_id, "cancel", by="relay")
        _resume(
            service,
            operation_id,
            "the job left the queue before the person answered",
            stage="overtaken",
        )
        return await service.refresh(operation_id)
    content = cast(dict[str, Any], (ask.answer or {}).get("content") or {})
    options = cast(dict[str, str], state.get("options") or {})
    target = options.get(str(content.get(FIELD) or ""), "") if ask.state == "accepted" else ""
    partition = state.get("partition")
    if not target:
        _resume(
            service,
            operation_id,
            f"the person keeps the job waiting in {partition}",
            stage="kept",
        )
        return await service.refresh(operation_id)
    try:
        await adapter.move_queue(handle, target)
    except Refusal as refusal:
        _resume(service, operation_id, refusal.message, stage="failed", error=refusal.message)
        return await service.refresh(operation_id)
    _resume(
        service,
        operation_id,
        f"moved the job from {partition} to {target}, as the person chose",
        stage="moved",
        queue_name=target,
    )
    return await service.refresh(operation_id)
