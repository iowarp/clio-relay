"""The same job already live on the site: the person chooses to use it or start another.

An agent may submit a job again that it already has running, after a restart or
a lost result, say. Relay never folds the two by itself, and the client passes
no key for it. When a run or service's spec hashes the same as one of the
site's that is submitted and not ended, its approval (the job's card, or the
approval relay asks for it) carries a typed fact naming that operation and its
state, and a choice: use the live one, or start another. Using it ends the new
operation as declined, naming the live one in ``use_operation_id``; starting
another submits it as asked. A job that asks the person nothing is submitted
as before.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from clio_relay.adapters.hooks import Ask, ChoiceOption, choice_field, choice_meta, with_choice_meta
from clio_relay.jobs import declines
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal, declined
from clio_relay.surface import job_card, job_watch

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

KEY = "same_run"
FIELD = "same_run"
META = "io.iowarp.clio-relay/same_run"
USE = "use the live one"
ANOTHER = "start another"
TITLE = "The same job is live"
LIVE = (OperationState.SUBMITTING, OperationState.QUEUED, OperationState.RUNNING)

OPTIONS = [
    ChoiceOption(
        value=USE,
        label="Use the live one",
        description="Nothing new is submitted; the agent is pointed at the live operation.",
        recommended=True,
    ),
    ChoiceOption(
        value=ANOTHER,
        label="Start another",
        description="Submits this one as well; both hold their own nodes.",
    ),
]


def _live(service: RelayService, record: OperationRecord) -> list[OperationRecord]:
    return [
        other
        for other in service.operations.all()
        if other.operation_id != record.operation_id
        and other.site == record.site
        and other.kind == record.kind
        and other.spec_digest == record.spec_digest
        and other.state in LIVE
    ]


async def twin(service: RelayService, record: OperationRecord) -> OperationRecord | None:
    """Return the newest live operation of the site with ``record``'s spec, as just observed."""
    if record.kind not in ("run", "serve") or not _live(service, record):
        return None
    await job_watch.current(service, record.site)
    found = _live(service, record)
    return max(found, key=lambda other: other.created_at) if found else None


def fact(other: OperationRecord) -> dict[str, Any]:
    """The typed fact on the card: which operation is live, and where it is."""
    return {
        "operation_id": other.operation_id,
        "state": other.state.value,
        "job_id": other.handle.native_id if other.handle else None,
        "created_at": other.created_at.isoformat(),
        "resource": job_card.job_uri(other.operation_id),
    }


def _message(other: OperationRecord) -> str:
    job = f", job {other.handle.native_id}" if other.handle else ""
    return (
        f"The same job is already {other.state.value} on this site: {other.operation_id}"
        f"{job}, submitted {other.created_at:%H:%M} UTC. Use it, or start another?"
    )


def fold(service: RelayService, into: Ask, record: OperationRecord, other: OperationRecord) -> None:
    """Put the fact and the choice in the job's own approval, so the person answers once."""
    message = _message(other)
    schema = dict(into.requested_schema or {})
    properties = dict(schema.get("properties") or {})
    properties[FIELD] = choice_field(OPTIONS, title=TITLE, description=message)
    schema["properties"] = properties
    into.requested_schema = schema
    into.message = f"{into.message}\n\n{message}"
    meta = with_choice_meta(into.meta, choice_meta(into.ask_id, FIELD, OPTIONS, title=TITLE))
    into.meta = {**meta, META: fact(other)}
    service.asks.put(into.ask_id, into)
    state = {"ask_id": into.ask_id, "operation_id": other.operation_id}
    record.result = {**(record.result or {}), KEY: state}


def chosen(service: RelayService, record: OperationRecord) -> Refusal | None:
    """Return the decline for an approval answered "use the live one", else ``None``."""
    state = cast(dict[str, Any], (record.result or {}).get(KEY) or {})
    found = service.asks.get(str(state.get("ask_id"))) if state else None
    if found is None or found.state != "accepted":
        return None
    content = cast(dict[str, Any], (found.answer or {}).get("content") or {})
    if content.get(FIELD) != USE:
        return None
    live = str(state["operation_id"])
    return declined(
        f"Start another of the job that {live} already runs",
        f"{declines.not_done(record)} The person chose the live operation {live}.",
        ask_id=found.ask_id,
        operation_id=record.operation_id,
        use_operation_id=live,
    )
