"""Starting again a step that an ended relay process left in ``submitting``.

A process that submits holds the operation's submit lease
(:meth:`~clio_relay.jobs.records.RecordStore.submitting`) from before the record
reads ``submitting`` until the step returns. The operating system drops the
lease when the process dies, and a cancelled call (a host that closed relay
while it worked) drops it too. So a ``submitting`` record with a free lease is
a step that nobody is doing. :meth:`RelayService.refresh` hands those here,
whoever calls it: the job watcher, a ``tasks/get``, a ``describe`` or a fresh
relay's first look. What happens depends on the step:

- the light install check (an ``install`` operation's submit) runs no Spack and
  starts no job, so relay runs it again at once;
- for a run's submit, relay first looks for the job by its operation (``find``)
  and follows a job it finds. A submit cut short may still reach the scheduler,
  so relay waits ``GRACE``. If the adapter can find jobs and still finds none,
  relay submits again. An adapter that cannot find jobs fails the run instead;
- for an install job's start (``ppl install +submit``, or installing
  jarvis-cd), when the adapter lists the site's jobs and no job named for the
  operation (``relay-<operation>``) is listed after the grace, relay starts it again from the
  person's answers. While one is listed, relay waits. An adapter that cannot
  list jobs waits ``START_GRACE`` and then fails the operation.

An operation restarts at most ``MAX_RESTARTS`` times. After that it fails with
the reason, so it can't loop. Each restart is a note in the operation's history.
"""

from __future__ import annotations

import logging
from contextlib import ExitStack
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Literal

from filelock import Timeout

from clio_relay.adapters.base import BaseAdapter, ListsLiveJobs
from clio_relay.jobs.handles import utc_now
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.surface import installing

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

logger = logging.getLogger(__name__)

GRACE = timedelta(minutes=2)
"""How long a submit cut short may still take to reach the site's scheduler."""

MAX_RESTARTS = 3
"""How many times one operation's interrupted steps start again before it fails."""

RESTARTS_KEY = "restarts"

Step = Literal["check", "submit", "start"]

_WHAT: dict[Step, str] = {
    "check": "the install check",
    "submit": "the submission",
    "start": "the install's start",
}


def step_of(record: OperationRecord) -> Step:
    """Name the step a ``submitting`` record is in."""
    if installing.state_of(record).get("stage") == "starting":
        return "start"
    return "check" if record.kind == "install" else "submit"


def can_find(adapter: BaseAdapter[Any]) -> bool:
    """Say whether the adapter finds a job by its operation, so a submit can be redone."""
    return getattr(type(adapter), "find", None) is not BaseAdapter.__dict__["find"]


def _claim(service: RelayService, operation_id: str) -> ExitStack | None:
    """Take the submit lease that nobody holds, or return None while a live process holds it."""
    if service.operations.submit_in_hand(operation_id):
        return None
    stack = ExitStack()
    try:
        stack.enter_context(service.operations.submitting(operation_id, timeout=0))
    except Timeout:
        return None
    return stack


async def restart(service: RelayService, operation_id: str, note: str) -> OperationRecord:
    """Start the operation's interrupted step again, holding its submit lease."""
    stack = _claim(service, operation_id)
    if stack is None:
        return service.operation(operation_id)
    with stack:
        with service.operations.locked(operation_id):
            record = service.operation(operation_id)
            if record.state != OperationState.SUBMITTING:
                return record
            step = step_of(record)
            done = int((record.result or {}).get(RESTARTS_KEY) or 0)
            spent = done >= MAX_RESTARTS
            if not spent:
                result: dict[str, Any] = {**(record.result or {}), RESTARTS_KEY: done + 1}
                if step == "start":
                    state = {**installing.state_of(record), "stage": "asking"}
                    result[installing.INSTALL_KEY] = state
                record.result = result
                record.move(OperationState.AWAITING_INPUT, note)
                service.operations.put(operation_id, record)
        if spent:
            return service.finish(
                operation_id,
                OperationState.FAILED,
                Refusal(
                    RefusalReason.SUBMIT_FAILED,
                    f"{_WHAT[step]} was cut short {done + 1} times; ask for it again",
                ),
            )
        logger.info("%s: %s", operation_id, note)
        if step == "start":
            return await service.advance(operation_id)
        return await service.submit(operation_id)


async def check_cut_short(service: RelayService, operation_id: str) -> OperationRecord:
    """Run an interrupted light install check again. It holds no job to look for."""
    return await restart(
        service, operation_id, "the install check was cut short; JARVIS checks again"
    )


async def submit_cut_short(service: RelayService, operation_id: str) -> OperationRecord:
    """Submit again a run whose submission was cut short and whose job ``find`` did not find."""
    return await restart(
        service,
        operation_id,
        "the submission was cut short and no job of it was found; relay submits it again",
    )


async def start_cut_short(
    service: RelayService,
    operation_id: str,
    adapter: BaseAdapter[Any],
    record: OperationRecord,
) -> OperationRecord:
    """Start an interrupted install again, once the site lists no job of it.

    Relay's jobs carry the operation in their scheduler name (``relay-<operation>``).
    """
    age = utc_now() - record.updated_at
    if installing.state_of(record).get("kind") == "jarvis":
        return await restart(
            service, operation_id, "installing jarvis-cd was cut short; relay installs it again"
        )
    if not isinstance(adapter, ListsLiveJobs):
        return _wait_or_fail(service, operation_id, record, age)
    if age <= GRACE:
        return record
    try:
        listed = await adapter.live_jobs([])
    except Refusal as refusal:
        return service.observation_failed(operation_id, refusal.message, "install")
    if any(operation_id in job.get("name", "") for job in listed):
        return _wait_or_fail(service, operation_id, record, age)
    return await restart(
        service,
        operation_id,
        "the install job's start was cut short and the site lists no job of it; "
        "relay starts it again",
    )


def _wait_or_fail(
    service: RelayService, operation_id: str, record: OperationRecord, age: timedelta
) -> OperationRecord:
    if age <= installing.START_GRACE:
        return record
    return service.finish(
        operation_id,
        OperationState.FAILED,
        Refusal(
            RefusalReason.SUBMIT_FAILED,
            "the install was interrupted before its job started; run the job again",
        ),
    )
