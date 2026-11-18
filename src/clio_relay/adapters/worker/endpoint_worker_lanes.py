"""Worker-slot lane resilience: never let a worker slot die silently (clio-relay#238).

A daemon-mode worker slot runs ``EndpointWorker._serve_worker_slot``'s ``while True:
worker.run_once()`` loop inside a ``ThreadPoolExecutor`` future. ``run_once`` wraps
per-job execution in its own ``try/except``, but its between-jobs housekeeping is not
job-scoped. An exception from that housekeeping used to kill the slot's thread with no
log line, and the executor's ``shutdown(wait=True)`` then blocked on the surviving
slots, leaving a live process with 0-byte logs.

:func:`run_worker_lane_iteration` is the barrier around the per-slot loop body: any
exception one iteration raises is caught, logged with ``exc_info`` (Python's logging
module writes WARNING-and-above to stderr through its last-resort handler even with no
``logging.basicConfig``), recorded on the endpoint's own registry metadata as
``lane_last_error`` (so ``worker status``/diagnostics can see it without reading a log
file), and the loop keeps going.

The typed reason below is a worker-lane-internal lifecycle event, not a door-surface
classification, so it is its own small frozen catalog in the same style as
``door_errors.REASONS`` rather than a new row there.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from clio_relay.models import EndpointRegistration, utc_now

logger = logging.getLogger(__name__)

#: Schema of the durable event payload / endpoint metadata recording one
#: otherwise-unhandled worker-slot lane iteration failure.
WORKER_LANE_ITERATION_FAILURE_SCHEMA = "clio-relay.worker-lane-iteration-failure.v1"

#: Typed reason: one worker-slot poll iteration raised an otherwise-unhandled
#: exception and was contained by the per-slot lane barrier.
WORKER_LANE_ITERATION_FAILED = "worker_lane_iteration_failed"

#: Endpoint registration metadata key carrying the most recent lane failure
#: (cleared once an iteration succeeds again).
LANE_LAST_ERROR_METADATA_KEY = "lane_last_error"

#: Bound on the exception detail text folded into a typed event/metadata
#: record -- mirrors the byte-budget doctrine other typed-reason payloads in
#: this codebase already apply (door_errors.py's MAX_MESSAGE_CHARS), sized
#: down for a field that is diagnostic context, not the primary message.
_MAX_DETAIL_CHARS = 2_000


def _bounded_detail(text: str) -> str:
    """Clip an exception's ``str()`` so one lane failure cannot bloat records."""
    if len(text) <= _MAX_DETAIL_CHARS:
        return text
    return text[: _MAX_DETAIL_CHARS - 1] + "…"


class WorkerLaneRegistrationQueue(Protocol):
    """The one durable-queue capability the lane barrier needs: re-register."""

    def register_endpoint(self, endpoint: EndpointRegistration) -> EndpointRegistration: ...


@dataclass(frozen=True, slots=True)
class WorkerLaneIterationFailure:
    """One otherwise-unhandled exception from a worker-slot poll iteration."""

    endpoint_id: str
    detail: str
    failed_at: datetime

    def as_payload(self) -> dict[str, object]:
        """Return the typed payload recorded on the event log and registry."""
        return {
            "schema_version": WORKER_LANE_ITERATION_FAILURE_SCHEMA,
            "reason": WORKER_LANE_ITERATION_FAILED,
            "endpoint_id": self.endpoint_id,
            "detail": self.detail,
            "failed_at": self.failed_at.isoformat(),
        }


def run_worker_lane_iteration(
    iterate: Callable[[], object],
    *,
    queue: WorkerLaneRegistrationQueue,
    endpoint: EndpointRegistration,
) -> EndpointRegistration:
    """Run one worker-slot poll iteration; never let it die silently.

    clio-relay#238 acceptance item 3: any slot/lane death must emit a typed
    reason to the worker log *and* the endpoint registry -- the 0-byte-log
    failure mode must be structurally impossible. This wrapper contains
    *any* exception an iteration raises, so a daemon-mode slot can never
    again die with zero evidence.

    Args:
        iterate: One worker-slot iteration, e.g. ``worker.run_once``. Its
            return value is discarded; it runs for effect.
        queue: The durable queue to heartbeat/re-register the endpoint on.
        endpoint: The current registration for this slot.

    Returns:
        The endpoint registration to keep looping with -- unchanged on
        success (unless a prior failure's ``lane_last_error`` needed
        clearing), or updated with a typed ``lane_last_error`` on failure.
    """
    try:
        iterate()
    except Exception as exc:  # noqa: BLE001 - the lane's last-resort barrier, #238
        failure = WorkerLaneIterationFailure(
            endpoint_id=endpoint.endpoint_id,
            detail=_bounded_detail(f"{type(exc).__name__}: {exc}"),
            failed_at=utc_now(),
        )
        logger.error(
            "clio-relay#238: worker lane iteration failed endpoint=%s detail=%s",
            endpoint.endpoint_id,
            failure.detail,
            exc_info=True,
        )
        updated_metadata = {**endpoint.metadata, LANE_LAST_ERROR_METADATA_KEY: failure.as_payload()}
        return queue.register_endpoint(endpoint.model_copy(update={"metadata": updated_metadata}))
    if LANE_LAST_ERROR_METADATA_KEY in endpoint.metadata:
        cleared_metadata = {
            key: value
            for key, value in endpoint.metadata.items()
            if key != LANE_LAST_ERROR_METADATA_KEY
        }
        return queue.register_endpoint(endpoint.model_copy(update={"metadata": cleared_metadata}))
    return endpoint
