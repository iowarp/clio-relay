"""A declined operation: the person said no, so relay did nothing, and that is no error.

The record ends ``cancelled`` with a ``declined`` refusal as its error, as it
always has. What a client sees is the typed outcome :func:`outcome` builds,
under ``declined``, with no ``error`` and no error result.
"""

from __future__ import annotations

from typing import Any

from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import declined_outcome

_NOT_DONE = {
    "run": "Relay submitted nothing, so the job did not run.",
    "install": "Relay installed nothing and submitted no install job.",
    "serve": "Relay started nothing, so the service did not start.",
}


def not_done(record: OperationRecord) -> str:
    """Say what relay did not do because the person declined ``record``'s ask."""
    return _NOT_DONE.get(str(record.kind), "Relay did nothing.")


def outcome(record: OperationRecord) -> dict[str, Any] | None:
    """The ``declined`` outcome of an operation the person declined, or None."""
    if record.state != OperationState.CANCELLED:
        return None
    return declined_outcome(
        record.error, operation_id=record.operation_id, not_done=not_done(record)
    )
