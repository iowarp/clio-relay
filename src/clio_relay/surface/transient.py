"""Transient inputs never outlive their job (RL16).

When an operation ends, relay drops from its own content store each input the
spec marks ``transient``, unless another open operation still lists the same
bytes. The adapter drops the site's copies; this is the relay host's side.
"""

from __future__ import annotations

import logging

from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.records import OperationRecord, RecordStore
from clio_relay.jobs.spec import JobSpec, ServiceSpec

logger = logging.getLogger(__name__)


def transient_digests(record: OperationRecord) -> set[str]:
    """Return the digests of the inputs an operation's spec marks transient."""
    if record.kind == "run":
        job = JobSpec.model_validate(record.spec)
    elif record.kind == "serve":
        job = ServiceSpec.model_validate(record.spec).job
    else:
        return set()
    return {item.sha256 for item in job.inputs if item.transient}


def drop_transient(
    content: ContentStore, operations: RecordStore[OperationRecord], record: OperationRecord
) -> list[str]:
    """Drop an ended operation's transient inputs from the store. Return what was dropped."""
    if not record.state.final:
        return []
    digests = transient_digests(record)
    if not digests:
        return []
    for other in operations.all():
        if other.operation_id != record.operation_id and not other.state.final:
            digests -= set(other.input_digests)
    dropped: list[str] = []
    for digest in sorted(digests):
        try:
            if content.drop(digest):
                dropped.append(digest)
        except OSError:
            logger.warning("dropping transient input %s", digest, exc_info=True)
    return dropped
