"""What relay adds to a job's outputs before it stores them (RL43).

Each output gets relay's provenance block under ``metadata["provenance"]``
unless it brought its own. A view descriptor under ``metadata["view"]`` is
checked against the shared schema; one that does not match is taken off the
output, and the reasons go under ``metadata["view_problems"]`` so a client
can tell why the output has no view. :func:`stdout_tail` is the end of a
finished job's standard output, which relay keeps with the operation.
"""

from __future__ import annotations

import logging
from typing import Any

from clio_relay.adapters.base import BaseAdapter
from clio_relay.jobs.handles import VIEW_METADATA, JobProgress, JobStatus, OutputRef
from clio_relay.jobs.records import OperationRecord
from clio_relay.serve.descriptor import problems
from clio_relay.serve.provenance import with_provenance

logger = logging.getLogger(__name__)

VIEW_PROBLEMS_METADATA = "view_problems"
OUTPUT_TAIL_BYTES = 8 * 1024


def checked_views(outputs: list[OutputRef]) -> list[OutputRef]:
    """Return the outputs with every view descriptor that fails the schema removed."""
    checked: list[OutputRef] = []
    for output in outputs:
        if VIEW_METADATA not in output.metadata:
            checked.append(output)
            continue
        found = problems(output.metadata[VIEW_METADATA])
        if not found:
            checked.append(output)
            continue
        logger.warning("dropping the view descriptor of %s: %s", output.name, "; ".join(found))
        metadata: dict[str, Any] = {
            key: value for key, value in output.metadata.items() if key != VIEW_METADATA
        }
        metadata[VIEW_PROBLEMS_METADATA] = found
        checked.append(output.model_copy(update={"metadata": metadata}))
    return checked


def marked_outputs(record: OperationRecord, outputs: list[OutputRef]) -> list[OutputRef]:
    """Return outputs with checked views and relay's provenance block."""
    return with_provenance(record, checked_views(outputs))


def marked_status(record: OperationRecord, status: JobStatus) -> JobStatus:
    """Return the status with its outputs and its progress outputs marked."""
    update: dict[str, Any] = {"outputs": marked_outputs(record, status.outputs)}
    if status.progress is not None:
        progress: JobProgress = status.progress
        update["progress"] = progress.model_copy(
            update={"outputs": marked_outputs(record, progress.outputs)}
        )
    return status.model_copy(update=update)


async def stdout_tail(adapter: BaseAdapter[Any], status: JobStatus) -> str | None:
    """Return the end of the job's standard output, if the adapter can read it."""
    if "files" not in type(adapter).implements:
        return None
    output = next((item for item in status.outputs if item.name.startswith("stdout")), None)
    if output is None:
        output = next((item for item in status.outputs if item.name.endswith(".out")), None)
    if output is None:
        return None
    try:
        files = await adapter.files()
        size = output.size if output.size is not None else (await files.stat(output.path)).size
        offset = max(0, (size or 0) - OUTPUT_TAIL_BYTES)
        data = await files.read(output.path, offset=offset, limit=OUTPUT_TAIL_BYTES)
    except Exception:
        # The job has ended either way; a tail it cannot read must not hold that back.
        logger.warning("reading the output tail of %s", output.path, exc_info=True)
        return None
    return data.decode("utf-8", errors="replace")
