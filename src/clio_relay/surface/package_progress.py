"""A package job's live step and outputs, from its JARVIS execution records (RL40, RL43).

Relay's package runner gives each job a private JARVIS root at
``<job dir>/.relay-jarvis``, and JARVIS keeps each execution's records under
``shared/<pipeline>/executions/<execution id>/``: ``progress/*.jsonl``, one
``jarvis.progress.v1`` line per step, and ``artifacts/*.jsonl``, one
``jarvis.artifact.v1`` line per change to an output. A package that follows the
clio step contract puts the step, the output it wrote and that output's view
descriptor under ``metadata.clio`` of both.

When an adapter's ``status`` reports no progress for a package job, relay reads
the tails of those files through the adapter's ``files`` verb. So every adapter
with ``files`` and a job directory reports the same step and outputs, with their
paths on the site and their view descriptors, while the job runs and after it ends.
"""

from __future__ import annotations

import asyncio
import json
import logging
import posixpath
from typing import Any, cast

from clio_relay.adapters.base import BaseAdapter
from clio_relay.jobs.handles import (
    VIEW_METADATA,
    FileAccess,
    JobHandle,
    JobProgress,
    JobStatus,
    OutputRef,
)
from clio_relay.jobs.records import OperationRecord

logger = logging.getLogger(__name__)

JARVIS_SHARED = ".relay-jarvis/shared"
"""Where the package runner's JARVIS keeps pipelines, under the job directory."""

TAIL_BYTES = 64 * 1024
"""How much of each record file relay reads: the latest lines are enough."""

EXECUTION = "execution"
"""The key in ``JobProgress.data`` naming the execution directory relay found."""

PROGRESS_SCHEMA = "jarvis.progress.v1"
ARTIFACT_SCHEMA = "jarvis.artifact.v1"


def job_dir(adapter: BaseAdapter[Any], handle: JobHandle) -> str | None:
    """Return the job's directory: the adapter's ``job_dir`` hook, or ``handle.data``."""
    hook = getattr(adapter, "job_dir", None)
    found: object = hook(handle) if callable(hook) else handle.data.get("job_dir")
    return found if isinstance(found, str) and found else None


def _dict(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return int(value)


def _name(path: str) -> str:
    return posixpath.basename(path.rstrip("/"))


async def _execution(files: FileAccess, directory: str) -> str | None:
    """Return the newest execution directory under the job's JARVIS root, if any."""
    shared = posixpath.join(directory, JARVIS_SHARED)
    newest: tuple[float, str] | None = None
    for pipeline in await files.list(shared):
        if pipeline.kind != "dir":
            continue
        executions = posixpath.join(shared, _name(pipeline.path), "executions")
        try:
            runs = await files.list(executions)
        except Exception:
            continue
        for run in runs:
            if run.kind != "dir":
                continue
            key = (run.modified.timestamp() if run.modified else 0.0, run.path)
            if newest is None or key > newest:
                newest = (key[0], posixpath.join(executions, _name(run.path)))
    return newest[1] if newest else None


async def _tail(files: FileAccess, path: str, size: int) -> list[dict[str, Any]]:
    offset = max(0, size - TAIL_BYTES)
    data = await files.read(path, offset=offset, limit=size - offset)
    lines = data.decode("utf-8", errors="replace").splitlines()
    if offset:
        lines = lines[1:]
    found: list[dict[str, Any]] = []
    for line in lines:
        try:
            item: object = json.loads(line)
        except ValueError:
            continue  # a line still being written
        if isinstance(item, dict):
            found.append(cast(dict[str, Any], item))
    return found


async def _records(files: FileAccess, directory: str) -> list[dict[str, Any]]:
    """Return the latest lines of every ``*.jsonl`` record file in ``directory``."""
    try:
        entries = await files.list(directory)
    except Exception:
        return []
    chosen = [
        entry for entry in entries if entry.kind == "file" and _name(entry.path).endswith(".jsonl")
    ]
    tails = await asyncio.gather(
        *(
            _tail(files, posixpath.join(directory, _name(entry.path)), entry.size or 0)
            for entry in chosen
        )
    )
    return [line for tail in tails for line in tail]


def _view_output(name: object, view: object) -> OutputRef | None:
    shown = _dict(view)
    path = shown.get("path")
    if not isinstance(path, str) or not path:
        return None
    return OutputRef(
        name=str(name or shown.get("name") or _name(path)),
        path=path,
        metadata={VIEW_METADATA: shown},
    )


def latest_step(lines: list[dict[str, Any]], execution: str) -> JobProgress | None:
    """Return the latest step the progress records name."""
    steps = [line for line in lines if line.get("schema_version") == PROGRESS_SCHEMA]
    if not steps:
        return None
    latest = max(
        steps, key=lambda line: (line.get("observed_at_epoch") or 0, line.get("sequence") or 0)
    )
    clio = _dict(_dict(latest.get("metadata")).get("clio"))
    output = _view_output(clio.get("output"), clio.get("view"))
    data: dict[str, Any] = {
        EXECUTION: execution,
        "state": latest.get("state"),
        "unit": latest.get("unit"),
        "package": latest.get("package_name"),
    }
    if clio.get("visible_steps") is not None:
        data["visible_steps"] = clio["visible_steps"]
    message = latest.get("message")
    return JobProgress(
        step=_int(clio.get("step", latest.get("current"))),
        total=_int(latest.get("total")),
        message=message if isinstance(message, str) else None,
        outputs=[output] if output is not None else [],
        data={key: value for key, value in data.items() if value is not None},
    )


def artifact_outputs(lines: list[dict[str, Any]]) -> list[OutputRef]:
    """Return each artifact's latest state as an output, with its view descriptor."""
    latest: dict[str, dict[str, Any]] = {}
    for line in lines:
        if line.get("schema_version") == ARTIFACT_SCHEMA and line.get("artifact_id"):
            latest[str(line["artifact_id"])] = line
    outputs: list[OutputRef] = []
    for item in latest.values():
        path = _dict(item.get("location")).get("value")
        if not isinstance(path, str) or not path:
            continue
        metadata: dict[str, Any] = {
            key: item[key] for key in ("state", "format", "media_type") if item.get(key)
        }
        view = _dict(_dict(item.get("metadata")).get("clio")).get("view")
        if isinstance(view, dict):
            metadata[VIEW_METADATA] = view
        outputs.append(
            OutputRef(
                name=str(item.get("logical_name") or item["artifact_id"]),
                path=path,
                size=_int(item.get("size_bytes")),
                metadata=metadata,
            )
        )
    return sorted(outputs, key=lambda output: output.name)


def _known_execution(record: OperationRecord) -> str | None:
    found = _dict(_dict(_dict(record.result).get("progress")).get("data")).get(EXECUTION)
    return found if isinstance(found, str) and found else None


async def with_package_progress(
    adapter: BaseAdapter[Any], record: OperationRecord, status: JobStatus
) -> JobStatus:
    """Add a package job's latest step and outputs to ``status`` when it has none.

    Reading is best effort: a site that cannot be read now leaves ``status``
    as the adapter reported it.
    """
    if status.progress is not None or record.kind != "run" or record.handle is None:
        return status
    if not isinstance(record.spec.get("package"), dict):
        return status
    if "files" not in type(adapter).implements:
        return status
    directory = job_dir(adapter, record.handle)
    if directory is None:
        return status
    try:
        files = await adapter.files()
        execution = _known_execution(record) or await _execution(files, directory)
        if execution is None:
            return status
        steps, artifacts = await asyncio.gather(
            _records(files, posixpath.join(execution, "progress")),
            _records(files, posixpath.join(execution, "artifacts")),
        )
    except Exception as exc:
        logger.debug("reading the package records of %s: %s", record.operation_id, exc)
        return status
    taken = {output.name for output in status.outputs}
    added = [output for output in artifact_outputs(artifacts) if output.name not in taken]
    update: dict[str, Any] = {"outputs": [*status.outputs, *added]}
    progress = latest_step(steps, execution)
    if progress is not None:
        update["progress"] = progress
    return status.model_copy(update=update)
