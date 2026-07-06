"""Shared relay operations for MCP, HTTP, and CLI surfaces."""

from __future__ import annotations

import base64
import os
import time
from pathlib import Path
from typing import Literal
from urllib.parse import unquote, urlparse

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError
from clio_relay.models import TERMINAL_STATES, Cursor, RelayJob
from clio_relay.spool import JobSpool


def wait_for_terminal(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    timeout_seconds: float,
    poll_seconds: float = 2.0,
) -> RelayJob:
    """Poll a job until it reaches a terminal state or timeout expires."""
    if timeout_seconds <= 0:
        raise ConfigurationError("timeout_seconds must be positive")
    if poll_seconds <= 0:
        raise ConfigurationError("poll_seconds must be positive")
    deadline = time.monotonic() + timeout_seconds
    while True:
        job = queue.get_job(job_id)
        if job.state in TERMINAL_STATES:
            return job
        if time.monotonic() >= deadline:
            raise TimeoutError(f"job did not reach terminal state before timeout: {job_id}")
        time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))


def monitor_job(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    cursor: int = 1,
    limit: int = 100,
) -> dict[str, object]:
    """Return job state plus event drain data from a cursor."""
    job = queue.get_job(job_id)
    events, next_cursor = queue.drain_events(Cursor(job_id=job_id, next_seq=cursor), limit=limit)
    return {
        "job": job.model_dump(mode="json"),
        "events": [event.model_dump(mode="json") for event in events],
        "next_cursor": next_cursor.next_seq,
        "terminal": job.state in TERMINAL_STATES,
    }


def read_job_log(
    settings: RelaySettings,
    job: RelayJob,
    *,
    stream_name: Literal["stdout", "stderr"],
    offset: int = 0,
    limit: int = 65536,
) -> dict[str, object]:
    """Read a cursor range from a job stdout/stderr log."""
    text, next_offset, eof = JobSpool(settings.spool_dir, job).read_log(
        stream_name,
        offset=offset,
        limit=limit,
    )
    return {
        "job_id": job.job_id,
        "stream": stream_name,
        "offset": offset,
        "next_offset": next_offset,
        "eof": eof,
        "text": text,
    }


def read_artifact_bytes(queue: ClioCoreQueue, artifact_id: str) -> dict[str, object]:
    """Read an artifact payload referenced by clio-core artifact metadata."""
    artifact = queue.get_artifact(artifact_id)
    path = _artifact_file_path(artifact.uri)
    data = path.read_bytes()
    return {
        "artifact": artifact.model_dump(mode="json"),
        "encoding": "base64",
        "data": base64.b64encode(data).decode("ascii"),
    }


def _artifact_file_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ConfigurationError(f"unsupported artifact URI scheme: {parsed.scheme}")
    path = unquote(parsed.path)
    if os.name == "nt" and len(path) >= 3 and path[0] == "/" and path[2] == ":":
        path = path[1:]
    return Path(path)
