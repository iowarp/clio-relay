"""JARVIS's execution record metadata, read for relay: install progress and the job start.

Split out of :mod:`clio_relay.jarvis_submit`, which builds the commands and maps the
record's state. This module depends on no other JARVIS module.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, cast

from clio_relay.jobs.handles import JobProgress


def _dict(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return int(value)


WAITING_ENV_LOCK = "waiting_env_lock"
"""The install phase while another install holds the shared Spack environment's lock."""
BUILD_REFUSED_PHASE = "refused"
"""The install phase when JARVIS refused to build outside an allocation (exit 4)."""


def install_progress(record: Mapping[str, Any]) -> JobProgress | None:
    """Return an install job's ``metadata.install`` (``jarvis.install.progress.v1``) as progress."""
    metadata = _dict(record.get("metadata"))
    if metadata.get("action") != "install":
        return None
    install = _dict(metadata.get("install"))
    if not install:
        return JobProgress(message="installing: waiting for the install job to start")
    phase = str(install.get("phase") or "installing")
    current = install.get("current_package")
    if phase == WAITING_ENV_LOCK:
        message = (
            "waiting for another install into the same Spack environment; "
            "it then reuses that install"
        )
    elif phase == BUILD_REFUSED_PHASE:
        message = "refused: JARVIS would not build outside an allocation; relay does not retry"
    elif isinstance(current, str) and current:
        message = f"{phase}: {current}"
    else:
        message = phase
    data = {
        key: install[key]
        for key in ("phase", "missing", "issue_count", "error", "log", "refused")
        if install.get(key) is not None
    }
    return JobProgress(
        step=_int(install.get("packages_done")),
        total=_int(install.get("packages_total")),
        message=message,
        data=data,
    )


def scheduler_start(record: Mapping[str, Any], job_id: str | None) -> datetime | None:
    """When the scheduler started job ``job_id``, as JARVIS recorded it inside the allocation.

    JARVIS writes ``metadata.scheduler_job`` once per execution (``provider``,
    ``job_id``, ``started_at``, ``source``), so a start recorded for another job
    of the same execution, such as its install job, is not this job's.
    """
    found = _dict(_dict(record.get("metadata")).get("scheduler_job"))
    at = found.get("started_at")
    if not isinstance(at, str) or job_id is None or str(found.get("job_id")) != job_id:
        return None
    try:
        started = datetime.fromisoformat(at.replace("Z", "+00:00"))
    except ValueError:
        return None
    return started if started.tzinfo else started.replace(tzinfo=UTC)
