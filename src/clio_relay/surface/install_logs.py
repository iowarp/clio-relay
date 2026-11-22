"""Reading an install's log and typed issues on the site, wherever JARVIS ran it.

An install job writes ``install/install.log`` and ``install/issues.jsonl`` in its
execution; JARVIS's build on the login host (:func:`~clio_relay.jarvis_exits.login_build`)
writes them next to the pipeline's shared install record, and the start document names
that log.
"""

from __future__ import annotations

import logging
import posixpath
from typing import Any

from clio_relay import install_failures, jarvis_submit
from clio_relay.adapters.base import BaseAdapter
from clio_relay.jarvis_exits import built_here
from clio_relay.jobs.handles import JobHandle

logger = logging.getLogger(__name__)

LOG_TAIL_BYTES = 4096


def log_path(handle: JobHandle) -> str | None:
    login = built_here(handle)
    if login is not None:
        log = login.get("log")
        return log if isinstance(log, str) and log else None
    execution = handle.data.get("execution_dir")
    job_dir = handle.data.get("job_dir") or handle.data.get("root")
    if not isinstance(execution, str) or not execution:
        if not isinstance(job_dir, str) or not job_dir:
            return None
        execution_id, pipeline = jarvis_submit.handle_pipeline(handle.data)
        execution = jarvis_submit.execution_dir(job_dir, execution_id, pipeline=pipeline)
    return posixpath.join(execution, "install", "install.log")


async def log_tail(
    adapter: BaseAdapter[Any], handle: JobHandle, offset: int
) -> tuple[str | None, int]:
    """Return the install log written since ``offset`` (its last part), and the new offset."""
    path = log_path(handle)
    if path is None or "files" not in type(adapter).implements:
        return None, offset
    try:
        files = await adapter.files()
        size = (await files.stat(path)).size or 0
        if size <= offset:
            return None, offset
        start = max(offset, size - LOG_TAIL_BYTES)
        data = await files.read(path, offset=start, limit=size - start)
    except Exception:
        # The log is not there until the install job starts, and a missed read is no loss.
        logger.debug("reading the install log %s", path, exc_info=True)
        return None, offset
    return data.decode("utf-8", errors="replace"), size


async def site_text(adapter: BaseAdapter[Any], path: str) -> str | None:
    """Return a small text file on the site, or None when the adapter cannot read it."""
    if "files" not in type(adapter).implements:
        return None
    try:
        data = await (await adapter.files()).read(path)
    except Exception:
        logger.warning("reading %s on the site", path, exc_info=True)
        return None
    return data.decode("utf-8", errors="replace")


async def login_report(adapter: BaseAdapter[Any], login: dict[str, Any]) -> str | None:
    """Return what JARVIS's build on the login host printed (its ``+json`` report)."""
    where = login.get("dir")
    if not isinstance(where, str) or not where:
        return None
    return await site_text(adapter, posixpath.join(where, "report.json"))


async def job_issues(adapter: BaseAdapter[Any], handle: JobHandle) -> list[dict[str, Any]]:
    """Return the typed issues the install job wrote to ``install/issues.jsonl``."""
    log = log_path(handle)
    if log is None:
        return []
    text = await site_text(adapter, posixpath.join(posixpath.dirname(log), "issues.jsonl"))
    return install_failures.read_jsonl(text) if text else []
