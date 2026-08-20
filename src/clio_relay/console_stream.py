"""Console (application stdout/stdlog) stream for JARVIS-backed mcp_call jobs.

clio-relay#259's pull-side substrate. An mcp_call job's own ``stdout``/
``stderr`` spool streams carry the MCP jsonrpc wire exchanged with the
launched MCP server subprocess -- verified live to contain zero application
output. The application's own stdout (LAMMPS thermo lines, etc.) is instead
collected by JARVIS-CD into ``<execution_root>/stdout.log`` on the local
filesystem and never enters relay on its own. This module feeds a THIRD job
log stream, ``console``, so ``GET /jobs/{id}/logs/console`` (the same door
that already serves ``stdout``/``stderr``, see :mod:`clio_relay.http_api`)
can serve that application output.

Two halves, deliberately asymmetric in how much they can guarantee:

* :func:`flush_terminal_console` / :func:`flush_terminal_console_from_path`
  -- THE GUARANTEE. Once a terminal ``jarvis_run`` result is available, the
  execution root is authoritative (declared in the result's own
  ``execution_record.metadata``, the exact derivation
  :func:`clio_relay.jarvis_execution_artifacts.execution_root_from_record`
  already trusts for execution-output artifacts). This always succeeds at
  making ``console`` complete, or reports a typed reason -- it never fails
  the job.
* :class:`ConsoleLiveTailer` -- the demo-critical, best-effort half. While
  the ``jarvis_run`` call is in flight, the execution root is not in any
  result yet, so it is derived from the call's ``pipeline_id`` plus the
  JARVIS shared-data root (mirroring ``jarvis_cd.core.config.Jarvis`` and
  ``jarvis_cd.core.pipeline.Pipeline``'s own on-disk layout:
  ``<shared_dir>/<pipeline_id>/executions/<execution_id>``) and picking the
  newest execution directory. This is a heuristic -- a pipeline id reused by
  two concurrent jobs could momentarily pick a sibling job's directory -- so
  it locks onto the first directory it finds and never switches, and the
  terminal flush above reconciles (and reports a typed mismatch reason) if
  its guess turns out wrong.

Every failure mode in both halves is a typed, deduplicated reason the caller
logs through the job's own event stream (:mod:`clio_relay.endpoint`) --
never a silent skip, and never a reason the job's own execution can fail on.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

import yaml

from clio_relay.jarvis_execution_artifacts import (
    MAX_EXECUTION_RESULT_BYTES,
    execution_root_from_record,
)
from clio_relay.models import McpCallSpec
from clio_relay.spool import (
    MAX_LOG_READ_BYTES,
    JobSpool,
    OwnedFileSizeLimitError,
    read_external_log_range,
    read_owned_regular_file_bytes,
)

CONSOLE_STREAM: Literal["console"] = "console"

#: Env var jarvis-cd's own ``Jarvis`` singleton reads before defaulting to
#: ``~/.ppi-jarvis`` (``jarvis_cd.core.config._JARVIS_ROOT_ENVIRONMENT``).
#: The mcp_call jarvis MCP server subprocess always runs on this same worker
#: host (never over a scheduler hop), so resolving it the same way here
#: matches what that child process itself resolves.
JARVIS_ROOT_ENV = "JARVIS_ROOT"
DEFAULT_JARVIS_ROOT_DIRNAME = ".ppi-jarvis"

CONSOLE_TAIL_CHUNK_BYTES = MAX_LOG_READ_BYTES
CONSOLE_TAIL_MIN_POLL_INTERVAL_SECONDS = 2.0


class ConsoleTailUnavailable(RuntimeError):
    """A typed, non-fatal reason console tailing could not proceed.

    Always caught by the caller (:class:`ConsoleLiveTailer`,
    :func:`flush_terminal_console`) -- this never escapes to fail a job.
    """

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class ConsoleTailStep:
    """One :meth:`ConsoleLiveTailer.poll` outcome."""

    appended: bool
    reason: str | None
    message: str | None


@dataclass(frozen=True, slots=True)
class ConsoleFlushOutcome:
    """Terminal-flush result for a job's ``console`` stream."""

    appended_bytes: int
    reason: str | None
    message: str | None


def resolve_jarvis_shared_dir(env: Mapping[str, str] | None = None) -> Path:
    """Resolve JARVIS's shared-data root the way jarvis-cd's own singleton does.

    ``$JARVIS_ROOT`` if set, else ``~/.ppi-jarvis``
    (``jarvis_cd.core.config.Jarvis.__init__``), then the ``shared_dir`` key
    declared in that root's ``jarvis_config.yaml``. clio-relay's own
    bootstrap requires ``config_dir``/``private_dir``/``shared_dir`` to be
    existing absolute directories before a deployment activates (see
    ``bootstrap.py``'s JARVIS root probe), so a deployed relay can trust that
    key is present -- this raises a typed reason rather than guessing a
    default when it is not.

    Raises:
        ConsoleTailUnavailable: The configuration file is missing, unreadable,
            not a mapping, or omits ``shared_dir``.
    """
    environment = os.environ if env is None else env
    configured = (environment.get(JARVIS_ROOT_ENV) or "").strip()
    jarvis_root = (
        Path(configured).expanduser() if configured else Path.home() / DEFAULT_JARVIS_ROOT_DIRNAME
    )
    config_path = jarvis_root / "jarvis_config.yaml"
    if not config_path.is_file():
        raise ConsoleTailUnavailable(
            "jarvis_config_missing",
            f"JARVIS configuration was not found: {config_path}",
        )
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ConsoleTailUnavailable(
            "jarvis_config_unreadable",
            f"JARVIS configuration could not be read: {config_path}: {exc}",
        ) from exc
    if not isinstance(raw, dict):
        raise ConsoleTailUnavailable(
            "jarvis_config_invalid",
            f"JARVIS configuration was not a mapping: {config_path}",
        )
    shared_dir = cast(dict[str, object], raw).get("shared_dir")
    if not isinstance(shared_dir, str) or not shared_dir:
        raise ConsoleTailUnavailable(
            "jarvis_shared_dir_undeclared",
            f"JARVIS configuration omitted shared_dir: {config_path}",
        )
    return Path(shared_dir).expanduser()


def _validated_pipeline_directory_component(pipeline_id: str) -> str:
    """Reject a pipeline id that is not one safe filesystem path component."""
    if (
        not pipeline_id
        or pipeline_id in {".", ".."}
        or "/" in pipeline_id
        or "\\" in pipeline_id
        or any(ord(character) < 32 or ord(character) == 127 for character in pipeline_id)
    ):
        raise ConsoleTailUnavailable(
            "pipeline_id_invalid",
            f"pipeline_id is not one safe path component: {pipeline_id!r}",
        )
    return pipeline_id


def newest_execution_dir(shared_dir: Path, pipeline_id: str) -> Path | None:
    """Return the most recently created execution directory for one pipeline.

    Mirrors jarvis-cd's own on-disk layout --
    ``<shared_dir>/<pipeline_id>/executions/<execution_id>``; see upstream
    ``jarvis_cd.core.config.Jarvis.get_pipeline_shared_dir`` and
    ``jarvis_cd.core.pipeline.Pipeline._execution_store``.

    Returns ``None`` when no execution directory exists yet -- the ordinary
    pre-creation race right after a job starts, not a failure the caller
    should report; it simply retries on the next poll.

    Raises:
        ConsoleTailUnavailable: ``pipeline_id`` is unsafe, or the executions
            directory exists but could not be listed (permissions, etc).
    """
    safe_pipeline_id = _validated_pipeline_directory_component(pipeline_id)
    executions_dir = shared_dir / safe_pipeline_id / "executions"
    try:
        candidates = [entry for entry in executions_dir.iterdir() if entry.is_dir()]
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ConsoleTailUnavailable(
            "executions_dir_unavailable",
            f"JARVIS executions directory could not be listed: {executions_dir}: {exc}",
        ) from exc
    if not candidates:
        return None
    return max(candidates, key=lambda entry: (entry.stat().st_mtime_ns, entry.name))


@dataclass
class ConsoleLiveTailer:
    """Poll-driven best-effort tail of one JARVIS execution's ``stdout.log``.

    Locks onto the first execution directory it finds under
    ``<shared_dir>/<pipeline_id>/executions`` and never switches once
    locked, bounding (not eliminating) exposure to a concurrently reused
    pipeline id picking up a sibling job's directory --
    :func:`flush_terminal_console` is the correctness guarantee regardless
    of what this best-effort half finds.

    Call :meth:`poll` from the job's existing subprocess poll cadence (every
    tick is fine -- this self-throttles to :attr:`min_poll_interval_seconds`
    internally, so it never hammers the filesystem).
    """

    spool: JobSpool
    pipeline_id: str
    env: Mapping[str, str] | None = None
    min_poll_interval_seconds: float = CONSOLE_TAIL_MIN_POLL_INTERVAL_SECONDS
    execution_root: Path | None = field(default=None, init=False)
    offset: int = field(default=0, init=False)
    truncated: bool = field(default=False, init=False)
    _reported_reasons: set[str] = field(default_factory=set[str], init=False)
    _last_poll_monotonic: float | None = field(default=None, init=False)

    def poll(self, *, now: float | None = None) -> ConsoleTailStep:
        """Advance the tail by one increment; safe and cheap to call often.

        Never raises -- every failure mode returns a typed reason (reported
        once per distinct reason so the caller does not spam an event per
        poll tick); the job's own execution is never affected by a tailing
        failure.
        """
        current = time.monotonic() if now is None else now
        if self.truncated:
            return ConsoleTailStep(appended=False, reason=None, message=None)
        if (
            self._last_poll_monotonic is not None
            and current - self._last_poll_monotonic < self.min_poll_interval_seconds
        ):
            return ConsoleTailStep(appended=False, reason=None, message=None)
        self._last_poll_monotonic = current
        if self.execution_root is None:
            try:
                shared_dir = resolve_jarvis_shared_dir(self.env)
                located = newest_execution_dir(shared_dir, self.pipeline_id)
            except ConsoleTailUnavailable as exc:
                return self._reason_step(exc.reason, str(exc))
            if located is None:
                return ConsoleTailStep(appended=False, reason=None, message=None)
            self.execution_root = located
        return self._tail_locked_execution()

    def _tail_locked_execution(self) -> ConsoleTailStep:
        assert self.execution_root is not None
        stdout_path = self.execution_root / "stdout.log"
        try:
            chunk, next_offset, _eof = read_external_log_range(
                stdout_path,
                offset=self.offset,
                limit=CONSOLE_TAIL_CHUNK_BYTES,
            )
        except (OSError, RuntimeError) as exc:
            return self._reason_step("stdout_log_unreadable", f"{stdout_path}: {exc}")
        if not chunk:
            return ConsoleTailStep(appended=False, reason=None, message=None)
        self.offset = next_offset
        result = self.spool.append_log(CONSOLE_STREAM, chunk)
        if result.truncated:
            self.truncated = True
        if result.truncation_event_required:
            self.spool.mark_truncation_event_recorded(CONSOLE_STREAM)
            return ConsoleTailStep(
                appended=True,
                reason="truncated",
                message=(f"console stream reached its {result.persisted_stream_bytes}-byte quota"),
            )
        return ConsoleTailStep(appended=True, reason=None, message=None)

    def _reason_step(self, reason: str, message: str) -> ConsoleTailStep:
        if reason in self._reported_reasons:
            return ConsoleTailStep(appended=False, reason=None, message=None)
        self._reported_reasons.add(reason)
        return ConsoleTailStep(appended=False, reason=reason, message=message)


def console_tailer_for_mcp_call(
    spec: McpCallSpec,
    *,
    spool: JobSpool,
) -> ConsoleLiveTailer | None:
    """Return a live console tailer for a ``jarvis_run`` mcp_call, else ``None``.

    Console tailing only applies to the ``jarvis_run`` tool: every other
    mcp_call tool (``jarvis_get_execution``, ``tools/list``, ...) has no
    application subprocess of its own, so its ``console`` stream simply
    stays empty (point 2c of #259 -- an empty 200 envelope with ``eof``,
    never a 500).
    """
    if spec.tool != "jarvis_run":
        return None
    pipeline_id = spec.arguments.get("pipeline_id")
    if not isinstance(pipeline_id, str) or not pipeline_id:
        return None
    return ConsoleLiveTailer(spool=spool, pipeline_id=pipeline_id)


def _same_directory(a: Path, b: Path) -> bool:
    try:
        return a.resolve() == b.resolve()
    except OSError:
        return False


def flush_terminal_console(
    spool: JobSpool,
    result_document: dict[str, Any],
    *,
    tailer: ConsoleLiveTailer | None = None,
) -> ConsoleFlushOutcome:
    """Guarantee the ``console`` stream carries the full application log.

    Reuses the exact execution-root derivation
    :func:`clio_relay.jarvis_execution_artifacts.execution_root_from_record`
    already trusts for execution-output artifacts: the terminal
    ``execution_record.metadata`` declares ``pipeline_snapshot_path`` or
    ``script_path``, whose parent directory is the execution root. This is
    authoritative -- unlike the live tailer's "pick the newest execution
    directory" heuristic, it never fails to bind to the correct execution.

    When ``tailer`` already tailed the SAME execution root, only the
    remainder past its offset is appended (no duplicated bytes). When it
    tailed a DIFFERENT root (the rare concurrent-pipeline-reuse case), or
    never locked onto one at all, the full log is flushed from the start and
    a typed mismatch reason is reported so the discrepancy is visible.
    """
    structured = result_document.get("structured_result")
    if not isinstance(structured, dict):
        return ConsoleFlushOutcome(appended_bytes=0, reason=None, message=None)
    record = cast(dict[str, Any], structured).get("execution_record")
    if not isinstance(record, dict):
        return ConsoleFlushOutcome(appended_bytes=0, reason=None, message=None)
    execution_root = execution_root_from_record(cast(dict[str, Any], record))
    if execution_root is None:
        return ConsoleFlushOutcome(
            appended_bytes=0,
            reason="execution_root_undeclared",
            message="terminal JARVIS result omitted an execution root",
        )
    stdout_path = execution_root / "stdout.log"
    offset = 0
    reason: str | None = None
    message: str | None = None
    if tailer is not None and tailer.execution_root is not None:
        if _same_directory(tailer.execution_root, execution_root):
            offset = tailer.offset
        else:
            reason = "console_live_tail_execution_mismatch"
            message = (
                f"live tail followed {tailer.execution_root} but the terminal "
                f"result named {execution_root}; re-flushing console from the start"
            )
    appended_total = 0
    while True:
        try:
            chunk, next_offset, eof = read_external_log_range(
                stdout_path,
                offset=offset,
                limit=CONSOLE_TAIL_CHUNK_BYTES,
            )
        except (OSError, RuntimeError) as exc:
            if appended_total == 0 and reason is None:
                reason = "stdout_log_unreadable"
                message = f"{stdout_path}: {exc}"
            break
        if not chunk:
            break
        result = spool.append_log(CONSOLE_STREAM, chunk)
        appended_total += result.accepted_bytes
        offset = next_offset
        if result.truncation_event_required:
            spool.mark_truncation_event_recorded(CONSOLE_STREAM)
            if reason is None:
                reason = "truncated"
                message = f"console stream reached its {result.persisted_stream_bytes}-byte quota"
        if result.truncated or eof:
            break
    return ConsoleFlushOutcome(appended_bytes=appended_total, reason=reason, message=message)


def flush_terminal_console_from_path(
    spool: JobSpool,
    result_path: Path,
    *,
    tailer: ConsoleLiveTailer | None = None,
) -> ConsoleFlushOutcome:
    """Read one bounded terminal ``mcp-result.json`` and flush its console log."""
    try:
        snapshot = read_owned_regular_file_bytes(
            result_path,
            owned_root=spool.path,
            max_bytes=MAX_EXECUTION_RESULT_BYTES,
        )
        if snapshot.data is None:
            raise RuntimeError("terminal JARVIS result bytes were unavailable")
        document = json.loads(snapshot.data.decode("utf-8"))
    except (
        OSError,
        OwnedFileSizeLimitError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        RuntimeError,
    ) as exc:
        return ConsoleFlushOutcome(
            appended_bytes=0,
            reason="mcp_result_unreadable",
            message=f"terminal JARVIS result could not be read for console flush: {exc}",
        )
    if not isinstance(document, dict):
        return ConsoleFlushOutcome(
            appended_bytes=0,
            reason="mcp_result_invalid",
            message="terminal JARVIS result must be a JSON object",
        )
    return flush_terminal_console(spool, cast(dict[str, Any], document), tailer=tailer)
