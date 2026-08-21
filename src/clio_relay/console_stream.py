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
    LogStreamName,
    OwnedFileSizeLimitError,
    read_external_log_range,
    read_owned_regular_file_bytes,
)

CONSOLE_STREAM: Literal["console"] = "console"
#: clio-relay#259 residual: the application's stderr, ``console``'s sibling
#: channel -- see :data:`clio_relay.spool.LogStreamName`'s own docstring for
#: why this is neither merged into ``console`` nor aliased onto ``stderr``.
CONSOLE_STDERR_STREAM: Literal["console_stderr"] = "console_stderr"

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
class _ChannelTailState:
    """Per-channel mutable tail state (clio-relay#259 residual, stderr).

    stdout and stderr are tailed off the SAME locked execution root but are
    otherwise fully independent: each has its own byte offset, truncation
    flag, reason-dedup set, and self-throttle clock, so a stderr-only
    failure or quota trip can never disturb the stdout channel (or vice
    versa).
    """

    offset: int = 0
    truncated: bool = False
    reported_reasons: set[str] = field(default_factory=set[str])
    last_poll_monotonic: float | None = None


@dataclass
class ConsoleLiveTailer:
    """Poll-driven best-effort tail of one JARVIS execution's application output.

    Locks onto the first execution directory it finds under
    ``<shared_dir>/<pipeline_id>/executions`` and never switches once
    locked, bounding (not eliminating) exposure to a concurrently reused
    pipeline id picking up a sibling job's directory --
    :func:`flush_terminal_console` / :func:`flush_terminal_console_stderr`
    are the correctness guarantee regardless of what this best-effort half
    finds.

    Call :meth:`poll` (stdout) AND :meth:`poll_stderr` from the job's
    existing subprocess poll cadence (every tick is fine -- each
    self-throttles to :attr:`min_poll_interval_seconds` independently, so
    neither ever hammers the filesystem).
    """

    spool: JobSpool
    pipeline_id: str
    env: Mapping[str, str] | None = None
    min_poll_interval_seconds: float = CONSOLE_TAIL_MIN_POLL_INTERVAL_SECONDS
    execution_root: Path | None = field(default=None, init=False)
    _stdout: _ChannelTailState = field(default_factory=_ChannelTailState, init=False)
    _stderr: _ChannelTailState = field(default_factory=_ChannelTailState, init=False)

    @property
    def offset(self) -> int:
        """The STDOUT channel's byte offset (pre-#259-stderr public name)."""
        return self._stdout.offset

    @offset.setter
    def offset(self, value: int) -> None:
        self._stdout.offset = value

    @property
    def truncated(self) -> bool:
        """Whether the STDOUT channel has been truncated (pre-#259-stderr name)."""
        return self._stdout.truncated

    @truncated.setter
    def truncated(self, value: bool) -> None:
        self._stdout.truncated = value

    @property
    def stderr_offset(self) -> int:
        """The STDERR channel's own byte offset."""
        return self._stderr.offset

    @stderr_offset.setter
    def stderr_offset(self, value: int) -> None:
        self._stderr.offset = value

    @property
    def stderr_truncated(self) -> bool:
        """Whether the STDERR channel has been truncated."""
        return self._stderr.truncated

    @stderr_truncated.setter
    def stderr_truncated(self, value: bool) -> None:
        self._stderr.truncated = value

    def poll(self, *, now: float | None = None) -> ConsoleTailStep:
        """Advance the STDOUT tail by one increment; safe and cheap to call often.

        Never raises -- every failure mode returns a typed reason (reported
        once per distinct reason so the caller does not spam an event per
        poll tick); the job's own execution is never affected by a tailing
        failure.
        """
        return self._poll_channel(
            now=now,
            state=self._stdout,
            log_filename="stdout.log",
            stream=CONSOLE_STREAM,
            unreadable_reason="stdout_log_unreadable",
        )

    def poll_stderr(self, *, now: float | None = None) -> ConsoleTailStep:
        """Advance the STDERR tail by one increment (clio-relay#259 residual).

        Mirrors :meth:`poll` exactly -- same self-throttling, same typed/
        deduplicated failure reasons, same "never raises into the job"
        guarantee -- but for the application's ``stderr.log`` into its own
        :data:`CONSOLE_STDERR_STREAM` spool stream.
        """
        return self._poll_channel(
            now=now,
            state=self._stderr,
            log_filename="stderr.log",
            stream=CONSOLE_STDERR_STREAM,
            unreadable_reason="stderr_log_unreadable",
        )

    def _poll_channel(
        self,
        *,
        now: float | None,
        state: _ChannelTailState,
        log_filename: str,
        stream: LogStreamName,
        unreadable_reason: str,
    ) -> ConsoleTailStep:
        current = time.monotonic() if now is None else now
        if state.truncated:
            return ConsoleTailStep(appended=False, reason=None, message=None)
        if (
            state.last_poll_monotonic is not None
            and current - state.last_poll_monotonic < self.min_poll_interval_seconds
        ):
            return ConsoleTailStep(appended=False, reason=None, message=None)
        state.last_poll_monotonic = current
        if self.execution_root is None:
            try:
                shared_dir = resolve_jarvis_shared_dir(self.env)
                located = newest_execution_dir(shared_dir, self.pipeline_id)
            except ConsoleTailUnavailable as exc:
                return self._reason_step(state, exc.reason, str(exc))
            if located is None:
                return ConsoleTailStep(appended=False, reason=None, message=None)
            self.execution_root = located
        return self._tail_locked_execution(
            state,
            log_filename=log_filename,
            stream=stream,
            unreadable_reason=unreadable_reason,
        )

    def _tail_locked_execution(
        self,
        state: _ChannelTailState,
        *,
        log_filename: str,
        stream: LogStreamName,
        unreadable_reason: str,
    ) -> ConsoleTailStep:
        assert self.execution_root is not None
        source_path = self.execution_root / log_filename
        try:
            chunk, next_offset, _eof = read_external_log_range(
                source_path,
                offset=state.offset,
                limit=CONSOLE_TAIL_CHUNK_BYTES,
            )
        except (OSError, RuntimeError) as exc:
            return self._reason_step(state, unreadable_reason, f"{source_path}: {exc}")
        if not chunk:
            return ConsoleTailStep(appended=False, reason=None, message=None)
        state.offset = next_offset
        result = self.spool.append_log(stream, chunk)
        if result.truncated:
            state.truncated = True
        if result.truncation_event_required:
            self.spool.mark_truncation_event_recorded(stream)
            return ConsoleTailStep(
                appended=True,
                reason="truncated",
                message=(f"{stream} stream reached its {result.persisted_stream_bytes}-byte quota"),
            )
        return ConsoleTailStep(appended=True, reason=None, message=None)

    def _reason_step(
        self,
        state: _ChannelTailState,
        reason: str,
        message: str,
    ) -> ConsoleTailStep:
        if reason in state.reported_reasons:
            return ConsoleTailStep(appended=False, reason=None, message=None)
        state.reported_reasons.add(reason)
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


def _flush_terminal_channel(
    spool: JobSpool,
    result_document: dict[str, Any],
    *,
    log_filename: str,
    stream: LogStreamName,
    unreadable_reason: str,
    mismatch_reason: str,
    tailer_offset: int | None,
    tailer_execution_root: Path | None,
) -> ConsoleFlushOutcome:
    """Shared terminal-flush body :func:`flush_terminal_console` and
    :func:`flush_terminal_console_stderr` both call, one per channel.

    ``tailer_offset``/``tailer_execution_root`` are the ALREADY-RESOLVED
    per-channel tailer state (the caller reads ``tailer.offset``/
    ``tailer.execution_root`` for stdout, ``tailer.stderr_offset`` for
    stderr -- both channels share the one locked ``execution_root``) so
    this function stays channel-agnostic.
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
    source_path = execution_root / log_filename
    offset = 0
    reason: str | None = None
    message: str | None = None
    if tailer_execution_root is not None:
        if _same_directory(tailer_execution_root, execution_root):
            offset = tailer_offset or 0
        else:
            reason = mismatch_reason
            message = (
                f"live tail followed {tailer_execution_root} but the terminal "
                f"result named {execution_root}; re-flushing {stream} from the start"
            )
    appended_total = 0
    while True:
        try:
            chunk, next_offset, eof = read_external_log_range(
                source_path,
                offset=offset,
                limit=CONSOLE_TAIL_CHUNK_BYTES,
            )
        except (OSError, RuntimeError) as exc:
            if appended_total == 0 and reason is None:
                reason = unreadable_reason
                message = f"{source_path}: {exc}"
            break
        if not chunk:
            break
        result = spool.append_log(stream, chunk)
        appended_total += result.accepted_bytes
        offset = next_offset
        if result.truncation_event_required:
            spool.mark_truncation_event_recorded(stream)
            if reason is None:
                reason = "truncated"
                message = f"{stream} stream reached its {result.persisted_stream_bytes}-byte quota"
        if result.truncated or eof:
            break
    return ConsoleFlushOutcome(appended_bytes=appended_total, reason=reason, message=message)


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
    return _flush_terminal_channel(
        spool,
        result_document,
        log_filename="stdout.log",
        stream=CONSOLE_STREAM,
        unreadable_reason="stdout_log_unreadable",
        mismatch_reason="console_live_tail_execution_mismatch",
        tailer_offset=None if tailer is None else tailer.offset,
        tailer_execution_root=None if tailer is None else tailer.execution_root,
    )


def flush_terminal_console_stderr(
    spool: JobSpool,
    result_document: dict[str, Any],
    *,
    tailer: ConsoleLiveTailer | None = None,
) -> ConsoleFlushOutcome:
    """Guarantee the ``console_stderr`` stream carries the full application log.

    clio-relay#259 residual: the exact same terminal-flush guarantee
    :func:`flush_terminal_console` gives stdout, mirrored for the
    application's stderr -- same authoritative execution-root derivation,
    same tailer-offset reconciliation (using the tailer's OWN
    ``stderr_offset``/``stderr_truncated`` state, never the stdout one), same
    typed/non-fatal reasons.
    """
    return _flush_terminal_channel(
        spool,
        result_document,
        log_filename="stderr.log",
        stream=CONSOLE_STDERR_STREAM,
        unreadable_reason="stderr_log_unreadable",
        mismatch_reason="console_stderr_live_tail_execution_mismatch",
        tailer_offset=None if tailer is None else tailer.stderr_offset,
        tailer_execution_root=None if tailer is None else tailer.execution_root,
    )


def flush_terminal_console_from_path(
    spool: JobSpool,
    result_path: Path,
    *,
    tailer: ConsoleLiveTailer | None = None,
) -> ConsoleFlushOutcome:
    """Read one bounded terminal ``mcp-result.json`` and flush its console log."""
    document = _terminal_result_document(spool, result_path)
    if isinstance(document, ConsoleFlushOutcome):
        return document
    return flush_terminal_console(spool, document, tailer=tailer)


def flush_terminal_console_stderr_from_path(
    spool: JobSpool,
    result_path: Path,
    *,
    tailer: ConsoleLiveTailer | None = None,
) -> ConsoleFlushOutcome:
    """Read one bounded terminal ``mcp-result.json`` and flush its console_stderr log."""
    document = _terminal_result_document(spool, result_path)
    if isinstance(document, ConsoleFlushOutcome):
        return document
    return flush_terminal_console_stderr(spool, document, tailer=tailer)


def _terminal_result_document(
    spool: JobSpool,
    result_path: Path,
) -> dict[str, Any] | ConsoleFlushOutcome:
    """Read one bounded terminal ``mcp-result.json``, shared by both channels'
    ``_from_path`` entry points. Returns a typed :class:`ConsoleFlushOutcome`
    (never raises) when the result cannot be read/parsed -- the caller
    returns it directly.
    """
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
    return cast(dict[str, Any], document)
