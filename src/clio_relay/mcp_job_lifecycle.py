"""Job cancel/observe/wait MCP tools: cancelling a job, observing it until
a pattern or terminal state, and the single-call bounded reconciliation
`relay_wait` performs.

Split out of mcp_job_lifecycle.py's own first pass (iowarp/clio-relay#231)
-- that pass landed at 813 lines as a single file, over the 800-line
ratchet cap, so it split along its own seam: this is the mutating/
bounded-reconciliation half; mcp_job_status.py holds the read-only
status/lineage half (`_status_job`/`_used_artifacts_tool`/`_used_by_tool`).
`_job_target`/`_require_local_job_cluster`, the shared cluster-target
resolver both halves use, stayed in mcp_job_status.py -- neither is
monkeypatched, so this module's plain cross-module import of them is a
normal one-directional leaf dependency, not a back-reference.

Every function here that branches to a remote or owned-session path calls
at least one of four names tests monkeypatch at `mcp_server_module.<name>`
(`should_execute_on_cluster`, `OwnedSessionApiClient`, `_remote_json`,
`run_remote_clio`); `_wait_job` alone calls twelve such names, including
several defined or monkeypatched elsewhere (`_job_logs`,
`_complete_local_artifacts`/`_verified_local_mcp_result`/
`_complete_remote_collection`/`_verified_mcp_result`/
`_complete_owned_collection`/`_verified_owned_mcp_result`/
`wait_for_terminal`). Every one of those call sites goes through the
function-scope `_mcp_server.<name>(...)` back-reference established in
slices 3-7. To avoid a transcription error across the ~30+ individual call
sites this half has, the extraction script that produced it
(extract_job_lifecycle.py) found and rewrote every one of them with
Python's own `ast` module (exact line/column spans), not a hand-written
per-function list or a regex over the source text.
"""

from __future__ import annotations

import re
from contextlib import suppress
from typing import Any, cast

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ObservationTimeoutError
from clio_relay.mcp_arguments import (
    _attach_wait_observation,
    _log_limit,
    _object,
    _observation_timeout_seconds,
    _optional_str,
    _required_durable_record_id,
    _response_page_limit,
)
from clio_relay.mcp_job_status import _job_target, _require_local_job_cluster
from clio_relay.mcp_remote_catalog import _route_revision
from clio_relay.mcp_remote_transport import (
    _owned_job_logs,
    _owned_json,
    _remote_job_logs,
    _validate_owned_job_status,
)
from clio_relay.mcp_result_verification import (
    _attach_terminal_mcp_evidence,
    _owned_mcp_result_is_required,
)
from clio_relay.models import TERMINAL_STATES, JobWaitResult, RelayJob
from clio_relay.observation import (
    MAX_OBSERVATION_SCAN_BYTES,
    compile_observation_pattern,
    normalize_pattern_scope,
    observe_until_pattern,
    observe_until_pattern_snapshots,
)
from clio_relay.queue_management import cancel_queue_job
from clio_relay.relay_ops import job_status, monitor_job, read_job_log
from clio_relay.remote_cli import remote_command_timeout
from clio_relay.session_api import OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS
from clio_relay.spool import CONSOLE_OBSERVE_TAIL_LIMIT_BYTES, LOG_STREAM_NAMES

JSON = dict[str, Any]

MAX_OBSERVE_MATCHES = 100
MAX_OBSERVE_MATCH_TEXT_CHARS = 1_024
REMOTE_WAIT_STATUS_TIMEOUT_SECONDS = 30.0


def _cancel_job(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    from clio_relay import mcp_server as _mcp_server

    target = _job_target(arguments)
    if target is not None and _mcp_server.should_execute_on_cluster(target):
        job_id = _required_durable_record_id(arguments, "job_id")
        cancel_scheduler = arguments.get("cancel_scheduler_job") is True
        if settings.owner_session_id is not None:
            with _mcp_server.OwnedSessionApiClient(definition=target, settings=settings) as client:
                result = _owned_json(
                    client,
                    method="POST",
                    path=f"/queue/jobs/{job_id}/cancel",
                    body={
                        "cluster": target.name,
                        "cancel_scheduler_job": cancel_scheduler,
                    },
                    label="owned remote job cancellation",
                )
            _validate_owned_job_status(result, job_id=job_id, cluster=target.name)
            job = _object(result["job"])
            result["job_id"] = job["job_id"]
            result["state"] = job["state"]
            result["cluster"] = target.name
            result["route_revision"] = _route_revision(target)
            return result
        command = ["job", "cancel", job_id]
        if cancel_scheduler:
            command.append("--cancel-scheduler-job")
        _mcp_server.run_remote_clio(target, command)
        result = _mcp_server._remote_json(target, ["job", "status", job_id], "remote job status")
        result["cancel_requested"] = True
        result["scheduler_policy"] = "request-scheduler" if cancel_scheduler else "relay-only"
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
        return result
    _require_local_job_cluster(
        queue,
        _required_durable_record_id(arguments, "job_id"),
        target,
    )
    result = cancel_queue_job(
        queue,
        _required_durable_record_id(arguments, "job_id"),
        scheduler_policy=(
            "request-scheduler" if arguments.get("cancel_scheduler_job") is True else "relay-only"
        ),
    )
    job = _object(result["job"])
    response: JSON = {
        **result,
        "job_id": job["job_id"],
        "state": job["state"],
    }
    if target is not None:
        response["cluster"] = target.name
        response["route_revision"] = _route_revision(target)
    return response


def _observe_job(arguments: JSON, *, queue: ClioCoreQueue, settings: RelaySettings) -> JSON:
    from clio_relay import mcp_server as _mcp_server

    job_id = _required_durable_record_id(arguments, "job_id")
    cursor = int(arguments.get("cursor", 1))
    limit = _response_page_limit(arguments)
    target = _job_target(arguments)
    until_pattern = _optional_str(arguments, "until_pattern")
    if until_pattern is not None:
        if arguments.get("pattern") is not None:
            raise ValueError("pattern and until_pattern cannot be used together")
        compiled = compile_observation_pattern(until_pattern)
        scopes = normalize_pattern_scope(arguments.get("pattern_scope"))
        if target is not None and _mcp_server.should_execute_on_cluster(target):
            result = _observe_remote_pattern(
                target,
                settings=settings,
                job_id=job_id,
                compiled=compiled,
                scopes=scopes,
                cursor=cursor,
                limit=limit,
                include_logs=arguments.get("include_logs", True) is not False,
                log_limit=_log_limit(arguments),
            )
            result["cluster"] = target.name
            result["route_revision"] = _route_revision(target)
            return result
        _require_local_job_cluster(queue, job_id, target)
        result = observe_until_pattern(
            queue,
            settings,
            job_id,
            compiled=compiled,
            scopes=scopes,
            cursor=cursor,
            limit=limit,
            include_logs=arguments.get("include_logs", True) is not False,
            log_limit=_log_limit(arguments),
        )
        if target is not None:
            result["cluster"] = target.name
            result["route_revision"] = _route_revision(target)
        return result
    owned_logs: JSON | None = None
    if target is not None and _mcp_server.should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            with _mcp_server.OwnedSessionApiClient(definition=target, settings=settings) as client:
                observed = _owned_json(
                    client,
                    method="GET",
                    path=f"/jobs/{job_id}/monitor",
                    query={"cursor": cursor, "limit": limit},
                    label="owned remote job monitor",
                )
                _validate_owned_job_status(observed, job_id=job_id, cluster=target.name)
                if arguments.get("include_logs", True) is not False:
                    owned_logs = _owned_job_logs(
                        client,
                        job_id,
                        limit=_log_limit(arguments),
                    )
        else:
            observed = _mcp_server._remote_json(
                target,
                ["job", "monitor", job_id, "--cursor", str(cursor), "--limit", str(limit)],
                "remote job monitor",
            )
    else:
        _require_local_job_cluster(queue, job_id, target)
        observed = monitor_job(queue, job_id, cursor=cursor, limit=limit)
    pattern = _optional_str(arguments, "pattern")
    matches: list[JSON] = []
    matches_truncated = False
    logs: JSON | None = None
    if arguments.get("include_logs", True) is not False:
        log_limit = _log_limit(arguments)
        if target is not None and _mcp_server.should_execute_on_cluster(target):
            if settings.owner_session_id is not None:
                if owned_logs is None:
                    raise ValueError("owned remote log retrieval did not complete")
                logs = owned_logs
            else:
                logs = _remote_job_logs(target, job_id, limit=log_limit)
        else:
            logs = _mcp_server._job_logs(queue, settings, job_id, limit=log_limit)
    if pattern is not None:
        compiled = re.compile(pattern)
        for event in cast(list[JSON], observed.get("events", [])):
            for text in _event_match_candidates(event):
                matches_truncated = _append_bounded_observe_matches(
                    matches,
                    compiled=compiled,
                    text=text,
                    identity={
                        "event_seq": event.get("seq"),
                        "event_type": event.get("event_type"),
                    },
                )
                if matches_truncated:
                    break
            if matches_truncated:
                break
        if logs is not None:
            for stream_name in ("stdout", "stderr"):
                if matches_truncated:
                    break
                stream = _object(logs[stream_name])
                text = stream.get("text")
                if not isinstance(text, str):
                    continue
                matches_truncated = _append_bounded_observe_matches(
                    matches,
                    compiled=compiled,
                    text=text,
                    identity={"source": stream_name},
                )
    result: JSON = {
        **observed,
        "matched": bool(matches),
        "matches": matches,
        "matches_truncated": matches_truncated,
    }
    if logs is not None:
        result["logs"] = logs
    if target is not None:
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
    return result


def _observe_remote_pattern(
    target: ClusterDefinition,
    *,
    settings: RelaySettings,
    job_id: str,
    compiled: re.Pattern[str],
    scopes: tuple[str, ...],
    cursor: int,
    limit: int,
    include_logs: bool,
    log_limit: int,
) -> JSON:
    """Hold a routed observation open over the remote monitor/log surfaces."""
    from clio_relay import mcp_server as _mcp_server

    next_cursor = cursor
    log_offsets = {stream: 0 for stream in ("stdout", "stderr")}

    def read_snapshot() -> JSON:
        nonlocal next_cursor
        if settings.owner_session_id is not None:
            with _mcp_server.OwnedSessionApiClient(definition=target, settings=settings) as client:
                observed = _owned_json(
                    client,
                    method="GET",
                    path=f"/jobs/{job_id}/monitor",
                    query={"cursor": next_cursor, "limit": limit},
                    label="owned remote pattern monitor",
                )
                _validate_owned_job_status(observed, job_id=job_id, cluster=target.name)
        else:
            observed = _mcp_server._remote_json(
                target,
                ["job", "monitor", job_id, "--cursor", str(next_cursor), "--limit", str(limit)],
                "remote pattern monitor",
            )
        returned_cursor = observed.get("next_cursor")
        if not isinstance(returned_cursor, int):
            raise ValueError("remote pattern monitor returned an invalid event cursor")
        next_cursor = returned_cursor
        return observed

    def read_logs() -> JSON | None:
        if not (set(scopes) & {"stdout", "stderr"}):
            return None
        logs: JSON = {}
        scan_limit = min(log_limit, MAX_OBSERVATION_SCAN_BYTES)
        for stream in ("stdout", "stderr"):
            if stream not in scopes:
                continue
            offset = log_offsets[stream]
            if settings.owner_session_id is not None:
                with _mcp_server.OwnedSessionApiClient(
                    definition=target, settings=settings
                ) as client:
                    page = _owned_json(
                        client,
                        method="GET",
                        path=f"/jobs/{job_id}/logs/{stream}",
                        query={"offset": offset, "limit": scan_limit},
                        label=f"owned remote pattern {stream} log",
                    )
            else:
                page = _mcp_server._remote_json(
                    target,
                    [
                        "job",
                        "read-log",
                        job_id,
                        "--stream",
                        stream,
                        "--offset",
                        str(offset),
                        "--limit",
                        str(scan_limit),
                    ],
                    f"remote pattern {stream} log",
                )
            next_offset = page.get("next_offset")
            if not isinstance(next_offset, int):
                raise ValueError(f"remote pattern {stream} log returned an invalid offset")
            log_offsets[stream] = next_offset
            logs[stream] = page
        return logs

    return observe_until_pattern_snapshots(
        read_snapshot,
        compiled=compiled,
        scopes=scopes,
        log_reader=read_logs,
        include_logs=include_logs,
    )


def _wait_job(arguments: JSON, *, queue: ClioCoreQueue, settings: RelaySettings) -> JSON:
    from clio_relay import mcp_server as _mcp_server

    job_id = _required_durable_record_id(arguments, "job_id")
    target = _job_target(arguments)
    timeout_seconds = _observation_timeout_seconds(arguments, "timeout_seconds")
    poll_seconds = _observation_timeout_seconds(arguments, "poll_seconds", default=2.0)
    logs: JSON | None = None
    if target is not None and _mcp_server.should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            try:
                with _mcp_server.OwnedSessionApiClient(
                    definition=target, settings=settings
                ) as client:
                    waited = _owned_json(
                        client,
                        method="POST",
                        path=f"/jobs/{job_id}/wait",
                        query={
                            "timeout_seconds": timeout_seconds,
                            "poll_seconds": poll_seconds,
                        },
                        label="owned remote job wait",
                        response_timeout_seconds=(
                            timeout_seconds + OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS
                        ),
                    )
                    if waited.get("job_id") != job_id or waited.get("cluster") != target.name:
                        raise ValueError("owned remote wait returned a different job")
            except ObservationTimeoutError:
                pass
            with _mcp_server.OwnedSessionApiClient(definition=target, settings=settings) as client:
                result = _owned_json(
                    client,
                    method="GET",
                    path=f"/jobs/{job_id}/status",
                    label="owned remote job status",
                )
                _validate_owned_job_status(result, job_id=job_id, cluster=target.name)
                source_job, observation_unknown = _observed_remote_wait_job(
                    result,
                    job_id=job_id,
                    cluster=target.name,
                )
                if observation_unknown:
                    _attach_wait_observation(
                        result,
                        observation_unknown=True,
                        timeout_seconds=timeout_seconds,
                    )
                    result["cluster"] = target.name
                    result["route_revision"] = _route_revision(target)
                    return result
                if arguments.get("include_logs", False) is True:
                    logs = _owned_job_logs(
                        client,
                        job_id,
                        limit=_log_limit(arguments),
                    )
                artifact_records = _mcp_server._complete_owned_collection(
                    client,
                    path=f"/jobs/{job_id}/artifacts",
                    record_key="artifacts",
                    label=f"owned remote artifacts for {job_id}",
                )
                parsed_result = _mcp_server._verified_owned_mcp_result(
                    client,
                    job_id,
                    artifact_records,
                    require_result=_owned_mcp_result_is_required(source_job),
                )
        else:
            try:
                with remote_command_timeout(
                    timeout_seconds + OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS
                ):
                    _mcp_server.run_remote_clio(
                        target,
                        [
                            "job",
                            "wait",
                            job_id,
                            "--timeout-seconds",
                            str(timeout_seconds),
                            "--poll-seconds",
                            str(poll_seconds),
                        ],
                    )
            except ObservationTimeoutError:
                pass
            with remote_command_timeout(REMOTE_WAIT_STATUS_TIMEOUT_SECONDS):
                result = _mcp_server._remote_json(
                    target, ["job", "status", job_id], "remote job status"
                )
            source_job, observation_unknown = _observed_remote_wait_job(
                result,
                job_id=job_id,
                cluster=target.name,
            )
            if observation_unknown:
                _attach_wait_observation(
                    result,
                    observation_unknown=True,
                    timeout_seconds=timeout_seconds,
                )
                result["cluster"] = target.name
                result["route_revision"] = _route_revision(target)
                return result
            if arguments.get("include_logs", False) is True:
                logs = _remote_job_logs(
                    target,
                    job_id,
                    limit=_log_limit(arguments),
                )
            artifact_records = _mcp_server._complete_remote_collection(
                target,
                ["job", "list-artifacts", job_id],
                record_key="artifacts",
                label=f"remote artifacts for {job_id}",
            )
            parsed_result = _mcp_server._verified_mcp_result(target, job_id, artifact_records)
    else:
        _require_local_job_cluster(queue, job_id, target)
        with suppress(TimeoutError):
            _mcp_server.wait_for_terminal(
                queue,
                job_id,
                timeout_seconds=timeout_seconds,
                poll_seconds=poll_seconds,
            )
        result = job_status(queue, job_id)
        source_job = RelayJob.model_validate(_object(result.get("job")))
        if source_job.job_id != job_id:
            raise ValueError("local wait status returned a different job")
        if source_job.state not in TERMINAL_STATES:
            _attach_wait_observation(
                result,
                observation_unknown=True,
                timeout_seconds=timeout_seconds,
            )
            return result
        if arguments.get("include_logs", False) is True:
            logs = _mcp_server._job_logs(
                queue,
                settings,
                source_job.job_id,
                limit=_log_limit(arguments),
            )
        artifact_records = _mcp_server._complete_local_artifacts(queue, source_job.job_id)
        parsed_result = _mcp_server._verified_local_mcp_result(
            queue,
            source_job.job_id,
            artifacts=artifact_records,
        )

    if target is not None:
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
    _attach_wait_observation(
        result,
        observation_unknown=False,
        timeout_seconds=timeout_seconds,
    )
    if parsed_result is not None:
        _attach_terminal_mcp_evidence(
            result,
            source_job=source_job,
            last_error=source_job.last_error,
            artifacts=artifact_records,
            parsed_result=parsed_result,
        )
    if logs is not None:
        result["logs"] = logs
    result["artifacts"] = artifact_records
    return result


def _observed_remote_wait_job(
    result: JSON,
    *,
    job_id: str,
    cluster: str,
) -> tuple[RelayJob, bool]:
    """Validate an exact remote job after one bounded terminal observation."""

    source_job = RelayJob.model_validate(_object(result.get("job")))
    if source_job.job_id != job_id or source_job.cluster != cluster:
        raise ValueError("remote wait returned a different job")
    terminal = source_job.state in TERMINAL_STATES
    if result.get("terminal") is not terminal:
        raise ValueError("remote wait status disagrees with its durable job state")
    return source_job, not terminal


def _relay_job_from_wait_document(document: JSON) -> RelayJob:
    """Validate current and legacy HTTP wait documents without discarding the outcome."""
    if "observation" not in document:
        return RelayJob.model_validate(document)
    result = JobWaitResult.model_validate(document)
    terminal = result.state in TERMINAL_STATES
    expected_outcome = "terminal" if terminal else "observation_unknown"
    if result.observation.outcome != expected_outcome:
        raise ValueError("remote wait observation disagrees with its durable job state")
    return result


def _job_logs(
    queue: ClioCoreQueue,
    settings: RelaySettings,
    job_id: str,
    *,
    limit: int,
) -> JSON:
    """Return the current log page for every stream a LOCAL job can carry.

    clio-relay#221/#259: includes ``console``/``console_stderr`` alongside
    ``stdout``/``stderr`` so a polling ``relay_observe`` caller sees mid-run
    application output exist without opening the SSE log-tail route --
    cheap and independent of it, per #221's design. A job that never writes
    those two streams (every mcp_call tool except ``jarvis_run``, and every
    non-mcp_call job) reads back an empty, ``eof: true`` page for them
    (``JobSpool.read_log`` never errors on an unwritten stream), so this
    never turns an ordinary job's observation into a new failure mode.

    Adversarial review (D12): console/console_stderr are capped at
    :data:`~clio_relay.spool.CONSOLE_OBSERVE_TAIL_LIMIT_BYTES` regardless of
    the caller's own ``limit`` -- this default view exists for "output
    exists mid-run" visibility, never full-content review (the SSE route is
    that surface), so it should not pay for two full 32-128 KiB fetches on
    streams the caller only needed a presence check on. stdout/stderr keep
    the caller's own ``limit`` unchanged.
    """
    job = queue.get_job(job_id)
    return {
        stream: read_job_log(
            settings,
            job,
            stream_name=stream,
            offset=0,
            limit=(
                limit
                if stream in ("stdout", "stderr")
                else min(limit, CONSOLE_OBSERVE_TAIL_LIMIT_BYTES)
            ),
        )
        for stream in LOG_STREAM_NAMES
    }


def _event_match_candidates(event: JSON) -> list[str]:
    candidates: list[str] = []
    for key in ("message", "event_type"):
        value = event.get(key)
        if isinstance(value, str):
            candidates.append(value)
    payload = event.get("payload")
    if isinstance(payload, dict):
        typed_payload = cast(JSON, payload)
        for key in ("text", "stdout", "stderr", "message"):
            value = typed_payload.get(key)
            if isinstance(value, str):
                candidates.append(value)
    return candidates


def _bounded_observe_value(value: str | None) -> str | None:
    """Bound one regex-derived value before returning it to an agent."""

    if value is None or len(value) <= MAX_OBSERVE_MATCH_TEXT_CHARS:
        return value
    return value[:MAX_OBSERVE_MATCH_TEXT_CHARS]


def _append_bounded_observe_matches(
    matches: list[JSON],
    *,
    compiled: re.Pattern[str],
    text: str,
    identity: JSON,
) -> bool:
    """Append bounded regex matches and report whether more matches were omitted."""

    for match in compiled.finditer(text):
        if len(matches) >= MAX_OBSERVE_MATCHES:
            return True
        start, end = match.span()
        context_start = max(0, start - MAX_OBSERVE_MATCH_TEXT_CHARS // 4)
        context_end = min(len(text), context_start + MAX_OBSERVE_MATCH_TEXT_CHARS)
        if context_end - context_start < MAX_OBSERVE_MATCH_TEXT_CHARS:
            context_start = max(0, context_end - MAX_OBSERVE_MATCH_TEXT_CHARS)
        raw_match = match.group(0)
        groups = [_bounded_observe_value(value) for value in match.groups()]
        groupdict = {key: _bounded_observe_value(value) for key, value in match.groupdict().items()}
        matches.append(
            {
                **identity,
                "text": text[context_start:context_end],
                "text_start": context_start,
                "text_truncated": context_start != 0 or context_end != len(text),
                "match": _bounded_observe_value(raw_match),
                "match_start": start,
                "match_end": end,
                "match_truncated": len(raw_match) > MAX_OBSERVE_MATCH_TEXT_CHARS,
                "groups": groups,
                "groupdict": groupdict,
            }
        )
    return False
