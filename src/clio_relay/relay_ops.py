"""Shared relay operations for MCP, HTTP, and CLI surfaces."""

from __future__ import annotations

import base64
import hmac
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Literal, cast
from urllib.parse import unquote, urlparse

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import (
    TERMINAL_STATES,
    Cursor,
    JobKind,
    JobState,
    MonitorRule,
    MonitorRuleAction,
    ProgressRecord,
    RelayEvent,
    RelayJob,
    RemoteAgentTaskSpec,
    utc_now,
)
from clio_relay.pagination import validate_response_page_limit
from clio_relay.progress_provenance import external_progress_metadata
from clio_relay.scheduler_status import relay_queue_status
from clio_relay.spool import (
    ARTIFACT_OWNERSHIP_SCHEMA,
    JobSpool,
    OwnedFileSizeLimitError,
    read_owned_regular_file_bytes,
)

MAX_ARTIFACT_CONTENT_BYTES = 16 * 1_048_576
MAX_MONITOR_RULE_RECORDS = 10_000


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
    limit = _response_page_limit(limit)
    events, next_cursor = queue.drain_events(Cursor(job_id=job_id, next_seq=cursor), limit=limit)
    status = job_status(queue, job_id)
    return {
        **status,
        "events": [event.model_dump(mode="json") for event in events],
        "next_cursor": next_cursor.next_seq,
    }


def job_status(queue: ClioCoreQueue, job_id: str) -> dict[str, object]:
    """Return current job, relay queue, and scheduler status."""
    job = queue.get_job(job_id)
    return {
        "job": job.model_dump(mode="json"),
        "relay_queue": relay_queue_status(queue, job),
        "scheduler": scheduler_status_for_job(queue, job_id),
        "terminal": job.state in TERMINAL_STATES,
    }


def scheduler_status_for_job(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    limit: int = 100,
) -> list[dict[str, object]]:
    """Return a bounded scheduler-status snapshot from exact job task indexes."""
    if limit < 1:
        raise ValueError("scheduler status limit must be positive")
    statuses: list[dict[str, object]] = []
    tasks, _truncated = queue.scan_job_tasks(job_id, limit=limit)
    for task in tasks:
        stored = task.metadata.get("scheduler_status")
        if isinstance(stored, dict):
            statuses.append(
                {
                    "task_id": task.task_id,
                    "task_name": task.name,
                    "status": stored,
                }
            )
            if len(statuses) >= limit:
                return statuses
            continue
        stored_items = task.metadata.get("scheduler_statuses")
        if not isinstance(stored_items, list):
            continue
        for item in cast(list[object], stored_items):
            if not isinstance(item, dict):
                continue
            statuses.append(
                {
                    "task_id": task.task_id,
                    "task_name": task.name,
                    "status": cast(dict[str, object], item),
                }
            )
            if len(statuses) >= limit:
                return statuses
    return statuses


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
    owned_root_uri = artifact.metadata.get("owned_root_uri")
    ownership_schema = artifact.metadata.get("ownership_schema")
    if owned_root_uri is None and ownership_schema is None:
        owned_root = queue.root.parent / "spool" / artifact.job_id
    elif (
        ownership_schema == ARTIFACT_OWNERSHIP_SCHEMA
        and isinstance(owned_root_uri, str)
        and owned_root_uri
    ):
        owned_root = _artifact_file_path(owned_root_uri)
    else:
        raise RelayError(f"artifact has invalid owned-root metadata: {artifact_id}")
    if owned_root.name != artifact.job_id:
        raise RelayError(
            f"artifact owned-root metadata does not name its durable job: {artifact_id}"
        )
    try:
        snapshot = read_owned_regular_file_bytes(
            path,
            owned_root=owned_root,
            max_bytes=MAX_ARTIFACT_CONTENT_BYTES,
        )
    except OwnedFileSizeLimitError as exc:
        raise RelayError(
            f"artifact content exceeds the {MAX_ARTIFACT_CONTENT_BYTES}-byte transfer limit: "
            f"{artifact_id}; use the cursor-based log endpoint for job logs"
        ) from exc
    except RuntimeError as exc:
        raise RelayError(f"artifact backing file is unsafe: {artifact_id}: {exc}") from exc
    data = snapshot.data
    if data is None:
        raise RelayError(f"artifact content capture failed: {artifact_id}")
    if artifact.size_bytes is not None and snapshot.size_bytes != artifact.size_bytes:
        raise RelayError(
            f"artifact size does not match durable metadata: {artifact_id} "
            f"({snapshot.size_bytes} != {artifact.size_bytes})"
        )
    if artifact.sha256 is not None and not hmac.compare_digest(snapshot.sha256, artifact.sha256):
        raise RelayError(f"artifact SHA-256 does not match durable metadata: {artifact_id}")
    return {
        "artifact": artifact.model_dump(mode="json"),
        "encoding": "base64",
        "data": base64.b64encode(data).decode("ascii"),
    }


def cancel_job(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    cancel_scheduler: bool = False,
    expected_state: JobState | None = None,
    expected_updated_at: datetime | None = None,
) -> RelayJob:
    """Request cancellation for a queued, leased, or running job."""
    job, _transitioned = queue.cancel_job_if_active(
        job_id,
        cancel_scheduler=cancel_scheduler,
        expected_state=expected_state,
        expected_updated_at=expected_updated_at,
    )
    return job


def evaluate_monitor_rules(queue: ClioCoreQueue, *, limit: int = 100) -> list[dict[str, object]]:
    """Evaluate enabled monitor rules and return triggered actions."""
    limit = _response_page_limit(limit)
    rules, truncated = queue.scan_monitor_rules(
        limit=MAX_MONITOR_RULE_RECORDS,
        enabled=True,
    )
    if truncated:
        raise ConfigurationError(
            "monitor rule evaluation exceeds the bounded source limit "
            f"{MAX_MONITOR_RULE_RECORDS}; no rules were evaluated"
        )
    results: list[dict[str, object]] = []
    for rule in rules:
        if rule.triggered_at is not None:
            continue
        pattern = re.compile(rule.pattern)
        events, cursor = queue.drain_events(
            Cursor(job_id=rule.job_id, next_seq=rule.next_seq),
            limit=limit,
        )
        if rule.action == MonitorRuleAction.RECORD_PROGRESS:
            progress_results: list[dict[str, object]] = []
            for event in events:
                if rule.event_types and event.event_type not in rule.event_types:
                    continue
                for match in _event_matches(event, pattern):
                    progress_results.append(_apply_monitor_rule_action(queue, rule, event, match))
            queue.update_monitor_rule(rule.model_copy(update={"next_seq": cursor.next_seq}))
            results.extend(progress_results)
            continue
        matched: tuple[RelayEvent, re.Match[str]] | None = None
        for event in events:
            if rule.event_types and event.event_type not in rule.event_types:
                continue
            for match in _event_matches(event, pattern):
                matched = (event, match)
                break
            if matched is not None:
                break
        updated = rule.model_copy(update={"next_seq": cursor.next_seq})
        if matched is None:
            queue.update_monitor_rule(updated)
            continue
        action_result = _apply_monitor_rule_action(queue, updated, matched[0], matched[1])
        triggered = updated.model_copy(update={"triggered_at": utc_now(), "enabled": False})
        queue.update_monitor_rule(triggered)
        results.append(action_result)
    return results


def _response_page_limit(limit: object) -> int:
    try:
        return validate_response_page_limit(limit)
    except ValueError as exc:
        raise ConfigurationError(str(exc)) from exc


def _event_search_text(event: RelayEvent) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    for candidate in (event.message, event.payload.get("text")):
        if not isinstance(candidate, str):
            continue
        normalized = candidate.strip()
        if normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(candidate)
    return candidates


def _event_matches(event: RelayEvent, pattern: re.Pattern[str]) -> list[re.Match[str]]:
    matches: list[re.Match[str]] = []
    for candidate in _event_search_text(event):
        matches.extend(pattern.finditer(candidate))
    return matches


def _apply_monitor_rule_action(
    queue: ClioCoreQueue,
    rule: MonitorRule,
    event: RelayEvent,
    match: re.Match[str],
) -> dict[str, object]:
    event_seq = event.seq
    payload: dict[str, object] = {"rule_id": rule.rule_id, "matched_seq": event_seq}
    if rule.action == MonitorRuleAction.EMIT_EVENT:
        queue.append_event(
            rule.job_id,
            "monitor.triggered",
            f"Monitor rule triggered: {rule.rule_id}",
            payload=payload,
        )
        return {"rule_id": rule.rule_id, "action": rule.action.value, "matched_seq": event_seq}
    if rule.action == MonitorRuleAction.SUBMIT_AGENT:
        cluster = _required_payload_str(rule, "cluster")
        prompt_path = _required_payload_str(rule, "prompt_path")
        mcp_config_path = _optional_payload_str(rule, "mcp_config_path")
        workdir = _optional_payload_str(rule, "workdir")
        timeout_seconds = rule.action_payload.get("timeout_seconds")
        if timeout_seconds is not None and not isinstance(timeout_seconds, int):
            raise ConfigurationError("monitor action timeout_seconds must be an integer")
        agent_job = queue.submit_job(
            RelayJob(
                cluster=cluster,
                kind=JobKind.REMOTE_AGENT,
                spec=RemoteAgentTaskSpec(
                    prompt_path=prompt_path,
                    mcp_config_path=mcp_config_path,
                    model=_optional_payload_str(rule, "model"),
                    workdir=workdir,
                    timeout_seconds=timeout_seconds,
                    context=_monitor_agent_context(rule, event, match),
                ),
                idempotency_key=f"monitor:{rule.rule_id}:{event_seq}",
            )
        )
        payload["submitted_job_id"] = agent_job.job_id
        queue.append_event(
            rule.job_id,
            "monitor.triggered",
            f"Monitor rule triggered and submitted agent job: {agent_job.job_id}",
            payload=payload,
        )
        return {
            "rule_id": rule.rule_id,
            "action": rule.action.value,
            "matched_seq": event_seq,
            "submitted_job_id": agent_job.job_id,
        }
    if rule.action == MonitorRuleAction.RECORD_PROGRESS:
        progress = queue.append_progress(
            ProgressRecord(
                job_id=rule.job_id,
                label=_progress_label(rule),
                current=_progress_float(rule, "current", match),
                total=_progress_float(rule, "total", match),
                unit=_optional_payload_str(rule, "unit"),
                message=_progress_message(rule, match),
                source_event_seq=event_seq,
                metadata=external_progress_metadata(
                    "monitor_rule",
                    {
                        "rule_id": rule.rule_id,
                        "event_type": event.event_type,
                        "match_groups": dict(match.groupdict()),
                    },
                ),
            )
        )
        payload["progress_id"] = progress.progress_id
        queue.append_event(
            rule.job_id,
            "monitor.triggered",
            f"Monitor rule triggered and recorded progress: {progress.progress_id}",
            payload=payload,
        )
        return {
            "rule_id": rule.rule_id,
            "action": rule.action.value,
            "matched_seq": event_seq,
            "progress_id": progress.progress_id,
        }
    raise ConfigurationError(f"unsupported monitor action: {rule.action}")


def _required_payload_str(rule: MonitorRule, key: str) -> str:
    value = rule.action_payload.get(key)
    if not isinstance(value, str) or value == "":
        raise ConfigurationError(f"monitor action requires string payload field: {key}")
    return value


def _monitor_agent_context(
    rule: MonitorRule,
    event: RelayEvent,
    match: re.Match[str],
) -> dict[str, object]:
    return {
        "monitor_rule_id": rule.rule_id,
        "source_job_id": event.job_id,
        "source_event_seq": event.seq,
        "source_event_type": event.event_type,
        "source_event_message": event.message,
        "source_event_payload": event.payload,
        "match_text": match.group(0),
        "match_groups": dict(match.groupdict()),
    }


def _optional_payload_str(rule: MonitorRule, key: str) -> str | None:
    value = rule.action_payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigurationError(f"monitor action payload field must be a string: {key}")
    return value


def _progress_label(rule: MonitorRule) -> str:
    value = rule.action_payload.get("label", "progress")
    if not isinstance(value, str) or value == "":
        raise ConfigurationError("monitor progress label must be a non-empty string")
    return value


def _progress_message(rule: MonitorRule, match: re.Match[str]) -> str | None:
    static = _optional_payload_str(rule, "message")
    if static is not None:
        return static
    group_name = _optional_payload_str(rule, "message_group")
    if group_name is None:
        return None
    return _progress_group_text(match, group_name)


def _progress_float(rule: MonitorRule, key: str, match: re.Match[str]) -> float | None:
    static_value = rule.action_payload.get(key)
    if static_value is not None:
        return _coerce_progress_float(static_value, key)
    group_name = _optional_payload_str(rule, f"{key}_group")
    if group_name is None:
        return None
    text = _progress_group_text(match, group_name)
    if text is None:
        return None
    return _coerce_progress_float(text, key)


def _progress_group_text(match: re.Match[str], group_name: str) -> str | None:
    try:
        value = match.group(int(group_name)) if group_name.isdigit() else match.group(group_name)
    except (IndexError, KeyError) as exc:
        raise ConfigurationError(f"monitor regex did not define group: {group_name}") from exc
    return value


def _coerce_progress_float(value: object, key: str) -> float:
    if isinstance(value, bool):
        raise ConfigurationError(f"monitor progress {key} must be numeric")
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError as exc:
            raise ConfigurationError(f"monitor progress {key} must be numeric") from exc
    raise ConfigurationError(f"monitor progress {key} must be numeric")


def _artifact_file_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ConfigurationError(f"unsupported artifact URI scheme: {parsed.scheme}")
    if parsed.netloc not in {"", "localhost"}:
        raise ConfigurationError("artifact file URIs with remote authorities are not supported")
    if parsed.query or parsed.fragment:
        raise ConfigurationError("artifact file URIs must not contain query or fragment data")
    path = unquote(parsed.path)
    if os.name == "nt" and len(path) >= 3 and path[0] == "/" and path[2] == ":":
        path = path[1:]
    return Path(path)
