"""Shared relay operations for MCP, HTTP, and CLI surfaces."""

from __future__ import annotations

import base64
import os
import re
import time
from pathlib import Path
from typing import Literal
from urllib.parse import unquote, urlparse

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError
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


def cancel_job(queue: ClioCoreQueue, job_id: str) -> RelayJob:
    """Request cancellation for a queued, leased, or running job."""
    job = queue.get_job(job_id)
    if job.state in TERMINAL_STATES:
        return job
    queue.append_event(
        job_id,
        "job.cancel_requested",
        "Cancellation requested",
        payload={"previous_state": job.state.value},
    )
    return queue.update_job_state(
        job_id,
        JobState.CANCELED,
        message="Job canceled",
    )


def evaluate_monitor_rules(queue: ClioCoreQueue, *, limit: int = 100) -> list[dict[str, object]]:
    """Evaluate enabled monitor rules and return triggered actions."""
    if limit <= 0:
        raise ConfigurationError("limit must be positive")
    results: list[dict[str, object]] = []
    for rule in queue.list_monitor_rules():
        if not rule.enabled or rule.triggered_at is not None:
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
        prompt_path = Path(_required_payload_str(rule, "prompt_path"))
        mcp_config_path = _optional_payload_path(rule, "mcp_config_path")
        workdir = _optional_payload_path(rule, "workdir")
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
                metadata={
                    "source": "monitor_rule",
                    "rule_id": rule.rule_id,
                    "event_type": event.event_type,
                    "match_groups": dict(match.groupdict()),
                },
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


def _optional_payload_str(rule: MonitorRule, key: str) -> str | None:
    value = rule.action_payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigurationError(f"monitor action payload field must be a string: {key}")
    return value


def _optional_payload_path(rule: MonitorRule, key: str) -> Path | None:
    value = _optional_payload_str(rule, key)
    if value is None:
        return None
    return Path(value)


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
    path = unquote(parsed.path)
    if os.name == "nt" and len(path) >= 3 and path[0] == "/" and path[2] == ":":
        path = path[1:]
    return Path(path)
