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
    MonitorRule,
    MonitorRuleAction,
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
        matched = None
        for event in events:
            if rule.event_types and event.event_type not in rule.event_types:
                continue
            if any(pattern.search(candidate) for candidate in _event_search_text(event)):
                matched = event
                break
        updated = rule.model_copy(update={"next_seq": cursor.next_seq})
        if matched is None:
            queue.update_monitor_rule(updated)
            continue
        action_result = _apply_monitor_rule_action(queue, updated, matched.seq)
        triggered = updated.model_copy(update={"triggered_at": utc_now(), "enabled": False})
        queue.update_monitor_rule(triggered)
        results.append(action_result)
    return results


def _event_search_text(event: RelayEvent) -> list[str]:
    candidates = [event.message]
    text = event.payload.get("text")
    if isinstance(text, str):
        candidates.append(text)
    return candidates


def _apply_monitor_rule_action(
    queue: ClioCoreQueue,
    rule: MonitorRule,
    event_seq: int,
) -> dict[str, object]:
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


def _artifact_file_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ConfigurationError(f"unsupported artifact URI scheme: {parsed.scheme}")
    path = unquote(parsed.path)
    if os.name == "nt" and len(path) >= 3 and path[0] == "/" and path[2] == ":":
        path = path[1:]
    return Path(path)
