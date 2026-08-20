"""Pattern-triggered observation over durable relay job sources."""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import cast

from clio_relay.bounded_payload import JSON
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ObservationPatternError, RelayError
from clio_relay.models import ProgressRecord
from clio_relay.relay_ops import monitor_job, read_job_log

MAX_OBSERVATION_PATTERN_BYTES = 512
MAX_OBSERVATION_EXCERPT_BYTES = 1_024
MAX_OBSERVATION_SCAN_BYTES = 32_768
PATTERN_OBSERVATION_POLL_SECONDS = 0.1
PATTERN_SCOPES = frozenset({"stdout", "stderr", "progress", "events"})


def compile_observation_pattern(pattern: str) -> re.Pattern[str]:
    """Compile a bounded observation regex and reject nested quantifiers."""
    if len(pattern.encode("utf-8")) > MAX_OBSERVATION_PATTERN_BYTES:
        raise ObservationPatternError(
            reason="observation_pattern_unsafe",
            message="observation regex is too long; use at most 512 UTF-8 bytes",
        )
    try:
        compiled = re.compile(pattern)
    except re.error as exc:
        raise ObservationPatternError(
            reason="observation_pattern_invalid",
            message=(
                "observation_pattern_invalid: invalid regular expression; "
                "fix the until_pattern syntax"
            ),
        ) from exc
    if _has_nested_quantifier(pattern):
        raise ObservationPatternError(
            reason="observation_pattern_unsafe",
            message="regular expression rejected as potentially catastrophic; simplify it",
        )
    return compiled


def normalize_pattern_scope(value: object) -> tuple[str, ...]:
    """Validate and canonicalize the streams selected for pattern observation."""
    if value is None:
        return tuple(sorted(PATTERN_SCOPES))
    if not isinstance(value, list) or not value:
        raise ValueError("pattern_scope must be a non-empty string array")
    selected_values: list[str] = []
    for item in cast(list[object], value):
        if not isinstance(item, str):
            raise ValueError("pattern_scope must be a non-empty string array")
        selected_values.append(item)
    selected = tuple(dict.fromkeys(selected_values))
    unknown = sorted(set(selected) - PATTERN_SCOPES)
    if unknown:
        raise ValueError(f"pattern_scope contains unsupported streams: {', '.join(unknown)}")
    return selected


def observe_until_pattern(
    queue: ClioCoreQueue,
    settings: RelaySettings,
    job_id: str,
    *,
    compiled: re.Pattern[str],
    scopes: tuple[str, ...],
    cursor: int,
    limit: int,
    include_logs: bool,
    log_limit: int,
    log_reader: Callable[..., dict[str, object]] | None = None,
) -> JSON:
    """Hold one observation open until a selected stream matches or the job terminates."""
    resolved_log_reader = log_reader or read_job_log
    event_cursor = cursor
    progress_cursor = 1
    log_offsets = {stream: 0 for stream in ("stdout", "stderr")}
    while True:
        snapshot = monitor_job(queue, job_id, cursor=event_cursor, limit=limit)
        next_event_cursor = snapshot.get("next_cursor")
        if not isinstance(next_event_cursor, int):
            raise RelayError("job monitor returned an invalid event cursor")
        event_cursor = next_event_cursor
        matched = _match_pattern_events(snapshot.get("events"), compiled, scopes)
        if matched is None and "progress" in scopes:
            progress, next_progress, total = queue.list_progress_page(
                job_id,
                cursor=progress_cursor,
                limit=limit,
            )
            for item in progress:
                matched = _match_pattern_text(
                    compiled,
                    _progress_text(item),
                    stream="progress",
                    position=item.sequence or progress_cursor,
                    timestamp=item.created_at.isoformat(),
                )
                if matched is not None:
                    break
            progress_cursor = next_progress if next_progress is not None else total + 1
        if matched is None:
            for stream in ("stdout", "stderr"):
                if stream not in scopes:
                    continue
                chunk = resolved_log_reader(
                    settings,
                    queue.get_job(job_id),
                    stream_name=stream,
                    offset=log_offsets[stream],
                    limit=MAX_OBSERVATION_SCAN_BYTES,
                )
                text = chunk.get("text")
                next_offset = chunk.get("next_offset")
                if not isinstance(text, str) or not isinstance(next_offset, int):
                    raise RelayError("job log reader returned an invalid observation chunk")
                matched = _match_pattern_text(
                    compiled,
                    text,
                    stream=stream,
                    position=log_offsets[stream],
                    timestamp=datetime.now(UTC).isoformat(),
                    position_is_byte_offset=True,
                )
                log_offsets[stream] = next_offset
                if matched is not None:
                    break
        if matched is not None or snapshot.get("terminal") is True:
            result = {
                **snapshot,
                "matched": matched is not None,
                "match": matched,
                "matches": [] if matched is None else [matched],
                "matches_truncated": False,
                "pattern_scope": list(scopes),
            }
            if include_logs:
                job = queue.get_job(job_id)
                result["logs"] = {
                    stream: resolved_log_reader(
                        settings,
                        job,
                        stream_name=stream,
                        offset=0,
                        limit=log_limit,
                    )
                    for stream in ("stdout", "stderr")
                }
            return result
        time.sleep(PATTERN_OBSERVATION_POLL_SECONDS)


def observe_until_pattern_snapshots(
    snapshot_reader: Callable[[], JSON],
    *,
    compiled: re.Pattern[str],
    scopes: tuple[str, ...],
    log_reader: Callable[[], JSON | None] | None = None,
    include_logs: bool = False,
) -> JSON:
    """Hold a routed observation open using monitor and current-log snapshots."""
    while True:
        snapshot = snapshot_reader()
        matched = _match_pattern_events(snapshot.get("events"), compiled, scopes)
        logs = log_reader() if log_reader is not None and matched is None else None
        if matched is None and logs is not None:
            for stream in ("stdout", "stderr"):
                if stream not in scopes:
                    continue
                raw_stream = logs.get(stream)
                if not isinstance(raw_stream, dict):
                    continue
                stream_document = cast(JSON, raw_stream)
                text = stream_document.get("text")
                if isinstance(text, str):
                    raw_offset = stream_document.get("offset", 0)
                    offset = raw_offset if isinstance(raw_offset, int) else 0
                    matched = _match_pattern_text(
                        compiled,
                        text,
                        stream=stream,
                        position=offset,
                        timestamp=datetime.now(UTC).isoformat(),
                        position_is_byte_offset=True,
                    )
                    if matched is not None:
                        break
        if matched is not None or snapshot.get("terminal") is True:
            result = {
                **snapshot,
                "matched": matched is not None,
                "match": matched,
                "matches": [] if matched is None else [matched],
                "matches_truncated": False,
                "pattern_scope": list(scopes),
            }
            if include_logs and logs is not None:
                result["logs"] = logs
            return result
        time.sleep(PATTERN_OBSERVATION_POLL_SECONDS)


def _has_nested_quantifier(pattern: str) -> bool:
    """Return whether a quantified group contains another quantifier."""
    groups: list[bool] = []
    escaped = False
    character_class = False
    quantifiers = frozenset("*+?{")
    for index, character in enumerate(pattern):
        if escaped:
            escaped = False
            continue
        if character == "\\":
            escaped = True
            continue
        if character == "[":
            character_class = True
        elif character == "]":
            character_class = False
        elif not character_class and character == "(":
            groups.append(False)
        elif not character_class and character in quantifiers and groups:
            groups[-1] = True
        elif not character_class and character == ")" and groups:
            had_quantifier = groups.pop()
            if had_quantifier and index + 1 < len(pattern) and pattern[index + 1] in quantifiers:
                return True
    return False


def _match_pattern_events(
    raw_events: object,
    compiled: re.Pattern[str],
    scopes: tuple[str, ...],
) -> JSON | None:
    """Find the first bounded event or progress match in one monitor page."""
    if not isinstance(raw_events, list):
        return None
    for raw_event in cast(list[object], raw_events):
        if not isinstance(raw_event, dict):
            continue
        event = cast(JSON, raw_event)
        raw_position = event.get("seq", 0)
        position = raw_position if isinstance(raw_position, int) else 0
        event_type = event.get("event_type")
        stream = (
            "progress"
            if event_type == "progress.updated"
            else "stdout"
            if isinstance(event_type, str) and event_type.startswith("stdout.")
            else "stderr"
            if isinstance(event_type, str) and event_type.startswith("stderr.")
            else "events"
        )
        if stream not in scopes:
            continue
        for value in (event.get("message"), event.get("event_type")):
            if isinstance(value, str):
                matched = _match_pattern_text(
                    compiled,
                    value,
                    stream=stream,
                    position=position,
                    timestamp=str(event.get("created_at", "")),
                )
                if matched is not None:
                    return matched
        payload = event.get("payload")
        if isinstance(payload, dict):
            for value in cast(JSON, payload).values():
                if isinstance(value, str):
                    matched = _match_pattern_text(
                        compiled,
                        value,
                        stream=stream,
                        position=position,
                        timestamp=str(event.get("created_at", "")),
                    )
                    if matched is not None:
                        return matched
    return None


def _progress_text(progress: ProgressRecord) -> str:
    """Render stable searchable text for one structured progress record."""
    return " ".join(
        value
        for value in (progress.label, progress.message, progress.unit)
        if isinstance(value, str)
    )


def _match_pattern_text(
    compiled: re.Pattern[str],
    text: str,
    *,
    stream: str,
    position: int,
    timestamp: str,
    position_is_byte_offset: bool = False,
) -> JSON | None:
    """Return one bounded match with a stream position and timestamp."""
    match = compiled.search(text)
    if match is None:
        return None
    start, end = match.span()
    start_offset = len(text[:start].encode("utf-8")) if position_is_byte_offset else start
    excerpt_start = max(0, start - 256)
    excerpt_end = min(len(text), excerpt_start + MAX_OBSERVATION_EXCERPT_BYTES)
    excerpt = _bounded_utf8(text[excerpt_start:excerpt_end])
    return {
        "stream": stream,
        "source": stream,
        "excerpt": excerpt,
        "position": position + start_offset,
        "timestamp": timestamp,
        "match": _bounded_utf8(match.group(0)),
        "match_start": start,
        "match_end": end,
    }


def _bounded_utf8(value: str) -> str:
    """Keep a returned regex value within the observation byte budget."""
    encoded = value.encode("utf-8")
    if len(encoded) <= MAX_OBSERVATION_EXCERPT_BYTES:
        return value
    return encoded[:MAX_OBSERVATION_EXCERPT_BYTES].decode("utf-8", errors="ignore")
