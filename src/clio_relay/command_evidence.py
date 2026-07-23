"""Bounded diagnostic evidence for commands executed by release gates."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import cast

EVIDENCE_EXCERPT_MAX_BYTES = 24_576
EVIDENCE_SUMMARY_TAIL_BYTES = 8_192
ERROR_DETAIL_MAX_BYTES = 4_096
ERROR_DETAIL_SUMMARY_TAIL_BYTES = 1_024
EXCERPT_STRATEGY = "diagnostic-head-summary-tail-v1"
PYTEST_FAILED_NODE_IDS_MARKER = "CLIO_RELAY_PYTEST_FAILED_NODE_IDS_V1="
PYTEST_FAILED_NODE_IDS_MAX_COUNT = 1_000
PYTEST_FAILED_NODE_ID_MAX_BYTES = 4_096
PYTEST_FAILED_NODE_IDS_MAX_BYTES = 64 * 1_024
PYTEST_FAILED_NODE_IDS_PAYLOAD_MAX_BYTES = 512 * 1_024

_TRACEBACK_MARKER = "Traceback (most recent call last):"


@dataclass(frozen=True)
class CommandEvidence:
    """A bounded command transcript and the metadata needed to interpret it."""

    output: str
    excerpt: str
    error_detail: str
    metadata: dict[str, object]


def command_evidence(
    stdout: str | None,
    stderr: str | None,
    *,
    exit_code: int,
) -> CommandEvidence:
    """Return bounded evidence preserving the first diagnostic and final summary."""
    normalized_stdout = stdout or ""
    normalized_stderr = stderr or ""
    output = "\n".join(part for part in (normalized_stdout, normalized_stderr) if part).strip()
    if not output:
        fallback = f"exit_code={exit_code}"
        return CommandEvidence(
            output="",
            excerpt=fallback,
            error_detail=fallback,
            metadata={
                "excerpt_strategy": EXCERPT_STRATEGY,
                "stdout_bytes": len(normalized_stdout.encode("utf-8")),
                "stderr_bytes": len(normalized_stderr.encode("utf-8")),
                "output_bytes": 0,
                "excerpt_bytes": len(fallback.encode("utf-8")),
                "error_detail_bytes": len(fallback.encode("utf-8")),
                "diagnostic_marker": None,
                "diagnostic_offset_bytes": None,
                "failed_test_ids": [],
                "failed_test_ids_truncated": False,
                "omitted_prefix_bytes": 0,
                "omitted_middle_bytes": 0,
                "truncated": False,
                "exit_code": exit_code,
            },
        )

    marker, diagnostic_offset = _first_diagnostic(output)
    excerpt, excerpt_metadata = _bounded_diagnostic_text(
        output,
        max_bytes=EVIDENCE_EXCERPT_MAX_BYTES,
        tail_bytes=EVIDENCE_SUMMARY_TAIL_BYTES,
        diagnostic_offset=diagnostic_offset,
    )
    error_detail, _ = _bounded_diagnostic_text(
        output,
        max_bytes=ERROR_DETAIL_MAX_BYTES,
        tail_bytes=ERROR_DETAIL_SUMMARY_TAIL_BYTES,
        diagnostic_offset=diagnostic_offset,
    )
    failed_test_ids, failed_test_ids_truncated = _pytest_failed_test_ids(normalized_stdout)
    metadata: dict[str, object] = {
        "excerpt_strategy": EXCERPT_STRATEGY,
        "stdout_bytes": len(normalized_stdout.encode("utf-8")),
        "stderr_bytes": len(normalized_stderr.encode("utf-8")),
        "output_bytes": len(output.encode("utf-8")),
        "excerpt_bytes": len(excerpt.encode("utf-8")),
        "error_detail_bytes": len(error_detail.encode("utf-8")),
        "diagnostic_marker": marker,
        "diagnostic_offset_bytes": (
            None if diagnostic_offset is None else len(output[:diagnostic_offset].encode("utf-8"))
        ),
        "exit_code": exit_code,
        "failed_test_ids": failed_test_ids,
        "failed_test_ids_truncated": failed_test_ids_truncated,
        **excerpt_metadata,
    }
    return CommandEvidence(
        output=output,
        excerpt=excerpt,
        error_detail=error_detail,
        metadata=metadata,
    )


def bounded_error_detail(value: str | None) -> str | None:
    """Return one UTF-8-safe diagnostic bounded for durable error records."""
    if value is None:
        return None
    normalized = value.encode("utf-8", errors="replace").decode("utf-8")
    _marker, diagnostic_offset = _first_diagnostic(normalized)
    bounded, _metadata = _bounded_diagnostic_text(
        normalized,
        max_bytes=ERROR_DETAIL_MAX_BYTES,
        tail_bytes=ERROR_DETAIL_SUMMARY_TAIL_BYTES,
        diagnostic_offset=diagnostic_offset,
    )
    return bounded


def _pytest_failed_test_ids(output: str) -> tuple[list[str], bool]:
    """Return bounded node IDs from the pytest release-gate JSON sentinel."""
    marker_offset = _last_line_marker_offset(output, PYTEST_FAILED_NODE_IDS_MARKER)
    if marker_offset is None:
        return [], False
    payload_start = marker_offset + len(PYTEST_FAILED_NODE_IDS_MARKER)
    payload_end = output.find("\n", payload_start)
    if payload_end < 0:
        payload_end = len(output)
    payload_text = output[payload_start:payload_end].removesuffix("\r")
    try:
        payload_bytes = payload_text.encode("utf-8")
    except UnicodeEncodeError:
        return [], True
    if len(payload_bytes) > PYTEST_FAILED_NODE_IDS_PAYLOAD_MAX_BYTES:
        return [], True
    try:
        payload_object: object = json.loads(payload_text)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return [], True
    if not isinstance(payload_object, dict):
        return [], True
    payload = cast(dict[object, object], payload_object)
    if set(payload) != {"node_ids", "truncated"}:
        return [], True
    raw_node_ids = payload["node_ids"]
    declared_truncated = payload["truncated"]
    if not isinstance(raw_node_ids, list) or type(declared_truncated) is not bool:
        return [], True
    node_id_objects = cast(list[object], raw_node_ids)

    test_ids: list[str] = []
    seen: set[str] = set()
    aggregate_bytes = 0
    truncated = declared_truncated
    for test_id in node_id_objects:
        if not isinstance(test_id, str):
            return [], True
        if test_id in seen:
            continue
        if len(test_ids) >= PYTEST_FAILED_NODE_IDS_MAX_COUNT:
            truncated = True
            break
        try:
            test_id_bytes = len(test_id.encode("utf-8"))
        except UnicodeEncodeError:
            truncated = True
            continue
        if test_id_bytes > PYTEST_FAILED_NODE_ID_MAX_BYTES:
            truncated = True
            continue
        if aggregate_bytes + test_id_bytes > PYTEST_FAILED_NODE_IDS_MAX_BYTES:
            truncated = True
            continue
        test_ids.append(test_id)
        seen.add(test_id)
        aggregate_bytes += test_id_bytes
    return test_ids, truncated


def _last_line_marker_offset(output: str, marker: str) -> int | None:
    """Return the last marker that begins a transcript line."""
    first_offset = 0 if output.startswith(marker) else None
    later_offset = output.rfind(f"\n{marker}")
    if later_offset >= 0:
        return later_offset + 1
    return first_offset


def _first_diagnostic(output: str) -> tuple[str | None, int | None]:
    matches: list[tuple[int, str]] = []
    offset = 0
    for line in output.splitlines(keepends=True):
        pytest_heading = line.strip().strip("=").strip().casefold()
        if pytest_heading in {"failures", "errors"}:
            matches.append((offset, f"pytest_{pytest_heading}"))
        offset += len(line)
    traceback_offset = output.find(_TRACEBACK_MARKER)
    if traceback_offset >= 0:
        matches.append((traceback_offset, _TRACEBACK_MARKER))
    if not matches:
        return None, None
    offset, marker = min(matches)
    return marker, offset


def _bounded_diagnostic_text(
    output: str,
    *,
    max_bytes: int,
    tail_bytes: int,
    diagnostic_offset: int | None,
) -> tuple[str, dict[str, int | bool]]:
    encoded = output.encode("utf-8")
    if len(encoded) <= max_bytes:
        return output, {
            "omitted_prefix_bytes": 0,
            "omitted_middle_bytes": 0,
            "truncated": False,
        }

    head_start = diagnostic_offset or 0
    diagnostic_offset_bytes = len(output[:head_start].encode("utf-8"))
    tail = _utf8_suffix(encoded, min(tail_bytes, max_bytes // 2))
    tail_size = len(tail.encode("utf-8"))
    tail_start = len(encoded) - tail_size
    prefix_bytes = min(diagnostic_offset_bytes, tail_start)
    diagnostic_bytes = encoded[prefix_bytes:tail_start] if prefix_bytes < tail_start else b""
    placeholder = "\n\n[... command output omitted ...]\n\n"
    placeholder_size = len(placeholder.encode("utf-8"))
    head_budget = max(0, max_bytes - tail_size - placeholder_size)
    head = _utf8_prefix(diagnostic_bytes, head_budget)
    head_size = len(head.encode("utf-8"))
    middle_bytes = max(0, tail_start - prefix_bytes - head_size)
    placeholder = (
        f"\n\n[... {prefix_bytes} prefix bytes and {middle_bytes} middle bytes omitted ...]\n\n"
    )
    placeholder_size = len(placeholder.encode("utf-8"))
    head_budget = max(0, max_bytes - tail_size - placeholder_size)
    head = _utf8_prefix(diagnostic_bytes, head_budget)
    head_size = len(head.encode("utf-8"))
    middle_bytes = max(0, tail_start - prefix_bytes - head_size)
    excerpt = f"{head}{placeholder}{tail}"
    while len(excerpt.encode("utf-8")) > max_bytes and head:
        excess = len(excerpt.encode("utf-8")) - max_bytes
        head = _utf8_prefix(head.encode("utf-8"), max(0, head_size - excess))
        head_size = len(head.encode("utf-8"))
        middle_bytes = max(0, tail_start - prefix_bytes - head_size)
        placeholder = (
            f"\n\n[... {prefix_bytes} prefix bytes and {middle_bytes} middle bytes omitted ...]\n\n"
        )
        excerpt = f"{head}{placeholder}{tail}"
    return excerpt, {
        "omitted_prefix_bytes": prefix_bytes,
        "omitted_middle_bytes": middle_bytes,
        "truncated": True,
    }


def _utf8_prefix(value: bytes, limit: int) -> str:
    if len(value) <= limit:
        return value.decode("utf-8")
    return value[:limit].decode("utf-8", errors="ignore")


def _utf8_suffix(value: bytes, limit: int) -> str:
    if len(value) <= limit:
        return value.decode("utf-8")
    return value[-limit:].decode("utf-8", errors="ignore")
