"""Tests for bounded release-command diagnostics."""

from __future__ import annotations

import json
from typing import cast

from clio_relay.command_evidence import (
    ERROR_DETAIL_MAX_BYTES,
    EVIDENCE_EXCERPT_MAX_BYTES,
    PYTEST_FAILED_NODE_ID_MAX_BYTES,
    PYTEST_FAILED_NODE_IDS_MARKER,
    PYTEST_FAILED_NODE_IDS_MAX_BYTES,
    PYTEST_FAILED_NODE_IDS_MAX_COUNT,
    PYTEST_PLATFORM_NODE_ID_MAX_BYTES,
    PYTEST_PLATFORM_NODE_IDS_MAX_BYTES,
    PYTEST_PLATFORM_NODE_IDS_MAX_COUNT,
    PYTEST_PLATFORM_PARTITION_MARKER,
    bounded_error_detail,
    command_evidence,
)


def _pytest_failure_sentinel(node_ids: list[str], *, truncated: bool = False) -> str:
    """Return one exact machine-readable pytest failure sentinel."""
    payload = json.dumps(
        {"node_ids": node_ids, "truncated": truncated},
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"{PYTEST_FAILED_NODE_IDS_MARKER}{payload}"


def _pytest_platform_sentinel(
    *,
    platform: str,
    selected_node_ids: list[str],
    excluded_node_ids: list[str],
    truncated: bool = False,
) -> str:
    """Return one exact machine-readable pytest platform sentinel."""
    payload = json.dumps(
        {
            "excluded_node_ids": excluded_node_ids,
            "platform": platform,
            "selected_node_ids": selected_node_ids,
            "truncated": truncated,
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"{PYTEST_PLATFORM_PARTITION_MARKER}{payload}"


def test_short_unicode_command_evidence_is_exact() -> None:
    """Short output is preserved without lossy Unicode handling."""
    evidence = command_evidence("progress λ🧪", "warning Δ", exit_code=7)

    assert evidence.output == "progress λ🧪\nwarning Δ"
    assert evidence.excerpt == evidence.output
    assert evidence.error_detail == evidence.output
    assert evidence.metadata["truncated"] is False
    assert evidence.metadata["exit_code"] == 7


def test_empty_command_evidence_records_exit_code() -> None:
    """A silent failure still leaves an actionable evidence record."""
    evidence = command_evidence("", None, exit_code=13)

    assert evidence.excerpt == "exit_code=13"
    assert evidence.error_detail == "exit_code=13"
    assert evidence.metadata["output_bytes"] == 0
    assert evidence.metadata["pytest_platform"] is None
    assert evidence.metadata["platform_selected_test_ids"] == []
    assert evidence.metadata["platform_excluded_test_ids"] == []
    assert evidence.metadata["platform_test_ids_truncated"] is False


def test_pytest_failure_ids_survive_transcript_truncation() -> None:
    """Every pytest failure node remains machine-readable when details are bounded."""
    node_ids = [
        "tests/test_queue.py::test_atomic",
        "tests/test_queue.py::test_second[param - value]",
    ]
    summary = "\n".join(
        [
            "==== short test summary info ====",
            "FAILED tests/test_queue.py::test_atomic - AssertionError",
            "FAILED tests/test_queue.py::test_second[param - value] - RuntimeError",
            "2 failed, 100 passed in 90.00s",
            _pytest_failure_sentinel(node_ids),
        ]
    )
    output = "==== FAILURES ====\nfirst cause\n" + ("detail\n" * 50_000) + summary

    evidence = command_evidence(output, None, exit_code=1)

    assert evidence.metadata["truncated"] is True
    assert evidence.metadata["failed_test_ids"] == node_ids
    assert evidence.metadata["failed_test_ids_truncated"] is False


def test_pytest_failure_id_metadata_has_count_and_byte_bounds() -> None:
    """Hostile pytest summaries cannot make validation metadata unbounded."""
    oversized = "tests/test_queue.py::test_" + ("x" * PYTEST_FAILED_NODE_ID_MAX_BYTES)
    ordinary = [
        f"tests/test_queue.py::test_case_{index:04d}" + ("x" * 80)
        for index in range(PYTEST_FAILED_NODE_IDS_MAX_COUNT + 5)
    ]
    output = _pytest_failure_sentinel([oversized, *ordinary])

    evidence = command_evidence(output, None, exit_code=1)

    failed_test_ids_value = evidence.metadata["failed_test_ids"]
    assert isinstance(failed_test_ids_value, list)
    failed_test_id_objects = cast(list[object], failed_test_ids_value)
    assert all(isinstance(node_id, str) for node_id in failed_test_id_objects)
    failed_test_ids = cast(list[str], failed_test_id_objects)
    assert oversized not in failed_test_ids
    assert len(failed_test_ids) <= PYTEST_FAILED_NODE_IDS_MAX_COUNT
    assert sum(len(node_id.encode("utf-8")) for node_id in failed_test_ids) <= (
        PYTEST_FAILED_NODE_IDS_MAX_BYTES
    )
    assert evidence.metadata["failed_test_ids_truncated"] is True


def test_human_pytest_summary_is_not_treated_as_structured_node_ids() -> None:
    """Ambiguous human summaries cannot forge or truncate parametrized IDs."""
    output = "FAILED tests/test_queue.py::test_case[a - b] - AssertionError\n1 failed in 0.01s"

    evidence = command_evidence(output, None, exit_code=1)

    assert evidence.metadata["failed_test_ids"] == []
    assert evidence.metadata["failed_test_ids_truncated"] is False


def test_malformed_pytest_failure_sentinel_fails_closed() -> None:
    """An invalid machine sentinel cannot be mistaken for complete evidence."""
    output = f'{PYTEST_FAILED_NODE_IDS_MARKER}{{"node_ids":"not-a-list","truncated":false}}'

    evidence = command_evidence(output, None, exit_code=1)

    assert evidence.metadata["failed_test_ids"] == []
    assert evidence.metadata["failed_test_ids_truncated"] is True


def test_stderr_cannot_override_authoritative_pytest_failure_ids() -> None:
    """Only the pytest terminal reporter's stdout channel can publish node IDs."""
    expected = ["tests/test_queue.py::test_authoritative[a - b]"]
    stdout = _pytest_failure_sentinel(expected)
    stderr = _pytest_failure_sentinel([])

    evidence = command_evidence(stdout, stderr, exit_code=1)

    assert evidence.metadata["failed_test_ids"] == expected
    assert evidence.metadata["failed_test_ids_truncated"] is False


def test_pytest_platform_partition_survives_transcript_truncation() -> None:
    """The exact selected and excluded marked tests remain machine-readable."""
    selected = ["tests/test_bootstrap.py::test_posix_selected"]
    excluded = ["tests/test_bootstrap.py::test_windows_excluded"]
    sentinel = _pytest_platform_sentinel(
        platform="posix",
        selected_node_ids=selected,
        excluded_node_ids=excluded,
    )
    output = ("collection detail\n" * 50_000) + sentinel

    evidence = command_evidence(output, None, exit_code=0)

    assert evidence.metadata["pytest_platform"] == "posix"
    assert evidence.metadata["platform_selected_test_ids"] == selected
    assert evidence.metadata["platform_excluded_test_ids"] == excluded
    assert evidence.metadata["platform_test_ids_truncated"] is False


def test_pytest_platform_partition_metadata_has_count_and_byte_bounds() -> None:
    """Hostile partition evidence cannot make validation metadata unbounded."""
    oversized = "tests/test_bootstrap.py::test_" + ("x" * PYTEST_PLATFORM_NODE_ID_MAX_BYTES)
    ordinary = [
        f"tests/test_bootstrap.py::test_case_{index:04d}" + ("x" * 80)
        for index in range(PYTEST_PLATFORM_NODE_IDS_MAX_COUNT + 5)
    ]
    sentinel = _pytest_platform_sentinel(
        platform="windows",
        selected_node_ids=[oversized, *ordinary],
        excluded_node_ids=[],
    )

    evidence = command_evidence(sentinel, None, exit_code=0)

    selected_value = evidence.metadata["platform_selected_test_ids"]
    assert isinstance(selected_value, list)
    selected_objects = cast(list[object], selected_value)
    assert all(isinstance(node_id, str) for node_id in selected_objects)
    selected = cast(list[str], selected_objects)
    assert oversized not in selected
    assert len(selected) <= PYTEST_PLATFORM_NODE_IDS_MAX_COUNT
    assert sum(len(node_id.encode("utf-8")) for node_id in selected) <= (
        PYTEST_PLATFORM_NODE_IDS_MAX_BYTES
    )
    assert evidence.metadata["platform_test_ids_truncated"] is True


def test_malformed_pytest_platform_partition_fails_closed() -> None:
    """An invalid platform sentinel cannot be mistaken for complete evidence."""
    output = (
        f"{PYTEST_PLATFORM_PARTITION_MARKER}"
        '{"excluded_node_ids":[],"platform":"nt",'
        '"selected_node_ids":[],"truncated":false}'
    )

    evidence = command_evidence(output, None, exit_code=0)

    assert evidence.metadata["pytest_platform"] is None
    assert evidence.metadata["platform_selected_test_ids"] == []
    assert evidence.metadata["platform_excluded_test_ids"] == []
    assert evidence.metadata["platform_test_ids_truncated"] is True


def test_large_pytest_output_keeps_first_failure_and_summary() -> None:
    """Collection noise is discarded before the first causal pytest failure."""
    prelude = "collecting tests 🧪\n" * 2_000
    first_failure = (
        "==== FAILURES ====\nFAILED test_queue.py::test_atomic\nroot cause: WinError 206\n"
    )
    middle = "secondary traceback detail λ\n" * 3_000
    summary = "==== short test summary info ====\n105 failed, 1138 passed in 822.52s"

    evidence = command_evidence(prelude + first_failure + middle + summary, "", exit_code=1)

    assert "root cause: WinError 206" in evidence.excerpt
    assert "105 failed, 1138 passed" in evidence.excerpt
    assert "root cause: WinError 206" in evidence.error_detail
    assert "105 failed, 1138 passed" in evidence.error_detail
    assert evidence.excerpt.count("collecting tests") < 5
    assert len(evidence.excerpt.encode("utf-8")) <= EVIDENCE_EXCERPT_MAX_BYTES
    assert len(evidence.error_detail.encode("utf-8")) <= ERROR_DETAIL_MAX_BYTES
    assert evidence.metadata["diagnostic_marker"] == "pytest_failures"
    omitted_prefix = evidence.metadata["omitted_prefix_bytes"]
    omitted_middle = evidence.metadata["omitted_middle_bytes"]
    assert isinstance(omitted_prefix, int) and omitted_prefix > 0
    assert isinstance(omitted_middle, int) and omitted_middle > 0
    assert evidence.metadata["truncated"] is True


def test_large_traceback_output_uses_traceback_as_diagnostic_head() -> None:
    """Non-pytest commands retain their first traceback and their final line."""
    prelude = "build output\n" * 3_000
    traceback = "Traceback (most recent call last):\nValueError: exact first cause\n"
    middle = "frame detail\n" * 4_000
    summary = "command failed with exit status 2"

    evidence = command_evidence(prelude + traceback + middle + summary, None, exit_code=2)

    assert "ValueError: exact first cause" in evidence.excerpt
    assert evidence.excerpt.endswith(summary)
    assert evidence.metadata["diagnostic_marker"] == "Traceback (most recent call last):"


def test_large_output_without_marker_keeps_start_and_tail() -> None:
    """Unknown output formats retain both initial context and final status."""
    output = "initial command context\n" + ("x" * 50_000) + "\nfinal failure status"

    evidence = command_evidence(output, None, exit_code=1)

    assert evidence.excerpt.startswith("initial command context")
    assert evidence.excerpt.endswith("final failure status")
    assert evidence.metadata["diagnostic_marker"] is None


def test_bounded_error_detail_handles_oversized_and_invalid_unicode() -> None:
    """Provider diagnostics are UTF-8 safe and fit durable record limits."""
    detail = bounded_error_detail("first cause\ud800" + ("x" * 300_000) + "final status")

    assert detail is not None
    assert detail.startswith("first cause?")
    assert detail.endswith("final status")
    assert len(detail.encode("utf-8")) <= ERROR_DETAIL_MAX_BYTES
