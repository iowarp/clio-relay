"""Tests for bounded release-command diagnostics."""

from __future__ import annotations

from clio_relay.command_evidence import (
    ERROR_DETAIL_MAX_BYTES,
    EVIDENCE_EXCERPT_MAX_BYTES,
    bounded_error_detail,
    command_evidence,
)


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
