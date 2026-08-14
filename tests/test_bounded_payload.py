"""Tests for the shared tiered byte-budget owner (clio-relay#231, R6).

``docs/design/relay-architecture-2026-08.md`` §6.4 names three tiers (T1
refusal text, T2 agent-parsed payload, T3 durable evidence) this module
implements as :func:`~clio_relay.bounded_payload.build_truncation_record`
(the ``clio-relay.truncation.v1`` record shared by T1 and T3),
:func:`~clio_relay.bounded_payload.bound_stream_capture` (T3's record-time
head+tail retention), and :func:`~clio_relay.bounded_payload.
build_delivery_refusal`/:func:`~clio_relay.bounded_payload.
is_delivery_refusal` (T2's typed over-budget refusal document). Each test
below is written against the doc's own worked shapes, plus sabotage twins
for the boundary/lookalike cases a naive implementation gets wrong.
"""

from __future__ import annotations

from pathlib import Path

from clio_relay.bounded_payload import (
    DELIVERY_FAILURE_SCHEMA_VERSION,
    STDERR_HEAD_MAX_BYTES,
    STDERR_TAIL_MAX_BYTES,
    STDOUT_HEAD_MAX_BYTES,
    STDOUT_TAIL_MAX_BYTES,
    TRUNCATION_SCHEMA_VERSION,
    bound_stream_capture,
    build_delivery_refusal,
    build_truncation_record,
    is_delivery_refusal,
)

# --------------------------------------------------------------------------- #
# build_truncation_record (T1/T3 shared shape)
# --------------------------------------------------------------------------- #


def test_build_truncation_record_reports_the_exact_elided_byte_count() -> None:
    record = build_truncation_record(
        retention="head_tail",
        original_bytes=1_000,
        retained_head_bytes=100,
        retained_tail_bytes=50,
        stream="stdout",
    )
    assert record["schema_version"] == TRUNCATION_SCHEMA_VERSION
    assert record["truncated"] is True
    assert record["retention"] == "head_tail"
    assert record["original_bytes"] == 1_000
    assert record["retained_head_bytes"] == 100
    assert record["retained_tail_bytes"] == 50
    assert record["elided_bytes"] == 850
    assert record["evidence_ref"] is None


def test_the_marker_names_original_and_retained_bytes() -> None:
    """The in-band marker (doc §6.4) is a full-line, human-legible statement
    of exactly how many bytes were elided and from which stream -- a reader
    who sees only the embedded marker text (no structured record alongside
    it) must still learn the elided count and the stream it came from.
    """
    record = build_truncation_record(
        retention="tail",
        original_bytes=5_000,
        retained_tail_bytes=2_000,
        stream="stderr",
    )
    assert record["marker"] == "[clio-relay: elided 3000 bytes of stderr]"
    assert str(record["elided_bytes"]) in record["marker"]
    assert "stderr" in record["marker"]


def test_build_truncation_record_honors_an_explicit_evidence_ref() -> None:
    record = build_truncation_record(
        retention="head",
        original_bytes=10,
        retained_head_bytes=5,
        evidence_ref="artifact-durable-copy",
    )
    assert record["evidence_ref"] == "artifact-durable-copy"


# --------------------------------------------------------------------------- #
# bound_stream_capture (T3: record-time head+tail retention)
# --------------------------------------------------------------------------- #


def test_a_38mib_stdout_capture_is_recorded_head_tail_with_a_typed_elision_marker() -> None:
    """Drives ``bound_stream_capture`` directly with a synthetic capture much
    larger than the default 1 MiB + 1 MiB stdout window -- the companion
    test in ``test_mcp_call_runner.py`` drives the same scenario through the
    runner's actual ``_write_mcp_result`` build path.
    """
    middle = "B" * (38 * 1024 * 1024)
    original = ("A" * STDOUT_HEAD_MAX_BYTES) + middle + ("C" * STDOUT_TAIL_MAX_BYTES)

    retained, record = bound_stream_capture(
        original,
        head_max=STDOUT_HEAD_MAX_BYTES,
        tail_max=STDOUT_TAIL_MAX_BYTES,
        stream_name="stdout",
    )

    assert record is not None
    assert record["schema_version"] == TRUNCATION_SCHEMA_VERSION
    assert record["retention"] == "head_tail"
    assert record["original_bytes"] == len(original)
    assert record["retained_head_bytes"] == STDOUT_HEAD_MAX_BYTES
    assert record["retained_tail_bytes"] == STDOUT_TAIL_MAX_BYTES
    assert record["elided_bytes"] == len(middle)
    assert "[clio-relay: elided" in retained
    assert retained.startswith("A" * 100)
    assert retained.endswith("C" * 100)
    assert "B" * 100 not in retained  # the elided middle never survives


def test_data_that_fits_within_the_window_is_returned_unchanged() -> None:
    data = "short capture"
    retained, record = bound_stream_capture(
        data, head_max=1024, tail_max=1024, stream_name="stdout"
    )
    assert retained == data
    assert record is None


def test_sabotage_twin_data_exactly_at_the_boundary_is_not_falsely_flagged_truncated() -> None:
    """Off-by-one guard: data whose size is EXACTLY ``head_max + tail_max``
    must not be reported as truncated -- a ``<`` where ``<=`` belongs would
    falsely elide the last byte of otherwise-complete evidence.
    """
    data = "x" * 20
    retained, record = bound_stream_capture(data, head_max=10, tail_max=10, stream_name="stdout")
    assert retained == data
    assert record is None


def test_sabotage_twin_one_byte_over_the_boundary_is_truncated() -> None:
    """The mirror of the boundary guard above: one byte over the window must
    actually elide something, not silently pass through.
    """
    data = "x" * 21
    retained, record = bound_stream_capture(data, head_max=10, tail_max=10, stream_name="stdout")
    assert record is not None
    assert record["elided_bytes"] == 1
    assert retained != data


def test_sabotage_twin_head_only_bound_reports_retention_head_with_zero_tail() -> None:
    data = "head" + ("x" * 30) + "TAIL-CONTENT"
    retained, record = bound_stream_capture(data, head_max=4, tail_max=0, stream_name="stdout")
    assert record is not None
    assert record["retention"] == "head"
    assert record["retained_tail_bytes"] == 0
    assert "TAIL-CONTENT" not in retained


def test_sabotage_twin_tail_only_bound_reports_retention_tail_with_zero_head() -> None:
    data = "HEAD-CONTENT" + ("x" * 30) + "tail"
    retained, record = bound_stream_capture(data, head_max=0, tail_max=4, stream_name="stdout")
    assert record is not None
    assert record["retention"] == "tail"
    assert record["retained_head_bytes"] == 0
    assert "HEAD-CONTENT" not in retained
    assert retained.endswith("tail")


def test_bound_stream_capture_supports_bytes_input_and_returns_bytes() -> None:
    data = b"\x00" * 40
    retained, record = bound_stream_capture(data, head_max=10, tail_max=10, stream_name="stdout")
    assert isinstance(retained, bytes)
    assert record is not None
    assert record["retained_head_bytes"] == 10
    assert record["retained_tail_bytes"] == 10


def test_default_stream_budgets_match_the_doc() -> None:
    """Doc §6.4's named defaults: stdout 1 MiB + 1 MiB, stderr 256 KiB + 256 KiB."""
    assert STDOUT_HEAD_MAX_BYTES == 1024 * 1024
    assert STDOUT_TAIL_MAX_BYTES == 1024 * 1024
    assert STDERR_HEAD_MAX_BYTES == 256 * 1024
    assert STDERR_TAIL_MAX_BYTES == 256 * 1024


# --------------------------------------------------------------------------- #
# build_delivery_refusal / is_delivery_refusal (T2)
# --------------------------------------------------------------------------- #


def test_a_structured_document_is_never_truncated_mid_field() -> None:
    """T2 (doc §6.4): an over-budget agent-parsed payload is never truncated.

    ``build_delivery_refusal`` returns a typed refusal document instead of a
    cut one -- the caller-supplied ``message``, however long, is carried
    whole, never sliced. (Real callers pass a short, fixed message; this
    proves the function itself applies no hidden budget to any field.)
    """
    huge_message = "field content " * 10_000
    document = build_delivery_refusal(
        code="inline_result_limit_exceeded",
        message=huge_message,
        max_bytes=65_536,
        remote_side_effects_may_have_occurred=True,
    )
    assert document["content_truncated"] is True
    assert document["result_available"] is False
    delivery = document["delivery"]
    assert delivery["message"] == huge_message  # never cut
    assert delivery["schema_version"] == DELIVERY_FAILURE_SCHEMA_VERSION
    assert delivery["status"] == "failed"
    assert delivery["code"] == "inline_result_limit_exceeded"
    assert delivery["max_inline_bytes"] == 65_536
    assert delivery["private_evidence_preserved"] is True
    assert delivery["remote_side_effects_may_have_occurred"] is True
    assert is_delivery_refusal(document)


def test_build_delivery_refusal_private_evidence_preserved_defaults_true() -> None:
    document = build_delivery_refusal(
        code="artifact_content_too_large",
        message="over budget",
        max_bytes=16 * 1_048_576,
        remote_side_effects_may_have_occurred=False,
    )
    assert document["delivery"]["private_evidence_preserved"] is True


def test_sabotage_twin_is_delivery_refusal_rejects_a_lookalike_missing_the_schema_tag() -> None:
    """A document that merely shares the ``result_available: False`` field
    name with a real T2 refusal -- but was not built by
    ``build_delivery_refusal`` -- must not be misidentified as one; only the
    exact schema tag on ``delivery`` counts.
    """
    lookalike = {"result_available": False, "delivery": {"status": "failed"}}
    assert not is_delivery_refusal(lookalike)


def test_sabotage_twin_is_delivery_refusal_false_for_an_ordinary_successful_document() -> None:
    ordinary = {"result_available": True, "data": "ordinary payload"}
    assert not is_delivery_refusal(ordinary)


def test_sabotage_twin_is_delivery_refusal_false_when_delivery_is_not_a_mapping() -> None:
    malformed = {"result_available": False, "delivery": "not-a-dict"}
    assert not is_delivery_refusal(malformed)


# --------------------------------------------------------------------------- #
# Worker-package vendoring (mirrors the process_containment.py precedent)
# --------------------------------------------------------------------------- #


def test_the_worker_vendored_copy_is_an_exact_mirror() -> None:
    """``jarvis-packages/clio_relay/clio_relay/bounded_payload.py`` is a
    deliberately vendored, byte-identical copy of
    ``src/clio_relay/bounded_payload.py`` -- the same self-contained-
    worker-package model ``process_containment.py`` already uses (doc §7's
    "precedent for byte-identical enforcement";
    ``test_process_containment.py::
    test_embedded_containment_source_is_an_exact_isolated_runtime_mirror``).
    ``runner.py`` (``jarvis-packages/clio_relay/clio_relay/mcp_call/
    runner.py``) imports ``clio_relay.bounded_payload`` and must resolve it
    from its own standalone package tree when deployed to a JARVIS worker,
    not from ``src/clio_relay``.
    """
    root = Path(__file__).parents[1]
    source = root / "src" / "clio_relay" / "bounded_payload.py"
    embedded = root / "jarvis-packages" / "clio_relay" / "clio_relay" / "bounded_payload.py"

    assert embedded.read_bytes() == source.read_bytes()
