"""Byte-identity and round-trip coverage for CQ4 record/codec owners."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from clio_relay import (
    queue_layout,
    queue_lease_records,
    queue_legacy_output_codec,
    queue_scheduler_cancel_records,
)
from clio_relay.models import (
    JobKind,
    RelayEvent,
    SchedulerCancelDisposition,
    SchedulerCancelDispositionState,
    SchedulerCancelPending,
)


def test_lease_record_codec_is_byte_identical_to_the_parent_facade() -> None:
    """The CQ4 lease owner preserves the pre-move JSON byte ordering and values.

    Post-CQ15 update: the facade's ``_lease_index_document`` re-export is gone
    -- both of its former callers (``queue_lease_indexes``,
    ``queue_lease_recovery``) now call ``queue_lease_records.
    lease_index_document`` directly, so there is no facade copy left to
    compare against. This still pins the exact expected byte string (design
    §4: preserve lookup-site ownership, not a dead facade shim).
    """
    identity = queue_lease_records.LeaseIndexIdentity(
        lease_id="lease-cq4",
        job_id="job-cq4",
        endpoint_id="endpoint-cq4",
        cluster="cluster-cq4",
        job_kind=JobKind.JARVIS,
        expires_at=datetime(2026, 8, 15, 12, 34, 56, 789, tzinfo=UTC),
    )
    owner_document = queue_lease_records.lease_index_document(identity)
    owner_bytes = json.dumps(owner_document).encode("utf-8")
    expected_bytes = (
        b'{"schema_version": "clio-relay.lease-operational-index.v2", '
        b'"lease_id": "lease-cq4", "job_id": "job-cq4", '
        b'"endpoint_id": "endpoint-cq4", "cluster": "cluster-cq4", '
        b'"job_kind": "jarvis", "expires_at": "2026-08-15T12:34:56.000789+00:00"}'
    )

    assert owner_bytes == expected_bytes
    decoded = queue_lease_records.lease_index_identity_from_document(
        json.loads(owner_bytes),
        label="CQ4 lease round trip",
    )
    assert json.dumps(queue_lease_records.lease_index_document(decoded)).encode() == owner_bytes


def test_scheduler_cancel_record_codec_is_byte_identical_to_parent_store_encoding() -> None:
    """The CQ4 scheduler owner preserves the exact pre-move Pydantic wire bytes."""
    requested_at = datetime(2026, 8, 15, 12, tzinfo=UTC)
    record = SchedulerCancelPending(
        job_id="job-cq4",
        cluster="cluster-cq4",
        requested_at=requested_at,
        reason="CQ4 byte proof",
        identity_resolution="resolved",
        dispositions=[
            SchedulerCancelDisposition(
                scheduler_job_id="scheduler-123",
                provider="slurm",
                state=SchedulerCancelDispositionState.RETRY_WAIT,
                attempts=2,
                next_attempt_at=requested_at + timedelta(seconds=30),
                updated_at=requested_at,
            )
        ],
        updated_at=requested_at,
    )
    parent_store_bytes = record.model_dump_json(indent=2, exclude_none=True).encode("utf-8")
    owner_bytes = queue_scheduler_cancel_records.encode_scheduler_cancel_pending(record)

    assert owner_bytes == parent_store_bytes
    decoded = queue_scheduler_cancel_records.decode_scheduler_cancel_pending(owner_bytes)
    assert queue_scheduler_cancel_records.encode_scheduler_cancel_pending(decoded) == owner_bytes


def _legacy_parent_bytes() -> bytes:
    text = "x" * (queue_layout.RECORD_FAMILY_MAX_BYTES["events"] + 1)
    return json.dumps(
        {
            "job_id": "job-cq4",
            "seq": 7,
            "event_type": "stdout.delta",
            "message": text,
            "level": "info",
            "created_at": "2026-08-15T12:00:00Z",
            "payload": {"stream": "stdout", "text": text},
        },
        separators=(",", ":"),
    ).encode("utf-8")


def test_legacy_output_codec_is_byte_identical_to_parent_replacement(
    tmp_path: Path,
) -> None:
    """The CQ4 legacy owner preserves archive replacement and receipt bytes."""
    original_bytes = _legacy_parent_bytes()
    path = tmp_path / "legacy-output.json"
    path.write_bytes(original_bytes)
    original = RelayEvent.model_validate_json(original_bytes)
    original_sha256 = hashlib.sha256(original_bytes).hexdigest()
    archive_relative_path = "legacy_output_archives/job-cq4/00000000000000000007.json"
    expected_replacement = original.model_copy(
        update={
            "message": (
                "Legacy stdout output archived "
                f"({queue_layout.RECORD_FAMILY_MAX_BYTES['events'] + 1} UTF-8 bytes)"
            ),
            "payload": {
                "stream": "stdout",
                "legacy_output": {
                    "schema_version": "clio-relay.legacy-output-compatibility.v1",
                    "archive_path": archive_relative_path,
                    "archive_sha256": original_sha256,
                    "archive_size_bytes": len(original_bytes),
                    "original_message_utf8_bytes": (
                        queue_layout.RECORD_FAMILY_MAX_BYTES["events"] + 1
                    ),
                    "original_payload_text_utf8_bytes": (
                        queue_layout.RECORD_FAMILY_MAX_BYTES["events"] + 1
                    ),
                    "representation": "archive",
                },
            },
        }
    )
    expected_parent_bytes = expected_replacement.model_dump_json(indent=2).encode("utf-8")

    owner_record = queue_legacy_output_codec.decode_v09_legacy_output_record(
        original_bytes,
        job_id="job-cq4",
        seq=7,
        ordinary_limit=queue_layout.RECORD_FAMILY_MAX_BYTES["events"],
        compatibility_schema="clio-relay.legacy-output-compatibility.v1",
    )
    stored_record = queue_legacy_output_codec.read_v09_legacy_output_record(
        path,
        job_id="job-cq4",
        seq=7,
    )

    assert owner_record.original_bytes == original_bytes
    assert owner_record.replacement_bytes == expected_parent_bytes
    assert stored_record.replacement_bytes == owner_record.replacement_bytes
    decoded_replacement = RelayEvent.model_validate_json(owner_record.replacement_bytes)
    assert decoded_replacement.model_dump_json(indent=2).encode("utf-8") == expected_parent_bytes
    expected_receipt = queue_legacy_output_codec.legacy_output_receipt(
        stored_record,
        receipt_schema="clio-relay.legacy-output-receipt.v1",
    )
    owner_receipt = queue_legacy_output_codec.legacy_output_receipt(
        owner_record,
        receipt_schema="clio-relay.legacy-output-receipt.v1",
    )
    assert json.dumps(owner_receipt).encode("utf-8") == json.dumps(expected_receipt).encode("utf-8")
