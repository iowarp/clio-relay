"""Pure legacy-output record decoding and encoding."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

from clio_relay.errors import QueueConflictError
from clio_relay.models import RelayEvent


@dataclass(frozen=True, slots=True)
class LegacyOutputRecord:
    """One exact v0.9 output event and its deterministic replacement."""

    original: RelayEvent
    original_bytes: bytes
    original_sha256: str
    archive_relative_path: str
    replacement: RelayEvent
    replacement_bytes: bytes
    replacement_sha256: str
    representation: Literal["payload_text", "archive"]


def decode_v09_legacy_output_record(
    original_bytes: bytes,
    *,
    job_id: str,
    seq: int,
    ordinary_limit: int,
    compatibility_schema: str,
) -> LegacyOutputRecord:
    """Decode one exact oversized v0.9 output event without performing I/O."""
    try:
        raw = json.loads(original_bytes)
    except json.JSONDecodeError as error:
        raise ValueError("legacy output event is not valid JSON") from error
    expected_keys = {
        "job_id",
        "seq",
        "event_type",
        "message",
        "level",
        "created_at",
        "payload",
    }
    if not isinstance(raw, dict):
        raise ValueError("legacy output event has an unknown top-level shape")
    document = cast(dict[str, object], raw)
    if set(document) != expected_keys:
        raise ValueError("legacy output event has an unknown top-level shape")
    payload = document.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("legacy output event payload is not the exact v0.9 shape")
    typed_payload = cast(dict[str, object], payload)
    if set(typed_payload) != {"stream", "text"}:
        raise ValueError("legacy output event payload is not the exact v0.9 shape")
    event_type = document.get("event_type")
    stream = typed_payload.get("stream")
    text = typed_payload.get("text")
    message = document.get("message")
    if (
        event_type not in {"stdout.delta", "stderr.delta"}
        or stream not in {"stdout", "stderr"}
        or event_type != f"{stream}.delta"
        or document.get("level") != "info"
        or not isinstance(text, str)
        or not isinstance(message, str)
        or (text.rstrip("\n") or f"{stream} output") != message
    ):
        raise ValueError("legacy output event is not an exact duplicated v0.9 delta")
    original = RelayEvent.model_validate(document)
    if original.job_id != job_id or original.seq != seq:
        raise ValueError("legacy output filename/content identity mismatch")
    original_sha256 = hashlib.sha256(original_bytes).hexdigest()
    archive_relative_path = (
        Path("legacy_output_archives") / job_id / f"{seq:020d}.json"
    ).as_posix()

    def replacement(representation: Literal["payload_text", "archive"]) -> RelayEvent:
        compatibility = {
            "schema_version": compatibility_schema,
            "archive_path": archive_relative_path,
            "archive_sha256": original_sha256,
            "archive_size_bytes": len(original_bytes),
            "original_message_utf8_bytes": len(message.encode("utf-8")),
            "original_payload_text_utf8_bytes": len(text.encode("utf-8")),
            "representation": representation,
        }
        replacement_message = (
            f"Legacy {stream} output preserved in payload.text "
            f"({len(text.encode('utf-8'))} UTF-8 bytes)"
            if representation == "payload_text"
            else f"Legacy {stream} output archived ({len(text.encode('utf-8'))} UTF-8 bytes)"
        )
        replacement_payload: dict[str, object] = {
            "stream": stream,
            "legacy_output": compatibility,
        }
        if representation == "payload_text":
            replacement_payload["text"] = text
        return original.model_copy(
            update={
                "message": replacement_message,
                "payload": replacement_payload,
            }
        )

    representation: Literal["payload_text", "archive"] = "payload_text"
    replacement_record = replacement(representation)
    replacement_bytes = replacement_record.model_dump_json(indent=2).encode("utf-8")
    if len(replacement_bytes) > ordinary_limit:
        representation = "archive"
        replacement_record = replacement(representation)
        replacement_bytes = replacement_record.model_dump_json(indent=2).encode("utf-8")
    if len(replacement_bytes) > ordinary_limit:
        raise QueueConflictError("legacy output compatibility event exceeds the event limit")
    return LegacyOutputRecord(
        original=original,
        original_bytes=original_bytes,
        original_sha256=original_sha256,
        archive_relative_path=archive_relative_path,
        replacement=replacement_record,
        replacement_bytes=replacement_bytes,
        replacement_sha256=hashlib.sha256(replacement_bytes).hexdigest(),
        representation=representation,
    )


def legacy_output_receipt(
    record: LegacyOutputRecord,
    *,
    receipt_schema: str,
) -> dict[str, object]:
    """Encode one deterministic legacy-output migration receipt."""
    return {
        "schema_version": receipt_schema,
        "job_id": record.original.job_id,
        "seq": record.original.seq,
        "event_type": record.original.event_type,
        "archive_path": record.archive_relative_path,
        "archive_sha256": record.original_sha256,
        "archive_size_bytes": len(record.original_bytes),
        "replacement_sha256": record.replacement_sha256,
        "replacement_size_bytes": len(record.replacement_bytes),
        "representation": record.representation,
    }
