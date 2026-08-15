import hashlib
import json
import os
import stat
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, NoReturn, cast

from clio_relay import queue_layout, queue_store_lock, queue_store_read
from clio_relay.errors import QueueConflictError
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.models import RelayEvent

LegacyQueueStateError = queue_store_lock.LegacyQueueStateError


def _raise_legacy(
    family: str, path: Path, reason: str, *, cause: BaseException | None = None
) -> NoReturn:
    error = LegacyQueueStateError(family=family, path=path, reason=reason)
    raise error from cause


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


@dataclass(frozen=True, slots=True)
class LegacyOutputAudit:
    """Bounded totals established before any legacy-output migration write."""

    marker_complete: bool
    event_records: int
    migration_records: int
    archive_bytes: int
    migration_keys: tuple[tuple[str, int], ...] = ()
    receipt_manifest_sha256: str | None = None


def marker_path(storage_root: Path) -> Path:
    """Return the durable legacy-output completion marker path."""
    return storage_root / "migrations" / "legacy-output-v1.json"


def archive_path(storage_root: Path, job_id: str, seq: int) -> Path:
    """Return one immutable legacy-output archive path."""
    return storage_root / "legacy_output_archives" / job_id / f"{seq:020d}.json"


def receipt_path(storage_root: Path, job_id: str, seq: int) -> Path:
    """Return one active legacy-output receipt path."""
    return storage_root / "legacy_output_receipts" / job_id / f"{seq:020d}.json"


def _require_regular_json(family: str, path: Path) -> None:
    try:
        details = os.lstat(path)
    except OSError as error:
        reason = f"cannot inspect canonical record: {type(error).__name__}"
        _raise_legacy(family, path, reason, cause=error)
    if (
        not path.name.endswith(".json")
        or not stat.S_ISREG(details.st_mode)
        or queue_layout.record_is_reparse(details)
    ):
        _raise_legacy(family, path, "canonical record is not an owned .json regular file")


def _require_durable_id(family: str, path: Path, value: object) -> str:
    if not isinstance(value, str):
        _raise_legacy(family, path, "canonical identity is not a string")
    try:
        return validate_durable_record_id(value)
    except ValueError as error:
        _raise_legacy(family, path, f"canonical identity is not portable: {error}", cause=error)


def iter_legacy_event_paths(
    storage_root: Path,
    family: str,
    *,
    max_directories: int,
    max_records: int,
) -> Iterable[tuple[Path, str, int]]:
    """Iterate bounded legacy event paths through the codec-owned lookup seam."""
    directory = queue_store_lock.require_legacy_family_directory(storage_root, family)
    if directory is None:
        return
    directory_count = record_count = 0
    try:
        with os.scandir(directory) as identity_entries:
            for identity_entry in identity_entries:
                directory_count += 1
                identity_directory = Path(identity_entry.path)
                if directory_count > max_directories:
                    reason = (
                        "event identity directories exceed the bounded legacy audit limit of "
                        f"{max_directories}"
                    )
                    _raise_legacy(family, directory, reason)
                try:
                    details = os.lstat(identity_directory)
                except OSError as error:
                    reason = f"cannot inspect event identity directory: {type(error).__name__}"
                    _raise_legacy(family, identity_directory, reason, cause=error)
                if not stat.S_ISDIR(details.st_mode) or queue_layout.record_is_reparse(details):
                    _raise_legacy(
                        family, identity_directory, "event identity entry is not an owned directory"
                    )
                identity = _require_durable_id(family, identity_directory, identity_directory.name)
                try:
                    with os.scandir(identity_directory) as event_entries:
                        for event_entry in event_entries:
                            record_count += 1
                            path = Path(event_entry.path)
                            if record_count > max_records:
                                reason = (
                                    "event family exceeds the bounded legacy audit limit of "
                                    f"{max_records} records"
                                )
                                _raise_legacy(family, identity_directory, reason)
                            _require_regular_json(family, path)
                            sequence_text = path.name.removesuffix(".json")
                            if (
                                len(sequence_text) != 20
                                or not sequence_text.isascii()
                                or not sequence_text.isdigit()
                            ):
                                _raise_legacy(
                                    family,
                                    path,
                                    "event filename is not a canonical 20-digit sequence",
                                )
                            yield path, identity, int(sequence_text)
                except OSError as error:
                    reason = f"cannot scan event identity directory: {type(error).__name__}"
                    _raise_legacy(family, identity_directory, reason, cause=error)
    except OSError as error:
        reason = f"cannot scan canonical event family: {type(error).__name__}"
        _raise_legacy(family, directory, reason, cause=error)


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


def _is_sha256_digest(value: object) -> bool:
    digest = value if isinstance(value, str) else ""
    return len(digest) == 64 and all(character in "0123456789abcdef" for character in digest)


def _read_unique_json_document(path: Path) -> object:
    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        document: dict[str, object] = {}
        for key, value in pairs:
            if key in document:
                raise QueueConflictError(f"duplicate JSON key {key!r} in {path}")
            document[key] = value
        return document

    try:
        payload = queue_store_read.read_bounded_record_bytes(path)
        return json.loads(payload, object_pairs_hook=unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise QueueConflictError(f"invalid JSON record {path}") from error


def read_legacy_output_marker(storage_root: Path) -> LegacyOutputAudit | None:
    """Read and validate the immutable legacy-output completion marker."""
    path = marker_path(storage_root)
    if queue_store_read.path_lstat(path) is None:
        return None
    if queue_store_lock.require_legacy_family_directory(storage_root, "migrations") is None:
        _raise_legacy("migrations", path, "legacy-output marker has no owned migrations directory")
    try:
        raw = _read_unique_json_document(path)
    except (OSError, ValueError, QueueConflictError) as error:
        _raise_legacy(
            "migrations",
            path,
            f"legacy-output marker is invalid: {type(error).__name__}",
            cause=error,
        )
    expected_keys = {
        "schema_version",
        "complete",
        "event_records",
        "migration_records",
        "archive_bytes",
        "receipt_manifest_sha256",
    }
    if not isinstance(raw, dict) or set(cast(dict[str, object], raw)) != expected_keys:
        _raise_legacy("migrations", path, "legacy-output marker has an unknown schema shape")
    marker = cast(dict[str, object], raw)
    event_records = marker.get("event_records")
    migration_records = marker.get("migration_records")
    archive_bytes = marker.get("archive_bytes")
    manifest = marker.get("receipt_manifest_sha256")
    if (
        marker.get("schema_version") != queue_layout.LEGACY_OUTPUT_MIGRATION_SCHEMA
        or marker.get("complete") is not True
        or isinstance(event_records, bool)
        or not isinstance(event_records, int)
        or not 0 <= event_records <= queue_layout.MAX_LEGACY_EVENT_AUDIT_RECORDS
        or isinstance(migration_records, bool)
        or not isinstance(migration_records, int)
        or not 0 <= migration_records <= queue_layout.MAX_LEGACY_OUTPUT_MIGRATION_RECORDS
        or isinstance(archive_bytes, bool)
        or not isinstance(archive_bytes, int)
        or not 0 <= archive_bytes <= queue_layout.MAX_LEGACY_OUTPUT_MIGRATION_BYTES
        or not _is_sha256_digest(manifest)
    ):
        _raise_legacy("migrations", path, "legacy-output marker fields are invalid")
    for family in (
        "legacy_output_archives",
        "legacy_output_receipts",
        "legacy_output_retired",
    ):
        if queue_store_lock.require_legacy_family_directory(storage_root, family) is None:
            _raise_legacy(
                family,
                storage_root / family,
                "legacy-output completion marker requires its owned record directory",
            )
    return LegacyOutputAudit(
        marker_complete=True,
        event_records=event_records,
        migration_records=migration_records,
        archive_bytes=archive_bytes,
        receipt_manifest_sha256=cast(str, manifest),
    )


def read_v09_legacy_output_record(
    path: Path,
    *,
    job_id: str,
    seq: int,
) -> LegacyOutputRecord:
    """Read and decode one exact oversized v0.9 output record."""
    details = os.lstat(path)
    ordinary_limit = queue_layout.RECORD_FAMILY_MAX_BYTES["events"]
    if details.st_size <= ordinary_limit:
        raise ValueError("legacy output source is not oversized")
    if details.st_size > queue_layout.MAX_LEGACY_OUTPUT_RECORD_BYTES:
        raise QueueConflictError(
            "legacy output event exceeds the bounded compatibility limit of "
            f"{queue_layout.MAX_LEGACY_OUTPUT_RECORD_BYTES} bytes"
        )
    original_bytes = queue_store_read.read_bounded_record_bytes_once(
        path,
        limit=queue_layout.MAX_LEGACY_OUTPUT_RECORD_BYTES,
    )
    return decode_v09_legacy_output_record(
        original_bytes,
        job_id=job_id,
        seq=seq,
        ordinary_limit=ordinary_limit,
        compatibility_schema=queue_layout.LEGACY_OUTPUT_COMPATIBILITY_SCHEMA,
    )


def validate_legacy_output_archive(path: Path, record: LegacyOutputRecord) -> None:
    """Require an archive to match the original event bytes exactly."""
    try:
        archived = queue_store_read.read_bounded_record_bytes(path)
    except (OSError, QueueConflictError) as error:
        _raise_legacy(
            "legacy_output_archives",
            path,
            f"legacy output archive is invalid: {type(error).__name__}",
            cause=error,
        )
    if archived != record.original_bytes:
        _raise_legacy(
            "legacy_output_archives",
            path,
            "legacy output archive does not exactly match its original event",
        )


def read_legacy_output_receipt_document(
    path: Path,
    *,
    job_id: str,
    seq: int,
) -> dict[str, object]:
    """Read one self-validating active or retired migration receipt."""
    family = queue_layout.record_family(path)
    try:
        raw = queue_store_read.read_json_document(path)
    except (OSError, ValueError, QueueConflictError) as error:
        _raise_legacy(
            family, path, f"legacy output receipt is invalid: {type(error).__name__}", cause=error
        )
    expected_keys = {
        "schema_version",
        "job_id",
        "seq",
        "event_type",
        "archive_path",
        "archive_sha256",
        "archive_size_bytes",
        "replacement_sha256",
        "replacement_size_bytes",
        "representation",
    }
    if not isinstance(raw, dict) or set(cast(dict[str, object], raw)) != expected_keys:
        _raise_legacy(family, path, "legacy output receipt has an unknown schema shape")
    receipt = cast(dict[str, object], raw)
    archive_size = receipt.get("archive_size_bytes")
    replacement_size = receipt.get("replacement_size_bytes")
    expected_archive_path = (
        Path("legacy_output_archives") / job_id / f"{seq:020d}.json"
    ).as_posix()
    event_limit = queue_layout.RECORD_FAMILY_MAX_BYTES["events"]
    if (
        receipt.get("schema_version") != queue_layout.LEGACY_OUTPUT_RECEIPT_SCHEMA
        or receipt.get("job_id") != job_id
        or receipt.get("seq") != seq
        or receipt.get("event_type") not in {"stdout.delta", "stderr.delta"}
        or receipt.get("archive_path") != expected_archive_path
        or not _is_sha256_digest(receipt.get("archive_sha256"))
        or isinstance(archive_size, bool)
        or not isinstance(archive_size, int)
        or not event_limit < archive_size <= queue_layout.MAX_LEGACY_OUTPUT_RECORD_BYTES
        or not _is_sha256_digest(receipt.get("replacement_sha256"))
        or isinstance(replacement_size, bool)
        or not isinstance(replacement_size, int)
        or not 0 < replacement_size <= event_limit
        or receipt.get("representation") not in {"payload_text", "archive"}
    ):
        _raise_legacy(family, path, "legacy output receipt fields are invalid")
    return receipt


def validate_legacy_output_receipt(path: Path, record: LegacyOutputRecord) -> None:
    """Require a receipt to match its archive and replacement record."""
    raw = read_legacy_output_receipt_document(
        path,
        job_id=record.original.job_id,
        seq=record.original.seq,
    )
    if raw != legacy_output_receipt(
        record,
        receipt_schema=queue_layout.LEGACY_OUTPUT_RECEIPT_SCHEMA,
    ):
        _raise_legacy(
            queue_layout.record_family(path),
            path,
            "legacy output receipt does not match archive and replacement",
        )


def legacy_output_receipt_manifest_sha256(
    receipt_paths: dict[tuple[str, int], Path],
) -> str:
    """Hash exact immutable receipt bytes independently of active/retired location."""
    digest = hashlib.sha256()
    for (job_id, seq), path in sorted(receipt_paths.items()):
        identity = f"{job_id}\0{seq:020d}\0".encode()
        receipt_bytes = queue_store_read.read_bounded_record_bytes(path)
        digest.update(len(identity).to_bytes(8, "big"))
        digest.update(identity)
        digest.update(len(receipt_bytes).to_bytes(8, "big"))
        digest.update(receipt_bytes)
    return digest.hexdigest()
