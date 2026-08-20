"""Pure validation/encode/decode logic for the durable reservation ledger.

This module owns the ledger's on-disk *content* rules -- job id and byte
validation, RFC 3339 UTC timestamp formatting/parsing, and the checksummed
JSON envelope -- independent of how the bytes reach or leave disk (that half
is ``storage_file_io.py``) and independent of the lock/read/write orchestration
around them (that half stays with ``StoragePolicy`` and its reservation-ledger
mixin, ``storage_reservation_ledger.py``). Nothing here performs I/O.

The ledger checksum detects corruption and torn/manual edits. It is not a MAC,
signature, or authenticity proof; filesystem ownership and private permissions
are the trust boundary for local policy state.
"""

# Every function here is a leaf primitive called only from the other owner
# modules composing StoragePolicy, or from tests -- never from within this
# file -- matching http_api.py's own decorator-only-caller precedent.
# pyright: reportUnusedFunction=false

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime
from typing import cast

from clio_relay.identifiers import validate_durable_record_id
from clio_relay.storage_policy_types import (
    _LEDGER_SCHEMA,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage] -- single-owner constant
    ReservationRecord,
    StorageLimits,
    StoragePolicyError,
    StorageReason,
    _LedgerState,  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
)


def _validate_job_id(job_id: str) -> None:
    try:
        validate_durable_record_id(job_id)
    except (TypeError, ValueError) as error:
        raise StoragePolicyError(
            StorageReason.INVALID_REQUEST,
            "job_id must be a lowercase portable durable record ID",
        ) from error


def _validate_reservation_bytes(core_bytes: int, spool_bytes: int, limits: StorageLimits) -> None:
    for name, value in (("core_bytes", core_bytes), ("spool_bytes", spool_bytes)):
        if type(value) is not int or value < 0:
            raise StoragePolicyError(
                StorageReason.INVALID_REQUEST,
                f"{name} must be a non-negative integer",
            )
    total = core_bytes + spool_bytes
    if total == 0:
        raise StoragePolicyError(
            StorageReason.INVALID_REQUEST,
            "a storage reservation must request at least one byte",
        )
    if total > limits.max_job_reservation_bytes:
        raise StoragePolicyError(
            StorageReason.PER_JOB_LIMIT,
            "job storage reservation exceeds the configured per-job limit",
            details={
                "requested_bytes": total,
                "max_job_reservation_bytes": limits.max_job_reservation_bytes,
            },
        )


def _format_timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise StoragePolicyError(
            StorageReason.INVALID_REQUEST,
            "storage policy clock must return a timezone-aware datetime",
        )
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: object) -> str:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "reservation created_at must be an RFC 3339 UTC timestamp",
        )
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "reservation created_at is invalid",
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "reservation created_at must use UTC",
        )
    return value


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _encode_ledger(ledger: _LedgerState) -> bytes:
    payload: dict[str, object] = {
        "schema": _LEDGER_SCHEMA,
        "generation": ledger.generation,
        "reservations": [record.to_dict() for record in ledger.reservations],
    }
    digest = hashlib.sha256(_canonical_json(payload)).hexdigest()
    envelope = {**payload, "checksum": f"sha256:{digest}"}
    return json.dumps(envelope, indent=2, sort_keys=True).encode("utf-8") + b"\n"


def _decode_ledger(value: object, limits: StorageLimits) -> _LedgerState:
    if not isinstance(value, dict):
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "storage reservation ledger has an invalid envelope",
        )
    envelope = cast(dict[object, object], value)
    if set(envelope) != {
        "schema",
        "generation",
        "reservations",
        "checksum",
    }:
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "storage reservation ledger has an invalid envelope",
        )
    if envelope["schema"] != _LEDGER_SCHEMA:
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "storage reservation ledger has an unsupported schema",
        )
    generation = envelope["generation"]
    if type(generation) is not int or generation < 0:
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "storage reservation ledger generation is invalid",
        )
    raw_reservations_value = envelope["reservations"]
    if not isinstance(raw_reservations_value, list):
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "storage reservation ledger records must be a list",
        )
    raw_reservations = cast(list[object], raw_reservations_value)
    if len(raw_reservations) > limits.max_reservations:
        raise StoragePolicyError(
            StorageReason.LEDGER_CAPACITY,
            "storage reservation ledger exceeds its configured record limit",
            details={"max_reservations": limits.max_reservations},
        )
    payload = {
        "schema": envelope["schema"],
        "generation": generation,
        "reservations": raw_reservations,
    }
    checksum = envelope["checksum"]
    expected = "sha256:" + hashlib.sha256(_canonical_json(payload)).hexdigest()
    if not isinstance(checksum, str) or not hmac.compare_digest(checksum, expected):
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "storage reservation ledger checksum does not match its content",
        )
    records: list[ReservationRecord] = []
    seen: set[str] = set()
    for raw_record in raw_reservations:
        if not isinstance(raw_record, dict):
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "storage reservation ledger contains an invalid record",
            )
        record_data = cast(dict[object, object], raw_record)
        if set(record_data) != {
            "job_id",
            "core_bytes",
            "spool_bytes",
            "created_at",
        }:
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "storage reservation ledger contains an invalid record",
            )
        job_id = record_data["job_id"]
        try:
            _validate_job_id(cast(str, job_id))
        except StoragePolicyError as exc:
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "storage reservation ledger contains an invalid job id",
            ) from exc
        if cast(str, job_id) in seen:
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "storage reservation ledger contains duplicate job ids",
            )
        core_bytes = record_data["core_bytes"]
        spool_bytes = record_data["spool_bytes"]
        if type(core_bytes) is not int or core_bytes < 0:
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "storage reservation core_bytes is invalid",
            )
        if type(spool_bytes) is not int or spool_bytes < 0:
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "storage reservation spool_bytes is invalid",
            )
        if core_bytes + spool_bytes == 0:
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "storage reservation cannot be empty",
            )
        if core_bytes + spool_bytes > limits.max_job_reservation_bytes:
            raise StoragePolicyError(
                StorageReason.LEDGER_MALFORMED,
                "stored reservation exceeds the configured per-job limit",
            )
        created_at = _parse_timestamp(record_data["created_at"])
        record = ReservationRecord(
            job_id=cast(str, job_id),
            core_bytes=core_bytes,
            spool_bytes=spool_bytes,
            created_at=created_at,
        )
        records.append(record)
        seen.add(record.job_id)
    if [record.job_id for record in records] != sorted(record.job_id for record in records):
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "storage reservation ledger records are not in canonical order",
        )
    return _LedgerState(generation=generation, reservations=tuple(records))
