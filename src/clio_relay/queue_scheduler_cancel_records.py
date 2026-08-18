"""Pure scheduler-cancellation record lookups and ordering."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from clio_relay.models import (
    RelayJob,
    SchedulerCancelDisposition,
    SchedulerCancelDispositionState,
    SchedulerCancelPending,
)


@dataclass(frozen=True, slots=True)
class SchedulerCancelIdentityRegistration:
    """Atomic result for one durable scheduler-cancellation identity registration."""

    record: SchedulerCancelPending
    disposition_created: bool


@dataclass(frozen=True, slots=True)
class SchedulerCancelAttemptClaim:
    """Exclusive durable lease for one external scheduler cancellation attempt."""

    claim_id: str
    scheduler_job_id: str
    provider: str
    attempt: int
    claimed_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class SchedulerCancelConfirmationClaim:
    """Exclusive durable lease for one scheduler cancellation confirmation poll."""

    claim_id: str
    scheduler_job_id: str
    provider: str
    confirmation_attempt: int
    claimed_at: datetime
    expires_at: datetime


def encode_scheduler_cancel_pending(record: SchedulerCancelPending) -> bytes:
    """Encode one pending record exactly as the durable queue store does."""
    return record.model_dump_json(indent=2, exclude_none=True).encode("utf-8")


def decode_scheduler_cancel_pending(payload: bytes) -> SchedulerCancelPending:
    """Decode one pending record exactly as the durable queue store does."""
    return SchedulerCancelPending.model_validate_json(payload)


def scheduler_cancellation_request(job: RelayJob) -> dict[str, object] | None:
    """Decode the exact cancellation-request envelope from job metadata."""
    raw = job.metadata.get("cancellation_request")
    if not isinstance(raw, dict):
        return None
    request = cast(dict[str, object], raw)
    if request.get("schema_version") != "clio-relay.cancellation-request.v1":
        return None
    return request


def cancellation_requested_at(request: dict[str, object]) -> datetime | None:
    """Decode one cancellation request timestamp."""
    raw = request.get("requested_at")
    if not isinstance(raw, str):
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def scheduler_cancel_record_is_due(record: SchedulerCancelPending, now: datetime) -> bool:
    """Return whether a pending record contains cancellation work that is due."""
    if record.complete:
        return False
    if record.identity_resolution == "pending":
        return True
    return any(scheduler_cancel_disposition_is_due(item, now) for item in record.dispositions)


def scheduler_cancel_disposition_is_due(
    disposition: SchedulerCancelDisposition,
    now: datetime,
) -> bool:
    """Return whether one disposition has work not held by a live attempt claim."""
    if disposition.state in {
        SchedulerCancelDispositionState.PENDING,
        SchedulerCancelDispositionState.RETRY_WAIT,
    }:
        if (
            disposition.attempt_claim_id is not None
            and disposition.attempt_claim_expires_at is not None
            and disposition.attempt_claim_expires_at > now
        ):
            return False
        return disposition.next_attempt_at is None or disposition.next_attempt_at <= now
    if disposition.state is not SchedulerCancelDispositionState.CANCEL_REQUESTED:
        return False
    if (
        disposition.confirmation_claim_id is not None
        and disposition.confirmation_claim_expires_at is not None
        and disposition.confirmation_claim_expires_at > now
    ):
        return False
    return disposition.next_attempt_at is None or disposition.next_attempt_at <= now


def scheduler_cancel_due_sort_key(record: SchedulerCancelPending) -> tuple[datetime, str]:
    """Return the stable due-time ordering key for a cancellation record."""
    due_times = [
        item.attempt_claim_expires_at
        or item.confirmation_claim_expires_at
        or item.next_attempt_at
        or record.requested_at
        for item in record.dispositions
        if item.state
        in {
            SchedulerCancelDispositionState.PENDING,
            SchedulerCancelDispositionState.RETRY_WAIT,
            SchedulerCancelDispositionState.CANCEL_REQUESTED,
        }
    ]
    return (min(due_times, default=record.requested_at), record.job_id)
