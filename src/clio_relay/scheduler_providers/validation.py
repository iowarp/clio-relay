"""General-purpose validators shared across scheduler providers.

These validators are not specific to any one scheduler backend (contrast
with ``slurm_connector.py``'s SLURM-allocation/connector-step validators,
which only that module's methods ever call).
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

from clio_relay.errors import ConfigurationError

from .constants import SCHEDULER_RECONCILIATION_MAX_AGE

_SCHEDULER_JOB_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:+-]*$")
_VALIDATION_JOB_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


def _validate_scheduler_job_id(value: str) -> None:
    if not _SCHEDULER_JOB_ID.fullmatch(value):
        raise ConfigurationError(f"invalid scheduler job id: {value!r}")


def _validate_validation_job_name(value: str) -> None:
    if not _VALIDATION_JOB_NAME.fullmatch(value):
        raise ConfigurationError(f"invalid scheduler validation job name: {value!r}")


def _validate_reconciliation_marker(value: str) -> None:
    if not value.startswith("clio-relay-") or not _VALIDATION_JOB_NAME.fullmatch(value):
        raise ConfigurationError(f"invalid scheduler reconciliation marker: {value!r}")


def _validate_scheduler_user(value: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", value):
        raise ConfigurationError(f"invalid scheduler reconciliation user: {value!r}")


def _validate_reconciliation_time(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ConfigurationError("scheduler reconciliation time must include a timezone")
    normalized = value.astimezone(UTC)
    now = datetime.now(UTC)
    if normalized > now + timedelta(minutes=5):
        raise ConfigurationError("scheduler reconciliation time is in the future")
    if normalized < now - SCHEDULER_RECONCILIATION_MAX_AGE:
        raise ConfigurationError("scheduler reconciliation intent exceeded its history window")
    return normalized


def _parse_slurm_reconciliation_time(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        local_timezone = datetime.now().astimezone().tzinfo
        parsed = parsed.replace(tzinfo=local_timezone)
    return parsed.astimezone(UTC)


def _normalize_provider_name(value: str) -> str:
    normalized = value.strip().lower().replace("_", "-")
    if not re.fullmatch(r"[a-z][a-z0-9-]*", normalized):
        raise ConfigurationError(f"invalid scheduler provider name: {value!r}")
    return normalized
