"""Generic assertion, coercion, and evidence-building primitives.

These have no queue-validation-specific behavior of their own -- they are
the shared "cast this loosely-typed JSON-shaped value or fail loudly"
vocabulary every sibling ``live_validation_*`` owner module builds its
checks on top of (bounded-mapping/list coercion, a single ``_require``
assertion, cluster-ownership guards, entry-parameter validation, evidence
excerpting, and combining a primary error with a cleanup error). Moved
verbatim out of ``queue_validation.py`` (iowarp/clio-relay#231-style split);
no behavior changed.
"""

from __future__ import annotations

import json
from typing import cast

from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.live_validation_constants import (
    MAX_VALIDATION_POLL_SECONDS,
    MAX_VALIDATION_SCHEDULER_TIMEOUT_SECONDS,
)
from clio_relay.models import RelayJob
from clio_relay.validation_report import EvidenceReference


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise RelayError(f"{label} is not an object")
    return {str(key): item for key, item in cast(dict[object, object], value).items()}


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise RelayError(f"{label} is not an array")
    return cast(list[object], value)


def _require_cluster(job: RelayJob, cluster: str) -> None:
    if job.cluster != cluster:
        raise ConfigurationError(
            f"job {job.job_id} belongs to cluster {job.cluster}, not requested cluster {cluster}"
        )


def _validate_options(
    *,
    older_than_seconds: int,
    scan_limit: int,
    scheduler_run_seconds: int,
    scheduler_timeout_seconds: float,
    scheduler_poll_seconds: float,
) -> None:
    if older_than_seconds < 1:
        raise ValueError("older_than_seconds must be at least 1")
    if scan_limit < 1:
        raise ValueError("scan_limit must be at least 1")
    if scheduler_run_seconds < 5 or scheduler_run_seconds > 300:
        raise ValueError("scheduler_run_seconds must be between 5 and 300")
    if not 0 < scheduler_timeout_seconds <= MAX_VALIDATION_SCHEDULER_TIMEOUT_SECONDS:
        raise ValueError(
            "scheduler_timeout_seconds must be greater than zero and no more than "
            f"{MAX_VALIDATION_SCHEDULER_TIMEOUT_SECONDS:g}"
        )
    if not 0 < scheduler_poll_seconds <= MAX_VALIDATION_POLL_SECONDS:
        raise ValueError(
            "scheduler_poll_seconds must be greater than zero and no more than "
            f"{MAX_VALIDATION_POLL_SECONDS:g}"
        )


def _combined_error(primary: Exception | None, cleanup: Exception | None) -> Exception | None:
    if primary is None:
        return cleanup
    if cleanup is None:
        return primary
    return RelayError(f"{primary}; additionally, {cleanup}")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RelayError(message)


def _evidence(kind: str, reference: str, payload: dict[str, object]) -> EvidenceReference:
    return EvidenceReference(
        kind=kind,
        reference=reference,
        excerpt=json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")),
    )
