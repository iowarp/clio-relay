"""Validation and serialization for worker job-kind concurrency limits."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from clio_relay.errors import ConfigurationError
from clio_relay.models import JobKind

type KindConcurrencyInput = Mapping[JobKind, object] | Mapping[str, object]


def normalize_kind_concurrency(
    limits: KindConcurrencyInput | None,
) -> dict[JobKind, int]:
    """Return validated job-kind limits keyed by :class:`JobKind`."""
    normalized: dict[JobKind, int] = {}
    for raw_kind, limit in (limits or {}).items():
        try:
            kind = raw_kind if isinstance(raw_kind, JobKind) else JobKind(raw_kind)
        except ValueError as exc:
            expected = ", ".join(kind.value for kind in JobKind)
            raise ConfigurationError(
                f"unknown job kind for worker concurrency: {raw_kind}; expected one of {expected}"
            ) from exc
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ConfigurationError(
                f"worker concurrency limit for {kind.value} must be an integer"
            )
        if limit < 1:
            raise ConfigurationError(
                f"worker concurrency limit for {kind.value} must be at least 1"
            )
        normalized[kind] = limit
    return normalized


def parse_kind_concurrency_options(values: Iterable[str] | None) -> dict[JobKind, int]:
    """Parse repeatable ``KIND=LIMIT`` command-line values."""
    parsed: dict[JobKind, int] = {}
    for value in values or ():
        raw_kind, separator, raw_limit = value.partition("=")
        raw_kind = raw_kind.strip()
        raw_limit = raw_limit.strip()
        if not separator or not raw_kind or not raw_limit:
            raise ConfigurationError("worker kind concurrency entries must use KIND=LIMIT")
        try:
            kind = JobKind(raw_kind)
        except ValueError as exc:
            expected = ", ".join(kind.value for kind in JobKind)
            raise ConfigurationError(
                f"unknown job kind for worker concurrency: {raw_kind}; expected one of {expected}"
            ) from exc
        if kind in parsed:
            raise ConfigurationError(f"worker kind concurrency was repeated for {kind.value}")
        try:
            limit = int(raw_limit)
        except ValueError as exc:
            raise ConfigurationError(
                f"worker concurrency limit for {kind.value} must be an integer"
            ) from exc
        parsed[kind] = limit
    return normalize_kind_concurrency(parsed)


def kind_concurrency_metadata(limits: KindConcurrencyInput | None) -> dict[str, int]:
    """Serialize validated limits for endpoint metadata and status output."""
    normalized = normalize_kind_concurrency(limits)
    return {kind.value: normalized[kind] for kind in JobKind if kind in normalized}
