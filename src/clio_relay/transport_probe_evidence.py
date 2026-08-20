"""Structured transport-probe evidence assembly.

Split out of ``transport_probe.py`` (iowarp/clio-relay#231): converting a
cleanup resource's outcome into a :class:`TransportCleanupResourceEvidence`
entry, folding a set of resources into one probe evidence line, and
attaching/reading bounded evidence lines on a raised error so a probe
failure still surfaces what cleanup actually observed. Every other
transport-probe owner module builds its cleanup evidence lines through
``_transport_resource_line``/``_process_cleanup_resource`` here rather than
constructing :class:`TransportCleanupResourceEvidence` inline.
"""

from __future__ import annotations

from typing import cast

from clio_relay.errors import RelayError
from clio_relay.validation_report import (
    TransportCleanupAction,
    TransportCleanupOutcome,
    TransportCleanupResourceEvidence,
    TransportProbeEvidence,
    transport_probe_evidence_line,
)

MAX_TRANSPORT_ERROR_EVIDENCE_LINES = 16


def transport_evidence_lines_from_error(error: BaseException) -> list[str]:
    """Return bounded structured transport evidence attached during cleanup."""
    raw = error.__dict__.get("_clio_relay_transport_evidence_lines")
    if not isinstance(raw, list):
        return []
    lines = [item for item in cast(list[object], raw) if isinstance(item, str)]
    return lines[:MAX_TRANSPORT_ERROR_EVIDENCE_LINES]


def _attach_transport_evidence(
    error: BaseException,
    lines: list[str],
) -> BaseException:
    structured = [line for line in lines if line.startswith("transport.probe_evidence=")][
        :MAX_TRANSPORT_ERROR_EVIDENCE_LINES
    ]
    existing = transport_evidence_lines_from_error(error)
    combined = list(dict.fromkeys([*existing, *structured]))[:MAX_TRANSPORT_ERROR_EVIDENCE_LINES]
    try:
        error.__dict__["_clio_relay_transport_evidence_lines"] = combined
    except (AttributeError, TypeError):
        wrapped = RelayError(str(error))
        wrapped.__dict__["_clio_relay_transport_evidence_lines"] = combined
        return wrapped
    return error


def _transport_resource_line(
    *,
    probe_id: str,
    cluster: str,
    cleanup_mode: str,
    resources: list[TransportCleanupResourceEvidence],
) -> str:
    return transport_probe_evidence_line(
        TransportProbeEvidence(
            probe_id=probe_id,
            cluster=cluster,
            cleanup_mode=cleanup_mode,
            resources=resources,
        )
    )


def _process_cleanup_resource(
    *,
    kind: str,
    resource_id: str,
    role: str,
    location: str,
    ownership_verified: bool,
    outcome: TransportCleanupOutcome,
    verified_after_operation: bool,
    observed_state: str | None,
    residual: bool,
    detail: str | None,
    action: TransportCleanupAction = "stop",
    metadata: dict[str, object] | None = None,
) -> TransportCleanupResourceEvidence:
    return TransportCleanupResourceEvidence(
        kind=kind,
        resource_id=resource_id,
        role=role,
        location=location,
        action=action,
        ownership_verified=ownership_verified,
        outcome=outcome,
        verified_after_operation=verified_after_operation,
        observed_state=observed_state,
        residual=residual,
        detail=detail,
        metadata=metadata or {},
    )
