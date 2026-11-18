"""What relay shows of a service: its endpoint without secrets, and a short status line."""

from __future__ import annotations

from typing import Any

from clio_relay.jobs.handles import Endpoint
from clio_relay.jobs.records import OperationRecord, OperationState


def public_endpoint(endpoint: Endpoint) -> dict[str, Any]:
    """Return what the record keeps of an endpoint: no port URLs.

    A port URL may carry a secret, so only the serve hook's ``ui_data`` hands it
    out, in the serve result's ``_meta`` (RL22).
    """
    return endpoint.model_dump(mode="json", exclude={"ports": {"__all__": {"url"}}})


def service_ready(record: OperationRecord) -> bool:
    """Return whether a service operation is running and has an endpoint."""
    return (
        record.kind == "serve"
        and record.state == OperationState.RUNNING
        and bool((record.result or {}).get("endpoint"))
    )


def endpoint_summary(record: OperationRecord) -> str | None:
    """Name a service's ports as ``name host:port``. No URLs, which may carry secrets."""
    endpoint: dict[str, Any] = (record.result or {}).get("endpoint") or {}
    ports: list[dict[str, Any]] = endpoint.get("ports") or []
    if not ports:
        return None
    return ", ".join(f"{port['name']} {port['host']}:{port['port']}" for port in ports)


def service_message(record: OperationRecord) -> str | None:
    """Return the latest word on a service that is not ready, or ``None``.

    It is the serve hook's latest ``JobStatus.message``, or why relay could not
    observe the service, so a refused or hung start is visible in the task status.
    """
    if record.kind != "serve" or service_ready(record):
        return None
    message = (record.result or {}).get("message")
    return message if isinstance(message, str) and message else None
