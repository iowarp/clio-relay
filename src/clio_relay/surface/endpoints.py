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


def address(port: dict[str, Any]) -> str:
    """Return a recorded port's `address`; a record older than the field has host and port."""
    found = port.get("address")
    if isinstance(found, str) and found:
        return found
    host = str(port["host"])
    return f"[{host}]:{port['port']}" if ":" in host else f"{host}:{port['port']}"


def service_ready(record: OperationRecord) -> bool:
    """Return whether a service operation is running and has an endpoint."""
    return (
        record.kind == "serve"
        and record.state == OperationState.RUNNING
        and bool((record.result or {}).get("endpoint"))
    )


def service_served(record: OperationRecord) -> bool:
    """Return whether a service operation has been ready once (``result.ready_at``).

    A serve task completes then, with the endpoint as its result, since a task
    client is woken by a status change and not by a new status message. The
    service keeps running until it is stopped or ends; the operation says so.
    """
    return record.kind == "serve" and bool((record.result or {}).get("ready_at"))


def service_error(record: OperationRecord) -> dict[str, Any] | None:
    """Return why relay cannot bring a running service to the desktop, or ``None``.

    A serve task ends then too, as an error carrying the typed reason, so the
    client wakes and need not wait for the job to end. The job runs on until it
    is stopped or ends.
    """
    found = (record.result or {}).get("service_error")
    if record.kind != "serve" or not isinstance(found, dict):
        return None
    return found  # pyright: ignore[reportUnknownVariableType]


def service_done(record: OperationRecord) -> bool:
    """Return whether a serve task has its answer: the service is served, or cannot be."""
    return service_served(record) or service_error(record) is not None


def endpoint_summary(record: OperationRecord) -> str | None:
    """Name a service's ports as ``name host:port``. No URLs, which may carry secrets."""
    endpoint: dict[str, Any] = (record.result or {}).get("endpoint") or {}
    ports: list[dict[str, Any]] = endpoint.get("ports") or []
    if not ports:
        return None
    return ", ".join(f"{port['name']} {address(port)}" for port in ports)


def service_message(record: OperationRecord) -> str | None:
    """Return the latest word on a service that is not ready, or ``None``.

    It is the serve hook's latest ``JobStatus.message``, or why relay could not
    observe the service, so a refused or hung start is visible in the task status.
    """
    if record.kind != "serve" or service_ready(record):
        return None
    message = (record.result or {}).get("message")
    return message if isinstance(message, str) and message else None
