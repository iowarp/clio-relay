"""Sandbox-browser observation evidence for the secure runtime probe.

Extracted from ``live_acceptance.py`` (#231 rework): the concern of turning
one raw ``live_acceptance_browser_http`` HTTP/SSE response into
:class:`~clio_relay.live_acceptance_models.SecureRuntimeHttpEvidence`,
correlating its decoded document against the application's declared
adapter and expected relay identity, polling until a command's effect is
durably observable, and proving a revoked capability URL is actually
unusable. This is the "what does this response mean" layer above the raw
transport in ``live_acceptance_browser_http``.
"""

from __future__ import annotations

import hashlib
import re
import time
import urllib.parse
from typing import Any, Literal, cast

from clio_relay.browser_gateway import BrowserAttachmentGrant
from clio_relay.errors import RelayError
from clio_relay.jarvis_service_runtime import JarvisServiceRuntimeBinding
from clio_relay.live_acceptance_browser_http import (
    _canonical_finite_json_bytes,
    _direct_browser_http_request,
    _require_media_type,
    _strict_finite_json,
    _strict_sse_data_document,
)
from clio_relay.live_acceptance_models import (
    MAX_SECURE_RUNTIME_RESPONSE_BYTES,
    MAX_SECURE_RUNTIME_SSE_EVENT_BYTES,
    SecureRuntimeEndpointAdapter,
    SecureRuntimeHttpEvidence,
    SecureRuntimeProbeConfig,
    _BrowserHttpRequestError,
    _secure_runtime_canonical_json_sha256,
    _secure_runtime_json_pointer_value,
)
from clio_relay.validation_report import EvidenceReference


def _correlate_secure_runtime_browser_document(
    document: dict[str, Any],
    observation: SecureRuntimeHttpEvidence,
    *,
    endpoint: Literal["health", "state", "command", "events"],
    adapter: SecureRuntimeEndpointAdapter,
    expected_service_instance_id: str,
    expected_execution_id: str,
    expected_dataset_descriptor_sha256: str,
    expected_command_id: str | None,
) -> tuple[SecureRuntimeHttpEvidence, int]:
    """Apply an application-owned adapter and bind selected values to relay identity."""
    for pointer, expected in adapter.assertions.items():
        observed = _secure_runtime_json_pointer_value(
            document,
            pointer,
            label=f"browser {endpoint} assertion",
        )
        if type(observed) is not type(expected) or observed != expected:
            raise RelayError(f"secure runtime browser {endpoint} assertion did not match")
    service_instance_id = _secure_runtime_json_pointer_value(
        document,
        adapter.service_instance_id_pointer,
        label=f"browser {endpoint} service identity",
    )
    if service_instance_id != expected_service_instance_id:
        raise RelayError(f"secure runtime browser {endpoint} changed service identity")
    revision = _secure_runtime_json_pointer_value(
        document,
        adapter.revision_pointer,
        label=f"browser {endpoint} revision",
    )
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
        raise RelayError(f"secure runtime browser {endpoint} omitted a valid revision")
    execution_id: str | None = None
    if adapter.execution_id_pointer is not None:
        selected_execution_id = _secure_runtime_json_pointer_value(
            document,
            adapter.execution_id_pointer,
            label=f"browser {endpoint} execution identity",
        )
        if selected_execution_id != expected_execution_id:
            raise RelayError(f"secure runtime browser {endpoint} changed execution identity")
        execution_id = expected_execution_id
    dataset_descriptor_sha256: str | None = None
    if adapter.dataset_descriptor_pointer is not None:
        descriptor = _secure_runtime_json_pointer_value(
            document,
            adapter.dataset_descriptor_pointer,
            label=f"browser {endpoint} dataset descriptor",
        )
        try:
            dataset_descriptor_sha256 = _secure_runtime_canonical_json_sha256(descriptor)
        except (TypeError, ValueError) as exc:
            raise RelayError(
                f"secure runtime browser {endpoint} dataset descriptor was not finite JSON"
            ) from exc
        if dataset_descriptor_sha256 != expected_dataset_descriptor_sha256:
            raise RelayError(f"secure runtime browser {endpoint} changed dataset identity")
    command_id: str | None = None
    if adapter.command_id_pointer is not None:
        selected_command_id = _secure_runtime_json_pointer_value(
            document,
            adapter.command_id_pointer,
            label=f"browser {endpoint} command identity",
        )
        if not isinstance(selected_command_id, str) or not selected_command_id:
            raise RelayError(f"secure runtime browser {endpoint} omitted command identity")
        command_id = selected_command_id
        if expected_command_id is not None and command_id != expected_command_id:
            raise RelayError(f"secure runtime browser {endpoint} changed command identity")
    return (
        observation.model_copy(
            update={
                "service_instance_id": expected_service_instance_id,
                "execution_id": execution_id,
                "dataset_descriptor_sha256": dataset_descriptor_sha256,
                "command_id": command_id,
                "revision": revision,
            }
        ),
        revision,
    )


def _browser_attachment_capability(grant: BrowserAttachmentGrant) -> str:
    """Require one identical one-time capability across every loopback attachment URL."""
    capabilities: set[str] = set()
    for value in (
        grant.connect_url,
        grant.health_url,
        grant.stream_url,
        grant.events_url,
        grant.state_url,
        grant.command_url,
    ):
        parsed = urllib.parse.urlsplit(value)
        query = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
        if (
            parsed.scheme != "http"
            or parsed.hostname != "127.0.0.1"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or set(query) != {"capability"}
            or len(query["capability"]) != 1
            or not query["capability"][0]
        ):
            raise RelayError("browser attachment returned an invalid capability URL")
        capabilities.add(query["capability"][0])
    if len(capabilities) != 1:
        raise RelayError("browser attachment URLs did not share one exact capability")
    capability = next(iter(capabilities))
    if re.fullmatch(r"[0-9a-f]{64}", capability) is None:
        raise RelayError("browser attachment did not return one 256-bit capability")
    return capability


def _browser_json_observation(
    url: str,
    *,
    endpoint: Literal["health", "state", "command"],
    method: Literal["GET", "POST"],
    body: dict[str, Any] | None,
    timeout_seconds: float,
) -> tuple[SecureRuntimeHttpEvidence, dict[str, Any]]:
    """Call one sandbox-browser JSON surface without persisting its capability URL."""
    encoded: bytes | None = None
    headers = {"Accept": "application/json", "Origin": "null"}
    if body is not None:
        encoded = _canonical_finite_json_bytes(body)
        headers["Content-Type"] = "application/json"
    response = _direct_browser_http_request(
        url,
        method=method,
        headers=headers,
        body=encoded,
        timeout_seconds=timeout_seconds,
        maximum_bytes=MAX_SECURE_RUNTIME_RESPONSE_BYTES,
        stop_after_sse_event=False,
    )
    _require_media_type(response.content_type, expected="application/json")
    decoded = _strict_finite_json(response.payload, label=f"browser {endpoint} response")
    if not isinstance(decoded, dict):
        raise RelayError(f"secure runtime browser {endpoint} response was not an object")
    document = {str(key): value for key, value in cast(dict[object, object], decoded).items()}
    observation = SecureRuntimeHttpEvidence(
        endpoint=endpoint,
        method=method,
        status_code=response.status_code,
        content_type=response.content_type[:256],
        body_sha256=hashlib.sha256(response.payload).hexdigest(),
        body_bytes=len(response.payload),
    )
    return observation, document


def _browser_sse_observation(
    url: str,
    *,
    timeout_seconds: float,
    expected_event_name: str,
) -> tuple[SecureRuntimeHttpEvidence, dict[str, Any]]:
    """Read exactly one bounded SSE event over a fresh browser-capability connection."""
    response = _direct_browser_http_request(
        url,
        method="GET",
        headers={"Accept": "text/event-stream", "Origin": "null"},
        body=None,
        timeout_seconds=timeout_seconds,
        maximum_bytes=MAX_SECURE_RUNTIME_SSE_EVENT_BYTES,
        stop_after_sse_event=True,
    )
    _require_media_type(response.content_type, expected="text/event-stream")
    document = _strict_sse_data_document(
        response.payload,
        expected_event_name=expected_event_name,
    )
    return (
        SecureRuntimeHttpEvidence(
            endpoint="events",
            method="GET",
            status_code=response.status_code,
            content_type=response.content_type[:256],
            body_sha256=hashlib.sha256(response.payload).hexdigest(),
            body_bytes=len(response.payload),
        ),
        document,
    )


def _observe_correlated_browser_triad(
    active_attachment: BrowserAttachmentGrant,
    *,
    config: SecureRuntimeProbeConfig,
    binding: JarvisServiceRuntimeBinding,
    health_command_id: str | None,
    state_command_id: str | None,
    event_command_id: str | None,
    event_name: str,
    timeout_seconds: float,
) -> tuple[
    SecureRuntimeHttpEvidence,
    SecureRuntimeHttpEvidence,
    SecureRuntimeHttpEvidence,
    set[int],
]:
    """Observe and correlate health/state/events, returning all three plus their revisions.

    The same health->state->events shape runs twice in the secure runtime
    acceptance lifecycle -- once before any command (every ``*_command_id``
    is ``None``) and once after reconnect (state/events already carry the
    prior command's identity; health never tracks a command). Each field's
    expected command id is an explicit parameter rather than inferred, so
    this stays a pure projection of the two call sites' existing behavior.
    """
    health, health_document = _browser_json_observation(
        active_attachment.health_url,
        endpoint="health",
        method="GET",
        body=None,
        timeout_seconds=timeout_seconds,
    )
    health, health_revision = _correlate_secure_runtime_browser_document(
        health_document,
        health,
        endpoint="health",
        adapter=config.protocol_adapter.health,
        expected_service_instance_id=binding.service_instance_id,
        expected_execution_id=binding.jarvis_execution_id,
        expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
        expected_command_id=health_command_id,
    )
    state, state_document = _browser_json_observation(
        active_attachment.state_url,
        endpoint="state",
        method="GET",
        body=None,
        timeout_seconds=timeout_seconds,
    )
    state, state_revision = _correlate_secure_runtime_browser_document(
        state_document,
        state,
        endpoint="state",
        adapter=config.protocol_adapter.state,
        expected_service_instance_id=binding.service_instance_id,
        expected_execution_id=binding.jarvis_execution_id,
        expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
        expected_command_id=state_command_id,
    )
    event, event_document = _browser_sse_observation(
        active_attachment.events_url,
        timeout_seconds=timeout_seconds,
        expected_event_name=event_name,
    )
    event, event_revision = _correlate_secure_runtime_browser_document(
        event_document,
        event,
        endpoint="events",
        adapter=config.protocol_adapter.events,
        expected_service_instance_id=binding.service_instance_id,
        expected_execution_id=binding.jarvis_execution_id,
        expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
        expected_command_id=event_command_id,
    )
    return health, state, event, {health_revision, state_revision, event_revision}


def _wait_for_changed_sse_event(
    url: str,
    *,
    previous: SecureRuntimeHttpEvidence,
    require_change: bool,
    timeout_seconds: float,
    poll_seconds: float,
    expected_event_name: str,
) -> tuple[SecureRuntimeHttpEvidence, dict[str, Any]]:
    """Reconnect to SSE until the configured command produces a new event digest."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = max(0.001, deadline - time.monotonic())
        observed, document = _browser_sse_observation(
            url,
            timeout_seconds=min(remaining, 10.0),
            expected_event_name=expected_event_name,
        )
        if not require_change or observed.body_sha256 != previous.body_sha256:
            return observed, document
        if time.monotonic() >= deadline:
            raise RelayError("secure runtime SSE did not change after its command")
        time.sleep(min(poll_seconds, max(0.001, deadline - time.monotonic())))


def _wait_for_changed_browser_state(
    url: str,
    *,
    previous: SecureRuntimeHttpEvidence,
    require_change: bool,
    timeout_seconds: float,
    poll_seconds: float,
) -> tuple[SecureRuntimeHttpEvidence, dict[str, Any]]:
    """Poll browser state until the configured command is durably observable."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = max(0.001, deadline - time.monotonic())
        observed, document = _browser_json_observation(
            url,
            endpoint="state",
            method="GET",
            body=None,
            timeout_seconds=min(remaining, 10.0),
        )
        if not require_change or observed.body_sha256 != previous.body_sha256:
            return observed, document
        if time.monotonic() >= deadline:
            raise RelayError("secure runtime state did not change after its command")
        time.sleep(min(poll_seconds, max(0.001, deadline - time.monotonic())))


def _browser_evidence_reference(
    attachment_id: str,
    observation: SecureRuntimeHttpEvidence,
) -> EvidenceReference:
    """Project a browser observation without including its one-time capability URL."""
    return EvidenceReference(
        kind="secure_runtime_browser_http",
        reference=f"browser-attachment://{attachment_id}/{observation.endpoint}",
        sha256=observation.body_sha256,
        metadata=observation.model_dump(mode="json"),
    )


def _assert_browser_capability_revoked(
    url: str,
    *,
    timeout_seconds: float,
    proxy_stopped: bool,
) -> None:
    """Require explicit denial, or a proven-stopped loopback proxy, for an old grant."""
    try:
        _direct_browser_http_request(
            url,
            method="GET",
            headers={"Accept": "application/json", "Origin": "null"},
            body=None,
            timeout_seconds=max(timeout_seconds, 0.1),
            maximum_bytes=1,
            stop_after_sse_event=False,
        )
    except _BrowserHttpRequestError as exc:
        if exc.kind in {"http_401", "http_403", "http_410"}:
            return
        if proxy_stopped and exc.kind in {"connection_refused", "connection_reset"}:
            return
        raise RelayError(
            f"revoked browser capability failed with non-revocation cause {exc.kind}"
        ) from exc
    raise RelayError("revoked browser capability remained usable")
