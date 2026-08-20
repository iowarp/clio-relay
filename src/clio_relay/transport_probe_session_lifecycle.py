"""SSH-forward transport-probe session lifecycle verification and evidence.

Split out of ``transport_probe.py`` (iowarp/clio-relay#231): the helpers
``run_ssh_forward_http_probe`` (still resident in ``transport_probe.py`` --
see that module's docstring for why) uses to reject a non-ready durable
remote session start, to prove a detach/teardown actually verified the
owned remote relay API's end state before reporting cleanup success, and to
render every session-lifecycle transition (verified or not) as a structured
transport-probe evidence line. None of these names are themselves a
monkeypatch seam in ``tests/test_transport_probe.py``, so a plain re-export
back into ``transport_probe.py`` is enough for its resident caller to keep
resolving them.
"""

from __future__ import annotations

import json
from typing import Literal, cast

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.session_lifecycle import SessionLifecycleReport
from clio_relay.session_wire_models import CleanupResource, OwnedSessionStartResult
from clio_relay.transport_probe_evidence import (
    _attach_transport_evidence,
    _process_cleanup_resource,
    _transport_resource_line,
)
from clio_relay.validation_report import TransportCleanupOutcome, TransportCleanupResourceEvidence


def _remote_session_start_not_ready_error(
    *,
    result: OwnedSessionStartResult,
    definition: ClusterDefinition,
) -> RelayError:
    """Preserve a non-ready durable start operation without opening a connector."""
    status_selector = result.status_selector.model_dump(mode="json")
    retry_selector = result.retry_selector.model_dump(mode="json")
    status_selector_json = json.dumps(status_selector, sort_keys=True, separators=(",", ":"))
    retry_selector_json = json.dumps(retry_selector, sort_keys=True, separators=(",", ":"))
    detail = (
        "owned remote session start is not ready; "
        f"state={result.state}; terminal={str(result.terminal).lower()}; "
        f"start_operation_id={result.start_operation_id}; "
        f"status_selector={status_selector_json}; retry_selector={retry_selector_json}"
    )
    if result.error is not None:
        detail = f"{detail}; observation={result.error}"
    outcome: TransportCleanupOutcome
    if result.state in {"starting", "ambiguous"}:
        outcome = "retained"
    elif result.state == "not_current":
        outcome = "replaced"
    else:
        outcome = "terminal"
    evidence_line = _transport_resource_line(
        probe_id=f"ssh-probe:{result.session_id}:start:{result.start_operation_id}",
        cluster=result.cluster,
        cleanup_mode="transport_probe_start_observation",
        resources=[
            _process_cleanup_resource(
                kind="relay_session_start_operation",
                resource_id=result.start_operation_id,
                role="remote_transport_session_start_operation",
                location=definition.ssh_host,
                action="retain",
                ownership_verified=True,
                outcome=outcome,
                verified_after_operation=True,
                observed_state=result.state,
                residual=False,
                detail=detail,
                metadata={
                    "ownership_scope": "start_operation_selector",
                    "session_id": result.session_id,
                    "session_generation_id": result.session_generation_id,
                    "start_operation_id": result.start_operation_id,
                    "cluster_route_revision": result.cluster_route_revision,
                    "remote_api_port": result.remote_api_port,
                    "terminal": result.terminal,
                    "retryable": result.retryable,
                    "transition_accepted": result.transition_accepted,
                    "transport_deadline_exceeded": result.transport_deadline_exceeded,
                    "status_selector": status_selector,
                    "retry_selector": retry_selector,
                },
            )
        ],
    )
    error = RelayError(detail)
    return cast(RelayError, _attach_transport_evidence(error, [evidence_line]))


def _verified_session_detach_lines(
    report: SessionLifecycleReport,
    *,
    session_id: str,
    session_generation_id: str,
) -> list[str]:
    if (
        report.mode != "detach"
        or report.session_id != session_id
        or report.session_generation_id != session_generation_id
    ):
        raise RelayError("remote session detach identity or generation did not match its start")
    if report.errors or report.residual_resources:
        detail = "; ".join(
            [
                *report.errors,
                *[item.detail or item.resource_id for item in report.residual_resources],
            ]
        )
        raise RelayError(f"remote session detach verification failed: {detail}")
    retained = [
        resource
        for resource in report.resources
        if resource.kind == "remote_relay_api"
        and resource.action == "retain"
        and resource.outcome == "retained"
        and resource.ownership_verified
        and resource.verified_after_operation
        and not resource.residual
    ]
    if not retained:
        raise RelayError("remote session detach did not prove an active owned relay API")
    return [
        "transport.remote_session=retained",
        f"transport.remote_session_resource={retained[0].resource_id}",
        "transport.remote_session_ownership=verified",
        "transport.cleanup=detached",
    ]


def _verified_session_teardown_lines(
    report: SessionLifecycleReport,
    *,
    session_id: str,
    session_generation_id: str,
) -> list[str]:
    """Reject SSH probe cleanup unless the owned remote API is proven absent."""
    identity_verified = (
        report.mode == "teardown"
        and report.session_id == session_id
        and report.session_generation_id == session_generation_id
    )
    prior_status = report.prior_session_status
    post_status = report.post_session_status
    transition_verified = (
        prior_status is not None
        and prior_status.session_generation_id == session_generation_id
        and prior_status.ownership_verified
        and post_status is not None
        and post_status.session_generation_id == session_generation_id
        and not post_status.running
        and post_status.ownership_verified
    )
    remote_apis = [resource for resource in report.resources if resource.kind == "remote_relay_api"]
    valid_outcomes = {"stopped", "missing"}
    verified = (
        identity_verified
        and transition_verified
        and not report.errors
        and not report.residual_resources
        and len(remote_apis) == 1
        and remote_apis[0].action == "stop"
        and remote_apis[0].outcome in valid_outcomes
        and remote_apis[0].ownership_verified
        and remote_apis[0].verified_after_operation
        and not remote_apis[0].residual
        and all(
            _verified_auxiliary_teardown_resource(resource)
            for resource in report.resources
            if resource.kind != "remote_relay_api"
        )
    )
    if not verified:
        raise RelayError(
            "owned SSH probe session cleanup was not verified: "
            + json.dumps(report.json_payload(), sort_keys=True)
        )
    summary = ",".join(f"{resource.kind}:{resource.outcome}" for resource in report.resources)
    return [
        "transport.remote_cleanup=passed",
        f"transport.remote_cleanup_resources={summary}",
        "transport.remote_cleanup_residuals=0",
        "transport.cleanup=passed",
    ]


def _verified_auxiliary_teardown_resource(resource: CleanupResource) -> bool:
    if resource.kind in {"desktop_connector", "remote_connector"}:
        return (
            resource.action == "stop"
            and resource.outcome in {"stopped", "missing"}
            and resource.verified_after_operation
            and not resource.residual
        )
    if resource.kind == "gateway_record":
        return (
            resource.action == "close"
            and resource.outcome == "closed"
            and resource.ownership_verified
            and resource.verified_after_operation
            and not resource.residual
        )
    return not resource.residual and resource.verified_after_operation


def _session_lifecycle_evidence_line(
    report: SessionLifecycleReport,
    *,
    cluster: str,
    session_id: str,
    session_generation_id: str,
) -> str:
    stable_session_id = f"{session_id}:{session_generation_id}"
    probe_id = f"ssh-probe:{stable_session_id}"
    report_detail = "; ".join(report.errors) if report.errors else None
    resources: list[TransportCleanupResourceEvidence] = []
    for resource in report.resources:
        metadata = {
            **resource.metadata,
            "cleanup_kind": resource.kind,
            "session_id": session_id,
            "session_generation_id": session_generation_id,
        }
        if resource.kind == "remote_relay_api":
            session_metadata = {
                **metadata,
                "api_pid": resource.resource_id,
                "prior_session_status": (
                    report.prior_session_status.model_dump(mode="json")
                    if report.prior_session_status is not None
                    else None
                ),
                "post_session_status": (
                    report.post_session_status.model_dump(mode="json")
                    if report.post_session_status is not None
                    else None
                ),
            }
            resources.append(
                _process_cleanup_resource(
                    kind="relay_session",
                    resource_id=stable_session_id,
                    role="remote_transport_session",
                    location=resource.location,
                    action=resource.action,
                    ownership_verified=resource.ownership_verified,
                    outcome=resource.outcome,
                    verified_after_operation=resource.verified_after_operation,
                    observed_state=resource.observed_state,
                    residual=resource.residual,
                    detail=resource.detail or report_detail,
                    metadata=session_metadata,
                )
            )
            resources.append(
                _process_cleanup_resource(
                    kind="relay_process",
                    resource_id=resource.resource_id,
                    role="remote_relay_api_process",
                    location=resource.location,
                    action=resource.action,
                    ownership_verified=resource.ownership_verified,
                    outcome=resource.outcome,
                    verified_after_operation=resource.verified_after_operation,
                    observed_state=resource.observed_state,
                    residual=resource.residual,
                    detail=resource.detail or report_detail,
                    metadata=metadata,
                )
            )
            continue
        canonical_kind = {
            "desktop_connector": "connector",
            "remote_connector": "connector",
            "gateway_record": "gateway_session",
            "worker_service": "relay_worker",
        }.get(resource.kind, resource.kind)
        resources.append(
            _process_cleanup_resource(
                kind=canonical_kind,
                resource_id=resource.resource_id,
                role=f"{resource.kind}:{resource.action}",
                location=resource.location,
                action=resource.action,
                ownership_verified=resource.ownership_verified,
                outcome=resource.outcome,
                verified_after_operation=resource.verified_after_operation,
                observed_state=resource.observed_state,
                residual=resource.residual,
                detail=resource.detail or report_detail,
                metadata=metadata,
            )
        )
    if not resources:
        resources.append(
            _process_cleanup_resource(
                kind="relay_session",
                resource_id=stable_session_id,
                role="remote_transport_session",
                location="remote",
                action="stop" if report.mode == "teardown" else "retain",
                ownership_verified=False,
                outcome="unknown",
                verified_after_operation=False,
                observed_state="running_or_unknown",
                residual=True,
                detail=report_detail or "session lifecycle report omitted resource results",
                metadata={
                    "session_id": session_id,
                    "session_generation_id": session_generation_id,
                },
            )
        )
    return _transport_resource_line(
        probe_id=probe_id,
        cluster=cluster,
        cleanup_mode=(
            "transport_probe_detach" if report.mode == "detach" else "transport_probe_teardown"
        ),
        resources=resources,
    )


def _unverified_session_evidence_line(
    *,
    cluster: str,
    definition: ClusterDefinition,
    session_id: str,
    session_generation_id: str,
    detail: str,
    action: Literal["retain", "stop"],
) -> str:
    stable_session_id = f"{session_id}:{session_generation_id}"
    return _transport_resource_line(
        probe_id=f"ssh-probe:{stable_session_id}",
        cluster=cluster,
        cleanup_mode=(
            "transport_probe_detach" if action == "retain" else "transport_probe_teardown"
        ),
        resources=[
            _process_cleanup_resource(
                kind="relay_session",
                resource_id=stable_session_id,
                role="remote_transport_session",
                location=definition.ssh_host,
                action=action,
                ownership_verified=False,
                outcome="failed",
                verified_after_operation=False,
                observed_state="running_or_unknown",
                residual=True,
                detail=detail,
                metadata={
                    "session_id": session_id,
                    "session_generation_id": session_generation_id,
                },
            )
        ],
    )
