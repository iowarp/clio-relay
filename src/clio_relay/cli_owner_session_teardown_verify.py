"""``_verify_owner_session_teardown`` (iowarp/clio-relay#231
continuation): the final cross-check that every relay job, scheduler
job, and gateway resource ``session teardown`` touched is accounted
for before the command reports success."""

from __future__ import annotations

from collections import Counter

import clio_relay.cli_owned_scheduler_cancel as cli_owned_scheduler_cancel
from clio_relay.errors import RelayError
from clio_relay.session_lifecycle import (
    SessionLifecycleReport,
    cleanup_connectors_cover_gateways,
)


def _verify_owner_session_teardown(
    report: SessionLifecycleReport,
    *,
    session_id: str,
    session_generation_id: str,
    stop_worker: bool,
) -> None:
    """Reject closure unless all requested owner-session cleanup is verified."""
    if report.mode != "teardown" or report.session_id != session_id:
        raise RelayError("session teardown report identity did not match the requested session")
    if report.session_generation_id != session_generation_id:
        raise RelayError("session teardown report generation did not match the quiesced generation")
    if report.errors:
        raise RelayError("session teardown reported errors: " + "; ".join(report.errors))
    if report.residual_resources:
        residual_ids = sorted(resource.resource_id for resource in report.residual_resources)
        raise RelayError(
            "session teardown left requested residual resources: " + ", ".join(residual_ids)
        )

    policy = report.cleanup_policy
    expected_policy_keys = {"stop_worker", "cancel_jobs", "cancel_scheduler_jobs"}
    if set(policy) != expected_policy_keys or any(type(policy[key]) is not bool for key in policy):
        raise RelayError("session teardown cleanup policy is incomplete or invalid")
    cancel_jobs = policy["cancel_jobs"]
    cancel_scheduler_jobs = policy["cancel_scheduler_jobs"]
    if policy["stop_worker"] is not stop_worker:
        raise RelayError("session teardown worker policy did not match the requested cleanup")
    if cancel_scheduler_jobs and not cancel_jobs:
        raise RelayError("session teardown scheduler cancellation requires relay cancellation")
    if report.relay_cancel_requested is not cancel_jobs:
        raise RelayError("session teardown relay-job disposition did not match cleanup policy")
    if report.scheduler_cancel_requested is not cancel_scheduler_jobs:
        raise RelayError("session teardown scheduler disposition did not match cleanup policy")

    allowed_resource_kinds = {
        "browser_proxy",
        "desktop_connector",
        "gateway_record",
        "relay_job",
        "remote_connector",
        "remote_relay_api",
        "remote_session_files",
        "scheduler_job",
        "scheduler_sentinel",
        "worker_service",
    }
    unknown_kinds = sorted(
        {resource.kind for resource in report.resources} - allowed_resource_kinds
    )
    if unknown_kinds:
        raise RelayError(
            "session teardown reported unknown cleanup resource kinds: " + ", ".join(unknown_kinds)
        )
    resource_keys = [(resource.kind, resource.resource_id) for resource in report.resources]
    duplicate_resource_keys = sorted(
        f"{kind}:{resource_id}"
        for (kind, resource_id), count in Counter(resource_keys).items()
        if count != 1
    )
    if duplicate_resource_keys:
        raise RelayError(
            "session teardown reported duplicate cleanup resources: "
            + ", ".join(duplicate_resource_keys)
        )

    prior_status = report.prior_session_status
    post_status = report.post_session_status
    if (
        prior_status is None
        or prior_status.session_generation_id != session_generation_id
        or not prior_status.ownership_verified
    ):
        raise RelayError("session teardown did not prove prior generation ownership")
    if (
        post_status is None
        or post_status.session_generation_id != session_generation_id
        or post_status.running
        or not post_status.ownership_verified
    ):
        raise RelayError("session teardown did not prove the owned API generation stopped")

    api_resources = [
        resource for resource in report.resources if resource.kind == "remote_relay_api"
    ]
    if len(api_resources) != 1:
        raise RelayError("session teardown must contain exactly one remote relay API result")
    api_resource = api_resources[0]
    if not (
        api_resource.action == "stop"
        and api_resource.outcome in {"stopped", "missing"}
        and api_resource.ownership_verified
        and api_resource.verified_after_operation
        and not api_resource.residual
    ):
        raise RelayError("session teardown did not verify remote relay API cleanup")

    session_file_resources = [
        resource for resource in report.resources if resource.kind == "remote_session_files"
    ]
    if len(session_file_resources) != 1:
        raise RelayError("session teardown must contain exactly one remote session-file result")
    session_files = session_file_resources[0]
    if not (
        session_files.resource_id == f"{session_id}:{session_generation_id}"
        and session_files.action == "close"
        and session_files.outcome == "closed"
        and session_files.ownership_verified
        and session_files.verified_after_operation
        and not session_files.residual
    ):
        raise RelayError("session teardown did not verify remote session-file cleanup")

    gateway_resources = [
        resource for resource in report.resources if resource.kind == "gateway_record"
    ]
    relay_resource_ids = {
        resource.resource_id for resource in report.resources if resource.kind == "relay_job"
    }
    gateway_resource_ids = {resource.resource_id for resource in gateway_resources}
    connector_resources = [
        resource
        for resource in report.resources
        if resource.kind in {"desktop_connector", "remote_connector"}
    ]
    if (gateway_resources or connector_resources) and not cleanup_connectors_cover_gateways(
        connector_resources,
        gateway_resources,
        mode="teardown",
    ):
        raise RelayError(
            "session teardown connector evidence did not cover each owned gateway exactly"
        )

    browser_resources = [
        resource for resource in report.resources if resource.kind == "browser_proxy"
    ]
    for resource in browser_resources:
        linked_gateway_id = resource.metadata.get("gateway_session_id")
        if not (
            isinstance(linked_gateway_id, str)
            and linked_gateway_id in gateway_resource_ids
            and resource.action == "stop"
            and resource.outcome in {"stopped", "missing"}
            and resource.ownership_verified
            and resource.verified_after_operation
            and not resource.residual
        ):
            raise RelayError(
                f"session teardown did not verify browser proxy cleanup: {resource.resource_id}"
            )

    for resource in report.resources:
        if resource.kind in {"desktop_connector", "remote_connector"} and not (
            resource.action == "stop"
            and resource.outcome in {"stopped", "missing"}
            and resource.ownership_verified
            and resource.verified_after_operation
            and not resource.residual
        ):
            raise RelayError(
                f"session teardown did not verify connector cleanup: {resource.resource_id}"
            )
        if resource.kind == "gateway_record" and not (
            resource.action == "close"
            and resource.outcome == "closed"
            and resource.ownership_verified
            and resource.verified_after_operation
            and not resource.residual
        ):
            raise RelayError(
                f"session teardown did not verify gateway closure: {resource.resource_id}"
            )
        if resource.kind == "relay_job":
            retained = resource.action == "retain" and resource.outcome in {
                "retained",
                "terminal",
            }
            canceled = resource.action == "cancel" and resource.outcome in {
                "canceled",
                "terminal",
            }
            disposition_matches_policy = (retained and not cancel_jobs) or (
                cancel_jobs
                and (canceled or (resource.action == "retain" and resource.outcome == "terminal"))
            )
            if not (
                disposition_matches_policy
                and resource.ownership_verified
                and resource.verified_after_operation
                and not resource.residual
            ):
                raise RelayError(
                    f"session teardown relay-job disposition contradicted cleanup policy: "
                    f"{resource.resource_id}"
                )
        if resource.kind == "scheduler_job":
            linked_relay_id = resource.metadata.get("relay_job_id")
            linked_gateway_id = resource.metadata.get("gateway_session_id")
            linked = (
                isinstance(linked_relay_id, str) and linked_relay_id in relay_resource_ids
            ) or (isinstance(linked_gateway_id, str) and linked_gateway_id in gateway_resource_ids)
            retained = resource.action == "retain" and (
                (
                    resource.outcome == "retained"
                    and resource.observed_state in {"submitted", "pending", "allocated", "running"}
                )
                or (
                    resource.outcome == "terminal"
                    and resource.observed_state in {"completed", "failed", "canceled"}
                )
                or (resource.outcome == "missing" and resource.observed_state == "missing")
            )
            canceled = resource.action == "cancel" and (
                (resource.outcome == "canceled" and resource.observed_state == "canceled")
                or (
                    resource.outcome == "terminal"
                    and resource.observed_state in {"completed", "failed"}
                )
            )
            if not (
                linked
                and resource.provider is not None
                and (
                    (retained and not cancel_scheduler_jobs) or (canceled and cancel_scheduler_jobs)
                )
                and resource.ownership_verified
                and resource.verified_after_operation
                and not resource.residual
            ):
                raise RelayError(
                    f"session teardown did not verify scheduler disposition: {resource.resource_id}"
                )

        if resource.kind == "scheduler_sentinel" and not (
            cancel_jobs
            and cancel_scheduler_jobs
            and resource.action == "retain"
            and resource.outcome == "retained"
            and not resource.ownership_verified
            and resource.verified_after_operation
            and resource.observed_state
            in cli_owned_scheduler_cancel.SCHEDULER_SENTINEL_PRESERVED_PHASES
            and not resource.residual
            and resource.metadata.get("unowned_sentinel") is True
            and resource.metadata.get("preservation_verified") is True
        ):
            raise RelayError(
                "session teardown did not verify scheduler sentinel preservation: "
                f"{resource.resource_id}"
            )

    worker_resources = [
        resource for resource in report.resources if resource.kind == "worker_service"
    ]
    if stop_worker:
        if len(worker_resources) != 1:
            raise RelayError("session teardown must contain exactly one worker service result")
        worker = worker_resources[0]
        if not (
            worker.action == "stop"
            and worker.outcome in {"stopped", "missing"}
            and worker.ownership_verified
            and worker.verified_after_operation
            and worker.observed_state in {"inactive", "not-found"}
            and not worker.residual
        ):
            raise RelayError("session teardown did not verify worker service inactivity")
    elif worker_resources:
        raise RelayError("session teardown reported worker cleanup when it was not requested")
