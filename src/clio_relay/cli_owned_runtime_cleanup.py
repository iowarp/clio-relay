"""Owned gateway/runtime session cleanup (iowarp/clio-relay#231
continuation): the desktop/remote connector and gateway-record cleanup
passes ``session teardown`` runs alongside relay-job cancellation."""

from __future__ import annotations

import json
from typing import Any, Literal, cast

import clio_relay.cli_owned_relay_jobs as cli_owned_relay_jobs
import clio_relay.cli_owned_scheduler_cancel as cli_owned_scheduler_cancel
import clio_relay.cli_remote_collection_pagination as cli_remote_collection_pagination
import clio_relay.core_queue as core_queue
import clio_relay.remote_cli as remote_cli
import clio_relay.service_runtime as service_runtime
import clio_relay.storage_runtime as storage_runtime
from clio_relay.cluster_config import (
    ClusterDefinition,
)
from clio_relay.config import RelaySettings
from clio_relay.errors import RelayError
from clio_relay.models import (
    GatewaySessionState,
)
from clio_relay.session_lifecycle import (
    CleanupResource,
    SessionLifecycleReport,
)

MAX_OWNER_GATEWAY_CLEANUP_PASSES = 4


def _cleanup_owned_runtime_sessions(
    *,
    cluster: str,
    definition: ClusterDefinition,
    owner_session_id: str,
    owner_session_generation_id: str | None = None,
    mode: Literal["detach", "teardown"],
    cancel_scheduler_jobs: bool,
    scheduler_sentinel_ids: tuple[str, ...] = (),
    owned_jobs: list[cli_owned_relay_jobs._OwnedRelayJob] | None = None,
) -> list[dict[str, object]]:
    """Clean exact owned gateways and rescan boundedly until admission is stable."""
    queue = storage_runtime.storage_managed_queue(RelaySettings.from_env())
    reports: list[dict[str, object]] = []
    if mode == "detach":
        target_ids = _owned_runtime_gateway_ids_needing_cleanup(
            queue=queue,
            definition=definition,
            cluster=cluster,
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
        )
        return _cleanup_owned_runtime_sessions_once(
            cluster=cluster,
            definition=definition,
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
            mode=mode,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            target_session_ids=target_ids,
        )
    for _pass in range(MAX_OWNER_GATEWAY_CLEANUP_PASSES):
        target_ids = _owned_runtime_gateway_ids_needing_cleanup(
            queue=queue,
            definition=definition,
            cluster=cluster,
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
        )
        if not target_ids:
            return reports
        if owner_session_generation_id is not None and scheduler_sentinel_ids:
            gateway_scheduler_job_ids = cli_owned_scheduler_cancel._owned_gateway_scheduler_job_ids(
                queue=queue,
                definition=definition,
                cluster=cluster,
                owner_session_id=owner_session_id,
                owner_session_generation_id=owner_session_generation_id,
            )
            cli_owned_scheduler_cancel._assert_scheduler_sentinels_unrelated(
                scheduler_sentinel_ids,
                owned_jobs or [],
                gateway_scheduler_job_ids=gateway_scheduler_job_ids,
            )
        pass_reports = _cleanup_owned_runtime_sessions_once(
            cluster=cluster,
            definition=definition,
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
            mode=mode,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            target_session_ids=target_ids,
        )
        reports.extend(pass_reports)
        if any(
            report.get("ok") is False or bool(report.get("residual_resources"))
            for report in pass_reports
        ):
            return reports
    residual_ids = _owned_runtime_gateway_ids_needing_cleanup(
        queue=queue,
        definition=definition,
        cluster=cluster,
        owner_session_id=owner_session_id,
        owner_session_generation_id=owner_session_generation_id,
    )
    if residual_ids:
        raise RelayError(
            "owned gateway cleanup did not converge after bounded rescans: "
            + ", ".join(sorted(residual_ids))
        )
    return reports


def _owned_runtime_gateway_ids_needing_cleanup(
    *,
    queue: core_queue.ClioCoreQueue,
    definition: ClusterDefinition,
    cluster: str,
    owner_session_id: str,
    owner_session_generation_id: str | None,
) -> set[str]:
    """Return the current non-closed owned gateway ids from local and remote stores."""
    local_gateways, local_truncated = queue.scan_gateway_sessions(
        limit=cli_remote_collection_pagination.MAX_INTERNAL_COLLECTION_RECORDS,
        cluster=cluster,
    )
    if local_truncated:
        raise RelayError(
            "local gateway cleanup discovery exceeds the bounded source limit; "
            "no gateway cleanup was attempted"
        )
    documents = [gateway.model_dump(mode="json") for gateway in local_gateways]
    if remote_cli.should_execute_on_cluster(definition):
        documents.extend(
            cli_remote_collection_pagination._complete_remote_source_collection(
                definition,
                ["gateway", "list", "--cluster", cluster],
                record_key="gateway_sessions",
                label=f"remote gateway cleanup discovery for {cluster}",
            )
        )
    targets: set[str] = set()
    for gateway in documents:
        session_id = gateway.get("session_id")
        metadata = gateway.get("metadata")
        if (
            not isinstance(session_id, str)
            or gateway.get("state") == GatewaySessionState.CLOSED.value
            or not isinstance(metadata, dict)
        ):
            continue
        typed_metadata = cast(dict[str, object], metadata)
        if (
            typed_metadata.get("owner") != "clio-relay"
            or typed_metadata.get("owner_session_id") != owner_session_id
        ):
            continue
        observed_generation = typed_metadata.get("owner_session_generation_id")
        if owner_session_generation_id is not None and observed_generation not in {
            None,
            owner_session_generation_id,
        }:
            continue
        targets.add(session_id)
    return targets


def _cleanup_owned_runtime_sessions_once(
    *,
    cluster: str,
    definition: ClusterDefinition,
    owner_session_id: str,
    owner_session_generation_id: str | None = None,
    mode: Literal["detach", "teardown"],
    cancel_scheduler_jobs: bool,
    target_session_ids: set[str],
) -> list[dict[str, object]]:
    settings = RelaySettings.from_env()
    queue = storage_runtime.storage_managed_queue(settings)
    queue.initialize()
    supervisor = service_runtime.ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster=cluster,
        definition=definition,
        token="",
        secret_key="",
    )
    reports: list[dict[str, object]] = []
    seen_session_ids: set[str] = set()
    local_gateways, local_truncated = queue.scan_gateway_sessions(
        limit=cli_remote_collection_pagination.MAX_INTERNAL_COLLECTION_RECORDS,
        cluster=cluster,
    )
    if local_truncated:
        max_records = cli_remote_collection_pagination.MAX_INTERNAL_COLLECTION_RECORDS
        raise RelayError(
            f"local gateway cleanup discovery exceeds the bounded source limit {max_records}; "
            "no gateway cleanup was attempted"
        )
    remote_gateways: list[dict[str, Any]] = []
    if remote_cli.should_execute_on_cluster(definition):
        remote_gateways = cli_remote_collection_pagination._complete_remote_source_collection(
            definition,
            ["gateway", "list", "--cluster", cluster],
            record_key="gateway_sessions",
            label=f"remote gateway cleanup discovery for {cluster}",
        )

    for gateway in local_gateways:
        if gateway.session_id not in target_session_ids:
            continue
        if gateway.state == GatewaySessionState.CLOSED and mode == "detach":
            continue
        if gateway.metadata.get("owner") != "clio-relay":
            continue
        if gateway.metadata.get("owner_session_id") != owner_session_id:
            continue
        gateway_generation = gateway.metadata.get("owner_session_generation_id")
        if owner_session_generation_id is not None:
            if not isinstance(gateway_generation, str) or not gateway_generation:
                reports.append(
                    _unverified_gateway_generation_report(
                        gateway_session_id=gateway.session_id,
                        location=str(settings.core_dir),
                        mode=mode,
                        expected_generation_id=owner_session_generation_id,
                        observed_generation_id=gateway_generation,
                    )
                )
                continue
            if gateway_generation != owner_session_generation_id:
                continue
        if mode == "detach":
            result = supervisor.detach(session_id=gateway.session_id)
        else:
            result = supervisor.stop(
                session_id=gateway.session_id,
                cancel_scheduler_job=cancel_scheduler_jobs,
            )
        reports.append(result.json_payload())
        seen_session_ids.add(gateway.session_id)
    for gateway in remote_gateways:
        remote_session_id = gateway.get("session_id")
        metadata = gateway.get("metadata")
        if (
            not isinstance(remote_session_id, str)
            or remote_session_id not in target_session_ids
            or remote_session_id in seen_session_ids
        ):
            continue
        if gateway.get("state") == GatewaySessionState.CLOSED.value and mode == "detach":
            continue
        if not isinstance(metadata, dict):
            continue
        typed_metadata = cast(dict[str, object], metadata)
        if typed_metadata.get("owner") != "clio-relay":
            continue
        if typed_metadata.get("owner_session_id") != owner_session_id:
            continue
        gateway_generation = typed_metadata.get("owner_session_generation_id")
        if owner_session_generation_id is not None:
            if not isinstance(gateway_generation, str) or not gateway_generation:
                reports.append(
                    _unverified_gateway_generation_report(
                        gateway_session_id=remote_session_id,
                        location=definition.ssh_host,
                        mode=mode,
                        expected_generation_id=owner_session_generation_id,
                        observed_generation_id=gateway_generation,
                    )
                )
                continue
            if gateway_generation != owner_session_generation_id:
                continue
        if mode == "detach":
            args = [
                "gateway",
                "detach-runtime",
                remote_session_id,
                "--cluster",
                cluster,
            ]
        else:
            args = [
                "gateway",
                "stop-runtime",
                remote_session_id,
                "--cluster",
                cluster,
                ("--cancel-scheduler-job" if cancel_scheduler_jobs else "--keep-scheduler-job"),
            ]
        remote_report = cast(object, json.loads(remote_cli.run_remote_clio(definition, args)))
        if not isinstance(remote_report, dict):
            raise RelayError(
                f"remote gateway cleanup did not return a JSON object: {remote_session_id}"
            )
        reports.append(
            {str(key): value for key, value in cast(dict[object, object], remote_report).items()}
        )
        seen_session_ids.add(remote_session_id)
    return reports


def _unverified_gateway_generation_report(
    *,
    gateway_session_id: str,
    location: str,
    mode: Literal["detach", "teardown"],
    expected_generation_id: str,
    observed_generation_id: object,
) -> dict[str, object]:
    """Return fail-closed evidence for an owner-session gateway without a generation."""
    detail = (
        "owned gateway record has no exact session generation; cleanup was refused: "
        f"gateway={gateway_session_id} expected={expected_generation_id} "
        f"observed={observed_generation_id!r}"
    )
    resource = CleanupResource(
        kind="gateway_record",
        resource_id=gateway_session_id,
        location=location,
        action="retain" if mode == "detach" else "close",
        ownership_verified=False,
        outcome="refused",
        verified_after_operation=False,
        residual=True,
        detail=detail,
        metadata={
            "expected_owner_session_generation_id": expected_generation_id,
            "observed_owner_session_generation_id": observed_generation_id,
        },
    )
    return {
        "resources": [resource.model_dump(mode="json")],
        "residual_resources": [resource.model_dump(mode="json")],
        "errors": [detail],
        "ok": False,
    }


def _merge_gateway_cleanup_resources(
    report: SessionLifecycleReport,
    gateway_reports: list[dict[str, object]],
) -> None:
    """Merge gateway connector cleanup into the owning desktop-session report."""
    for gateway_report in gateway_reports:
        raw_errors = gateway_report.get("errors")
        if isinstance(raw_errors, list):
            for raw_error in cast(list[object], raw_errors):
                if isinstance(raw_error, str) and raw_error not in report.errors:
                    report.errors.append(raw_error)
        raw_resources = gateway_report.get("resources")
        if not isinstance(raw_resources, list):
            report.errors.append("gateway cleanup report did not contain resource evidence")
            continue
        for raw_resource in cast(list[object], raw_resources):
            resource = CleanupResource.model_validate(raw_resource)
            if any(
                existing.kind == resource.kind
                and existing.resource_id == resource.resource_id
                and existing.action == resource.action
                for existing in report.resources
            ):
                continue
            report.resources.append(resource)
