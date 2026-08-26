"""``session teardown``'s job-quiesce/cancel phase (iowarp/clio-relay#231
continuation, ``cli_session_teardown.py`` split): quiesce intake under
one immutable cleanup operation id, refuse to proceed if unversioned
legacy jobs are found, list and (if requested) cancel this owner
session's active relay jobs, and preflight/record any scheduler
preservation sentinels a gateway allocation still names.

``_list_owned_jobs``/``_read_owned_job`` were nested closures in the
pre-split module; they move here as top-level functions with explicit
parameters instead, since ``cli_session_teardown_finalize`` needs the
same two lookups later in the same run and a nested closure cannot be
shared across modules. The phase body itself wraps them back into
locally nested closures with the pre-split module's exact call shape,
so the rest of the body -- moved verbatim except for the shared-state
plumbing -- is unchanged.
"""

from __future__ import annotations

from typing import cast

import clio_relay.cli_owned_relay_jobs as cli_owned_relay_jobs
import clio_relay.cli_owned_relay_jobs_remote_listing as cli_owned_relay_jobs_remote_listing
import clio_relay.cli_owned_runtime_cleanup as cli_owned_runtime_cleanup
import clio_relay.cli_owned_scheduler_cancel as cli_owned_scheduler_cancel
import clio_relay.core_queue as core_queue
import clio_relay.validation_report as validation_report_module
from clio_relay.cli_session_teardown_state import _TeardownState
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.validation_report import CleanupEvidence, ValidationResource


def _list_owned_jobs(
    *,
    remote_execution: bool,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
    session_generation_id: str,
    queue: core_queue.ClioCoreQueue,
    scheduler_provider: str,
    include_terminal: bool = False,
) -> list[cli_owned_relay_jobs._OwnedRelayJob]:
    if remote_execution:
        return cli_owned_relay_jobs_remote_listing.list_remote_owned_active_cluster_jobs(
            definition,
            cluster,
            owner_session_id=session_id,
            owner_session_generation_id=session_generation_id,
            include_terminal=include_terminal,
        )
    return cli_owned_relay_jobs._list_owned_active_cluster_jobs(
        queue,
        cluster,
        owner_session_id=session_id,
        owner_session_generation_id=session_generation_id,
        scheduler_provider=scheduler_provider,
        include_terminal=include_terminal,
    )


def _list_legacy_jobs(
    *,
    remote_execution: bool,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
    queue: core_queue.ClioCoreQueue,
    scheduler_provider: str,
) -> list[cli_owned_relay_jobs._OwnedRelayJob]:
    """Discover unversioned records without treating them as this generation's jobs."""
    if remote_execution:
        return cli_owned_relay_jobs_remote_listing.list_remote_owned_active_cluster_jobs(
            definition,
            cluster,
            owner_session_id=session_id,
            owner_session_generation_id=None,
            include_terminal=True,
        )
    return cli_owned_relay_jobs._list_owned_active_cluster_jobs(
        queue,
        cluster,
        owner_session_id=session_id,
        owner_session_generation_id=None,
        scheduler_provider=scheduler_provider,
        include_terminal=True,
    )


def _read_owned_job(
    job_id: str,
    *,
    queue: core_queue.ClioCoreQueue,
    definition: ClusterDefinition,
    remote_execution: bool,
    cluster: str,
    session_id: str,
    session_generation_id: str,
) -> cli_owned_relay_jobs._OwnedRelayJob:
    return cli_owned_relay_jobs._read_owned_relay_job(
        queue=queue,
        definition=definition,
        remote_execution=remote_execution,
        cluster=cluster,
        job_id=job_id,
        owner_session_id=session_id,
        owner_session_generation_id=session_generation_id,
    )


def _run_teardown_jobs_phase(state: _TeardownState) -> None:
    """Quiesce admission, list/cancel owned jobs, and preflight scheduler sentinels."""
    seed_report = state.seed_report
    cluster = state.cluster
    session_id = state.session_id
    session_generation_id = state.session_generation_id
    cleanup_operation_id = state.cleanup_operation_id
    local_admission_session_id = state.local_admission_session_id
    remote_execution = state.remote_execution
    queue = cast(core_queue.ClioCoreQueue, state.queue)
    definition = state.definition
    cancel_jobs = state.cancel_jobs
    stop_worker = state.stop_worker
    cancel_scheduler_jobs = state.cancel_scheduler_jobs
    scheduler_sentinel_ids = state.scheduler_sentinel_ids
    canonical_report_path = state.canonical_report_path
    relay_cancel_timeout_seconds = state.relay_cancel_timeout_seconds
    relay_cancel_poll_seconds = state.relay_cancel_poll_seconds
    pre_teardown_status = state.pre_teardown_status

    partial = seed_report
    partial.cleanup = CleanupEvidence(
        requested=True,
        mode="teardown",
        operation_id=cleanup_operation_id,
        cancel_relay_jobs=cancel_jobs,
        cancel_scheduler_jobs=cancel_jobs and cancel_scheduler_jobs,
        stop_worker=stop_worker,
        actions=[
            {
                "kind": "owner_session_admission",
                "resource_id": f"{session_id}:{session_generation_id}",
                "action": "quiesce",
                "outcome": "pending",
                "verified_after_operation": False,
                "residual": True,
            },
            {
                "kind": "remote_relay_api",
                "resource_id": session_id,
                "action": "stop",
                "outcome": "pending",
                "verified_after_operation": False,
                "residual": True,
            },
        ],
    )
    admission_resource = ValidationResource(
        kind="owner_session_admission",
        resource_id=f"{session_id}:{session_generation_id}",
        role="cleanup_admission",
        cluster=cluster,
        state="pending",
        metadata={
            "operation_id": cleanup_operation_id,
            "local_admission_session_id": local_admission_session_id,
            "remote_execution": remote_execution,
        },
    )
    api_resource = ValidationResource(
        kind="remote_relay_api",
        resource_id=session_id,
        role="cleanup_target",
        cluster=cluster,
        state="running" if pre_teardown_status.get("running") is True else "stopped",
        metadata={
            "session_generation_id": session_generation_id,
            "ownership_verified": pre_teardown_status.get("ownership_verified") is True,
            "cleanup_operation_id": cleanup_operation_id,
        },
    )
    admission_resource_index = len(partial.resources)
    partial.resources.extend([admission_resource, api_resource])
    partial.cleanup.remaining_resources.extend([admission_resource, api_resource])
    state.canonical_report = partial
    validation_report_module.write_validation_report(partial, canonical_report_path)
    cleanup_intent = cli_owned_relay_jobs._quiesce_owner_session_intake(
        queue=queue,
        definition=definition,
        remote_execution=remote_execution,
        session_id=session_id,
        local_admission_session_id=local_admission_session_id,
        session_generation_id=session_generation_id,
        cleanup_operation_id=cleanup_operation_id,
        stop_worker=stop_worker,
        cancel_jobs=cancel_jobs,
        cancel_scheduler_jobs=cancel_scheduler_jobs,
    )
    state.cleanup_intent = cleanup_intent
    partial.resources[admission_resource_index] = partial.resources[
        admission_resource_index
    ].model_copy(update={"state": "quiesced"})
    partial.cleanup.remaining_resources[0] = partial.resources[admission_resource_index]
    partial.cleanup.actions[0].update(
        {
            "outcome": "quiesced",
            "verified_after_operation": True,
        }
    )
    validation_report_module.write_validation_report(partial, canonical_report_path)

    def list_owned_jobs(
        *, include_terminal: bool = False
    ) -> list[cli_owned_relay_jobs._OwnedRelayJob]:
        return _list_owned_jobs(
            remote_execution=remote_execution,
            definition=definition,
            cluster=cluster,
            session_id=session_id,
            session_generation_id=session_generation_id,
            queue=queue,
            scheduler_provider=definition.scheduler_provider,
            include_terminal=include_terminal,
        )

    def list_legacy_jobs() -> list[cli_owned_relay_jobs._OwnedRelayJob]:
        return _list_legacy_jobs(
            remote_execution=remote_execution,
            definition=definition,
            cluster=cluster,
            session_id=session_id,
            queue=queue,
            scheduler_provider=definition.scheduler_provider,
        )

    def read_owned_job(job_id: str) -> cli_owned_relay_jobs._OwnedRelayJob:
        return _read_owned_job(
            job_id,
            queue=queue,
            definition=definition,
            remote_execution=remote_execution,
            cluster=cluster,
            session_id=session_id,
            session_generation_id=session_generation_id,
        )

    legacy_jobs = list_legacy_jobs()
    if legacy_jobs:
        for legacy_job in legacy_jobs:
            resource = ValidationResource(
                kind="relay_job",
                resource_id=legacy_job.job_id,
                role="ambiguous_legacy_owner_session",
                cluster=cluster,
                state=legacy_job.relay_state.value,
                provider=legacy_job.scheduler_provider,
                metadata={
                    "ownership_verified": False,
                    "expected_owner_session_generation_id": session_generation_id,
                    "observed_owner_session_generation_id": None,
                    "mutation_refused": True,
                },
            )
            partial.resources.append(resource)
            partial.cleanup.remaining_resources.append(resource)
        validation_report_module.write_validation_report(partial, canonical_report_path)
        raise RelayError(
            "owner-session cleanup found unversioned legacy jobs whose generation cannot be "
            "proven; no relay or scheduler cancellation was attempted: "
            + ", ".join(sorted(job.job_id for job in legacy_jobs))
        )

    owned_jobs = list_owned_jobs()
    if cancel_jobs:
        for job in owned_jobs:
            resource = ValidationResource(
                kind="relay_job",
                resource_id=job.job_id,
                role="cleanup_cancel_target",
                cluster=cluster,
                state=job.relay_state.value,
                provider=job.scheduler_provider,
                metadata={
                    "action": "cancel",
                    "ownership_verified": True,
                    "owner_session_generation_id": session_generation_id,
                    "cleanup_operation_id": cleanup_operation_id,
                },
            )
            partial.resources.append(resource)
            partial.cleanup.remaining_resources.append(resource)
            partial.cleanup.actions.append(
                {
                    "kind": "relay_job",
                    "resource_id": job.job_id,
                    "action": "cancel",
                    "outcome": "pending",
                    "verified_after_operation": False,
                    "residual": True,
                }
            )
        validation_report_module.write_validation_report(partial, canonical_report_path)
    gateway_scheduler_job_ids = (
        cli_owned_scheduler_cancel._owned_gateway_scheduler_job_ids(
            queue=queue,
            definition=definition,
            cluster=cluster,
            owner_session_id=session_id,
            owner_session_generation_id=session_generation_id,
        )
        if scheduler_sentinel_ids
        else ()
    )
    for scheduler_job_id in gateway_scheduler_job_ids:
        scheduler_resource = ValidationResource(
            kind="scheduler_job",
            resource_id=scheduler_job_id,
            role="gateway_cleanup_target",
            cluster=cluster,
            state="discovered",
            provider=definition.scheduler_provider,
            metadata={
                "action": "cancel" if cancel_scheduler_jobs else "retain",
                "ownership_verified": True,
                "owner_session_generation_id": session_generation_id,
                "cleanup_operation_id": cleanup_operation_id,
            },
        )
        partial.resources.append(scheduler_resource)
        partial.cleanup.remaining_resources.append(scheduler_resource)
        partial.cleanup.actions.append(
            {
                "kind": "scheduler_job",
                "resource_id": scheduler_job_id,
                "action": "cancel" if cancel_scheduler_jobs else "retain",
                "outcome": "pending",
                "verified_after_operation": False,
                "residual": True,
                "source": "gateway",
            }
        )
    if gateway_scheduler_job_ids:
        validation_report_module.write_validation_report(partial, canonical_report_path)
    scheduler_sentinel_pre_phases = cli_owned_scheduler_cancel._preflight_scheduler_sentinels(
        definition,
        scheduler_sentinel_ids,
        owned_jobs,
        gateway_scheduler_job_ids=gateway_scheduler_job_ids,
        owner_session_id=session_id,
        owner_session_generation_id=session_generation_id,
    )
    state.scheduler_sentinel_pre_phases = scheduler_sentinel_pre_phases
    canceled: list[str] = []
    state.canceled = canceled
    if cancel_jobs:
        try:
            cancellation_targets = (
                cli_owned_relay_jobs._cancel_remote_owned_jobs(
                    definition,
                    cluster,
                    owned_jobs,
                    owner_session_id=session_id,
                    owner_session_generation_id=session_generation_id,
                )
                if remote_execution
                else cli_owned_relay_jobs._cancel_local_owned_jobs(queue, owned_jobs)
            )
            canceled.extend(
                cli_owned_relay_jobs._wait_for_owned_relay_cancellations(
                    cancellation_targets,
                    read_owned_job=read_owned_job,
                    timeout_seconds=relay_cancel_timeout_seconds,
                    poll_seconds=relay_cancel_poll_seconds,
                )
            )
        except BaseException as exc:
            for action_evidence in partial.cleanup.actions:
                if action_evidence.get("kind") == "relay_job":
                    action_evidence.update(
                        {
                            "outcome": "failed",
                            "verified_after_operation": False,
                            "residual": True,
                            "detail": str(exc),
                        }
                    )
            validation_report_module.write_validation_report(partial, canonical_report_path)
            raise
        canceled_ids = set(canceled)
        for index, resource in enumerate(partial.resources):
            if resource.kind == "relay_job" and resource.resource_id in canceled_ids:
                partial.resources[index] = resource.model_copy(update={"state": "canceled"})
        partial.cleanup.remaining_resources = [
            resource
            for resource in partial.cleanup.remaining_resources
            if not (resource.kind == "relay_job" and resource.resource_id in canceled_ids)
        ]
        for action_evidence in partial.cleanup.actions:
            if (
                action_evidence.get("kind") == "relay_job"
                and action_evidence.get("resource_id") in canceled_ids
            ):
                action_evidence.update(
                    {
                        "outcome": "canceled",
                        "verified_after_operation": True,
                        "residual": False,
                    }
                )
        validation_report_module.write_validation_report(partial, canonical_report_path)
    gateway_reports = cli_owned_runtime_cleanup._cleanup_owned_runtime_sessions(
        cluster=cluster,
        definition=definition,
        owner_session_id=session_id,
        owner_session_generation_id=session_generation_id,
        mode="teardown",
        cancel_scheduler_jobs=cancel_scheduler_jobs,
        scheduler_sentinel_ids=scheduler_sentinel_ids,
        owned_jobs=owned_jobs,
    )
    state.gateway_reports = gateway_reports
    state.owned_jobs = owned_jobs
