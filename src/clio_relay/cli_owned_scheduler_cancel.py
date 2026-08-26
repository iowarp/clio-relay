"""Owned scheduler-job cancellation and sentinel preservation (iowarp/
clio-relay#231 continuation): the ``--preserve-scheduler-job-id``
sentinel bookkeeping and the scheduler-side cancellation helpers
``session teardown`` uses once relay-level cancellation is complete."""

from __future__ import annotations

import json
import time
from typing import Literal, cast

import typer

import clio_relay.cli_owned_relay_jobs as cli_owned_relay_jobs
import clio_relay.cli_remote_collection_pagination as cli_remote_collection_pagination
import clio_relay.core_queue as core_queue
import clio_relay.remote_channel_dispatch as remote_channel_dispatch
import clio_relay.remote_cli as remote_cli
import clio_relay.scheduler_providers as scheduler_providers
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.models import (
    JobState,
    SchedulerPhase,
    SchedulerStatus,
)
from clio_relay.session_lifecycle import CleanupResource

MAX_SCHEDULER_STATUS_BATCH = 256

#: Review S1(b) compat: the server-side ownership gate on GET /scheduler/
#: jobs/{id}/status refuses any id this session does not own. Sentinel ids
#: are UNOWNED by design, so their two pollers pass channel_eligible=False
#: and record this cause instead of routing over the channel.
_SCHEDULER_JOB_NOT_OWNED_CAUSE = "scheduler_job_not_owned_by_session"


SCHEDULER_SENTINEL_ACTIVE_PHASES = frozenset({"submitted", "pending", "allocated", "running"})


SCHEDULER_SENTINEL_PRESERVED_PHASES = SCHEDULER_SENTINEL_ACTIVE_PHASES | {"completed"}


def _normalize_scheduler_sentinel_ids(values: list[str]) -> tuple[str, ...]:
    """Validate and de-duplicate scheduler preservation sentinel ids."""
    normalized: list[str] = []
    for value in values:
        scheduler_job_id = value.strip()
        if not scheduler_job_id:
            raise typer.BadParameter("--preserve-scheduler-job-id cannot be empty")
        if scheduler_job_id not in normalized:
            normalized.append(scheduler_job_id)
    return tuple(normalized)


def _owned_gateway_scheduler_job_ids(
    *,
    queue: core_queue.ClioCoreQueue,
    definition: ClusterDefinition,
    cluster: str,
    owner_session_id: str,
    owner_session_generation_id: str,
) -> tuple[str, ...]:
    """Discover every exact-generation gateway scheduler allocation without mutation."""
    local_gateways, local_truncated = queue.scan_gateway_sessions(
        limit=cli_remote_collection_pagination.MAX_INTERNAL_COLLECTION_RECORDS,
        cluster=cluster,
    )
    if local_truncated:
        raise RelayError(
            "local gateway scheduler discovery exceeds the bounded source limit; "
            "no scheduler cancellation was attempted"
        )
    documents = [gateway.model_dump(mode="json") for gateway in local_gateways]
    if remote_cli.should_execute_on_cluster(definition):
        documents.extend(
            cli_remote_collection_pagination._complete_remote_source_collection(
                definition,
                ["gateway", "list", "--cluster", cluster],
                record_key="gateway_sessions",
                label=f"remote gateway scheduler discovery for {cluster}",
            )
        )
    ids_by_gateway: dict[str, set[str]] = {}
    for gateway in documents:
        session_id = gateway.get("session_id")
        metadata = gateway.get("metadata")
        if not isinstance(session_id, str) or not isinstance(metadata, dict):
            continue
        typed_metadata = cast(dict[str, object], metadata)
        if (
            typed_metadata.get("owner") != "clio-relay"
            or typed_metadata.get("owner_session_id") != owner_session_id
            or typed_metadata.get("owner_session_generation_id") != owner_session_generation_id
        ):
            continue
        exact_ids = ids_by_gateway.setdefault(session_id, set())
        scheduler_job_id = gateway.get("scheduler_job_id")
        if isinstance(scheduler_job_id, str) and scheduler_job_id:
            exact_ids.add(scheduler_job_id)
        raw_gateway = gateway.get("gateway")
        if not isinstance(raw_gateway, dict):
            continue
        ownership_intents = cast(dict[str, object], raw_gateway).get("ownership_intents")
        if not isinstance(ownership_intents, dict):
            continue
        raw_scheduler_intent = cast(dict[str, object], ownership_intents).get(
            "scheduler_submission"
        )
        if not isinstance(raw_scheduler_intent, dict):
            continue
        scheduler_intent = cast(dict[str, object], raw_scheduler_intent)
        intent_state = scheduler_intent.get("state")
        intent_scheduler_job_id = scheduler_intent.get("scheduler_job_id")
        if isinstance(intent_scheduler_job_id, str) and intent_scheduler_job_id:
            exact_ids.add(intent_scheduler_job_id)
        if intent_state in {"starting", "recorded"} and not exact_ids:
            raise RelayError(
                "owned gateway has an unresolved scheduler submission; no scheduler "
                f"cancellation was attempted: {session_id}"
            )
        if len(exact_ids) > 1:
            raise RelayError(
                "owned gateway scheduler identity disagrees across durable evidence; no "
                f"scheduler cancellation was attempted: {session_id}"
            )
    return tuple(sorted({job_id for ids in ids_by_gateway.values() for job_id in ids}))


def _assert_scheduler_sentinels_unrelated(
    scheduler_sentinel_ids: tuple[str, ...],
    jobs: list[cli_owned_relay_jobs._OwnedRelayJob],
    *,
    gateway_scheduler_job_ids: tuple[str, ...] = (),
) -> None:
    """Fail closed if a preservation sentinel appears in session-owned job evidence."""
    session_scheduler_ids = {
        scheduler_job_id
        for job in jobs
        for scheduler_job_id in (*job.scheduler_job_ids, *job.unowned_scheduler_job_ids)
    }
    session_scheduler_ids.update(gateway_scheduler_job_ids)
    conflicts = sorted(set(scheduler_sentinel_ids) & session_scheduler_ids)
    if conflicts:
        raise RelayError(
            "scheduler preservation sentinel ids appeared in owned or unowned scheduler "
            "evidence for the target session generation; no scheduler cancellation was "
            "attempted: " + ", ".join(conflicts)
        )


def _preflight_scheduler_sentinels(
    definition: ClusterDefinition,
    scheduler_sentinel_ids: tuple[str, ...],
    jobs: list[cli_owned_relay_jobs._OwnedRelayJob],
    *,
    gateway_scheduler_job_ids: tuple[str, ...] = (),
    owner_session_id: str,
    owner_session_generation_id: str,
) -> dict[str, str]:
    """Prove unrelated scheduler sentinels are active before cleanup mutation."""
    _assert_scheduler_sentinels_unrelated(
        scheduler_sentinel_ids,
        jobs,
        gateway_scheduler_job_ids=gateway_scheduler_job_ids,
    )
    provider = definition.scheduler_provider
    observed_phases: dict[str, str] = {}
    errors: list[str] = []
    for scheduler_job_id in scheduler_sentinel_ids:
        phase, error = _scheduler_phase_after_operation(
            definition,
            scheduler_job_id,
            provider=provider,
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
            channel_eligible=False,
        )
        normalized_phase = phase.strip().lower() if phase is not None else "unknown"
        if error is not None or normalized_phase not in SCHEDULER_SENTINEL_ACTIVE_PHASES:
            errors.append(
                f"{scheduler_job_id} phase={normalized_phase}"
                + (f" error={error}" if error is not None else "")
            )
            continue
        observed_phases[scheduler_job_id] = normalized_phase
    if errors:
        raise RelayError(
            "scheduler preservation sentinels must be unrelated active jobs before "
            "cancellation; " + "; ".join(errors)
        )
    return observed_phases


def _scheduler_sentinel_preservation_resources(
    definition: ClusterDefinition,
    pre_phases: dict[str, str],
    *,
    owner_session_id: str,
    owner_session_generation_id: str,
) -> tuple[list[CleanupResource], list[str]]:
    """Re-poll scheduler sentinels and emit canonical preservation evidence."""
    provider = definition.scheduler_provider
    resources: list[CleanupResource] = []
    errors: list[str] = []
    for scheduler_job_id, pre_phase in pre_phases.items():
        phase, poll_error = _scheduler_phase_after_operation(
            definition,
            scheduler_job_id,
            provider=provider,
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
            channel_eligible=False,
        )
        post_phase = phase.strip().lower() if phase is not None else "unknown"
        preserved = poll_error is None and post_phase in SCHEDULER_SENTINEL_PRESERVED_PHASES
        detail = (
            "unrelated scheduler sentinel remained active after owned cancellation"
            if preserved and post_phase != "completed"
            else "unrelated scheduler sentinel completed naturally during owned cancellation"
            if preserved
            else "unrelated scheduler sentinel preservation was not proven"
            + (f": {poll_error}" if poll_error is not None else f": phase={post_phase}")
        )
        resource = CleanupResource(
            kind="scheduler_sentinel",
            resource_id=scheduler_job_id,
            location=definition.ssh_host,
            action="retain",
            ownership_verified=False,
            outcome="retained" if preserved else "failed",
            provider=provider,
            verified_after_operation=preserved,
            observed_state=post_phase,
            residual=not preserved,
            detail=detail,
            metadata={
                "unowned_sentinel": True,
                "active_before_operation": True,
                "preservation_verified": preserved,
                "pre_phase": pre_phase,
                "post_phase": post_phase,
            },
        )
        resources.append(resource)
        if not preserved:
            errors.append(f"scheduler sentinel {scheduler_job_id} was not preserved: {detail}")
    return resources, errors


def _owned_job_cleanup_resources(
    jobs: list[cli_owned_relay_jobs._OwnedRelayJob],
    *,
    definition: ClusterDefinition,
    location: str,
    cancel_jobs: bool,
    cancel_scheduler_jobs: bool,
    post_operation_jobs: list[cli_owned_relay_jobs._OwnedRelayJob] | None = None,
    owner_session_id: str,
    owner_session_generation_id: str,
) -> list[CleanupResource]:
    resources: list[CleanupResource] = []
    scheduler_phases = (
        _scheduler_phases_after_operation(
            definition,
            tuple(
                (job.scheduler_provider, scheduler_job_id)
                for job in jobs
                for scheduler_job_id in job.scheduler_job_ids
            ),
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
        )
        if not cancel_scheduler_jobs
        else {}
    )
    post_by_id = {
        job.job_id: job for job in (post_operation_jobs if post_operation_jobs is not None else [])
    }
    for job in jobs:
        relay_active = job.relay_state in {
            JobState.QUEUED,
            JobState.LEASED,
            JobState.RUNNING,
        }
        post_job = post_by_id.get(job.job_id)
        canceled_with_cleanup = (
            post_job is not None
            and post_job.relay_state is JobState.CANCELED
            and post_job.relay_cancellation_requested
            and post_job.relay_cancellation_acknowledged
            and post_job.relay_cancellation_scheduler_requested is False
        )
        completed_before_request = (
            post_job is not None
            and post_job.relay_state in {JobState.SUCCEEDED, JobState.FAILED}
            and not post_job.relay_cancellation_requested
        )
        relay_verified = (
            canceled_with_cleanup or completed_before_request
            if cancel_jobs and relay_active
            else post_job is not None
        )
        if not relay_active:
            relay_action: Literal["retain", "stop", "close", "cancel"] = "retain"
            relay_outcome: Literal[
                "retained",
                "stopped",
                "closed",
                "canceled",
                "terminal",
                "missing",
                "refused",
                "failed",
            ] = "terminal"
            relay_verified = True
            relay_detail = (
                f"relay job was already terminal ({job.relay_state.value}); "
                "owned scheduler resources were evaluated independently"
            )
        else:
            relay_action = "cancel" if cancel_jobs else "retain"
            if cancel_jobs and canceled_with_cleanup:
                relay_outcome = "canceled"
                relay_detail = (
                    "worker cleanup acknowledged the durable relay-only cancellation request"
                )
            elif cancel_jobs and completed_before_request:
                relay_outcome = "terminal"
                relay_detail = "relay job completed before the cancellation request won the race"
            elif not cancel_jobs and relay_verified:
                relay_outcome = "retained"
                relay_detail = "relay job ownership matched and retention was verified"
            else:
                relay_outcome = "failed"
                relay_detail = "owned relay job cancellation or retention was not verified"
        resources.append(
            CleanupResource(
                kind="relay_job",
                resource_id=job.job_id,
                location=location,
                action=relay_action,
                ownership_verified=True,
                outcome=relay_outcome,
                verified_after_operation=relay_verified,
                residual=not relay_verified,
                detail=relay_detail,
                metadata={"scheduler_job_ids": list(job.scheduler_job_ids)},
            )
        )
        for scheduler_job_id in job.scheduler_job_ids:
            if cancel_jobs and cancel_scheduler_jobs:
                continue
            scheduler_verified = False
            phase: str | None = None
            status_error: str | None = None
            if not cancel_scheduler_jobs:
                phase, status_error = scheduler_phases[(job.scheduler_provider, scheduler_job_id)]
                scheduler_verified = phase in {
                    "submitted",
                    "pending",
                    "allocated",
                    "running",
                    "completed",
                    "failed",
                    "canceled",
                    "missing",
                }
            scheduler_terminal = phase in {"completed", "failed", "canceled", "missing"}
            resources.append(
                CleanupResource(
                    kind="scheduler_job",
                    resource_id=scheduler_job_id,
                    location=location,
                    action="retain",
                    ownership_verified=True,
                    outcome=(
                        "missing"
                        if phase == "missing"
                        else "terminal"
                        if scheduler_verified and scheduler_terminal
                        else "retained"
                        if scheduler_verified
                        else "failed"
                    ),
                    provider=job.scheduler_provider,
                    verified_after_operation=scheduler_verified,
                    observed_state=phase,
                    residual=not scheduler_verified,
                    detail=(
                        "scheduler cancellation was not requested; no active scheduler record "
                        "remained after the operation"
                        if phase == "missing"
                        else (
                            "scheduler cancellation was not requested; "
                            f"post-operation phase={phase}"
                        )
                        if scheduler_verified
                        else "scheduler preservation was not verified"
                        + (f": {status_error}" if status_error else "")
                    ),
                    metadata={"relay_job_id": job.job_id},
                )
            )
        for scheduler_job_id in job.unowned_scheduler_job_ids:
            resources.append(
                CleanupResource(
                    kind="scheduler_job",
                    resource_id=scheduler_job_id,
                    location=location,
                    action=("cancel" if cancel_jobs and cancel_scheduler_jobs else "retain"),
                    ownership_verified=False,
                    outcome="refused",
                    provider=job.scheduler_provider,
                    verified_after_operation=False,
                    residual=True,
                    detail=(
                        "scheduler identity was observed but no ownership record bound it "
                        "to this relay job and task with an authenticated JARVIS proof"
                    ),
                )
            )
    return resources


def _scheduler_phase_after_operation(
    definition: ClusterDefinition,
    scheduler_job_id: str,
    *,
    provider: str,
    owner_session_id: str,
    owner_session_generation_id: str,
    channel_eligible: bool = True,
) -> tuple[str | None, str | None]:
    def ssh_status() -> object:
        return json.loads(
            remote_cli.run_remote_clio(
                definition,
                [
                    "scheduler",
                    "status",
                    scheduler_job_id,
                    "--cluster",
                    definition.name,
                    "--provider",
                    provider,
                ],
            )
        )

    try:
        if remote_cli.should_execute_on_cluster(definition):
            if channel_eligible:
                raw_status = remote_channel_dispatch.dial_or_route_json(
                    definition=definition,
                    owner_session_id=owner_session_id,
                    owner_session_generation_id=owner_session_generation_id,
                    operation="scheduler_status",
                    method="GET",
                    path=f"/scheduler/jobs/{scheduler_job_id}/status",
                    query={"provider": provider},
                    ssh_fallback=ssh_status,
                )
            else:
                # Review S1(b): sentinel ids are UNOWNED by design -- skip
                # the channel (it would 403) and record why.
                remote_channel_dispatch.record_per_operation_ssh_fallback(
                    operation="scheduler_status",
                    cluster=definition.name,
                    cause=_SCHEDULER_JOB_NOT_OWNED_CAUSE,
                    detail=f"scheduler job {scheduler_job_id} is a preservation sentinel",
                )
                raw_status = ssh_status()
            if not isinstance(raw_status, dict):
                raise RelayError("scheduler status did not return a JSON object")
            phase = cast(dict[str, object], raw_status).get("phase")
            active_record_found = cast(dict[str, object], raw_status).get("active_record_found")
            if phase == SchedulerPhase.UNKNOWN.value and active_record_found is False:
                return "missing", None
            return (str(phase), None) if isinstance(phase, str) else (None, None)
        status = scheduler_providers.provider_for_scheduler(provider).poll(scheduler_job_id)
        if status.phase is SchedulerPhase.UNKNOWN and status.active_record_found is False:
            return "missing", None
        return status.phase.value, None
    except (RelayError, json.JSONDecodeError) as exc:
        return None, str(exc)


def _scheduler_phases_after_operation(
    definition: ClusterDefinition,
    identities: tuple[tuple[str, str], ...],
    *,
    owner_session_id: str,
    owner_session_generation_id: str,
) -> dict[tuple[str, str], tuple[str | None, str | None]]:
    """Observe exact scheduler identities with bounded remote process reuse."""
    unique_identities = tuple(dict.fromkeys(identities))
    if not remote_cli.should_execute_on_cluster(definition):
        return {
            identity: _scheduler_phase_after_operation(
                definition,
                identity[1],
                provider=identity[0],
                owner_session_id=owner_session_id,
                owner_session_generation_id=owner_session_generation_id,
            )
            for identity in unique_identities
        }

    connection, cause = remote_channel_dispatch.live_matching_connection_with_cause(
        definition=definition,
        owner_session_id=owner_session_id,
        owner_session_generation_id=owner_session_generation_id,
    )
    if connection is None:
        remote_channel_dispatch.record_per_operation_ssh_fallback(
            operation="scheduler_status_batch",
            cluster=definition.name,
            cause=cause or remote_channel_dispatch.FALLBACK_CAUSE_NO_LIVE_CHANNEL,
        )
    by_provider: dict[str, list[str]] = {}
    for provider, scheduler_job_id in unique_identities:
        by_provider.setdefault(provider, []).append(scheduler_job_id)
    observations: dict[tuple[str, str], tuple[str | None, str | None]] = {}
    for provider, scheduler_job_ids in by_provider.items():
        for offset in range(0, len(scheduler_job_ids), MAX_SCHEDULER_STATUS_BATCH):
            batch = scheduler_job_ids[offset : offset + MAX_SCHEDULER_STATUS_BATCH]
            try:
                if connection is not None:
                    raw = connection.request_json(
                        method="POST",
                        path="/scheduler/status-batch",
                        body={"provider": provider, "scheduler_job_ids": list(batch)},
                    )
                else:
                    args = [
                        "scheduler",
                        "status-batch",
                        "--cluster",
                        definition.name,
                        "--provider",
                        provider,
                    ]
                    for scheduler_job_id in batch:
                        args.extend(["--scheduler-job-id", scheduler_job_id])
                    raw = cast(object, json.loads(remote_cli.run_remote_clio(definition, args)))
                if not isinstance(raw, dict):
                    raise RelayError("scheduler status batch did not return a JSON object")
                document = cast(dict[str, object], raw)
                raw_statuses = document.get("statuses")
                if (
                    document.get("schema_version") != "clio-relay.scheduler-status-batch.v1"
                    or document.get("scheduler") != provider
                    or not isinstance(raw_statuses, list)
                ):
                    raise RelayError("scheduler status batch envelope is invalid")
                statuses: dict[str, SchedulerStatus] = {}
                for raw_status in cast(list[object], raw_statuses):
                    status = SchedulerStatus.model_validate(raw_status)
                    if status.scheduler != provider or status.scheduler_job_id in statuses:
                        raise RelayError("scheduler status batch identity is invalid")
                    statuses[status.scheduler_job_id] = status
                if set(statuses) != set(batch):
                    raise RelayError("scheduler status batch omitted or added job identities")
                for scheduler_job_id in batch:
                    status = statuses[scheduler_job_id]
                    phase = (
                        "missing"
                        if status.phase is SchedulerPhase.UNKNOWN
                        and status.active_record_found is False
                        else status.phase.value
                    )
                    observations[(provider, scheduler_job_id)] = (phase, None)
            except (RelayError, ValueError, json.JSONDecodeError) as exc:
                error = str(exc)
                for scheduler_job_id in batch:
                    observations[(provider, scheduler_job_id)] = (None, error)
    return observations


def _cleanup_command_deadline_seconds(deadline: float) -> float:
    """Bound one remote-cleanup dial by the shared per-job deadline."""
    return min(
        cli_owned_relay_jobs.REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS,
        max(0.01, deadline - time.monotonic()),
    )


def _cancel_owned_scheduler_jobs(
    definition: ClusterDefinition,
    jobs: list[cli_owned_relay_jobs._OwnedRelayJob],
    *,
    timeout_seconds: float = 30.0,
    poll_seconds: float = 0.5,
    owner_session_id: str,
    owner_session_generation_id: str,
) -> tuple[list[CleanupResource], list[str]]:
    resources: list[CleanupResource] = []
    errors: list[str] = []
    seen: set[tuple[str, str]] = set()
    for job in jobs:
        for scheduler_job_id in job.scheduler_job_ids:
            identity = (job.scheduler_provider, scheduler_job_id)
            if identity in seen:
                continue
            seen.add(identity)
            resource, error = _cancel_owned_scheduler_job(
                definition,
                scheduler_job_id,
                relay_job_id=job.job_id,
                provider=job.scheduler_provider,
                timeout_seconds=timeout_seconds,
                poll_seconds=poll_seconds,
                owner_session_id=owner_session_id,
                owner_session_generation_id=owner_session_generation_id,
            )
            resources.append(resource)
            if error is not None:
                errors.append(error)
    return resources, errors


def _cancel_owned_scheduler_job(
    definition: ClusterDefinition,
    scheduler_job_id: str,
    *,
    relay_job_id: str,
    provider: str,
    timeout_seconds: float,
    poll_seconds: float,
    owner_session_id: str,
    owner_session_generation_id: str,
) -> tuple[CleanupResource, str | None]:
    deadline = time.monotonic() + timeout_seconds

    def bounded_ssh_cancel() -> object:
        with remote_cli.remote_command_timeout(_cleanup_command_deadline_seconds(deadline)):
            return json.loads(
                remote_cli.run_remote_clio(
                    definition,
                    [
                        "scheduler",
                        "cancel",
                        scheduler_job_id,
                        "--cluster",
                        definition.name,
                        "--provider",
                        provider,
                    ],
                )
            )

    def bounded_ssh_status() -> object:
        with remote_cli.remote_command_timeout(_cleanup_command_deadline_seconds(deadline)):
            return json.loads(
                remote_cli.run_remote_clio(
                    definition,
                    [
                        "scheduler",
                        "status",
                        scheduler_job_id,
                        "--cluster",
                        definition.name,
                        "--provider",
                        provider,
                    ],
                )
            )

    accepted = False
    cancel_detail: str | None = None
    try:
        if remote_cli.should_execute_on_cluster(definition):
            raw_cancel = remote_channel_dispatch.dial_or_route_json(
                definition=definition,
                owner_session_id=owner_session_id,
                owner_session_generation_id=owner_session_generation_id,
                operation="scheduler_cancel",
                method="POST",
                path=f"/scheduler/jobs/{scheduler_job_id}/cancel",
                body={"provider": provider},
                response_timeout_seconds=_cleanup_command_deadline_seconds(deadline),
                ssh_fallback=bounded_ssh_cancel,
            )
            # Review M2: mirror the local branch below -- populate cancel_detail
            # from the channel's {accepted, returncode, stdout, stderr} shape.
            if isinstance(raw_cancel, dict):
                cancel_document = cast(dict[str, object], raw_cancel)
                accepted = cancel_document.get("accepted") is True
                raw_stderr = cancel_document.get("stderr")
                raw_stdout = cancel_document.get("stdout")
                stderr_text = raw_stderr.strip() if isinstance(raw_stderr, str) else ""
                stdout_text = raw_stdout.strip() if isinstance(raw_stdout, str) else ""
                cancel_detail = stderr_text or stdout_text or None
        else:
            result = scheduler_providers.provider_for_scheduler(provider).cancel(scheduler_job_id)
            accepted = result.returncode == 0
            cancel_detail = result.stderr.strip() or result.stdout.strip() or None
    except (RelayError, json.JSONDecodeError) as exc:
        cancel_detail = str(exc)

    last_phase = "unknown"
    while time.monotonic() < deadline:
        try:
            if remote_cli.should_execute_on_cluster(definition):
                raw_status = remote_channel_dispatch.dial_or_route_json(
                    definition=definition,
                    owner_session_id=owner_session_id,
                    owner_session_generation_id=owner_session_generation_id,
                    operation="scheduler_status",
                    method="GET",
                    path=f"/scheduler/jobs/{scheduler_job_id}/status",
                    query={"provider": provider},
                    response_timeout_seconds=_cleanup_command_deadline_seconds(deadline),
                    ssh_fallback=bounded_ssh_status,
                )
                if not isinstance(raw_status, dict):
                    raise RelayError("scheduler status did not return a JSON object")
                phase = cast(dict[str, object], raw_status).get("phase")
                last_phase = str(phase) if phase is not None else "unknown"
            else:
                last_phase = (
                    scheduler_providers.provider_for_scheduler(provider)
                    .poll(scheduler_job_id)
                    .phase.value
                )
        except (RelayError, json.JSONDecodeError) as exc:
            cancel_detail = str(exc)
        if last_phase == "canceled":
            return (
                CleanupResource(
                    kind="scheduler_job",
                    resource_id=scheduler_job_id,
                    location=definition.ssh_host,
                    action="cancel",
                    ownership_verified=True,
                    outcome="canceled",
                    provider=provider,
                    verified_after_operation=True,
                    observed_state=last_phase,
                    detail="scheduler reported the canceled terminal phase",
                    metadata={"relay_job_id": relay_job_id},
                ),
                None,
            )
        if last_phase in {"completed", "failed"}:
            return (
                CleanupResource(
                    kind="scheduler_job",
                    resource_id=scheduler_job_id,
                    location=definition.ssh_host,
                    action="cancel",
                    ownership_verified=True,
                    outcome="terminal",
                    provider=provider,
                    verified_after_operation=True,
                    observed_state=last_phase,
                    detail=(
                        "scheduler reached a terminal phase during the cancellation race; "
                        f"cancellation is not claimed: accepted={accepted}, phase={last_phase}"
                        + (f", detail={cancel_detail}" if cancel_detail else "")
                    ),
                    metadata={"relay_job_id": relay_job_id},
                ),
                None,
            )
        time.sleep(poll_seconds)

    detail = (
        f"scheduler cancellation was not confirmed: accepted={accepted}, phase={last_phase}"
        + (f", detail={cancel_detail}" if cancel_detail else "")
    )
    return (
        CleanupResource(
            kind="scheduler_job",
            resource_id=scheduler_job_id,
            location=definition.ssh_host,
            action="cancel",
            ownership_verified=True,
            outcome="failed",
            provider=provider,
            residual=True,
            detail=detail,
            metadata={"relay_job_id": relay_job_id},
        ),
        detail,
    )
