"""Owned relay job discovery, reading, and cancellation (iowarp/clio-
relay#231 continuation): the ``_OwnedRelayJob`` projection and the
listing/cancellation helpers ``session teardown`` uses to find and
cancel this owner session's active relay jobs, local or remote."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import cast
from uuid import uuid4

import clio_relay.core_queue as core_queue
import clio_relay.remote_channel_dispatch as remote_channel_dispatch
import clio_relay.remote_cli as remote_cli
from clio_relay.cluster_config import (
    ClusterDefinition,
)
from clio_relay.errors import RelayError
from clio_relay.models import (
    JobState,
)
from clio_relay.owner_session_admission import (
    owner_session_admission_status,
)
from clio_relay.pagination import MAX_RESPONSE_PAGE_RECORDS
from clio_relay.relay_ops import cancel_job as request_cancel_job

REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS = 120.0


@dataclass(frozen=True)
class _OwnedRelayJob:
    job_id: str
    relay_state: JobState
    scheduler_job_ids: tuple[str, ...]
    scheduler_provider: str
    owner_session_generation_id: str | None = None
    unowned_scheduler_job_ids: tuple[str, ...] = ()
    relay_cancellation_requested: bool = False
    relay_cancellation_acknowledged: bool = False
    relay_cancellation_scheduler_requested: bool | None = None


def _quiesce_owner_session_intake(
    *,
    queue: core_queue.ClioCoreQueue,
    definition: ClusterDefinition,
    remote_execution: bool,
    session_id: str,
    local_admission_session_id: str,
    session_generation_id: str,
    cleanup_operation_id: str,
    stop_worker: bool,
    cancel_jobs: bool,
    cancel_scheduler_jobs: bool,
) -> dict[str, object]:
    """Quiesce desktop and authoritative intake under one immutable operation id."""
    existing_local_intent = queue.get_owner_session_cleanup_intent(
        local_admission_session_id,
        session_generation_id=session_generation_id,
    )
    if existing_local_intent is None:
        queue.mirror_owner_session_generation_open(
            local_admission_session_id,
            session_generation_id=session_generation_id,
        )
    local_intent = queue.set_owner_session_closing(
        local_admission_session_id,
        session_generation_id=session_generation_id,
        operation_id=cleanup_operation_id,
        stop_worker=stop_worker,
        cancel_jobs=cancel_jobs,
        cancel_scheduler_jobs=cancel_scheduler_jobs,
    )
    if not remote_execution:
        authoritative_intent = queue.set_owner_session_closing(
            session_id,
            session_generation_id=session_generation_id,
            operation_id=cleanup_operation_id,
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
        )
        _require_matching_cleanup_intents(
            authoritative_intent,
            local_intent,
            cleanup_operation_id=cleanup_operation_id,
        )
        return authoritative_intent
    command = [
        "session",
        "quiesce-intake",
        "--session-id",
        session_id,
        "--session-generation-id",
        session_generation_id,
        "--cleanup-operation-id",
        cleanup_operation_id,
    ]
    if stop_worker:
        command.append("--cleanup-stop-worker")
    if cancel_jobs:
        command.append("--cleanup-cancel-jobs")
    if cancel_scheduler_jobs:
        command.append("--cleanup-cancel-scheduler-jobs")
    raw_result = remote_channel_dispatch.dial_or_route_json(
        definition=definition,
        owner_session_id=session_id,
        owner_session_generation_id=session_generation_id,
        operation="quiesce_owner_session_intake",
        method="POST",
        path="/session/quiesce-intake",
        body={
            "cleanup_operation_id": cleanup_operation_id,
            "stop_worker": stop_worker,
            "cancel_jobs": cancel_jobs,
            "cancel_scheduler_jobs": cancel_scheduler_jobs,
        },
        ssh_fallback=lambda: json.loads(remote_cli.run_remote_clio(definition, command)),
    )
    if not isinstance(raw_result, dict):
        raise RelayError("remote owner-session intake quiescence returned no evidence")
    result = cast(dict[str, object], raw_result)
    if (
        result.get("session_id") != session_id
        or result.get("session_generation_id") != session_generation_id
        or result.get("intake") != "quiesced"
    ):
        raise RelayError("remote owner-session intake quiescence identity did not match")
    raw_intent = result.get("cleanup_intent")
    if not isinstance(raw_intent, dict):
        raise RelayError("remote owner-session intake quiescence omitted cleanup intent")
    intent = {str(key): value for key, value in cast(dict[object, object], raw_intent).items()}
    expected_policy = {
        "stop_worker": stop_worker,
        "cancel_jobs": cancel_jobs,
        "cancel_scheduler_jobs": cancel_scheduler_jobs,
    }
    if (
        intent.get("schema_version") != "clio-relay.owner-session-cleanup-intent.v1"
        or intent.get("owner_session_id") != session_id
        or intent.get("session_generation_id") != session_generation_id
        or intent.get("operation_id") != cleanup_operation_id
        or any(intent.get(key) is not value for key, value in expected_policy.items())
    ):
        raise RelayError("remote owner-session cleanup intent did not match requested policy")
    _require_matching_cleanup_intents(
        intent,
        local_intent,
        cleanup_operation_id=cleanup_operation_id,
    )
    return intent


def _require_matching_cleanup_intents(
    authoritative: dict[str, object],
    local: dict[str, object],
    *,
    cleanup_operation_id: str,
) -> None:
    """Require identical operation and policy across authoritative and desktop records."""
    keys = (
        "operation_id",
        "session_generation_id",
        "stop_worker",
        "cancel_jobs",
        "cancel_scheduler_jobs",
    )
    if (
        authoritative.get("operation_id") != cleanup_operation_id
        or local.get("operation_id") != cleanup_operation_id
        or any(authoritative.get(key) != local.get(key) for key in keys)
    ):
        raise RelayError("desktop and authoritative owner-session cleanup intents did not match")


def _owner_session_admission_status(
    *,
    queue: core_queue.ClioCoreQueue,
    definition: ClusterDefinition,
    remote_execution: bool,
    session_id: str,
    session_generation_id: str,
) -> dict[str, object]:
    """Read owner-session intake (clio-relay#179: rides GET /session/admission-status when live)."""
    remote_cli_runner = remote_cli.run_remote_clio
    if remote_execution:
        remote_cli_runner = (
            remote_channel_dispatch.channel_backed_json_runner(
                definition=definition,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
                operation="owner_session_admission_status",
                method="GET",
                path="/session/admission-status",
            )
            or remote_cli_runner
        )
    return owner_session_admission_status(
        queue=queue,
        definition=definition,
        remote_execution=remote_execution,
        session_id=session_id,
        session_generation_id=session_generation_id,
        remote_cli_runner=remote_cli_runner,
    )


def _select_owner_session_cleanup_operation(
    *,
    authoritative_status: dict[str, object],
    local_intent: dict[str, object] | None,
    session_id: str,
    session_generation_id: str,
    stop_worker: bool,
    cancel_jobs: bool,
    cancel_scheduler_jobs: bool,
) -> str:
    """Reuse a retry operation or choose one id before the first cleanup mutation."""
    import clio_relay.cli as cli

    cli._require_durable_session_identity(
        session_generation_id,
        field="session_generation_id",
    )
    if not (
        authoritative_status.get("owner_session_id") == session_id
        and authoritative_status.get("session_generation_id") == session_generation_id
    ):
        raise RelayError("owner-session cleanup admission identity changed")
    if not (
        authoritative_status.get("open") is True or authoritative_status.get("closing") is True
    ):
        raise RelayError("owner-session generation is neither open nor a resumable cleanup")
    raw_authoritative_intent = authoritative_status.get("cleanup_intent")
    authoritative_intent = (
        cast(dict[str, object], raw_authoritative_intent)
        if isinstance(raw_authoritative_intent, dict)
        else None
    )
    expected_policy = {
        "stop_worker": stop_worker,
        "cancel_jobs": cancel_jobs,
        "cancel_scheduler_jobs": cancel_scheduler_jobs,
    }
    operation_ids: set[str] = set()
    for intent in (authoritative_intent, local_intent):
        if intent is None:
            continue
        if intent.get("session_generation_id") != session_generation_id or any(
            intent.get(key) is not value for key, value in expected_policy.items()
        ):
            raise RelayError("owner-session cleanup retry changed generation or policy")
        operation_id = intent.get("operation_id")
        if not isinstance(operation_id, str):
            raise RelayError("owner-session cleanup retry omitted its operation id")
        cli._require_durable_session_identity(operation_id, field="operation_id")
        operation_ids.add(operation_id)
    if len(operation_ids) > 1:
        raise RelayError("desktop and authoritative cleanup operation ids disagree")
    return next(iter(operation_ids), f"cleanup_{uuid4().hex}")


def _list_owned_active_cluster_jobs(
    queue: core_queue.ClioCoreQueue,
    cluster: str,
    *,
    owner_session_id: str,
    owner_session_generation_id: str | None = None,
    scheduler_provider: str,
    include_terminal: bool = False,
) -> list[_OwnedRelayJob]:
    owned: list[_OwnedRelayJob] = []
    membership_generations = [owner_session_generation_id]
    for membership_generation in membership_generations:
        cursor: str | None = None
        expected_total: int | None = None
        processed_source = 0
        while True:
            jobs, next_cursor, total, source_window_count = queue.list_owner_session_jobs_page(
                owner_session_id,
                session_generation_id=membership_generation,
                cursor=cursor,
                limit=MAX_RESPONSE_PAGE_RECORDS,
                cluster=cluster,
                include_terminal=include_terminal,
            )
            if expected_total is not None and total != expected_total:
                raise RelayError("owner-session membership changed during local discovery")
            expected_total = total
            processed_source += source_window_count
            for job in jobs:
                job_document = job.model_dump(mode="json")
                tasks, tasks_truncated = queue.scan_job_tasks(job.job_id, limit=1_000)
                if tasks_truncated:
                    raise RelayError(f"owner-session task discovery was truncated: {job.job_id}")
                task_documents = [task.model_dump(mode="json") for task in tasks]
                candidate = _owned_relay_job(
                    job_document,
                    task_documents,
                    scheduler_provider=scheduler_provider,
                )
                if include_terminal or _relay_job_needs_cleanup(candidate):
                    owned.append(candidate)
            if next_cursor is None:
                if processed_source != total:
                    raise RelayError("owner-session membership ended before its declared total")
                break
            if cursor is not None and next_cursor <= cursor:
                raise RelayError("owner-session membership cursor did not advance")
            cursor = next_cursor
    return owned


def _cancel_local_owned_jobs(
    queue: core_queue.ClioCoreQueue,
    jobs: list[_OwnedRelayJob],
) -> list[str]:
    requested: list[str] = []
    for job in jobs:
        if job.relay_state not in {JobState.QUEUED, JobState.LEASED, JobState.RUNNING}:
            continue
        canceled_job = request_cancel_job(
            queue,
            job.job_id,
            cancel_scheduler=False,
        )
        observed = _owned_relay_job(
            canceled_job.model_dump(mode="json"),
            [],
            scheduler_provider=job.scheduler_provider,
        )
        if observed.relay_state in {JobState.SUCCEEDED, JobState.FAILED}:
            continue
        _require_durable_relay_cancellation(observed)
        requested.append(canceled_job.job_id)
    return requested


def _cancel_remote_owned_jobs(
    definition: ClusterDefinition,
    cluster: str,
    jobs: list[_OwnedRelayJob],
    *,
    owner_session_id: str,
    owner_session_generation_id: str,
) -> list[str]:
    requested: list[str] = []
    for job in jobs:
        if job.relay_state not in {JobState.QUEUED, JobState.LEASED, JobState.RUNNING}:
            continue
        raw_result = remote_channel_dispatch.dial_or_route_json(
            definition=definition,
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
            operation="cancel_remote_owned_job",
            method="POST",
            path=f"/queue/jobs/{job.job_id}/cancel",
            body={"cluster": cluster, "cancel_scheduler_job": False},
            ssh_fallback=lambda job=job: json.loads(
                remote_cli.run_remote_clio(
                    definition,
                    ["queue", "cancel", job.job_id, "--cluster", cluster, "--keep-scheduler-job"],
                )
            ),
        )
        if not isinstance(raw_result, dict):
            raise RelayError(f"owned relay cancellation returned no result: {job.job_id}")
        result = cast(dict[str, object], raw_result)
        if not isinstance(result.get("cancellation_requested"), bool):
            raise RelayError(f"owned relay cancellation omitted request evidence: {job.job_id}")
        raw_job = result.get("job")
        if not isinstance(raw_job, dict):
            raise RelayError(f"owned relay cancellation omitted its job: {job.job_id}")
        observed = _owned_relay_job(
            {str(key): value for key, value in cast(dict[object, object], raw_job).items()},
            [],
            scheduler_provider=job.scheduler_provider,
        )
        if observed.job_id != job.job_id:
            raise RelayError(f"owned relay cancellation returned a different job: {job.job_id}")
        if observed.relay_state in {JobState.SUCCEEDED, JobState.FAILED}:
            continue
        _require_durable_relay_cancellation(observed)
        requested.append(job.job_id)
    return requested


def _require_durable_relay_cancellation(job: _OwnedRelayJob) -> None:
    """Require the exact relay-only request and any terminal cleanup acknowledgment."""
    if (
        not job.relay_cancellation_requested
        or job.relay_cancellation_scheduler_requested is not False
    ):
        raise RelayError(f"owned relay job cancellation was not durable: {job.job_id}")
    if job.relay_state is JobState.CANCELED and not job.relay_cancellation_acknowledged:
        raise RelayError(
            f"owned relay job was canceled without worker cleanup acknowledgment: {job.job_id}"
        )


def _read_owned_relay_job(
    *,
    queue: core_queue.ClioCoreQueue,
    definition: ClusterDefinition,
    remote_execution: bool,
    cluster: str,
    job_id: str,
    owner_session_id: str,
    owner_session_generation_id: str,
) -> _OwnedRelayJob:
    """Read one exact cancellation target and reverify its owner-session identity."""
    if remote_execution:
        raw_status = remote_channel_dispatch.dial_or_route_json(
            definition=definition,
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
            operation="read_owned_relay_job",
            method="GET",
            path=f"/jobs/{job_id}/status",
            ssh_fallback=lambda: json.loads(
                remote_cli.run_remote_clio(definition, ["job", "status", job_id])
            ),
        )
        if not isinstance(raw_status, dict):
            raise RelayError(f"remote relay cancellation status was not an object: {job_id}")
        raw_job = cast(dict[str, object], raw_status).get("job")
        if not isinstance(raw_job, dict):
            raise RelayError(f"remote relay cancellation status omitted its job: {job_id}")
        document = {str(key): value for key, value in cast(dict[object, object], raw_job).items()}
    else:
        document = queue.get_job(job_id).model_dump(mode="json")
    if document.get("job_id") != job_id or document.get("cluster") != cluster:
        raise RelayError(f"relay cancellation target identity changed: {job_id}")
    if not _job_is_owned_by_session(
        document,
        owner_session_id,
        owner_session_generation_id=owner_session_generation_id,
    ):
        raise RelayError(f"relay cancellation target ownership changed: {job_id}")
    return _owned_relay_job(
        document,
        [],
        scheduler_provider=definition.scheduler_provider,
    )


def _wait_for_owned_relay_cancellations(
    job_ids: list[str],
    *,
    read_owned_job: Callable[[str], _OwnedRelayJob],
    timeout_seconds: float,
    poll_seconds: float,
) -> list[str]:
    """Wait boundedly for worker cleanup to acknowledge exact durable cancel requests."""
    if timeout_seconds <= 0:
        raise ValueError("relay cancellation timeout must be positive")
    if poll_seconds <= 0:
        raise ValueError("relay cancellation polling interval must be positive")
    pending = dict.fromkeys(job_ids)
    if len(pending) != len(job_ids):
        raise RelayError("relay cancellation targets must be unique")
    deadline = time.monotonic() + timeout_seconds
    last_states: dict[str, str] = {}
    while pending:
        for job_id in list(pending):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                detail = ", ".join(
                    f"{pending_id}={last_states.get(pending_id, 'missing')}"
                    for pending_id in sorted(pending)
                )
                raise RelayError(
                    "timed out waiting for worker-acknowledged relay cancellation: " + detail
                )
            with remote_cli.remote_command_timeout(
                min(REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS, remaining)
            ):
                observed = read_owned_job(job_id)
            last_states[job_id] = observed.relay_state.value
            _require_durable_relay_cancellation(observed)
            if observed.relay_state is JobState.CANCELED:
                if not observed.relay_cancellation_acknowledged:
                    raise RelayError(
                        "owned relay cancellation reached CANCELED without cleanup evidence: "
                        f"{job_id}"
                    )
                pending.pop(job_id)
                continue
            if observed.relay_state in {JobState.SUCCEEDED, JobState.FAILED}:
                raise RelayError(
                    "owned relay cancellation became terminal without acknowledged cleanup: "
                    f"{job_id} ({observed.relay_state.value})"
                )
        if not pending:
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            detail = ", ".join(
                f"{job_id}={last_states.get(job_id, 'missing')}" for job_id in sorted(pending)
            )
            raise RelayError(
                "timed out waiting for worker-acknowledged relay cancellation: " + detail
            )
        time.sleep(min(poll_seconds, remaining))
    return list(job_ids)


def _job_is_owned_by_session(
    job: dict[str, object],
    owner_session_id: str,
    *,
    owner_session_generation_id: str | None = None,
) -> bool:
    metadata = job.get("metadata")
    if not isinstance(metadata, dict):
        return False
    typed_metadata = cast(dict[str, object], metadata)
    if (
        typed_metadata.get("owner") != "clio-relay"
        or typed_metadata.get("owner_session_id") != owner_session_id
    ):
        return False
    recorded_generation = typed_metadata.get("owner_session_generation_id")
    return recorded_generation == owner_session_generation_id


def _relay_cancellation_evidence(
    job_id: str,
    metadata: dict[str, object],
) -> tuple[bool, bool, bool | None]:
    """Parse the durable cancellation request and cleanup acknowledgment contract."""
    raw_request = metadata.get("cancellation_request")
    if raw_request is None:
        return False, False, None
    if not isinstance(raw_request, dict):
        raise RelayError(f"owned relay job has invalid cancellation evidence: {job_id}")
    request = cast(dict[str, object], raw_request)
    requested_at = request.get("requested_at")
    previous_state = request.get("previous_state")
    cancel_scheduler = request.get("cancel_scheduler")
    if (
        request.get("schema_version") != "clio-relay.cancellation-request.v1"
        or not isinstance(requested_at, str)
        or previous_state
        not in {
            JobState.QUEUED.value,
            JobState.LEASED.value,
            JobState.RUNNING.value,
        }
        or not isinstance(cancel_scheduler, bool)
    ):
        raise RelayError(f"owned relay job has invalid cancellation evidence: {job_id}")
    try:
        parsed_requested_at = datetime.fromisoformat(requested_at)
    except ValueError as exc:
        raise RelayError(
            f"owned relay job has invalid cancellation request time: {job_id}"
        ) from exc
    if parsed_requested_at.tzinfo is None:
        raise RelayError(f"owned relay job cancellation request time is naive: {job_id}")
    acknowledged = request.get("cleanup_acknowledged") is True
    acknowledged_at = request.get("acknowledged_at")
    if acknowledged:
        if not isinstance(acknowledged_at, str):
            raise RelayError(
                f"owned relay job cancellation acknowledgment omitted its time: {job_id}"
            )
        try:
            parsed_acknowledged_at = datetime.fromisoformat(acknowledged_at)
        except ValueError as exc:
            raise RelayError(
                f"owned relay job has invalid cancellation acknowledgment time: {job_id}"
            ) from exc
        if parsed_acknowledged_at.tzinfo is None:
            raise RelayError(f"owned relay job cancellation acknowledgment time is naive: {job_id}")
    elif acknowledged_at is not None:
        raise RelayError(
            f"owned relay job has an acknowledgment time without cleanup proof: {job_id}"
        )
    return True, acknowledged, cancel_scheduler


def _owned_relay_job(
    job: dict[str, object],
    tasks: list[dict[str, object]],
    *,
    scheduler_provider: str,
) -> _OwnedRelayJob:
    job_id = job.get("job_id")
    if not isinstance(job_id, str):
        raise RelayError("owned relay job is missing a job id")
    raw_state = job.get("state")
    if not isinstance(raw_state, str):
        raise RelayError(f"owned relay job is missing its state: {job_id}")
    try:
        relay_state = JobState(raw_state)
    except ValueError as exc:
        raise RelayError(f"owned relay job has an invalid state: {job_id}: {raw_state}") from exc
    job_metadata = job.get("metadata")
    if not isinstance(job_metadata, dict):
        raise RelayError(f"owned relay job is missing metadata: {job_id}")
    typed_job_metadata = cast(dict[str, object], job_metadata)
    raw_generation_id = typed_job_metadata.get("owner_session_generation_id")
    if raw_generation_id is not None and not isinstance(raw_generation_id, str):
        raise RelayError(f"owned relay job has an invalid session generation: {job_id}")
    (
        cancellation_requested,
        cancellation_acknowledged,
        cancellation_scheduler_requested,
    ) = _relay_cancellation_evidence(job_id, typed_job_metadata)
    documents = [job, *tasks]
    observed_scheduler_job_ids: list[str] = []
    owned_scheduler_job_ids: list[str] = []
    provider = _normalized_scheduler_provider(scheduler_provider)
    task_ids = {
        task_id for task in tasks if isinstance((task_id := task.get("task_id")), str) and task_id
    }
    for document in documents:
        metadata = document.get("metadata")
        if not isinstance(metadata, dict):
            continue
        typed_metadata = cast(dict[str, object], metadata)
        runtime = typed_metadata.get("runtime_metadata")
        if isinstance(runtime, dict):
            typed_runtime = cast(dict[str, object], runtime)
            _append_scheduler_job_id(
                observed_scheduler_job_ids,
                typed_runtime.get("scheduler_job_id"),
            )
        _append_scheduler_job_id(
            observed_scheduler_job_ids,
            typed_metadata.get("scheduler_job_id"),
        )
        stored_ids = typed_metadata.get("scheduler_job_ids")
        if isinstance(stored_ids, list):
            for stored_id in cast(list[object], stored_ids):
                _append_scheduler_job_id(observed_scheduler_job_ids, stored_id)
        scheduler_status = typed_metadata.get("scheduler_status")
        if isinstance(scheduler_status, dict):
            typed_status = cast(dict[str, object], scheduler_status)
            _append_scheduler_job_id(
                observed_scheduler_job_ids,
                typed_status.get("scheduler_job_id"),
            )
        ownership_records = typed_metadata.get("scheduler_job_ownership")
        if not isinstance(ownership_records, list):
            continue
        document_task_id = document.get("task_id")
        for raw_record in cast(list[object], ownership_records):
            if not isinstance(raw_record, dict):
                continue
            record = cast(dict[str, object], raw_record)
            scheduler_job_id = record.get("scheduler_job_id")
            _append_scheduler_job_id(observed_scheduler_job_ids, scheduler_job_id)
            if not isinstance(scheduler_job_id, str) or not scheduler_job_id:
                continue
            record_task_id = record.get("task_id")
            record_provider = record.get("scheduler_provider")
            record_execution_id = record.get("execution_id")
            source = record.get("runtime_metadata_source")
            expected_proofs = {
                "jarvis_mcp": {"owned_jarvis_run_mcp_result"},
                "jarvis_sidecar": {
                    "authenticated_runtime_sidecar",
                    "exact_scheduler_marker_reconciliation",
                },
                "relay_reconciliation": {"exact_scheduler_marker_reconciliation"},
            }.get(source if isinstance(source, str) else "", set())
            if (
                record.get("ownership_verified") is not True
                or record.get("relay_job_id") != job_id
                or not isinstance(document_task_id, str)
                or not isinstance(record_task_id, str)
                or record_task_id not in task_ids
                or document_task_id != record_task_id
                or not isinstance(record_provider, str)
                or _normalized_scheduler_provider(record_provider) != provider
                or not isinstance(record_execution_id, str)
                or not record_execution_id
                or not expected_proofs
                or typed_metadata.get("runtime_metadata_source") != source
                or record.get("proof") not in expected_proofs
            ):
                continue
            _append_scheduler_job_id(owned_scheduler_job_ids, scheduler_job_id)
    unowned_scheduler_job_ids = [
        scheduler_job_id
        for scheduler_job_id in observed_scheduler_job_ids
        if scheduler_job_id not in owned_scheduler_job_ids
    ]
    return _OwnedRelayJob(
        job_id=job_id,
        relay_state=relay_state,
        scheduler_job_ids=tuple(owned_scheduler_job_ids),
        scheduler_provider=provider,
        owner_session_generation_id=raw_generation_id,
        unowned_scheduler_job_ids=tuple(unowned_scheduler_job_ids),
        relay_cancellation_requested=cancellation_requested,
        relay_cancellation_acknowledged=cancellation_acknowledged,
        relay_cancellation_scheduler_requested=cancellation_scheduler_requested,
    )


def _relay_job_needs_cleanup(job: _OwnedRelayJob) -> bool:
    return (
        job.relay_state in {JobState.QUEUED, JobState.LEASED, JobState.RUNNING}
        or bool(job.scheduler_job_ids)
        or bool(job.unowned_scheduler_job_ids)
    )


def _normalized_scheduler_provider(value: str) -> str:
    return value.strip().lower().replace("_", "-")


def _append_scheduler_job_id(target: list[str], value: object) -> None:
    if isinstance(value, str) and value and value not in target:
        target.append(value)
