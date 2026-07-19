from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import cast

from pytest import MonkeyPatch, raises

import clio_relay.core_queue as core_queue_module
import clio_relay.queue_management as queue_management_module
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, QueueConflictError
from clio_relay.models import (
    Cursor,
    EndpointRegistration,
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    Lease,
    McpAdmissionClass,
    McpCallSpec,
    ProgressRecord,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
    SchedulerPhase,
    SchedulerStatus,
    utc_now,
)
from clio_relay.queue_management import (
    cancel_queue_job,
    cleanup_stale_jobs,
    diagnose_job,
    diagnose_queue,
    discover_stale_jobs,
    list_queue_jobs,
    worker_status,
)


def test_queue_list_filters_active_jobs_and_reports_position(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    first = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "first"]),
            idempotency_key="queue-first",
        )
    )
    second = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "second"]),
            idempotency_key="queue-second",
        )
    )
    queue.update_job_state(first.job_id, JobState.SUCCEEDED)

    active = list_queue_jobs(queue, cluster="ares")
    all_jobs = list_queue_jobs(queue, cluster="ares", include_terminal=True)
    active_jobs = cast(list[dict[str, object]], active["jobs"])
    active_job = cast(dict[str, object], active_jobs[0]["job"])

    assert active["count"] == 1
    assert active_job["job_id"] == second.job_id
    assert active_jobs[0]["relay_queue"] == {
        "state": "queued",
        "jobs_ahead": 0,
        "position": 1,
    }
    assert all_jobs["count"] == 2


def test_equal_timestamp_jobs_follow_durable_submission_sequence(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    submitted_at = utc_now()
    first = queue.submit_job(
        RelayJob(
            job_id="job_z-first",
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "first"]),
            idempotency_key="same-time-first",
            created_at=submitted_at,
            updated_at=submitted_at,
        )
    )
    second = queue.submit_job(
        RelayJob(
            job_id="job_a-second",
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "second"]),
            idempotency_key="same-time-second",
            created_at=submitted_at,
            updated_at=submitted_at,
        )
    )

    listed = list_queue_jobs(queue, cluster="ares", limit=10, scan_limit=10)
    summaries = cast(list[dict[str, object]], listed["jobs"])
    evidence_by_id = {
        cast(dict[str, object], summary["job"])["job_id"]: cast(
            dict[str, object], summary["relay_queue"]
        )
        for summary in summaries
    }
    lease = queue.acquire_next_job("worker", cluster="ares")

    assert evidence_by_id[first.job_id]["position"] == 1
    assert evidence_by_id[second.job_id]["position"] == 2
    assert lease is not None
    assert lease.job_id == first.job_id


def test_queue_list_filters_kind_and_reports_bounded_scan(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "jarvis"]),
            idempotency_key="bounded-jarvis",
        )
    )
    queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/prompt.md"),
            idempotency_key="bounded-agent",
        )
    )
    queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/other.md"),
            idempotency_key="bounded-agent-2",
        )
    )

    result = list_queue_jobs(
        queue,
        cluster="ares",
        kind=JobKind.REMOTE_AGENT,
        limit=1,
        scan_limit=2,
    )
    jobs = cast(list[dict[str, object]], result["jobs"])

    assert jobs == []
    assert result["count"] == 0
    assert result["source_cursor"] == 1
    assert result["source_limit"] == 1
    assert result["source_next_cursor"] == 2
    assert result["source_total"] == 3
    assert result["scan_truncated"] is False
    assert result["result_truncated"] is True


def test_specific_job_diagnosis_exposes_reason_and_operational_evidence(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    blocker = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="diagnosis-blocker",
        )
    )
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="diagnosis-target",
        )
    )
    queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="worker",
            pid=123,
        )
    )
    queue.append_progress(ProgressRecord(job_id=job.job_id, label="queued", message="waiting"))
    queue.append_task(
        RelayTask(
            job_id=job.job_id,
            name="scheduler",
            metadata={
                "scheduler_status": SchedulerStatus(
                    scheduler="slurm",
                    scheduler_job_id="1234",
                    phase=SchedulerPhase.PENDING,
                ).model_dump(mode="json")
            },
        )
    )

    diagnosis = diagnose_job(queue, job.job_id, cluster="ares", stale_after_seconds=3600)
    queue_evidence = cast(dict[str, object], diagnosis["queue"])
    worker = cast(dict[str, object], diagnosis["worker"])

    assert diagnosis["reason"] == "blocked_by_admissible_jobs_ahead"
    assert diagnosis["terminal"] is False
    assert queue_evidence["blocking_job_ids"] == [blocker.job_id]
    assert queue_evidence["raw_preceding_job_ids"] == [blocker.job_id]
    admission = cast(dict[str, object], queue_evidence["admission"])
    assert admission["analysis_complete"] is True
    assert admission["target_admissible_now"] is False
    assert admission["target_ineligibility"] == "admissible_predecessors_consumed_capacity"
    assert cast(dict[str, object], diagnosis["lease"])["present"] is False
    assert worker["healthy_worker_count"] == 1
    assert cast(list[dict[str, object]], diagnosis["scheduler"])[0]["task_id"]
    assert cast(dict[str, object], diagnosis["last_event"])["event_type"] == "task.queued"
    assert cast(dict[str, object], diagnosis["last_progress"])["message"] == "waiting"


def test_diagnosis_skips_saturated_kind_without_false_head_of_line_blocker(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    policy = {"kind_concurrency": {"remote_agent": 1}}
    supervisor = queue.register_endpoint(
        EndpointRegistration(
            endpoint_id="worker-supervisor",
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="worker",
            pid=101,
            metadata={**policy, "concurrency": 2, "worker_supervisor": True},
        )
    )
    slots = [
        queue.register_endpoint(
            EndpointRegistration(
                endpoint_id=f"worker-slot-{index}",
                role=EndpointRole.WORKER,
                cluster="ares",
                hostname="worker",
                pid=101,
                metadata={
                    **policy,
                    "concurrency": 1,
                    "worker_slot": index,
                    "parent_endpoint_id": supervisor.endpoint_id,
                },
            )
        )
        for index in range(2)
    ]
    running_remote = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/running.md"),
            idempotency_key="diagnosis-running-remote",
        )
    )
    saturated_remote = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/saturated.md"),
            idempotency_key="diagnosis-saturated-remote",
        )
    )
    jarvis = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="diagnosis-eligible-jarvis",
        )
    )
    lease = queue.acquire_next_job(
        slots[0].endpoint_id,
        cluster="ares",
        kind_concurrency={JobKind.REMOTE_AGENT: 1},
    )

    diagnosis = diagnose_job(queue, jarvis.job_id, cluster="ares")

    assert lease is not None and lease.job_id == running_remote.job_id
    queue_evidence = cast(dict[str, object], diagnosis["queue"])
    admission = cast(dict[str, object], queue_evidence["admission"])
    assert diagnosis["reason"] == "eligible_for_admission"
    assert queue_evidence["raw_preceding_job_ids"] == [saturated_remote.job_id]
    assert queue_evidence["blocking_job_ids"] == []
    assert admission["analysis_complete"] is True
    assert admission["target_admissible_now"] is True
    assert admission["configured_worker_slots"] == 2
    assert admission["free_worker_slots"] == 1
    assert admission["effective_blocking_job_ids"] == []
    assert admission["skipped_predecessors"] == [
        {"job_id": saturated_remote.job_id, "reason": "kind_capacity_saturated"}
    ]

    next_lease = queue.acquire_next_job(
        slots[1].endpoint_id,
        cluster="ares",
        kind_concurrency={JobKind.REMOTE_AGENT: 1},
    )
    assert next_lease is not None and next_lease.job_id == jarvis.job_id


def test_diagnosis_fails_closed_when_lease_index_validation_fails(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    blocker = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="diagnosis-index-blocker",
        )
    )
    target = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="diagnosis-index-target",
        )
    )
    queue.register_endpoint(
        EndpointRegistration(
            endpoint_id="worker",
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="worker",
            pid=104,
        )
    )

    def fail_validation(*, cluster: str) -> tuple[dict[JobKind, int], int]:
        assert cluster == "ares"
        raise QueueConflictError("corrupt lease index " + "x" * 1_100)

    monkeypatch.setattr(queue, "lease_admission_capacity_snapshot", fail_validation)

    diagnosis = diagnose_job(queue, target.job_id, cluster="ares")

    queue_evidence = cast(dict[str, object], diagnosis["queue"])
    admission = cast(dict[str, object], queue_evidence["admission"])
    assert diagnosis["reason"] == "admission_analysis_incomplete"
    assert queue_evidence["raw_preceding_job_ids"] == [blocker.job_id]
    assert queue_evidence["blocking_job_ids"] == []
    assert admission["analysis_complete"] is False
    assert admission["incomplete_reasons"] == ["lease_index_validation_failed"]
    assert admission["lease_index_validated"] is False
    assert len(cast(str, admission["lease_index_validation_error"])) == 1_000
    assert admission["lease_index_validation_error_truncated"] is True


def test_diagnosis_models_predecessor_consuming_last_global_lease_slot(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(core_queue_module, "MAX_LIVE_LEASE_RECORDS", 2)
    monkeypatch.setattr(queue_management_module, "MAX_LIVE_LEASE_RECORDS", 2)
    queue = ClioCoreQueue(tmp_path / "core")
    external = queue.submit_job(
        RelayJob(
            cluster="homelab",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/external.md"),
            idempotency_key="diagnosis-global-external",
        )
    )
    external_lease = queue.acquire_next_job("external-worker", cluster="homelab")
    workers = [
        queue.register_endpoint(
            EndpointRegistration(
                endpoint_id=f"ares-worker-{index}",
                role=EndpointRole.WORKER,
                cluster="ares",
                hostname=f"worker-{index}",
                pid=200 + index,
            )
        )
        for index in range(2)
    ]
    predecessor = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="diagnosis-global-predecessor",
        )
    )
    target = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="diagnosis-global-target",
        )
    )

    diagnosis = diagnose_job(queue, target.job_id, cluster="ares")

    assert external_lease is not None and external_lease.job_id == external.job_id
    queue_evidence = cast(dict[str, object], diagnosis["queue"])
    admission = cast(dict[str, object], queue_evidence["admission"])
    assert diagnosis["reason"] == "blocked_by_admissible_jobs_ahead"
    assert queue_evidence["blocking_job_ids"] == [predecessor.job_id]
    assert admission["target_admissible_now"] is False
    assert admission["target_ineligibility"] == (
        "admissible_predecessors_consumed_global_lease_capacity"
    )
    assert admission["global_lease_capacity_remaining"] == 1
    assert admission["remaining_global_lease_capacity_at_target"] == 0
    assert admission["simulated_global_lease_count_at_target"] == 2
    assert admission["effective_blocking_job_ids"] == [predecessor.job_id]

    predecessor_lease = queue.acquire_next_job(workers[0].endpoint_id, cluster="ares")
    target_lease = queue.acquire_next_job(workers[1].endpoint_id, cluster="ares")
    assert predecessor_lease is not None and predecessor_lease.job_id == predecessor.job_id
    assert target_lease is None
    assert queue.get_job(target.job_id).state is JobState.QUEUED


def test_diagnosis_ignores_expired_terminal_lease_history(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    historical = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/historical.md"),
            idempotency_key="diagnosis-expired-history",
        )
    )
    historical_lease = queue.acquire_next_job(
        "retired-worker",
        cluster="ares",
        ttl_seconds=-1,
    )
    queue.update_job_state(historical.job_id, JobState.SUCCEEDED)
    live_other_cluster = queue.submit_job(
        RelayJob(
            cluster="homelab",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/live-other-cluster.md"),
            idempotency_key="diagnosis-live-other-cluster",
        )
    )
    other_lease = queue.acquire_next_job("homelab-worker", cluster="homelab")
    expired_other_cluster = queue.submit_job(
        RelayJob(
            cluster="homelab",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="diagnosis-expired-other-cluster",
        )
    )
    expired_other_lease = queue.acquire_next_job(
        "retired-homelab-worker",
        cluster="homelab",
        ttl_seconds=-1,
    )
    queue.update_job_state(expired_other_cluster.job_id, JobState.SUCCEEDED)
    queue.register_endpoint(
        EndpointRegistration(
            endpoint_id="current-worker",
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="worker",
            pid=103,
        )
    )
    target = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="diagnosis-after-expired-history",
        )
    )

    diagnosis = diagnose_job(queue, target.job_id, cluster="ares")

    assert historical_lease is not None
    assert other_lease is not None and other_lease.job_id == live_other_cluster.job_id
    assert expired_other_lease is not None
    admission = cast(
        dict[str, object],
        cast(dict[str, object], diagnosis["queue"])["admission"],
    )
    assert diagnosis["reason"] == "eligible_for_admission"
    assert admission["analysis_complete"] is True
    assert admission["global_lease_count"] == 2
    assert admission["lease_index_validated"] is True
    assert admission["active_lease_count"] == 0
    assert admission["expired_cluster_lease_job_ids"] == []


def test_specific_job_operations_reject_cluster_mismatch(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="cluster-mismatch",
        )
    )

    with raises(ConfigurationError, match="belongs to cluster ares"):
        diagnose_job(queue, job.job_id, cluster="homelab")
    with raises(ConfigurationError, match="belongs to cluster ares"):
        cancel_queue_job(queue, job.job_id, cluster="homelab")
    assert queue.get_job(job.job_id).state == JobState.QUEUED


def test_stale_discovery_and_cleanup_require_explicit_queued_cancellation(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/prompt.md"),
            idempotency_key="stale-queued",
        )
    )
    old = utc_now() - timedelta(hours=3)
    aged = job.model_copy(update={"created_at": old, "updated_at": old})
    (queue.root / "jobs" / f"{job.job_id}.json").write_text(
        aged.model_dump_json(indent=2), encoding="utf-8"
    )

    discovered = discover_stale_jobs(
        queue,
        cluster="ares",
        older_than_seconds=3600,
        kind=JobKind.REMOTE_AGENT,
    )
    dry_run = cleanup_stale_jobs(
        queue,
        cluster="ares",
        older_than_seconds=3600,
        kind=JobKind.REMOTE_AGENT,
        cancel_queued=True,
        dry_run=True,
    )
    safe_execute = cleanup_stale_jobs(
        queue,
        cluster="ares",
        older_than_seconds=3600,
        cancel_queued=False,
        dry_run=False,
    )

    assert discovered["count"] == 1
    assert dry_run["canceled_count"] == 0
    assert cast(list[dict[str, object]], dry_run["planned"])[0]["action"] == (
        "cancel_queued_relay_job"
    )
    assert safe_execute["canceled_count"] == 0
    assert safe_execute["scheduler_cancel_requested"] is False
    assert queue.get_job(job.job_id).state == JobState.QUEUED

    canceled = cleanup_stale_jobs(
        queue,
        cluster="ares",
        older_than_seconds=3600,
        cancel_queued=True,
        dry_run=False,
    )
    events, _ = queue.drain_events(job_id_cursor(job.job_id), limit=20)

    assert canceled["canceled_count"] == 1
    assert canceled["scheduler_cancel_requested"] is False
    assert queue.get_job(job.job_id).state == JobState.CANCELED
    assert (
        next(event for event in events if event.event_type == "job.cancel_requested").payload[
            "cancel_scheduler"
        ]
        is False
    )


def test_stale_cleanup_cancels_unowned_running_job_relay_only(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/prompt.md"),
            idempotency_key="stale-unowned-running",
        )
    )
    queue.update_job_state(
        job.job_id,
        JobState.RUNNING,
        leased_by="missing-worker",
    )

    preview = cleanup_stale_jobs(
        queue,
        cluster="ares",
        older_than_seconds=3600,
        dry_run=True,
    )
    executed = cleanup_stale_jobs(
        queue,
        cluster="ares",
        older_than_seconds=3600,
        dry_run=False,
    )

    assert cast(list[dict[str, object]], preview["planned"])[0]["action"] == (
        "cancel_stale_relay_job"
    )
    assert executed["canceled_count"] == 0
    assert executed["conflict_count"] == 1
    assert executed["scheduler_cancel_requested"] is False
    pending = queue.get_job(job.job_id)
    assert pending.state == JobState.RUNNING
    assert isinstance(pending.metadata.get("cancellation_request"), dict)


def test_queue_diagnose_and_cleanup_stale_expired_lease(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="expired-lease",
        )
    )
    lease = queue.acquire_next_job("endpoint-1", cluster="ares", ttl_seconds=-1)

    diagnostics = diagnose_queue(queue, cluster="ares")
    dry_run = cleanup_stale_jobs(queue, cluster="ares", dry_run=True)
    recovered = cleanup_stale_jobs(queue, cluster="ares", dry_run=False)
    diagnostic_issues = cast(list[dict[str, object]], diagnostics["issues"])
    recoverable = cast(list[dict[str, object]], dry_run["recoverable"])
    recoverable_job = cast(dict[str, object], recoverable[0]["job"])

    assert lease is not None
    assert diagnostics["issue_count"] == 1
    assert diagnostic_issues[0]["code"] == "expired_lease"
    assert dry_run["recovered_count"] == 0
    assert recoverable_job["job_id"] == job.job_id
    assert recovered["recovered_count"] == 1
    assert queue.get_job(job.job_id).state.value == "queued"


def test_stale_lease_with_active_scheduler_is_canceled_without_resubmission(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="expired-lease-active-scheduler",
        )
    )
    queue.acquire_next_job("endpoint-1", cluster="ares", ttl_seconds=-1)
    queue.append_task(
        RelayTask(
            job_id=job.job_id,
            name="scheduler",
            metadata={
                "scheduler_status": SchedulerStatus(
                    scheduler="slurm",
                    scheduler_job_id="1234",
                    phase=SchedulerPhase.PENDING,
                ).model_dump(mode="json")
            },
        )
    )
    queued_after_stale = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "next"]),
            idempotency_key="after-expired-lease-active-scheduler",
        )
    )
    queue.register_endpoint(
        EndpointRegistration(
            endpoint_id="endpoint-2",
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="worker-2",
            pid=456,
        )
    )
    next_lease = queue.acquire_next_job("endpoint-2", cluster="ares")

    diagnosis = diagnose_job(queue, job.job_id, cluster="ares")
    state_before_cleanup = queue.get_job(job.job_id).state
    cleaned = cleanup_stale_jobs(queue, cluster="ares", dry_run=False)

    assert next_lease is not None and next_lease.job_id == queued_after_stale.job_id
    assert state_before_cleanup is JobState.LEASED
    assert diagnosis["reason"] == "stale_lease_scheduler_active"
    assert cleaned["recovered_count"] == 0
    assert cleaned["canceled_count"] == 0
    assert cleaned["conflict_count"] == 1
    assert cleaned["scheduler_cancel_requested"] is False
    pending = queue.get_job(job.job_id)
    assert pending.state is JobState.LEASED
    assert isinstance(pending.metadata.get("cancellation_request"), dict)


def test_queue_cancel_requires_explicit_scheduler_policy(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="cancel-policy",
        )
    )

    result = cancel_queue_job(queue, job.job_id, scheduler_policy="relay-only")
    events, _ = queue.drain_events(job_id_cursor(job.job_id), limit=20)
    result_job = cast(dict[str, object], result["job"])

    assert result["scheduler_cancel_requested"] is False
    assert result["scheduler_policy"] == "relay-only"
    assert result_job["state"] == "canceled"
    assert events[-2].payload["cancel_scheduler"] is False
    request = cast(dict[str, object], queue.get_job(job.job_id).metadata["cancellation_request"])
    assert request["schema_version"] == "clio-relay.cancellation-request.v1"
    assert request["cancel_scheduler"] is False


def test_queue_cancel_can_request_scheduler_policy(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="cancel-scheduler-policy",
        )
    )

    result = cancel_queue_job(queue, job.job_id, scheduler_policy="request-scheduler")
    events, _ = queue.drain_events(job_id_cursor(job.job_id), limit=20)

    assert result["scheduler_cancel_requested"] is True
    assert result["scheduler_policy"] == "request-scheduler"
    assert events[-2].payload["cancel_scheduler"] is True
    request = cast(dict[str, object], queue.get_job(job.job_id).metadata["cancellation_request"])
    assert request["cancel_scheduler"] is True


def test_worker_status_reports_capacity_and_leases(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="node",
            pid=123,
            metadata={"concurrency": 4},
        )
    )
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="worker-status",
        )
    )
    queue.acquire_next_job("endpoint-1", cluster="ares", ttl_seconds=60)

    status = worker_status(queue, cluster="ares")
    active_leases = cast(list[dict[str, object]], status["active_leases"])

    assert status["worker_count"] == 1
    assert status["configured_concurrency"] == 4
    assert len(active_leases) == 1
    assert active_leases[0]["job_id"] == job.job_id


def test_worker_status_ignores_stale_workers(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    endpoint = queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="old-node",
            pid=123,
            metadata={"concurrency": 8},
        )
    )
    stale = endpoint.model_copy(update={"last_seen_at": utc_now() - timedelta(seconds=120)})
    endpoint_path = queue.root / "endpoints" / f"{endpoint.endpoint_id}.json"
    endpoint_path.write_text(stale.model_dump_json(indent=2), encoding="utf-8")

    status = worker_status(queue, cluster="ares", fresh_seconds=60)

    assert status["worker_count"] == 0
    assert status["configured_concurrency"] == 0
    assert status["registered_worker_count"] == 1
    assert status["stale_worker_count"] == 1
    assert status["workers"] == []


def test_worker_status_counts_active_slots_not_supervisor(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    parent = queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="node",
            pid=123,
            metadata={"concurrency": 4, "worker_supervisor": True},
        )
    )
    for index in range(4):
        queue.register_endpoint(
            EndpointRegistration(
                role=EndpointRole.WORKER,
                cluster="ares",
                hostname="node",
                pid=123,
                metadata={
                    "worker_slot": index,
                    "parent_endpoint_id": parent.endpoint_id,
                    "concurrency": 1,
                },
            )
        )

    status = worker_status(queue, cluster="ares")

    assert status["worker_count"] == 4
    assert status["configured_concurrency"] == 4
    assert status["registered_worker_count"] == 5
    assert status["stale_worker_count"] == 0


def test_worker_status_reports_reserved_control_capacity_and_usage(tmp_path: Path) -> None:
    """Operators can distinguish reserved query capacity from workload slots."""
    queue = ClioCoreQueue(tmp_path / "core")
    parent = queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="node",
            pid=123,
            metadata={
                "concurrency": 3,
                "workload_concurrency": 2,
                "control_query_concurrency": 1,
                "worker_supervisor": True,
            },
        )
    )
    slots: list[EndpointRegistration] = []
    for index, admission_class in enumerate(
        (
            McpAdmissionClass.WORKLOAD,
            McpAdmissionClass.WORKLOAD,
            McpAdmissionClass.CONTROL_QUERY,
        )
    ):
        workload = admission_class is McpAdmissionClass.WORKLOAD
        slots.append(
            queue.register_endpoint(
                EndpointRegistration(
                    role=EndpointRole.WORKER,
                    cluster="ares",
                    hostname="node",
                    pid=123,
                    metadata={
                        "worker_slot": index,
                        "parent_endpoint_id": parent.endpoint_id,
                        "concurrency": 1,
                        "workload_concurrency": 1 if workload else 0,
                        "control_query_concurrency": 0 if workload else 1,
                        "mcp_admission_class": admission_class.value,
                    },
                )
            )
        )
    source = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "30"]),
            idempotency_key="status-source",
        )
    )
    query = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server="science-mcp",
                expected_server_artifact_digest="a" * 64,
                admission_class=McpAdmissionClass.CONTROL_QUERY,
                tool="inspect",
            ),
            idempotency_key="status-query",
        )
    )
    assert queue.acquire_job(source.job_id, slots[0].endpoint_id, cluster="ares") is not None
    assert queue.acquire_job(query.job_id, slots[2].endpoint_id, cluster="ares") is not None

    status = worker_status(queue, cluster="ares")

    assert status["configured_concurrency"] == 3
    assert status["configured_workload_concurrency"] == 2
    assert status["configured_control_query_concurrency"] == 1
    assert status["control_query_concurrency_consistent"] is True
    assert status["active_leases_by_mcp_admission_class"] == {
        "workload": 1,
        "control_query": 1,
    }


def test_worker_status_selects_newest_complete_generation_during_restart(
    tmp_path: Path,
) -> None:
    """Fresh records from the replaced process cannot double-count capacity."""
    queue = ClioCoreQueue(tmp_path / "core")
    old_parent = _register_supervised_worker_generation(queue, pid=101)
    new_parent = _register_supervised_worker_generation(queue, pid=202)

    status = worker_status(queue, cluster="ares")

    assert status["worker_generation_id"] == new_parent.endpoint_id
    assert status["worker_generation_complete"] is True
    assert status["fresh_worker_generation_count"] == 2
    assert status["worker_count"] == 3
    assert status["configured_concurrency"] == 3
    assert status["configured_workload_concurrency"] == 2
    assert status["configured_control_query_concurrency"] == 1
    assert status["control_query_concurrency_consistent"] is True
    assert old_parent.endpoint_id != new_parent.endpoint_id


def test_worker_status_rejects_incomplete_newest_generation_during_restart(
    tmp_path: Path,
) -> None:
    """A complete stale generation cannot mask a replacement still starting."""
    queue = ClioCoreQueue(tmp_path / "core")
    _register_supervised_worker_generation(queue, pid=101)
    new_parent = _register_supervised_worker_generation(
        queue,
        pid=202,
        slot_indices=(0, 2),
    )

    status = worker_status(queue, cluster="ares")

    assert status["worker_generation_id"] == new_parent.endpoint_id
    assert status["worker_generation_complete"] is False
    assert status["fresh_worker_generation_count"] == 2
    assert status["worker_count"] == 2
    assert status["configured_concurrency"] == 2
    assert status["configured_workload_concurrency"] is None
    assert status["configured_control_query_concurrency"] is None
    assert status["control_query_concurrency_consistent"] is False


def test_worker_status_fails_closed_for_fresh_slot_with_missing_parent(tmp_path: Path) -> None:
    """An orphan slot remains visible but cannot establish usable capacity."""
    queue = ClioCoreQueue(tmp_path / "core")
    queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="node",
            pid=202,
            metadata={
                "worker_slot": 0,
                "parent_endpoint_id": "missing-parent",
                "concurrency": 1,
                "workload_concurrency": 1,
                "control_query_concurrency": 0,
                "mcp_admission_class": McpAdmissionClass.WORKLOAD.value,
            },
        )
    )

    status = worker_status(queue, cluster="ares")

    assert status["worker_generation_id"] == "missing-parent"
    assert status["worker_generation_complete"] is False
    assert status["worker_count"] == 1
    assert status["configured_workload_concurrency"] is None
    assert status["configured_control_query_concurrency"] is None
    assert status["control_query_concurrency_consistent"] is False


def test_stale_discovery_fails_closed_when_active_window_truncates(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    endpoint = queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="worker",
            pid=123,
        )
    )
    jobs = [
        queue.submit_job(
            RelayJob(
                cluster="ares",
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(command=["true"]),
                idempotency_key=f"exact-lease-{index}",
            )
        )
        for index in range(3)
    ]
    first_lease = queue.acquire_next_job(endpoint.endpoint_id, cluster="ares")
    assert first_lease is not None
    queue.update_job_state(jobs[0].job_id, JobState.RUNNING)
    for job in jobs[1:]:
        lease = Lease.new(job.job_id, endpoint.endpoint_id, 300)
        queue._write(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue.root / "leases" / f"{lease.lease_id}.json",
            lease,
        )
        queue._write(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue._job_record_path(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                "leases_by_job",
                job.job_id,
                lease.lease_id,
            ),
            lease,
        )
        queue.update_job_state(
            job.job_id,
            JobState.RUNNING,
            leased_by=endpoint.endpoint_id,
        )

    discovered = discover_stale_jobs(
        queue,
        cluster="ares",
        older_than_seconds=3_600,
        limit=1,
        scan_limit=2,
    )
    cleaned = cleanup_stale_jobs(
        queue,
        cluster="ares",
        older_than_seconds=3_600,
        dry_run=False,
        limit=1,
        scan_limit=2,
    )

    assert discovered["active_scan_truncated"] is True, discovered
    assert discovered["endpoint_scan_truncated"] is False, discovered
    assert discovered["lease_scan_truncated"] is False, discovered
    assert discovered["classification_complete"] is False, discovered
    assert discovered["lease_scan_truncated"] is False
    assert discovered["jobs"] == []
    assert cleaned["planned"] == []
    assert all(queue.get_job(job.job_id).state is JobState.RUNNING for job in jobs)
    assert all("cancellation_request" not in queue.get_job(job.job_id).metadata for job in jobs)


def test_diagnostics_honor_the_caller_active_scan_limit(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    jobs = [
        queue.submit_job(
            RelayJob(
                cluster="ares",
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(command=["true"]),
                idempotency_key=f"bounded-diagnosis-{index}",
            )
        )
        for index in range(3)
    ]

    exact = diagnose_job(
        queue,
        jobs[-1].job_id,
        cluster="ares",
        scan_limit=1,
    )
    summary = diagnose_queue(queue, cluster="ares", limit=1, scan_limit=1)

    exact_queue = cast(dict[str, object], exact["queue"])
    assert exact_queue["scan_truncated"] is True
    assert exact_queue["position_exact"] is False
    assert summary["checked_jobs"] == 1
    assert summary["scan_limit"] == 1
    assert summary["scan_truncated"] is True


def test_stale_discovery_fails_closed_when_exact_lease_index_truncates(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    endpoint = queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="worker",
            pid=123,
        )
    )
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="truncated-exact-lease",
        )
    )
    for _index in range(2):
        lease = Lease.new(job.job_id, endpoint.endpoint_id, 300)
        queue._write(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue.root / "leases" / f"{lease.lease_id}.json",
            lease,
        )
        queue._write(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue._job_record_path(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                "leases_by_job",
                job.job_id,
                lease.lease_id,
            ),
            lease,
        )
    queue.update_job_state(job.job_id, JobState.RUNNING, leased_by=endpoint.endpoint_id)

    result = discover_stale_jobs(
        queue,
        cluster="ares",
        older_than_seconds=1,
        limit=1,
        scan_limit=1,
    )

    assert result["classification_complete"] is False
    assert result["scan_truncated"] is True
    assert result["lease_scan_truncated"] is True
    assert result["lease_scan_truncated_job_ids"] == [job.job_id]
    assert result["jobs"] == []


def _register_supervised_worker_generation(
    queue: ClioCoreQueue,
    *,
    pid: int,
    slot_indices: tuple[int, ...] = (0, 1, 2),
) -> EndpointRegistration:
    """Register one synthetic two-workload/one-control worker generation."""
    parent = queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="node",
            pid=pid,
            metadata={
                "concurrency": 3,
                "workload_concurrency": 2,
                "control_query_concurrency": 1,
                "worker_supervisor": True,
                "kind_concurrency": {"jarvis": 2},
            },
        )
    )
    for index in slot_indices:
        workload = index < 2
        queue.register_endpoint(
            EndpointRegistration(
                role=EndpointRole.WORKER,
                cluster="ares",
                hostname="node",
                pid=pid,
                metadata={
                    "worker_slot": index,
                    "parent_endpoint_id": parent.endpoint_id,
                    "concurrency": 1,
                    "workload_concurrency": 1 if workload else 0,
                    "control_query_concurrency": 0 if workload else 1,
                    "mcp_admission_class": (
                        McpAdmissionClass.WORKLOAD.value
                        if workload
                        else McpAdmissionClass.CONTROL_QUERY.value
                    ),
                    "kind_concurrency": {"jarvis": 2},
                },
            )
        )
    return parent


def job_id_cursor(job_id: str) -> Cursor:
    return Cursor(job_id=job_id)
