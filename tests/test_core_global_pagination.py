from __future__ import annotations

import json
from pathlib import Path

import pytest

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.models import (
    EndpointRegistration,
    EndpointRole,
    GatewaySession,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    MonitorRule,
    RelayJob,
)


def _job(key: str, *, cluster: str = "cluster-a") -> RelayJob:
    return RelayJob(
        cluster=cluster,
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key=key,
    )


def test_global_pages_are_stable_bounded_and_filter_one_source_window(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    endpoint_a = queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="cluster-a",
            hostname="worker-a",
            pid=1,
        )
    )
    queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="cluster-b",
            hostname="worker-b",
            pid=2,
        )
    )
    endpoint_page, endpoint_cursor, endpoint_total = queue.list_endpoints_page(
        limit=1,
        cluster="cluster-a",
    )
    assert [endpoint.endpoint_id for endpoint in endpoint_page] == [endpoint_a.endpoint_id]
    assert endpoint_cursor == 2
    assert endpoint_total == 2

    first = queue.submit_job(_job("page-job-1"))
    second = queue.submit_job(_job("page-job-2", cluster="cluster-b"))
    third = queue.submit_job(_job("page-job-3"))

    first_page, next_cursor, first_total = queue.list_jobs_page(limit=2)
    assert [job.job_id for job in first_page] == [first.job_id, second.job_id]
    assert next_cursor == 3
    assert first_total == 3

    fourth = queue.submit_job(_job("page-job-4"))
    second_page, end_cursor, second_total = queue.list_jobs_page(
        cursor=next_cursor or 1,
        limit=2,
    )
    assert [job.job_id for job in second_page] == [third.job_id, fourth.job_id]
    assert end_cursor is None
    assert second_total == 4
    filtered_jobs, filtered_cursor, filtered_total = queue.list_jobs_page(
        limit=2,
        cluster="cluster-a",
    )
    assert [job.job_id for job in filtered_jobs] == [first.job_id]
    assert filtered_cursor == 3
    assert filtered_total == 4

    gateway_a = queue.create_gateway_session(GatewaySession(cluster="cluster-a", name="gateway-a"))
    gateway_b = queue.create_gateway_session(
        GatewaySession(
            cluster="cluster-b",
            name="gateway-b",
            state=GatewaySessionState.READY,
        )
    )
    gateway_page, gateway_cursor, gateway_total = queue.list_gateway_sessions_page(
        limit=1,
        cluster="cluster-a",
    )
    assert [session.session_id for session in gateway_page] == [gateway_a.session_id]
    assert gateway_cursor == 2
    assert gateway_total == 2
    gateway_tail, gateway_end, _ = queue.list_gateway_sessions_page(
        cursor=gateway_cursor or 1,
        state=GatewaySessionState.READY,
    )
    assert [session.session_id for session in gateway_tail] == [gateway_b.session_id]
    assert gateway_end is None

    enabled = queue.append_monitor_rule(MonitorRule(job_id=first.job_id, pattern="ready"))
    disabled = queue.append_monitor_rule(
        MonitorRule(job_id=second.job_id, pattern="done", enabled=False)
    )
    rule_page, rule_cursor, rule_total = queue.list_monitor_rules_page(
        limit=1,
        enabled=True,
    )
    assert [rule.rule_id for rule in rule_page] == [enabled.rule_id]
    assert rule_cursor == 2
    assert rule_total == 2
    rule_tail, rule_end, _ = queue.list_monitor_rules_page(
        cursor=rule_cursor or 1,
        enabled=False,
    )
    assert [rule.rule_id for rule in rule_tail] == [disabled.rule_id]
    assert rule_end is None

    with pytest.raises(ValueError, match="between 1 and 500"):
        queue.list_jobs_page(limit=501)
    with pytest.raises(ValueError, match="greater than or equal to 1"):
        queue.list_gateway_sessions_page(cursor=0)


def test_global_order_seal_field_removal_fails_closed(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    queue.initialize()

    migration_path = tmp_path / "migrations" / "index-v1.json"
    migration = json.loads(migration_path.read_text(encoding="utf-8"))
    migration.pop("global_order_families", None)
    migration["complete"] = True
    migration_path.write_text(json.dumps(migration), encoding="utf-8")

    with pytest.raises(
        QueueConflictError,
        match="sealed index migration state is invalid",
    ):
        queue.initialize()


def test_global_order_reverse_mapping_tamper_fails_closed(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(_job("global-order-tamper"))
    mapping_path = next((tmp_path / "global_order" / "jobs" / "by_id").glob("*.json"))
    mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
    mapping["record_id"] = f"{job.job_id}-forged"
    mapping_path.write_text(json.dumps(mapping), encoding="utf-8")

    with pytest.raises(QueueConflictError, match="reverse mapping mismatch"):
        queue.list_jobs_page()
