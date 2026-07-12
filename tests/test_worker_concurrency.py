"""Tests for durable total and per-job-kind worker concurrency controls."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import cast

import pytest
from typer.testing import CliRunner

from clio_relay.cli import app
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.deployment import render_endpoint_user_service
from clio_relay.errors import ConfigurationError
from clio_relay.models import (
    EndpointRegistration,
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    RelayJob,
    RemoteAgentTaskSpec,
)
from clio_relay.queue_management import worker_status
from clio_relay.worker_concurrency import (
    kind_concurrency_metadata,
    normalize_kind_concurrency,
    parse_kind_concurrency_options,
)


def test_kind_concurrency_options_are_validated_and_stable() -> None:
    """Operator values must produce a deterministic typed policy."""
    parsed = parse_kind_concurrency_options(["remote_agent=2", "mcp_call=1", "jarvis=3"])

    assert parsed == {
        JobKind.REMOTE_AGENT: 2,
        JobKind.MCP_CALL: 1,
        JobKind.JARVIS: 3,
    }
    assert kind_concurrency_metadata(parsed) == {
        "jarvis": 3,
        "remote_agent": 2,
        "mcp_call": 1,
    }


@pytest.mark.parametrize(
    "values",
    [
        ["remote_agent"],
        ["unknown=1"],
        ["remote_agent=x"],
        ["remote_agent=0"],
        ["remote_agent=1", "remote_agent=2"],
    ],
)
def test_kind_concurrency_options_reject_invalid_values(values: list[str]) -> None:
    """Malformed, unknown, duplicate, and nonpositive limits must fail."""
    with pytest.raises(ConfigurationError):
        parse_kind_concurrency_options(values)

    with pytest.raises(ConfigurationError):
        normalize_kind_concurrency({JobKind.REMOTE_AGENT: True})


def test_kind_limit_skips_saturated_jobs_without_head_of_line_blocking(
    tmp_path: Path,
) -> None:
    """A saturated kind must not prevent another eligible kind from leasing."""
    root = tmp_path / "core"
    first_queue = ClioCoreQueue(root)
    first_remote = first_queue.submit_job(_remote_job("remote-first"))
    second_remote = first_queue.submit_job(_remote_job("remote-second"))
    jarvis = first_queue.submit_job(_jarvis_job("jarvis-after-remotes"))
    limits = {JobKind.REMOTE_AGENT: 1}

    first_lease = first_queue.acquire_next_job(
        "worker-1",
        cluster="ares",
        kind_concurrency=limits,
    )
    second_queue = ClioCoreQueue(root)
    second_lease = second_queue.acquire_next_job(
        "worker-2",
        cluster="ares",
        kind_concurrency=limits,
    )

    assert first_lease is not None
    assert first_lease.job_id == first_remote.job_id
    assert second_lease is not None
    assert second_lease.job_id == jarvis.job_id
    assert second_queue.get_job(second_remote.job_id).state == JobState.QUEUED

    first_queue.release_lease(first_lease.lease_id)
    third_lease = second_queue.acquire_next_job(
        "worker-3",
        cluster="ares",
        kind_concurrency=limits,
    )

    assert third_lease is not None
    assert third_lease.job_id == second_remote.job_id


def test_kind_limit_is_atomic_across_queue_instances(tmp_path: Path) -> None:
    """Distinct worker processes must not race past the same durable cap."""
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    queue.submit_job(_remote_job("race-one"))
    queue.submit_job(_remote_job("race-two"))
    barrier = Barrier(2)

    def acquire(index: int) -> str | None:
        instance = ClioCoreQueue(root)
        barrier.wait()
        lease = instance.acquire_next_job(
            f"worker-{index}",
            cluster="ares",
            kind_concurrency={JobKind.REMOTE_AGENT: 1},
        )
        return None if lease is None else lease.job_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(acquire, (1, 2)))

    assert sum(result is not None for result in results) == 1
    assert len(queue.list_leases(cluster="ares")) == 1


def test_worker_status_reports_policy_usage_and_conflicts(tmp_path: Path) -> None:
    """Operations must expose effective limits without hiding stale conflicts."""
    queue = ClioCoreQueue(tmp_path / "core")
    queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="node-1",
            pid=101,
            metadata={
                "concurrency": 4,
                "kind_concurrency": {"remote_agent": 2, "mcp_call": 1},
            },
        )
    )
    remote = queue.submit_job(_remote_job("status-remote"))
    lease = queue.acquire_next_job(
        "worker-status",
        cluster="ares",
        kind_concurrency={JobKind.REMOTE_AGENT: 2},
    )

    status = worker_status(queue, cluster="ares")

    assert lease is not None and lease.job_id == remote.job_id
    assert status["configured_kind_concurrency"] == {
        "remote_agent": 2,
        "mcp_call": 1,
    }
    assert status["kind_concurrency_consistent"] is True
    assert status["active_leases_by_kind"] == {
        "jarvis": 0,
        "remote_agent": 1,
        "mcp_call": 0,
    }

    queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="node-2",
            pid=102,
            metadata={
                "concurrency": 1,
                "kind_concurrency": {"remote_agent": 1},
            },
        )
    )
    conflicting = worker_status(queue, cluster="ares")

    assert conflicting["configured_kind_concurrency"] is None
    assert conflicting["kind_concurrency_consistent"] is False
    configurations = cast(list[dict[str, int]], conflicting["kind_concurrency_configurations"])
    assert {tuple(item.items()) for item in configurations} == {
        (("remote_agent", 2), ("mcp_call", 1)),
        (("remote_agent", 1),),
    }


def test_endpoint_cli_persists_repeatable_kind_limits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The operator CLI must persist the exact policy on the worker record."""
    core = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))

    result = CliRunner().invoke(
        app,
        [
            "endpoint",
            "start",
            "--role",
            "worker",
            "--cluster",
            "ares",
            "--scheduler-provider",
            "external",
            "--once",
            "--concurrency",
            "4",
            "--kind-concurrency",
            "remote_agent=2",
            "--kind-concurrency",
            "mcp_call=1",
        ],
    )

    assert result.exit_code == 0, result.output
    endpoints = ClioCoreQueue(core).list_endpoints(cluster="ares")
    assert len(endpoints) == 1
    assert endpoints[0].metadata["concurrency"] == 4
    assert endpoints[0].metadata["kind_concurrency"] == {
        "remote_agent": 2,
        "mcp_call": 1,
    }


@pytest.mark.parametrize(
    "arguments",
    [
        ["--kind-concurrency", "remote_agent"],
        ["--kind-concurrency", "unknown=1"],
        ["--kind-concurrency", "remote_agent=x"],
        ["--kind-concurrency", "remote_agent=0"],
        [
            "--kind-concurrency",
            "remote_agent=1",
            "--kind-concurrency",
            "remote_agent=2",
        ],
    ],
)
def test_endpoint_cli_rejects_invalid_kind_limits(arguments: list[str]) -> None:
    """Invalid policies must fail before a worker is registered."""
    result = CliRunner().invoke(
        app,
        [
            "endpoint",
            "start",
            "--role",
            "worker",
            "--cluster",
            "ares",
            "--scheduler-provider",
            "external",
            "--once",
            *arguments,
        ],
    )

    assert result.exit_code == 2
    assert "concurrency" in result.output.lower()


def test_user_service_renders_deterministic_kind_limits() -> None:
    """Installed workers must retain the same policy after service restart."""
    rendered = render_endpoint_user_service(
        cluster="ares",
        definition=ClusterDefinition(
            name="ares",
            ssh_host="ares",
            scheduler_provider="slurm",
        ),
        concurrency=4,
        kind_concurrency={
            JobKind.MCP_CALL: 1,
            JobKind.REMOTE_AGENT: 2,
        },
    )

    assert (
        "--concurrency 4 --kind-concurrency remote_agent=2 "
        "--kind-concurrency mcp_call=1 --scheduler-provider slurm"
    ) in rendered

    with pytest.raises(ConfigurationError):
        render_endpoint_user_service(
            cluster="ares",
            definition=ClusterDefinition(name="ares", ssh_host="ares"),
            kind_concurrency={"unknown": 1},
        )


def _remote_job(key: str) -> RelayJob:
    return RelayJob(
        cluster="ares",
        kind=JobKind.REMOTE_AGENT,
        spec=RemoteAgentTaskSpec(prompt_path=f"/tmp/{key}.txt"),
        idempotency_key=key,
    )


def _jarvis_job(key: str) -> RelayJob:
    return RelayJob(
        cluster="ares",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["echo", key]),
        idempotency_key=key,
    )
