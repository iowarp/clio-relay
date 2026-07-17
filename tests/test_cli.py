from __future__ import annotations

import base64
import hashlib
import json
import subprocess
import sys
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from _pytest.monkeypatch import MonkeyPatch
from click import unstyle
from typer.testing import CliRunner

from clio_relay import cli
from clio_relay.cli import app
from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    ClusterTargetIdentity,
    FrpTransportConfig,
)
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, QueueConflictError, RelayError
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    jarvis_cd_lock_binding_expectation,
    jarvis_user_contract,
)
from clio_relay.models import (
    ArtifactRef,
    Cursor,
    EndpointRegistration,
    EndpointRole,
    GatewaySession,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    JobState,
    McpCallSpec,
    RelayJob,
    RelayTask,
    SchedulerPhase,
    SchedulerStatus,
)
from clio_relay.scheduler_providers import SchedulerProvider
from clio_relay.service_runtime import ServiceRuntimeSupervisor
from clio_relay.session_lifecycle import (
    CleanupResource,
    RemoteSessionStateEvidence,
    SessionLifecycleReport,
)
from clio_relay.validation_report import (
    EvidenceReference,
    LiveValidationReport,
    ValidationRecorder,
    ValidationResource,
    new_live_validation_report,
)
from tests.queue_validation_fixtures import (
    DeterministicQueueValidationProvider,
    LiveWorkerFleet,
)


@pytest.fixture(autouse=True)
def _default_cli_mode(monkeypatch: MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")


def _write_test_cluster(
    root: Path,
    name: str = "ares",
    *,
    frp_server_addr: str = "relay.example.test",
    scheduler_provider: str = "external",
) -> None:
    ClusterRegistry(
        clusters={
            name: ClusterDefinition(
                name=name,
                ssh_host=name,
                scheduler_provider=scheduler_provider,
                frp_transport=FrpTransportConfig(server_addr=frp_server_addr),
            )
        }
    ).save(root / ".clio-relay" / "clusters.json")


def _verified_jarvis_nested_runtime() -> dict[str, object]:
    """Return complete discovery evidence for the relay's built-in JARVIS child."""
    expected = jarvis_cd_lock_binding_expectation()
    return {
        "schema_version": "clio-kit.locked-server.v4",
        "server_name": "jarvis",
        "persistent_tool": True,
        "locked_runtime_verified": True,
        "jarvis_cd_lock_binding": {
            "schema_version": "clio-relay.jarvis-cd-lock-binding.v1",
            "dependency": "jarvis-cd",
            "verified": True,
            "error": None,
            "expected_version": expected["version"],
            "expected_url": expected["url"],
            "expected_sha256": expected["sha256"],
            "observed_version": expected["version"],
            "observed_source_url": expected["url"],
            "observed_wheel_url": expected["url"],
            "observed_wheel_sha256": expected["sha256"],
            "jarvis_mcp_package_entry_count": 1,
            "resolved_dependency_entry_count": 1,
            "observed_resolved_dependency_entries": [{"name": "jarvis-cd"}],
            "metadata_requirement_entry_count": 1,
            "observed_metadata_requirement_entries": [
                {"name": "jarvis-cd", "url": expected["url"]}
            ],
            "observed_metadata_requirement_urls": [expected["url"]],
            "package_entry_count": 1,
            "wheel_entry_count": 1,
        },
    }


def _write_passing_validation_report(
    path: Path,
    *,
    scenario: str,
    cluster: str,
) -> str:
    """Write one valid stale-success fixture and return its durable report id."""
    report = new_live_validation_report(scenario=scenario, cluster=cluster)
    recorder = ValidationRecorder(report)
    with recorder.check("stale.success", "stale success fixture") as evidence:
        evidence.append(EvidenceReference(kind="stale", excerpt="previous invocation"))
    recorder.finish()
    recorder.write(path)
    return report.report_id


def _owned_session_status(
    *,
    session_id: str = "session-1",
    generation_id: str = "generation-1",
    running: bool = True,
) -> dict[str, object]:
    return {
        "owner": "clio-relay",
        "session_id": session_id,
        "session_generation_id": generation_id,
        "running": running,
        "ownership_verified": running,
    }


def _verified_teardown_report(
    *,
    cluster: str = "ares",
    session_id: str = "session-1",
    generation_id: str = "generation-1",
    resources: list[CleanupResource] | None = None,
) -> SessionLifecycleReport:
    observed_at = datetime.now(UTC)
    return SessionLifecycleReport(
        cluster=cluster,
        session_id=session_id,
        session_generation_id=generation_id,
        mode="teardown",
        prior_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id=generation_id,
            process_start_marker="start-123",
            running=True,
            ownership_verified=True,
            observed_at=observed_at,
            started_at=observed_at,
        ),
        post_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id=generation_id,
            process_start_marker="start-123",
            running=False,
            ownership_verified=True,
            observed_at=observed_at,
            started_at=observed_at,
        ),
        resources=resources
        or [
            CleanupResource(
                kind="remote_relay_api",
                resource_id="123",
                location=cluster,
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
            )
        ],
    )


def _fake_owned_session_status(**_kwargs: object) -> dict[str, object]:
    return _owned_session_status()


def _fake_verified_teardown(**_kwargs: object) -> SessionLifecycleReport:
    return _verified_teardown_report()


def _fake_empty_runtime_cleanup(**_kwargs: object) -> list[dict[str, object]]:
    return []


def _fake_empty_owned_jobs(*_args: object, **_kwargs: object) -> list[object]:
    return []


def _activate_owner_session(
    queue: ClioCoreQueue,
    *,
    session_id: str = "session-1",
    generation_id: str = "generation-1",
) -> None:
    selected = queue.prepare_owner_session_start(
        session_id,
        recorded_generation_id=None,
        candidate_generation_id=generation_id,
    )
    assert selected == generation_id


def test_console_safe_text_replaces_non_console_characters(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "stdout", SimpleNamespace(encoding="cp1252"))

    assert cli._console_safe_text("× ╰─▶") == "× ???"  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def test_cluster_scoped_desktop_admission_isolates_same_session_id(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    cluster_a_admission = cli._desktop_owner_session_admission_id(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster="cluster-a",
        session_id="shared-session",
    )
    cluster_b_admission = cli._desktop_owner_session_admission_id(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster="cluster-b",
        session_id="shared-session",
    )
    assert cluster_a_admission != cluster_b_admission
    queue.mirror_owner_session_generation_open(
        cluster_a_admission,
        session_generation_id="generation-a",
    )
    queue.mirror_owner_session_generation_open(
        cluster_b_admission,
        session_generation_id="generation-b",
    )
    queue.set_owner_session_closing(
        cluster_a_admission,
        session_generation_id="generation-a",
        operation_id="cleanup_cluster_a",
    )

    with pytest.raises(QueueConflictError, match="closing and rejects new work"):
        queue.create_gateway_session(
            GatewaySession(
                cluster="cluster-a",
                name="blocked-on-a",
                metadata={
                    "owner": "clio-relay",
                    "owner_session_id": "shared-session",
                    "owner_session_generation_id": "generation-a",
                    "owner_session_admission_id": cluster_a_admission,
                },
            )
        )
    admitted = queue.create_gateway_session(
        GatewaySession(
            cluster="cluster-b",
            name="allowed-on-b",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "shared-session",
                "owner_session_generation_id": "generation-b",
                "owner_session_admission_id": cluster_b_admission,
            },
        )
    )
    assert admitted.cluster == "cluster-b"


def test_endpoint_worker_with_explicit_provider_does_not_require_remote_registry(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeWorker:
        def register(self) -> None:
            captured["registered"] = True

        def run_once(self) -> None:
            captured["ran_once"] = True

        def close(self) -> None:
            captured["closed"] = True

    def make_worker(**kwargs: object) -> FakeWorker:
        captured.update(kwargs)
        return FakeWorker()

    def fail_registry_lookup(cluster: str) -> ClusterDefinition:
        raise AssertionError(f"unexpected registry lookup for {cluster}")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "EndpointWorker", make_worker)
    monkeypatch.setattr(cli, "_require_cluster", fail_registry_lookup)

    result = CliRunner().invoke(
        app,
        [
            "endpoint",
            "start",
            "--role",
            "worker",
            "--cluster",
            "homelab",
            "--scheduler-provider",
            "external",
            "--once",
        ],
    )

    assert result.exit_code == 0
    assert captured["cluster"] == "homelab"
    assert captured["registered"] is True
    assert captured["ran_once"] is True
    assert captured["closed"] is True
    provider = cast(SchedulerProvider, captured["scheduler_provider"])
    assert provider.name == "external"


def test_endpoint_worker_without_explicit_provider_uses_cluster_registry(
    monkeypatch: MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    definition = ClusterDefinition(
        name="configured-cluster",
        ssh_host="configured-cluster",
        scheduler_provider="slurm",
    )

    class FakeWorker:
        def register(self) -> None:
            captured["registered"] = True

        def run_once(self) -> None:
            captured["ran_once"] = True

        def close(self) -> None:
            captured["closed"] = True

    def make_worker(**kwargs: object) -> FakeWorker:
        captured.update(kwargs)
        return FakeWorker()

    def load_cluster(cluster: str) -> ClusterDefinition:
        captured["registry_cluster"] = cluster
        return definition

    monkeypatch.setattr(cli, "EndpointWorker", make_worker)
    monkeypatch.setattr(cli, "_require_cluster", load_cluster)

    result = CliRunner().invoke(
        app,
        [
            "endpoint",
            "start",
            "--role",
            "worker",
            "--cluster",
            "configured-cluster",
            "--once",
        ],
    )

    assert result.exit_code == 0
    assert captured["registry_cluster"] == "configured-cluster"
    assert captured["closed"] is True
    provider = cast(SchedulerProvider, captured["scheduler_provider"])
    assert provider.name == "slurm"


def test_cli_lists_artifacts(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    artifact_path = tmp_path / "stdout.log"
    artifact_path.write_text("hello\n", encoding="utf-8")
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-artifacts",
        )
    )
    artifact = queue.append_artifact(
        ArtifactRef(job_id=job.job_id, uri=artifact_path.as_uri(), kind="stdout")
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["job", "list-artifacts", job.job_id])

    assert result.exit_code == 0
    page = json.loads(result.output)
    assert page["artifacts"][0]["artifact_id"] == artifact.artifact_id
    assert page["artifacts"][0]["kind"] == "stdout"
    assert page["cursor"] == 1
    assert page["limit"] == 100
    assert page["next_cursor"] is None
    assert page["total"] == 1


def test_cli_lists_tasks(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-tasks",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="jarvis.execution"))
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["job", "tasks", job.job_id])

    assert result.exit_code == 0
    page = json.loads(result.output)
    assert page["tasks"][0]["task_id"] == task.task_id
    assert page["tasks"][0]["name"] == "jarvis.execution"
    assert page["cursor"] == 1
    assert page["limit"] == 100
    assert page["next_cursor"] is None
    assert page["total"] == 1


def test_cli_repairs_lease_operational_indexes(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="cli-repair-lease-indexes",
        )
    )
    lease = queue.acquire_job(job.job_id, "worker", cluster=job.cluster)
    assert lease is not None
    identity = queue._lease_index_identity(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        lease,
        job=queue.get_job(job.job_id),
    )
    endpoint_ref = queue._lease_endpoint_ref_path(identity)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    endpoint_ref.unlink()
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["queue", "repair-lease-indexes"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["complete"] is True
    assert payload["record_count"] == 1
    assert endpoint_ref.is_file()


def test_cli_audits_lease_capacity_and_exits_nonzero_on_mismatch(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="cli-audit-lease-capacity",
        )
    )
    lease = queue.acquire_job(job.job_id, "worker", cluster=job.cluster)
    assert lease is not None
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    valid = CliRunner().invoke(app, ["queue", "audit-lease-capacity"])

    assert valid.exit_code == 0
    report = json.loads(valid.output)
    assert report["schema_version"] == "clio-relay.lease-capacity-audit.v1"
    assert report["valid"] is True

    aggregate_path = core_dir / "lease_capacity" / "aggregate.json"
    aggregate = json.loads(aggregate_path.read_text(encoding="utf-8"))
    aggregate["document_sha256"] = "0" * 64
    aggregate_path.write_text(json.dumps(aggregate), encoding="utf-8")

    invalid = CliRunner().invoke(app, ["queue", "audit-lease-capacity"])

    assert invalid.exit_code == 1
    invalid_report = json.loads(invalid.output)
    assert invalid_report["valid"] is False
    assert invalid_report["mismatches"][0]["type"] == "audit_error"


def test_cli_records_and_reads_task_events(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-task-events",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="remote-agent.discovery"))
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    record = CliRunner().invoke(
        app,
        [
            "job",
            "record-task-event",
            task.task_id,
            "--event-type",
            "dataset_found",
            "--label",
            "dataset",
            "--summary",
            "Found staged dataset",
            "--status",
            "succeeded",
            "--path-ref",
            "/mnt/common/datasets/example_001",
        ],
    )
    read = CliRunner().invoke(app, ["job", "task-events", task.task_id])

    assert record.exit_code == 0
    assert read.exit_code == 0
    payload = json.loads(read.output)
    assert payload["events"][0]["event_type"] == "dataset_found"
    assert payload["events"][0]["path_refs"] == ["/mnt/common/datasets/example_001"]


def test_cli_job_watch_accepts_zero_cursor(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-watch-zero-cursor",
        )
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["job", "watch", job.job_id, "--cursor", "0"])

    assert result.exit_code == 0
    assert "job.queued" in result.output
    assert "next_cursor=2" in result.output


def test_cli_job_monitor_accepts_zero_cursor(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-monitor-zero-cursor",
        )
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["job", "monitor", job.job_id, "--cursor", "0"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["events"][0]["event_type"] == "job.queued"
    assert payload["next_cursor"] == 2


def test_cli_gateway_session_lifecycle(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    gateway_json = tmp_path / "gateway.json"
    gateway_json.write_text('{"strategy":"ssh_forward","remote_port":11111}', encoding="utf-8")
    resources_json = tmp_path / "resources.json"
    resources_json.write_text('{"nodes":1,"exclusive":true}', encoding="utf-8")

    created = CliRunner().invoke(
        app,
        [
            "gateway",
            "create",
            "--cluster",
            "test-cluster",
            "--name",
            "live-service-example",
            "--gateway-json-file",
            str(gateway_json),
            "--resources-json-file",
            str(resources_json),
            "--stdout-uri",
            "file:///tmp/stdout.log",
            "--stderr-uri",
            "file:///tmp/stderr.log",
            "--log-uri",
            "file:///tmp/service.log",
            "--artifact",
            "artifact://session/startup",
        ],
    )
    assert created.exit_code == 0
    session_id = json.loads(created.output)["session_id"]

    updated = CliRunner().invoke(
        app,
        [
            "gateway",
            "update",
            session_id,
            "--state",
            "ready",
            "--node",
            "ares-comp-01",
            "--gateway-json",
            '{"strategy":"ssh_forward","local_port":5900}',
            "--resources-json",
            '{"nodes":2}',
            "--stdout-uri",
            "file:///tmp/updated-stdout.log",
            "--log-uri",
            "file:///tmp/updated.log",
            "--artifact",
            "artifact://session/updated",
        ],
    )
    listed = CliRunner().invoke(app, ["gateway", "list", "--cluster", "test-cluster"])
    closed = CliRunner().invoke(app, ["gateway", "close", session_id])

    assert updated.exit_code == 0
    assert listed.exit_code == 0
    assert closed.exit_code == 0
    assert json.loads(updated.output)["state"] == GatewaySessionState.READY.value
    assert json.loads(created.output)["gateway"]["remote_port"] == 11111
    assert json.loads(created.output)["requested_resources"]["exclusive"] is True
    assert json.loads(created.output)["stdout_uri"] == "file:///tmp/stdout.log"
    assert json.loads(created.output)["log_uris"] == ["file:///tmp/service.log"]
    assert json.loads(created.output)["artifacts"] == ["artifact://session/startup"]
    assert json.loads(updated.output)["requested_resources"] == {"nodes": 2}
    assert json.loads(updated.output)["scheduler_job_id"] is None
    assert json.loads(updated.output)["stdout_uri"] == "file:///tmp/updated-stdout.log"
    assert json.loads(updated.output)["log_uris"] == ["file:///tmp/updated.log"]
    assert json.loads(updated.output)["artifacts"] == ["artifact://session/updated"]
    listed_page = json.loads(listed.output)
    assert listed_page["gateway_sessions"][0]["session_id"] == session_id
    assert listed_page["source_cursor"] == 1
    assert listed_page["source_limit"] == 100
    assert listed_page["source_next_cursor"] is None
    assert listed_page["source_total"] == 1
    assert json.loads(closed.output)["state"] == GatewaySessionState.CLOSED.value


def test_cli_job_status_includes_relay_queue(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-status",
        )
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["job", "status", job.job_id])

    assert result.exit_code == 0
    status = json.loads(result.output)
    assert status["job"]["job_id"] == job.job_id
    assert status["relay_queue"] == {"state": "queued", "jobs_ahead": 0, "position": 1}


def test_cli_queue_management_commands(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-queue-management",
        )
    )
    queue.acquire_next_job("endpoint-1", cluster="test-cluster", ttl_seconds=-1)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()

    listed = runner.invoke(
        app,
        [
            "queue",
            "list",
            "--cluster",
            "test-cluster",
            "--kind",
            "jarvis",
            "--limit",
            "1",
        ],
    )
    diagnosed = runner.invoke(
        app,
        ["queue", "diagnose", job.job_id, "--cluster", "test-cluster"],
    )
    stale = runner.invoke(
        app,
        [
            "queue",
            "stale",
            "--cluster",
            "test-cluster",
            "--older-than",
            "1h",
        ],
    )
    cleanup = runner.invoke(
        app,
        [
            "queue",
            "cleanup-stale",
            "--cluster",
            "test-cluster",
            "--no-dry-run",
        ],
    )
    canceled = runner.invoke(
        app,
        ["queue", "cancel", job.job_id, "--cluster", "test-cluster"],
    )

    assert listed.exit_code == 0
    assert diagnosed.exit_code == 0
    assert stale.exit_code == 0
    assert cleanup.exit_code == 0
    assert canceled.exit_code == 0
    assert json.loads(listed.output)["count"] == 1
    assert json.loads(listed.output)["jobs"][0]["job"]["kind"] == "jarvis"
    assert json.loads(diagnosed.output)["reason"] == "stale_lease"
    assert json.loads(stale.output)["jobs"][0]["job"]["job_id"] == job.job_id
    assert json.loads(cleanup.output)["recovered_count"] == 1
    assert json.loads(canceled.output)["scheduler_policy"] == "relay-only"


def test_cli_queue_validation_writes_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, "test-cluster", scheduler_provider="slurm")
    fleet = LiveWorkerFleet(tmp_path).start()
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(fleet.settings.core_dir))
    report_path = tmp_path / "queue-validation.json"

    def queue_validation_provider(_name: str | None) -> DeterministicQueueValidationProvider:
        return fleet.scheduler

    monkeypatch.setattr(cli, "validation_provider_for_scheduler", queue_validation_provider)
    try:
        result = CliRunner().invoke(
            app,
            [
                "queue",
                "validate",
                "--cluster",
                "test-cluster",
                "--older-than",
                "1s",
                "--scheduler-timeout-seconds",
                "30",
                "--scheduler-poll-seconds",
                "0.02",
                "--report",
                str(report_path),
            ],
        )
    finally:
        fleet.close()

    failure_report = (
        report_path.read_text(encoding="utf-8") if report_path.exists() else "<report not written>"
    )
    assert result.exit_code == 0, (
        f"output={result.output!r}\nexception={result.exception!r}\nreport={failure_report}"
    )
    assert "validation.status=passed" in result.output
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["scenario"] == "queue-management"
    assert {check["check_id"] for check in report["checks"]} == {
        "queue.kind-concurrency-parallel",
        "queue.kind-concurrency-worker-enforced",
        "queue.lease-capacity-audit-initial",
        "queue.lease-capacity-audit-final",
        "queue.list-bounded",
        "queue.diagnose-specific-reason",
        "queue.stale-dry-run",
        "queue.stale-cleanup-executed",
        "queue.cancel-running-worker-process",
        "queue.scheduler-preserved-default",
        "queue.worker-containment-enforced",
    }
    assert report["cleanup"]["cancel_scheduler_jobs"] is False


def test_cli_worker_status_reports_registered_capacity(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="test-cluster",
            hostname="node",
            pid=123,
            metadata={"concurrency": 3},
        )
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["worker", "status", "--cluster", "test-cluster"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["worker_count"] == 1
    assert payload["configured_concurrency"] == 3


def test_cli_job_submit_can_request_exclusive_scheduler(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    yaml_path = tmp_path / "pipeline.yaml"
    yaml_path.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    _write_test_cluster(tmp_path, name="test-cluster", scheduler_provider="slurm")
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "job",
            "submit",
            "--cluster",
            "test-cluster",
            "--jarvis-yaml",
            str(yaml_path),
            "--exclusive",
        ],
    )

    assert result.exit_code == 0
    job = ClioCoreQueue(core_dir).list_jobs()[0]
    assert isinstance(job.spec, JarvisRunSpec)
    assert "exclusive: true" in str(job.spec.pipeline_yaml)


def test_cli_job_submit_pipeline_creates_named_jarvis_job(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(
        app,
        [
            "job",
            "submit-pipeline",
            "--cluster",
            "ares",
            "--pipeline-name",
            "site_simulation_4node",
            "--idempotency-key",
            "named-pipeline",
        ],
    )

    assert result.exit_code == 0
    job = ClioCoreQueue(core_dir).get_job(result.output.strip())
    assert isinstance(job.spec, JarvisRunSpec)
    assert job.spec.pipeline_name == "site_simulation_4node"
    assert job.spec.pipeline_yaml is None


def test_cli_creates_and_evaluates_monitor_rule(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-monitor",
        )
    )
    queue.append_event(job.job_id, "stdout.delta", "step 25", payload={"text": "step 25\n"})
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()

    add_result = runner.invoke(
        app,
        [
            "monitor",
            "add-regex",
            job.job_id,
            "--pattern",
            "step 25",
            "--event-type",
            "stdout.delta",
        ],
    )
    run_result = runner.invoke(app, ["monitor", "run-once"])

    assert add_result.exit_code == 0
    assert json.loads(add_result.output)["job_id"] == job.job_id
    assert run_result.exit_code == 0
    assert json.loads(run_result.output)[0]["action"] == "emit_event"


def test_cli_accepts_json_object_from_file(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    payload_path = tmp_path / "progress-payload.json"
    payload_path.write_text(
        json.dumps(
            {
                "label": "iteration",
                "current_group": "step",
                "total": 100,
                "unit": "step",
            }
        ),
        encoding="utf-8-sig",
    )
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-json-file",
        )
    )
    queue.append_event(job.job_id, "stdout.delta", "step 25", payload={"text": "step 25\n"})
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()

    add_result = runner.invoke(
        app,
        [
            "monitor",
            "add-regex",
            job.job_id,
            "--pattern",
            r"step (?P<step>\d+)",
            "--action",
            "record_progress",
            "--event-type",
            "stdout.delta",
            "--action-payload-json",
            f"@{payload_path}",
        ],
    )
    run_result = runner.invoke(app, ["monitor", "run-once"])

    assert add_result.exit_code == 0
    assert run_result.exit_code == 0
    progress = ClioCoreQueue(core_dir).list_progress(job.job_id)
    assert progress[0].label == "iteration"
    assert progress[0].current == 25


def test_cli_rejects_invalid_json_object() -> None:
    result = CliRunner().invoke(
        app,
        [
            "monitor",
            "add-regex",
            "job_abc",
            "--pattern",
            "step",
            "--action-payload-json",
            "{bad}",
        ],
    )

    assert result.exit_code != 0
    assert "value must be valid JSON" in result.output
    assert "Traceback" not in result.output


def test_cli_record_progress_cannot_spoof_package_progress(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-record-progress",
        )
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(
        app,
        [
            "job",
            "record-progress",
            job.job_id,
            "--metadata-json",
            '{"source":"jarvis_package","package_name":"site.simulation","run_id":"spoofed"}',
        ],
    )

    assert result.exit_code == 0
    progress = ClioCoreQueue(core_dir).list_progress(job.job_id)[0]
    assert progress.metadata["source"] == "external_cli"
    assert "package_name" not in progress.metadata
    assert "run_id" not in progress.metadata


def test_cli_tests_ssh_transport(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    calls: list[dict[str, object]] = []

    def fake_probe(**kwargs: object) -> list[str]:
        calls.append(kwargs)
        return [
            "transport.protocol=ssh_forward",
            "transport.healthz=ok",
            "transport.cleanup=passed",
        ]

    monkeypatch.setattr("clio_relay.cli.run_ssh_forward_http_probe", fake_probe)
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "api-token")
    report_path = tmp_path / "ssh-transport.json"

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-ssh-transport",
            "--cluster",
            "ares",
            "--local-bind-port",
            "19001",
            "--remote-api-port",
            "9001",
            "--session-id",
            "session-1",
            "--validation-report",
            str(report_path),
            "--validation-launcher",
            "uvx",
            "--validation-install-source",
            "wheel:clio_relay-1.0.0-py3-none-any.whl",
        ],
    )

    assert result.exit_code == 0
    assert "transport.healthz=ok" in result.output
    assert calls[0]["cluster"] == "ares"
    assert calls[0]["local_bind_port"] == 19001
    assert calls[0]["remote_api_port"] == 9001
    assert calls[0]["session_id"] == "session-1"
    assert calls[0]["api_token"] == "api-token"
    assert calls[0]["detach_remote"] is False
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "passed"
    assert report["scenario"] == "transport"
    assert report["install_source"]["launcher"] == "uvx"
    assert {check["check_id"] for check in report["checks"]} >= {
        "transport.ssh",
        "transport.cleanup",
    }
    assert report["resources"] == [
        {
            "cluster": "ares",
            "kind": "connector",
            "metadata": {
                "cleanup_verified": True,
                "remote_session_retained": False,
                "transport_mode": "ssh-forward",
            },
            "provider": None,
            "references": [],
            "resource_id": "session-1",
            "role": "ssh_forward_probe",
            "state": "stopped",
        }
    ]
    assert report["cleanup"]["remaining_resources"] == []


def test_cli_ssh_transport_detach_report_models_retention_without_residual(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)

    def fake_probe(**kwargs: object) -> list[str]:
        assert kwargs["detach_remote"] is True
        return [
            "transport.protocol=ssh_forward",
            "transport.healthz=ok",
            "transport.remote_session=retained",
            "transport.remote_session_ownership=verified",
            "transport.cleanup=detached",
        ]

    monkeypatch.setattr("clio_relay.cli.run_ssh_forward_http_probe", fake_probe)
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "api-token")
    report_path = tmp_path / "ssh-detach.json"

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-ssh-transport",
            "--cluster",
            "ares",
            "--local-bind-port",
            "19011",
            "--session-id",
            "session-detach-1",
            "--detach-remote",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 0, result.output
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "passed"
    assert report["cleanup"]["mode"] == "transport_probe_detach"
    assert report["cleanup"]["remaining_resources"] == []
    resources = {item["kind"]: item for item in report["resources"]}
    assert resources["connector"]["state"] == "stopped"
    assert resources["connector"]["metadata"]["remote_session_retained"] is True
    assert resources["relay_session"]["state"] == "retained"
    assert resources["relay_session"]["metadata"]["verified_after_operation"] is True
    retained_actions = [
        action for action in report["cleanup"]["actions"] if action["action"] == "retain"
    ]
    assert retained_actions[0]["outcome"] == "retained"


def test_cli_tests_http_transport_and_writes_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    calls: list[dict[str, object]] = []

    def fake_probe(**kwargs: object) -> list[str]:
        calls.append(kwargs)
        return [
            "transport.protocol=wss",
            "transport.healthz=ok",
            "transport.cleanup=passed",
        ]

    def fake_worker_identity(
        report: LiveValidationReport,
        definition: ClusterDefinition,
        *,
        observed_worker_info: dict[str, object] | None = None,
    ) -> None:
        assert observed_worker_info is None
        assert definition.name == "ares"
        recorder = ValidationRecorder(report)
        with recorder.check("worker.artifact-version", "verified remote worker") as evidence:
            evidence.append(EvidenceReference(kind="test", excerpt="worker verified"))
        recorder.add_resource(
            ValidationResource(
                kind="relay_worker",
                resource_id="worker:ares",
                cluster="ares",
                state="running",
            )
        )

    monkeypatch.setattr("clio_relay.cli.run_frp_http_probe", fake_probe)
    monkeypatch.setattr(cli, "_attach_verified_remote_worker", fake_worker_identity)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "stcp-secret")
    report_path = tmp_path / "relay-transport.json"

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-http-transport",
            "--cluster",
            "ares",
            "--local-bind-port",
            "19002",
            "--remote-api-port",
            "9002",
            "--proxy-name",
            "relay-probe-1",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert calls[0]["token"] == "frp-token"
    assert calls[0]["secret_key"] == "stcp-secret"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "passed"
    assert {check["check_id"] for check in report["checks"]} >= {
        "transport.relay",
        "transport.cleanup",
        "worker.artifact-version",
    }
    assert {resource["kind"] for resource in report["resources"]} == {
        "connector",
        "relay_worker",
    }


def test_cli_tests_direct_transport_and_writes_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)

    def fake_probe(**_kwargs: object) -> list[str]:
        return [
            "direct_transport.result=xtcp",
            "transport.protocol=wss",
            "transport.proxy_type=xtcp",
            "transport.healthz=ok",
            "transport.cleanup=passed",
        ]

    monkeypatch.setattr("clio_relay.cli.run_frp_direct_http_probe", fake_probe)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "xtcp-secret")
    report_path = tmp_path / "direct-transport.json"

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-direct-transport",
            "--cluster",
            "ares",
            "--local-bind-port",
            "19004",
            "--proxy-name",
            "direct-probe-1",
            "--no-allow-stcp-fallback",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "passed"
    assert {check["check_id"] for check in report["checks"]} >= {
        "transport.direct",
        "transport.cleanup",
    }
    assert report["resources"][0]["role"] == "frp_xtcp_probe"


def test_cli_transport_failure_writes_partial_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)

    def failing_probe(**kwargs: object) -> list[str]:
        del kwargs
        raise RelayError("live transport failed")

    monkeypatch.setattr("clio_relay.cli.run_frp_http_probe", failing_probe)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "stcp-secret")
    report_path = tmp_path / "failed-transport.json"

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-http-transport",
            "--cluster",
            "ares",
            "--local-bind-port",
            "19003",
            "--proxy-name",
            "relay-probe-failed",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["checks"][-1]["check_id"] == "transport.completed"
    assert report["checks"][-1]["status"] == "failed"
    assert report["resources"][0]["state"] == "unknown"
    assert report["cleanup"]["remaining_resources"][0]["resource_id"] == ("relay-probe-failed")


def test_cli_transport_worker_identity_failure_fails_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)

    def fake_probe(**_kwargs: object) -> list[str]:
        return [
            "transport.protocol=wss",
            "transport.healthz=ok",
            "transport.cleanup=passed",
        ]

    def fail_worker_identity(
        _report: LiveValidationReport,
        _definition: ClusterDefinition,
        *,
        observed_worker_info: dict[str, object] | None = None,
    ) -> None:
        assert observed_worker_info is None
        raise ConfigurationError("remote wheel hash does not match")

    monkeypatch.setattr(cli, "run_frp_http_probe", fake_probe)
    monkeypatch.setattr(cli, "_attach_verified_remote_worker", fail_worker_identity)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "stcp-secret")
    report_path = tmp_path / "worker-mismatch.json"

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-http-transport",
            "--cluster",
            "ares",
            "--local-bind-port",
            "19005",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    worker_checks = [
        check for check in report["checks"] if check["check_id"] == "worker.installation-info"
    ]
    assert worker_checks[0]["status"] == "failed"


def test_cli_session_lifecycle_commands(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    started: list[dict[str, object]] = []
    torn_down: list[dict[str, object]] = []
    remote_calls: list[list[str]] = []

    def fake_start(**kwargs: object) -> list[str]:
        started.append(kwargs)
        return ["session_started=session-1", "session_generation_id=generation-1"]

    def fake_status(**kwargs: object) -> dict[str, object]:
        return _owned_session_status(session_id=cast(str, kwargs["session_id"]))

    def fake_teardown(**kwargs: object) -> SessionLifecycleReport:
        torn_down.append(kwargs)
        report = _verified_teardown_report()
        if kwargs.get("stop_worker") is True:
            report.resources.append(
                CleanupResource(
                    kind="worker_service",
                    resource_id="clio-relay-worker-ares.service",
                    location="ares",
                    action="stop",
                    ownership_verified=True,
                    outcome="stopped",
                    verified_after_operation=True,
                    observed_state="inactive",
                )
            )
        return report

    def fake_run_remote_clio(_definition: ClusterDefinition, arguments: list[str]) -> str:
        remote_calls.append(arguments)
        return "{}"

    monkeypatch.setattr("clio_relay.cli.start_remote_session", fake_start)
    monkeypatch.setattr("clio_relay.cli.status_remote_session", fake_status)
    monkeypatch.setattr("clio_relay.cli.teardown_remote_session", fake_teardown)
    monkeypatch.setattr("clio_relay.cli.run_remote_clio", fake_run_remote_clio)
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "api-token")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    _activate_owner_session(ClioCoreQueue(tmp_path / "core"))
    runner = CliRunner()

    start_result = runner.invoke(
        app,
        [
            "session",
            "start",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--remote-api-port",
            "9001",
            "--replace",
        ],
    )
    status_result = runner.invoke(
        app,
        ["session", "status", "--cluster", "ares", "--session-id", "session-1"],
    )
    teardown_result = runner.invoke(
        app,
        [
            "session",
            "teardown",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--stop-worker",
            "--keep-jobs",
        ],
    )

    assert start_result.exit_code == 0
    assert "session_started=session-1" in start_result.output
    assert started[0]["api_token"] == "api-token"
    assert started[0]["replace"] is True
    assert remote_calls == []
    assert (tmp_path / ".clio-relay" / "session-transitions").is_dir()
    assert status_result.exit_code == 0
    assert json.loads(status_result.output)["running"] is True
    assert teardown_result.exit_code == 0
    assert torn_down[0]["stop_worker"] is True
    assert torn_down[0]["cluster"] == "ares"
    assert torn_down[0]["expected_session_generation_id"] == "generation-1"


def test_cli_session_start_does_not_reopen_intake_when_process_start_fails(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "api-token")
    remote_calls: list[list[str]] = []

    def fail_start(**_kwargs: object) -> list[str]:
        raise RelayError("remote process start failed")

    def record_remote(_definition: ClusterDefinition, arguments: list[str]) -> str:
        remote_calls.append(arguments)
        return "{}"

    monkeypatch.setattr(cli, "start_remote_session", fail_start)
    monkeypatch.setattr(cli, "run_remote_clio", record_remote)

    result = CliRunner().invoke(
        app,
        ["session", "start", "--cluster", "ares", "--session-id", "session-1"],
    )

    assert result.exit_code == 1
    assert remote_calls == []


def test_cli_session_submit_jarvis_uses_identity_proven_client(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "api-token")
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: acceptance\npkgs: []\n", encoding="utf-8")
    captured: dict[str, object] = {}

    def submit_owned(**kwargs: object) -> RelayJob:
        captured.update(kwargs)
        selected_settings = cast(cli.RelaySettings, kwargs["settings"])
        payload = cast(dict[str, object], kwargs["payload"])
        return RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml=cast(str, payload["pipeline_yaml"])),
            idempotency_key=cast(str, payload["idempotency_key"]),
            metadata={
                "owner": "clio-relay",
                "owner_session_id": selected_settings.owner_session_id,
                "owner_session_generation_id": selected_settings.owner_session_generation_id,
            },
        )

    monkeypatch.setattr(cli, "submit_owned_session_job", submit_owned)

    result = CliRunner().invoke(
        app,
        [
            "session",
            "submit-jarvis",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--session-generation-id",
            "generation-1",
            "--pipeline-yaml-file",
            str(pipeline),
            "--idempotency-key",
            "acceptance-submit",
        ],
    )

    assert result.exit_code == 0, result.output
    document = json.loads(result.output)
    assert document["metadata"]["owner_session_generation_id"] == "generation-1"
    settings = cast(cli.RelaySettings, captured["settings"])
    assert settings.api_token == "api-token"
    assert settings.owner_session_cluster == "ares"
    assert settings.remote_cluster is None
    assert settings.owner_session_id == "session-1"
    assert settings.owner_session_generation_id == "generation-1"
    assert cast(dict[str, object], captured["payload"])["pipeline_yaml"] == (
        "name: acceptance\npkgs: []\n"
    )


def test_cli_session_detach_never_records_owner_session_closure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    _activate_owner_session(ClioCoreQueue(core_dir))
    lifecycle_events: list[str] = []

    def fake_detach(**_kwargs: object) -> SessionLifecycleReport:
        lifecycle_events.append("observe_remote_api")
        return SessionLifecycleReport(
            cluster="ares",
            session_id="session-1",
            session_generation_id="generation-1",
            mode="detach",
            resources=[
                CleanupResource(
                    kind="remote_relay_api",
                    resource_id="123",
                    location="ares",
                    action="retain",
                    ownership_verified=True,
                    outcome="retained",
                    verified_after_operation=True,
                )
            ],
        )

    def fake_gateway_cleanup(**_kwargs: object) -> list[dict[str, object]]:
        lifecycle_events.append("cleanup_desktop_connectors")
        return []

    monkeypatch.setattr(cli, "detach_remote_session", fake_detach)
    monkeypatch.setattr(cli, "_cleanup_owned_runtime_sessions", fake_gateway_cleanup)

    result = CliRunner().invoke(
        app,
        ["session", "detach", "--cluster", "ares", "--session-id", "session-1"],
    )

    queue = ClioCoreQueue(core_dir)
    assert result.exit_code == 0, result.output
    assert lifecycle_events == [
        "observe_remote_api",
        "cleanup_desktop_connectors",
        "observe_remote_api",
    ]
    assert queue.owner_session_is_closing("session-1") is False
    assert queue.get_owner_session_closed("session-1") is None


def test_cli_session_detach_default_report_failure_controls_exit(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))

    def incomplete_detach(**_kwargs: object) -> SessionLifecycleReport:
        return SessionLifecycleReport(
            cluster="ares",
            session_id="session-1",
            session_generation_id=None,
            mode="detach",
            resources=[
                CleanupResource(
                    kind="remote_relay_api",
                    resource_id="123",
                    location="ares",
                    action="retain",
                    ownership_verified=True,
                    outcome="retained",
                    verified_after_operation=True,
                )
            ],
        )

    def forbidden_gateway_cleanup(**_kwargs: object) -> list[dict[str, object]]:
        raise AssertionError("unverified detach must not mutate gateway connectors")

    monkeypatch.setattr(cli, "detach_remote_session", incomplete_detach)
    monkeypatch.setattr(cli, "_cleanup_owned_runtime_sessions", forbidden_gateway_cleanup)

    result = CliRunner().invoke(
        app,
        ["session", "detach", "--cluster", "ares", "--session-id", "session-1"],
    )

    assert result.exit_code == 1
    reports = list((tmp_path / ".clio-relay" / "validation-reports").glob("*.json"))
    assert len(reports) == 1
    canonical = json.loads(reports[0].read_text(encoding="utf-8"))
    assert canonical["status"] == "failed"
    detach_check = next(
        check for check in canonical["checks"] if check["check_id"] == "cleanup.detach"
    )
    assert detach_check["status"] == "failed"


def test_cli_session_detach_rejects_generation_change_after_connector_cleanup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    observations = iter(("generation-1", "generation-2"))
    cleanup_calls: list[str] = []

    def changing_detach(**_kwargs: object) -> SessionLifecycleReport:
        generation_id = next(observations)
        return SessionLifecycleReport(
            cluster="ares",
            session_id="session-1",
            session_generation_id=generation_id,
            mode="detach",
            resources=[
                CleanupResource(
                    kind="remote_relay_api",
                    resource_id="123",
                    location="ares",
                    action="retain",
                    ownership_verified=True,
                    outcome="retained",
                    verified_after_operation=True,
                )
            ],
        )

    def record_gateway_cleanup(**_kwargs: object) -> list[dict[str, object]]:
        cleanup_calls.append("called")
        return []

    monkeypatch.setattr(cli, "detach_remote_session", changing_detach)
    monkeypatch.setattr(cli, "_cleanup_owned_runtime_sessions", record_gateway_cleanup)

    result = CliRunner().invoke(
        app,
        ["session", "detach", "--cluster", "ares", "--session-id", "session-1"],
    )

    assert result.exit_code == 1
    assert "owned session generation changed during desktop detach" in result.output
    assert cleanup_calls == ["called"]
    reports = list((tmp_path / ".clio-relay" / "validation-reports").glob("*.json"))
    canonical = json.loads(reports[0].read_text(encoding="utf-8"))
    assert canonical["status"] == "failed"
    assert canonical["cleanup"]["remaining_resources"] == []


def test_cli_session_reopen_preserves_prior_generation_closure_history(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()

    prepared = runner.invoke(
        app,
        [
            "session",
            "prepare-start",
            "--session-id",
            "session-1",
            "--candidate-generation-id",
            "generation-1",
        ],
    )
    quiesced = runner.invoke(
        app,
        [
            "session",
            "quiesce-intake",
            "--session-id",
            "session-1",
            "--session-generation-id",
            "generation-1",
        ],
    )
    closed = runner.invoke(
        app,
        [
            "session",
            "mark-closed",
            "--session-id",
            "session-1",
            "--session-generation-id",
            "generation-1",
        ],
    )
    reopened = runner.invoke(
        app,
        [
            "session",
            "prepare-start",
            "--session-id",
            "session-1",
            "--recorded-generation-id",
            "generation-1",
            "--candidate-generation-id",
            "generation-2",
        ],
    )
    resumed = runner.invoke(
        app,
        [
            "session",
            "resume-intake",
            "--session-id",
            "session-1",
            "--session-generation-id",
            "generation-2",
        ],
    )

    queue = ClioCoreQueue(core_dir)
    assert prepared.exit_code == 0, prepared.output
    assert quiesced.exit_code == 0, quiesced.output
    assert closed.exit_code == 0, closed.output
    assert reopened.exit_code == 0, reopened.output
    assert resumed.exit_code == 0, resumed.output
    assert queue.owner_session_is_closing("session-1") is False
    assert (
        queue.get_owner_session_closed(
            "session-1",
            session_generation_id="generation-1",
        )
        is not None
    )
    assert (
        queue.get_owner_session_closed(
            "session-1",
            session_generation_id="generation-2",
        )
        is None
    )


def test_cli_session_prepare_start_preserves_active_generation_and_resources(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()
    first = runner.invoke(
        app,
        [
            "session",
            "prepare-start",
            "--session-id",
            "session-1",
            "--candidate-generation-id",
            "generation-1",
        ],
    )
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="preserve-generation-job",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "session-1",
                "owner_session_generation_id": "generation-1",
            },
        )
    )
    gateway = queue.create_gateway_session(
        GatewaySession(
            cluster="ares",
            name="preserve-generation-gateway",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "session-1",
                "owner_session_generation_id": "generation-1",
            },
        )
    )

    replacement = runner.invoke(
        app,
        [
            "session",
            "prepare-start",
            "--session-id",
            "session-1",
            "--recorded-generation-id",
            "generation-1",
            "--candidate-generation-id",
            "generation-2",
        ],
    )
    dead_api_recovery = runner.invoke(
        app,
        [
            "session",
            "prepare-start",
            "--session-id",
            "session-1",
            "--candidate-generation-id",
            "generation-3",
        ],
    )

    assert first.exit_code == 0, first.output
    assert replacement.exit_code == 0, replacement.output
    assert dead_api_recovery.exit_code == 0, dead_api_recovery.output
    assert json.loads(replacement.output)["session_generation_id"] == "generation-1"
    assert json.loads(dead_api_recovery.output)["session_generation_id"] == "generation-1"
    assert queue.get_job(job.job_id).metadata["owner_session_generation_id"] == "generation-1"
    assert (
        queue.get_gateway_session(gateway.session_id).metadata["owner_session_generation_id"]
        == "generation-1"
    )


def test_cli_session_prepare_start_refuses_new_generation_before_closure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    queue = ClioCoreQueue(core_dir)
    _activate_owner_session(queue)
    queue.set_owner_session_closing(
        "session-1",
        session_generation_id="generation-1",
    )

    result = CliRunner().invoke(
        app,
        [
            "session",
            "prepare-start",
            "--session-id",
            "session-1",
            "--recorded-generation-id",
            "generation-1",
            "--candidate-generation-id",
            "generation-2",
        ],
    )

    assert result.exit_code == 1
    assert "unfinished generation transition" in result.output
    assert queue.owner_session_is_closing("session-1") is True
    assert (
        queue.get_owner_session_closed(
            "session-1",
            session_generation_id="generation-1",
        )
        is None
    )


def test_cli_session_teardown_failure_leaves_generation_quiesced_not_closed(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    _activate_owner_session(ClioCoreQueue(core_dir))
    monkeypatch.setattr(
        cli,
        "status_remote_session",
        _fake_owned_session_status,
    )
    failed_report = _verified_teardown_report()
    failed_report.resources.append(
        CleanupResource(
            kind="remote_connector",
            resource_id="connector-123",
            location="ares",
            action="stop",
            ownership_verified=True,
            outcome="failed",
            residual=True,
            detail="connector still running",
        )
    )

    def fake_failed_teardown(**_kwargs: object) -> SessionLifecycleReport:
        return failed_report

    monkeypatch.setattr(
        cli,
        "teardown_remote_session",
        fake_failed_teardown,
    )

    result = CliRunner().invoke(
        app,
        ["session", "teardown", "--cluster", "ares", "--session-id", "session-1"],
    )

    queue = ClioCoreQueue(core_dir)
    assert result.exit_code == 1
    assert queue.owner_session_is_closing("session-1") is True
    assert queue.get_owner_session_closed("session-1") is None


def test_cli_session_teardown_requires_connectors_for_each_gateway(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    queue = ClioCoreQueue(core_dir)
    _activate_owner_session(queue)
    monkeypatch.setattr(cli, "status_remote_session", _fake_owned_session_status)
    monkeypatch.setattr(cli, "teardown_remote_session", _fake_verified_teardown)

    def gateway_without_connectors(**_kwargs: object) -> list[dict[str, object]]:
        return [
            {
                "resources": [
                    CleanupResource(
                        kind="gateway_record",
                        resource_id="gateway-1",
                        location="desktop",
                        action="close",
                        ownership_verified=True,
                        outcome="closed",
                        verified_after_operation=True,
                    ).model_dump(mode="json")
                ],
                "errors": [],
            }
        ]

    monkeypatch.setattr(cli, "_cleanup_owned_runtime_sessions", gateway_without_connectors)

    result = CliRunner().invoke(
        app,
        ["session", "teardown", "--cluster", "ares", "--session-id", "session-1"],
    )

    assert result.exit_code == 1
    assert "connector evidence did not cover each owned gateway" in result.output
    assert queue.owner_session_is_closing("session-1") is True
    assert queue.get_owner_session_closed("session-1") is None


def test_cli_session_teardown_retries_same_policy_after_closure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    queue = ClioCoreQueue(core_dir)
    _activate_owner_session(queue)
    intent = queue.set_owner_session_closing(
        "session-1",
        session_generation_id="generation-1",
    )
    queue.set_owner_session_closed(
        "session-1",
        session_generation_id="generation-1",
    )
    monkeypatch.setattr(cli, "status_remote_session", _fake_owned_session_status)
    monkeypatch.setattr(cli, "teardown_remote_session", _fake_verified_teardown)
    monkeypatch.setattr(cli, "_cleanup_owned_runtime_sessions", _fake_empty_runtime_cleanup)

    result = CliRunner().invoke(
        app,
        ["session", "teardown", "--cluster", "ares", "--session-id", "session-1"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["cleanup_operation_id"] == intent["operation_id"]
    assert payload["cleanup_policy"] == {
        "stop_worker": False,
        "cancel_jobs": False,
        "cancel_scheduler_jobs": False,
    }


def test_cli_session_teardown_failure_preserves_stopped_api_evidence(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    core_dir = tmp_path / "core"
    report_path = tmp_path / "teardown-partial.json"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    queue = ClioCoreQueue(core_dir)
    _activate_owner_session(queue)
    list_calls = [0]
    gateway_cleanup_calls: list[str] = []

    def fail_after_api_stop(*_args: object, **kwargs: object) -> list[object]:
        if kwargs.get("owner_session_generation_id") is None:
            return []
        list_calls[0] += 1
        if list_calls[0] == 1:
            return []
        raise RelayError("post-API owner-session rescan failed")

    def forbidden_gateway_cleanup(**_kwargs: object) -> list[dict[str, object]]:
        gateway_cleanup_calls.append("gateway")
        return []

    monkeypatch.setattr(cli, "status_remote_session", _fake_owned_session_status)
    monkeypatch.setattr(cli, "teardown_remote_session", _fake_verified_teardown)
    monkeypatch.setattr(cli, "_list_owned_active_cluster_jobs", fail_after_api_stop)
    monkeypatch.setattr(cli, "_cleanup_owned_runtime_sessions", forbidden_gateway_cleanup)

    result = CliRunner().invoke(
        app,
        [
            "session",
            "teardown",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--validation-report",
            str(report_path),
        ],
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    session_resource = next(
        resource for resource in report["resources"] if resource["kind"] == "relay_session"
    )
    process_resource = next(
        resource for resource in report["resources"] if resource["kind"] == "relay_process"
    )
    assert result.exit_code == 1
    assert "post-API owner-session rescan failed" in result.output
    assert report["status"] == "failed"
    assert session_resource["resource_id"] == "session-1:generation-1"
    assert session_resource["state"] == "stopped"
    assert process_resource["resource_id"] == "123"
    assert process_resource["state"] == "stopped"
    assert report["cleanup"]["actions"][0]["kind"] == "remote_relay_api"
    assert gateway_cleanup_calls == []
    assert queue.owner_session_is_closing("session-1") is True
    assert queue.get_owner_session_closed("session-1") is None


def test_cli_session_teardown_rejects_generation_change_before_closure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    _activate_owner_session(ClioCoreQueue(core_dir))

    def fake_generation_one_status(**_kwargs: object) -> dict[str, object]:
        return _owned_session_status(generation_id="generation-1")

    def fake_generation_two_teardown(**_kwargs: object) -> SessionLifecycleReport:
        return _verified_teardown_report(generation_id="generation-2")

    monkeypatch.setattr(cli, "status_remote_session", fake_generation_one_status)
    monkeypatch.setattr(cli, "teardown_remote_session", fake_generation_two_teardown)

    result = CliRunner().invoke(
        app,
        ["session", "teardown", "--cluster", "ares", "--session-id", "session-1"],
    )

    queue = ClioCoreQueue(core_dir)
    assert result.exit_code == 1
    assert "generation did not match" in result.output
    assert queue.owner_session_is_closing("session-1") is True
    assert queue.get_owner_session_closed("session-1") is None


def test_cli_remote_teardown_writes_closure_only_in_remote_authoritative_core(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    local_core_dir = tmp_path / "desktop-core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(local_core_dir))
    monkeypatch.setenv("CLIO_RELAY_OWNER_SESSION_ID", "session-1")
    monkeypatch.setenv("CLIO_RELAY_SESSION_GENERATION_ID", "generation-1")
    monkeypatch.setenv("CLIO_RELAY_OWNER_SESSION_CLUSTER", "ares")
    monkeypatch.delenv("CLIO_RELAY_REMOTE_CLUSTER", raising=False)
    monkeypatch.delenv("CLIO_RELAY_CLI_MODE", raising=False)
    remote_calls: list[list[str]] = []
    monkeypatch.setattr(
        cli,
        "status_remote_session",
        _fake_owned_session_status,
    )
    monkeypatch.setattr(
        cli,
        "teardown_remote_session",
        _fake_verified_teardown,
    )
    monkeypatch.setattr(cli, "_cleanup_owned_runtime_sessions", _fake_empty_runtime_cleanup)
    monkeypatch.setattr(
        cli,
        "_list_remote_owned_active_cluster_jobs",
        _fake_empty_owned_jobs,
    )

    def fake_remote(_definition: ClusterDefinition, args: list[str]) -> str:
        remote_calls.append(args)
        if args[:2] == ["session", "admission-status"]:
            return json.dumps(
                {
                    "schema_version": "clio-relay.owner-session-admission-status.v1",
                    "owner_session_id": "session-1",
                    "session_generation_id": "generation-1",
                    "active_generation_id": "generation-1",
                    "closing_generation_id": None,
                    "active": True,
                    "closing": False,
                    "closed": False,
                    "open": True,
                    "cleanup_intent": None,
                }
            )
        if args[:2] == ["session", "quiesce-intake"]:
            operation_id = args[args.index("--cleanup-operation-id") + 1]
            local_admission_id = cli._desktop_owner_session_admission_id(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                cluster="ares",
                session_id="session-1",
            )
            with pytest.raises(QueueConflictError, match="closing and rejects new work"):
                ClioCoreQueue(local_core_dir).create_gateway_session(
                    GatewaySession(
                        cluster="ares",
                        name="late-owned-gateway",
                        metadata={
                            "owner": "clio-relay",
                            "owner_session_id": "session-1",
                            "owner_session_generation_id": "generation-1",
                            "owner_session_admission_id": local_admission_id,
                        },
                    )
                )
            return json.dumps(
                {
                    "session_id": "session-1",
                    "session_generation_id": "generation-1",
                    "intake": "quiesced",
                    "cleanup_intent": {
                        "schema_version": "clio-relay.owner-session-cleanup-intent.v1",
                        "operation_id": operation_id,
                        "owner_session_id": "session-1",
                        "session_generation_id": "generation-1",
                        "stop_worker": False,
                        "cancel_jobs": False,
                        "cancel_scheduler_jobs": False,
                    },
                }
            )
        return json.dumps(
            {
                "owner_session_id": "session-1",
                "session_generation_id": "generation-1",
                "residual_resource_ids": [],
            }
        )

    monkeypatch.setattr(cli, "run_remote_clio", fake_remote)

    def forbid_worker_verification(
        _report: LiveValidationReport,
        _definition: ClusterDefinition,
        *,
        observed_worker_info: dict[str, object] | None = None,
    ) -> None:
        del observed_worker_info
        raise AssertionError("cleanup without an artifact digest must not claim worker identity")

    monkeypatch.setattr(cli, "_attach_verified_remote_worker", forbid_worker_verification)

    def observe_no_worker(
        _definition: ClusterDefinition,
    ) -> tuple[None, None]:
        return None, None

    monkeypatch.setattr(cli, "_observe_worker_before_cleanup", observe_no_worker)

    report_path = tmp_path / "remote-teardown.json"
    result = CliRunner().invoke(
        app,
        [
            "session",
            "teardown",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert remote_calls[0] == [
        "session",
        "admission-status",
        "--session-id",
        "session-1",
        "--session-generation-id",
        "generation-1",
    ]
    assert remote_calls[1][:6] == [
        "session",
        "quiesce-intake",
        "--session-id",
        "session-1",
        "--session-generation-id",
        "generation-1",
    ]
    operation_id = remote_calls[1][remote_calls[1].index("--cleanup-operation-id") + 1]
    assert operation_id.startswith("cleanup_")
    assert remote_calls[2] == [
        "session",
        "mark-closed",
        "--session-id",
        "session-1",
        "--session-generation-id",
        "generation-1",
    ]
    local_queue = ClioCoreQueue(local_core_dir)
    assert local_queue.owner_session_is_closing("session-1") is False
    assert local_queue.get_owner_session_closed("session-1") is None
    local_admission_id = cli._desktop_owner_session_admission_id(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster="ares",
        session_id="session-1",
    )
    local_closure = local_queue.get_owner_session_closed(
        local_admission_id,
        session_generation_id="generation-1",
    )
    assert local_closure is not None
    assert local_closure.residual_resource_ids == []
    validation_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert validation_report["status"] == "passed"
    assert validation_report["install_source"]["artifact_sha256"] is None
    assert validation_report["install_source"]["artifact_identity_verified"] is False
    assert not any(check["check_id"].startswith("worker.") for check in validation_report["checks"])


def test_cli_session_start_requires_token_by_default(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.delenv("CLIO_RELAY_API_TOKEN", raising=False)

    result = CliRunner().invoke(
        app,
        [
            "session",
            "start",
            "--cluster",
            "ares",
            "--session-id",
            "session-without-token",
        ],
    )

    assert result.exit_code == 2


def test_cli_jarvis_mcp_preflight_failure_writes_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    report_path = tmp_path / "jarvis-preflight-failed.json"

    result = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-validate",
            "--cluster",
            "ares",
            "--arguments-json",
            "{}",
            "--report",
            str(report_path),
        ],
    )

    assert result.exit_code != 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["checks"][-1]["check_id"] == "jarvis-mcp.preflight"


def test_jarvis_live_progress_observation_requires_running_warming_record() -> None:
    progress = [
        {
            "progress_id": "progress-live",
            "metadata": {
                "source": "jarvis_execution",
                "provider_source_authority": "jarvis_mcp_progress_notification",
                "producer_validated": True,
                "execution_binding_validated": False,
                "progress_transport_sequence": 1,
            },
        }
    ]
    running: dict[str, object] = {
        "job": {"state": "running", "updated_at": "2026-07-11T10:00:00Z"},
        "terminal": False,
    }

    observation = cli._live_jarvis_progress_observation(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        progress,
        running,
    )

    assert observation == {
        "progress_id": "progress-live",
        "job_state": "running",
        "job_updated_at": "2026-07-11T10:00:00Z",
        "terminal": False,
        "progress_transport_sequence": 1,
    }
    assert (
        cli._live_jarvis_progress_observation(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            progress,
            {"job": {"state": "succeeded"}, "terminal": True},
        )
        is None
    )


def test_cli_scheduler_preflight_failure_writes_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    ClusterRegistry.default().save(tmp_path / ".clio-relay" / "clusters.json")
    report_path = tmp_path / "scheduler-preflight-failed.json"

    result = CliRunner().invoke(
        app,
        [
            "scheduler",
            "validate-lifecycle",
            "--cluster",
            "missing",
            "--report",
            str(report_path),
        ],
    )

    assert result.exit_code != 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["checks"][-1]["check_id"] == "scheduler.preflight"


@pytest.mark.parametrize(
    ("command", "report_option", "check_id"),
    [
        (
            [
                "relay-host",
                "test-http-transport",
                "--cluster",
                "missing",
                "--local-bind-port",
                "19101",
            ],
            "--validation-report",
            "transport.preflight",
        ),
        (
            [
                "relay-host",
                "test-direct-transport",
                "--cluster",
                "missing",
                "--local-bind-port",
                "19102",
            ],
            "--validation-report",
            "transport.preflight",
        ),
        (
            [
                "relay-host",
                "test-ssh-transport",
                "--cluster",
                "missing",
                "--local-bind-port",
                "19103",
            ],
            "--validation-report",
            "transport.preflight",
        ),
        (
            ["live-test", "--cluster", "missing"],
            "--report",
            "live.preflight",
        ),
        (
            [
                "session",
                "detach",
                "--cluster",
                "missing",
                "--session-id",
                "owned-session",
            ],
            "--validation-report",
            "session.detach.preflight",
        ),
        (
            [
                "session",
                "teardown",
                "--cluster",
                "missing",
                "--session-id",
                "owned-session",
            ],
            "--validation-report",
            "session.teardown.preflight",
        ),
    ],
)
def test_cli_acceptance_preflight_failure_always_writes_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    command: list[str],
    report_option: str,
    check_id: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    ClusterRegistry.default().save(tmp_path / ".clio-relay" / "clusters.json")
    report_path = tmp_path / f"{check_id.replace('.', '-')}.json"

    result = CliRunner().invoke(app, [*command, report_option, str(report_path)])

    assert result.exit_code != 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["checks"][-1]["check_id"] == check_id


@pytest.mark.parametrize(
    ("command", "mode", "cancel_relay_jobs", "cancel_scheduler_jobs", "stop_worker"),
    [
        (
            [
                "session",
                "teardown",
                "--cluster",
                "missing",
                "--session-id",
                "owned-session",
                "--stop-worker",
                "--cancel-jobs",
                "--cancel-scheduler-jobs",
            ],
            "teardown",
            True,
            True,
            True,
        ),
        (
            [
                "session",
                "detach",
                "--cluster",
                "missing",
                "--session-id",
                "owned-session",
            ],
            "detach",
            False,
            False,
            False,
        ),
        (
            [
                "gateway",
                "stop-runtime",
                "gateway-owned",
                "--cluster",
                "missing",
                "--cancel-scheduler-job",
            ],
            "teardown",
            False,
            True,
            False,
        ),
        (
            [
                "gateway",
                "detach-runtime",
                "gateway-owned",
                "--cluster",
                "missing",
            ],
            "detach",
            False,
            False,
            False,
        ),
    ],
)
def test_cli_cleanup_failure_report_preserves_requested_policy_from_command_entry(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    command: list[str],
    mode: str,
    cancel_relay_jobs: bool,
    cancel_scheduler_jobs: bool,
    stop_worker: bool,
) -> None:
    monkeypatch.chdir(tmp_path)
    ClusterRegistry.default().save(tmp_path / ".clio-relay" / "clusters.json")
    report_path = tmp_path / "cleanup-entry-failed.json"

    result = CliRunner().invoke(
        app,
        [*command, "--validation-report", str(report_path)],
    )

    assert result.exit_code != 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    cleanup = report["cleanup"]
    assert cleanup["requested"] is True
    assert cleanup["mode"] == mode
    assert cleanup["cancel_relay_jobs"] is cancel_relay_jobs
    assert cleanup["cancel_scheduler_jobs"] is cancel_scheduler_jobs
    assert cleanup["stop_worker"] is stop_worker
    assert cleanup["actions"][0]["outcome"] == "pending"


def test_cli_invalid_scheduler_only_teardown_records_the_rejected_requested_policy(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    report_path = tmp_path / "invalid-scheduler-only-policy.json"

    result = CliRunner().invoke(
        app,
        [
            "session",
            "teardown",
            "--cluster",
            "test-cluster",
            "--session-id",
            "owned-session",
            "--cancel-scheduler-jobs",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code != 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["checks"][-1]["check_id"] == "session.teardown.preflight"
    assert report["cleanup"]["requested"] is True
    assert report["cleanup"]["mode"] == "teardown"
    assert report["cleanup"]["cancel_relay_jobs"] is False
    assert report["cleanup"]["cancel_scheduler_jobs"] is True


def test_cli_session_detach_failure_writes_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)

    def fail_detach(**_kwargs: object) -> SessionLifecycleReport:
        raise RelayError("remote session ownership check failed")

    monkeypatch.setattr(cli, "detach_remote_session", fail_detach)
    report_path = tmp_path / "detach-failed.json"

    result = CliRunner().invoke(
        app,
        [
            "session",
            "detach",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["scenario"] == "cleanup"
    assert report["status"] == "failed"
    assert report["checks"][-1]["check_id"] == "session.detach"


def test_cli_session_teardown_failure_writes_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)

    def fail_teardown(**_kwargs: object) -> SessionLifecycleReport:
        raise RelayError("remote process identity changed")

    monkeypatch.setattr(cli, "teardown_remote_session", fail_teardown)
    report_path = tmp_path / "teardown-failed.json"

    result = CliRunner().invoke(
        app,
        [
            "session",
            "teardown",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["scenario"] == "cleanup"
    assert report["status"] == "failed"
    assert report["checks"][-1]["check_id"] == "session.teardown"


def test_failed_acceptance_report_overwrites_passed_partial_and_is_idempotent(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "partial-then-failed.json"
    partial = new_live_validation_report(scenario="cleanup", cluster="ares")
    recorder = ValidationRecorder(partial)
    with recorder.check("cleanup.preflight", "verify cleanup preflight") as evidence:
        evidence.append(
            EvidenceReference(
                kind="cleanup_preflight",
                excerpt="owned cleanup preflight passed",
            )
        )
    recorder.finish()
    recorder.write(report_path)
    assert json.loads(report_path.read_text(encoding="utf-8"))["status"] == "passed"

    error = RelayError("post-operation verification failed")
    cli._write_failed_acceptance_report(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        path=report_path,
        scenario="cleanup",
        cluster="ares",
        check_id="cleanup.post-operation",
        summary="verify cleanup post-operation state",
        error=error,
        launcher="uv-tool",
        install_source="wheel:clio_relay-1.0.0.whl",
        artifact=None,
        partial_report=partial,
    )
    failed_once = json.loads(report_path.read_text(encoding="utf-8"))
    assert failed_once["status"] == "failed"
    assert [check["check_id"] for check in failed_once["checks"]] == [
        "cleanup.preflight",
        "cleanup.post-operation",
    ]
    cli._write_failed_acceptance_report(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        path=report_path,
        scenario="cleanup",
        cluster="ares",
        check_id="cleanup.post-operation",
        summary="verify cleanup post-operation state",
        error=error,
        launcher="uv-tool",
        install_source="wheel:clio_relay-1.0.0.whl",
        artifact=None,
        partial_report=partial,
    )
    assert json.loads(report_path.read_text(encoding="utf-8")) == failed_once


def test_release_validate_local_replaces_stale_success_on_preflight_failure(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "local-release.json"
    stale_report_id = _write_passing_validation_report(
        report_path,
        scenario="local-release",
        cluster="local",
    )

    result = CliRunner().invoke(
        app,
        [
            "release",
            "validate-local",
            "--project-root",
            str(tmp_path / "missing-checkout"),
            "--report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    current = LiveValidationReport.model_validate_json(report_path.read_text(encoding="utf-8"))
    assert current.report_id != stale_report_id
    assert current.scenario == "local-release"
    assert current.cluster == "local"
    assert current.status.value == "failed"
    assert current.checks[-1].check_id == "local-release.completed"
    assert "has no pyproject.toml" in (current.error or "")


def test_live_test_replaces_stale_success_when_secret_resolution_fails(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.delenv("CLIO_RELAY_FRP_TOKEN", raising=False)
    monkeypatch.delenv("CLIO_RELAY_STCP_SECRET", raising=False)
    report_path = tmp_path / "live-test.json"
    stale_report_id = _write_passing_validation_report(
        report_path,
        scenario="live-test",
        cluster="ares",
    )

    result = CliRunner().invoke(
        app,
        [
            "live-test",
            "--cluster",
            "ares",
            "--verify-transport",
            "--report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    current = LiveValidationReport.model_validate_json(report_path.read_text(encoding="utf-8"))
    assert current.report_id != stale_report_id
    assert current.scenario == "live-test"
    assert current.cluster == "ares"
    assert current.status.value == "failed"
    assert current.checks[-1].check_id == "live.completed"
    assert "frp token" in (current.error or "")


def test_cli_session_teardown_defaults_to_keep_jobs(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="keep-job",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "session-1",
                "owner_session_generation_id": "generation-1",
            },
        )
    )
    torn_down: list[dict[str, object]] = []

    def fake_teardown(**kwargs: object) -> SessionLifecycleReport:
        torn_down.append(kwargs)
        return _verified_teardown_report()

    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    _activate_owner_session(queue)
    monkeypatch.setattr(
        "clio_relay.cli.status_remote_session",
        _fake_owned_session_status,
    )
    monkeypatch.setattr("clio_relay.cli.teardown_remote_session", fake_teardown)

    result = CliRunner().invoke(
        app,
        ["session", "teardown", "--cluster", "ares", "--session-id", "session-1"],
        input="\n",
    )

    assert result.exit_code == 0
    assert "Cancel queued or running jobs" not in result.output
    payload = json.loads(result.output)
    assert payload["relay_jobs"]["cancel_requested"] is False
    retained = [resource for resource in payload["resources"] if resource["kind"] == "relay_job"]
    assert retained[0]["resource_id"] == job.job_id
    assert retained[0]["outcome"] == "retained"
    assert retained[0]["verified_after_operation"] is True
    refreshed_queue = ClioCoreQueue(tmp_path / "core")
    assert refreshed_queue.get_job(job.job_id).state == JobState.QUEUED
    closure = refreshed_queue.get_owner_session_closed(
        "session-1",
        session_generation_id="generation-1",
    )
    assert closure is not None
    assert closure.residual_resource_ids == []
    assert any(resource["kind"] == "owner_session" for resource in payload["resources"])
    assert torn_down[0]["stop_worker"] is False


def test_cli_teardown_refuses_implicit_legacy_job_ownership(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    submitted = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="legacy-owner-session-job",
        )
    )
    legacy = submitted.model_copy(
        update={
            "metadata": {
                "owner": "clio-relay",
                "owner_session_id": "session-1",
            }
        }
    )
    for family in ("jobs", "jobs_active", "jobs_queued"):
        (core_dir / family / f"{legacy.job_id}.json").write_text(
            legacy.model_dump_json(),
            encoding="utf-8",
        )
    queue.update_job_state(legacy.job_id, JobState.SUCCEEDED)
    _activate_owner_session(queue)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    monkeypatch.setattr(cli, "status_remote_session", _fake_owned_session_status)
    monkeypatch.setattr(cli, "teardown_remote_session", _fake_verified_teardown)

    report_path = tmp_path / "legacy-ambiguity.json"
    result = CliRunner().invoke(
        app,
        [
            "session",
            "teardown",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--cancel-jobs",
            "--cancel-scheduler-jobs",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    assert "unversioned legacy jobs" in result.output
    legacy_closure = queue.get_owner_session_closed(
        "session-1",
        session_generation_id=None,
    )
    assert legacy_closure is None
    assert queue.plan_terminal_job_gc(legacy.job_id).eligible is False
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert any(
        resource["resource_id"] == legacy.job_id
        for resource in report["cleanup"]["remaining_resources"]
    )
    legacy_resource = next(
        resource for resource in report["resources"] if resource["resource_id"] == legacy.job_id
    )
    assert legacy_resource["metadata"]["mutation_refused"] is True


def test_cli_session_teardown_can_cancel_active_jobs(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    queue = ClioCoreQueue(tmp_path / "core")
    active = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="cancel-active-job",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "session-1",
                "owner_session_generation_id": "generation-1",
            },
        )
    )
    unrelated_same_cluster = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="keep-unrelated-ares-job",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "another-session",
                "owner_session_generation_id": "another-generation",
            },
        )
    )
    other_cluster = queue.submit_job(
        RelayJob(
            cluster="other",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="keep-other-cluster-job",
        )
    )
    torn_down: list[dict[str, object]] = []

    def fake_teardown(**kwargs: object) -> SessionLifecycleReport:
        torn_down.append(kwargs)
        return _verified_teardown_report()

    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    _activate_owner_session(queue)
    monkeypatch.setattr(
        "clio_relay.cli.status_remote_session",
        _fake_owned_session_status,
    )
    monkeypatch.setattr("clio_relay.cli.teardown_remote_session", fake_teardown)

    result = CliRunner().invoke(
        app,
        [
            "session",
            "teardown",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--cancel-jobs",
        ],
    )

    refreshed = ClioCoreQueue(tmp_path / "core")
    assert result.exit_code == 0
    assert json.loads(result.output)["relay_jobs"]["canceled_job_ids"] == [active.job_id]
    assert refreshed.get_job(active.job_id).state == JobState.CANCELED
    assert refreshed.get_job(unrelated_same_cluster.job_id).state == JobState.QUEUED
    assert refreshed.get_job(other_cluster.job_id).state == JobState.QUEUED
    assert torn_down[0]["stop_worker"] is False


@pytest.mark.parametrize("active_state", [JobState.LEASED, JobState.RUNNING])
def test_cli_session_teardown_waits_for_worker_acknowledged_cancellation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    active_state: JobState,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key=f"async-cancel-{active_state.value}",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "session-1",
                "owner_session_generation_id": "generation-1",
            },
        )
    )
    endpoint = queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="worker",
            pid=123,
        )
    )
    lease = queue.acquire_next_job(
        endpoint.endpoint_id,
        cluster="ares",
        ttl_seconds=60,
    )
    assert lease is not None
    if active_state is JobState.RUNNING:
        queue.update_job_state(job.job_id, JobState.RUNNING)
    cleanup_observations: list[JobState] = []
    sleep_calls: list[float] = []

    def acknowledge_after_poll(seconds: float) -> None:
        sleep_calls.append(seconds)
        queue.acknowledge_job_cancellation(job.job_id)
        queue.release_lease(lease.lease_id)

    def verified_runtime_cleanup(**_kwargs: object) -> list[dict[str, object]]:
        cleanup_observations.append(queue.get_job(job.job_id).state)
        return []

    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    _activate_owner_session(queue)
    monkeypatch.setattr(cli, "status_remote_session", _fake_owned_session_status)
    monkeypatch.setattr(cli, "teardown_remote_session", _fake_verified_teardown)
    monkeypatch.setattr(cli, "_cleanup_owned_runtime_sessions", verified_runtime_cleanup)
    monkeypatch.setattr(cli, "sleep", acknowledge_after_poll)

    result = CliRunner().invoke(
        app,
        [
            "session",
            "teardown",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--cancel-jobs",
            "--relay-cancel-timeout-seconds",
            "1",
            "--relay-cancel-poll-seconds",
            "0.01",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["relay_jobs"]["canceled_job_ids"] == [job.job_id]
    assert sleep_calls
    assert cleanup_observations
    assert set(cleanup_observations) == {JobState.CANCELED}
    canceled = queue.get_job(job.job_id)
    request = cast(dict[str, object], canceled.metadata["cancellation_request"])
    assert canceled.state is JobState.CANCELED
    assert request["cleanup_acknowledged"] is True
    relay_resource = next(
        resource for resource in payload["resources"] if resource["kind"] == "relay_job"
    )
    assert relay_resource["outcome"] == "canceled"
    assert relay_resource["verified_after_operation"] is True


def test_cli_session_teardown_does_not_stop_runtime_before_cancel_acknowledgment(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="cancel-ack-timeout",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "session-1",
                "owner_session_generation_id": "generation-1",
            },
        )
    )
    endpoint = queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="worker",
            pid=123,
        )
    )
    assert (
        queue.acquire_next_job(
            endpoint.endpoint_id,
            cluster="ares",
            ttl_seconds=60,
        )
        is not None
    )
    destructive_calls: list[str] = []
    clock = [0.0]

    def advance_clock(seconds: float) -> None:
        clock[0] += seconds

    def forbidden_runtime_cleanup(**_kwargs: object) -> list[dict[str, object]]:
        destructive_calls.append("gateway")
        return []

    def forbidden_teardown(**_kwargs: object) -> SessionLifecycleReport:
        destructive_calls.append("api")
        return _verified_teardown_report()

    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    _activate_owner_session(queue)
    monkeypatch.setattr(cli, "status_remote_session", _fake_owned_session_status)
    monkeypatch.setattr(cli, "_cleanup_owned_runtime_sessions", forbidden_runtime_cleanup)
    monkeypatch.setattr(cli, "teardown_remote_session", forbidden_teardown)
    monkeypatch.setattr(cli, "monotonic", lambda: clock[0])
    monkeypatch.setattr(cli, "sleep", advance_clock)
    report_path = tmp_path / "cancel-timeout-report.json"

    result = CliRunner().invoke(
        app,
        [
            "session",
            "teardown",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--cancel-jobs",
            "--relay-cancel-timeout-seconds",
            "0.02",
            "--relay-cancel-poll-seconds",
            "0.01",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    assert "worker-acknowledged relay cancellation" in result.output
    assert destructive_calls == []
    pending = queue.get_job(job.job_id)
    assert pending.state is JobState.LEASED
    assert isinstance(pending.metadata.get("cancellation_request"), dict)
    assert queue.owner_session_is_closing("session-1") is True
    assert queue.get_owner_session_closed("session-1") is None
    intent = queue.get_owner_session_cleanup_intent(
        "session-1",
        session_generation_id="generation-1",
    )
    assert intent is not None
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["cleanup"]["requested"] is True
    assert report["cleanup"]["mode"] == "teardown"
    assert report["cleanup"]["operation_id"] == intent["operation_id"]
    assert report["cleanup"]["cancel_relay_jobs"] is True
    assert report["cleanup"]["cancel_scheduler_jobs"] is False
    assert report["cleanup"]["stop_worker"] is False
    relay_action = next(
        action
        for action in report["cleanup"]["actions"]
        if action["kind"] == "relay_job" and action["resource_id"] == job.job_id
    )
    assert relay_action["action"] == "cancel"
    assert relay_action["outcome"] == "failed"
    assert relay_action["verified_after_operation"] is False
    assert relay_action["residual"] is True
    relay_resource = next(
        resource
        for resource in report["resources"]
        if resource["kind"] == "relay_job" and resource["resource_id"] == job.job_id
    )
    assert relay_resource["state"] == JobState.LEASED.value
    assert any(
        resource["kind"] == "relay_job" and resource["resource_id"] == job.job_id
        for resource in report["cleanup"]["remaining_resources"]
    )


def test_cli_session_scheduler_cancellation_requires_explicit_flag(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="cancel-scheduler-explicit",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "session-1",
                "owner_session_generation_id": "generation-1",
            },
        )
    )

    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    _activate_owner_session(queue)

    def fake_teardown(**_kwargs: object) -> SessionLifecycleReport:
        return _verified_teardown_report()

    monkeypatch.setattr(
        "clio_relay.cli.status_remote_session",
        _fake_owned_session_status,
    )
    monkeypatch.setattr("clio_relay.cli.teardown_remote_session", fake_teardown)
    result = CliRunner().invoke(
        app,
        [
            "session",
            "teardown",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--cancel-jobs",
            "--cancel-scheduler-jobs",
        ],
    )

    assert result.exit_code == 0
    assert json.loads(result.output)["relay_jobs"]["scheduler_cancel_requested"] is True
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    cancel_events = [event for event in events if event.event_type == "job.cancel_requested"]
    # Session cleanup has one scheduler cancellation path below; relay queue
    # cancellation never races it with a worker-side provider call.
    assert cancel_events[-1].payload["cancel_scheduler"] is False


def test_cli_session_scheduler_cancellation_is_owned_and_canonical(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, scheduler_provider="slurm")
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    owned = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="owned-scheduler-cancel",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "session-1",
                "owner_session_generation_id": "generation-1",
            },
        )
    )
    task = queue.append_task(RelayTask(job_id=owned.job_id, name="jarvis.execution"))
    queue.update_task_metadata(
        task.task_id,
        {
            "scheduler": "slurm",
            "runtime_metadata_source": "jarvis_sidecar",
            "scheduler_job_ids": ["validation-123"],
            "scheduler_job_ownership": [
                {
                    "scheduler_job_id": "validation-123",
                    "scheduler_provider": "slurm",
                    "relay_job_id": owned.job_id,
                    "task_id": task.task_id,
                    "execution_id": "execution-validation-123",
                    "runtime_metadata_source": "jarvis_sidecar",
                    "ownership_verified": True,
                    "proof": "authenticated_runtime_sidecar",
                }
            ],
            "scheduler_status": {
                "scheduler": "slurm",
                "scheduler_job_id": "validation-123",
                "phase": "running",
            },
        },
    )
    unrelated = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="unrelated-scheduler-job",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "session-2",
                "owner_session_generation_id": "generation-2",
            },
        )
    )
    canceled_scheduler_ids: list[str] = []
    sentinel_poll_counts = {
        "unrelated-sentinel-1": 0,
        "unrelated-sentinel-2": 0,
    }

    class ConfirmingScheduler:
        name = "slurm"

        def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
            canceled_scheduler_ids.append(scheduler_job_id)
            return subprocess.CompletedProcess(["scancel", scheduler_job_id], 0, "", "")

        def poll(self, scheduler_job_id: str) -> SchedulerStatus:
            if scheduler_job_id == "unrelated-sentinel-1":
                sentinel_poll_counts[scheduler_job_id] += 1
                phase = SchedulerPhase.PENDING
            elif scheduler_job_id == "unrelated-sentinel-2":
                sentinel_poll_counts[scheduler_job_id] += 1
                phase = (
                    SchedulerPhase.RUNNING
                    if sentinel_poll_counts[scheduler_job_id] == 1
                    else SchedulerPhase.COMPLETED
                )
            else:
                phase = (
                    SchedulerPhase.CANCELED
                    if scheduler_job_id in canceled_scheduler_ids
                    else SchedulerPhase.RUNNING
                )
            return SchedulerStatus(
                scheduler="slurm",
                scheduler_job_id=scheduler_job_id,
                phase=phase,
            )

    def fake_teardown(**_kwargs: object) -> SessionLifecycleReport:
        return SessionLifecycleReport(
            cluster="ares",
            session_id="session-1",
            session_generation_id="generation-1",
            mode="teardown",
            prior_session_status=RemoteSessionStateEvidence(
                api_pid=123,
                session_generation_id="generation-1",
                process_start_marker="start-123",
                running=True,
                ownership_verified=True,
                observed_at=datetime.now(UTC),
                started_at=datetime.now(UTC),
            ),
            post_session_status=RemoteSessionStateEvidence(
                api_pid=123,
                session_generation_id="generation-1",
                process_start_marker="start-123",
                running=False,
                ownership_verified=True,
                observed_at=datetime.now(UTC),
                started_at=datetime.now(UTC),
            ),
            resources=[
                CleanupResource(
                    kind="remote_relay_api",
                    resource_id="123",
                    location="ares",
                    action="stop",
                    ownership_verified=True,
                    outcome="stopped",
                    verified_after_operation=True,
                )
            ],
        )

    report_path = tmp_path / "owned-cancel-report.json"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    _activate_owner_session(queue)
    monkeypatch.setattr(
        "clio_relay.cli.status_remote_session",
        _fake_owned_session_status,
    )
    monkeypatch.setattr("clio_relay.cli.teardown_remote_session", fake_teardown)

    def confirming_provider(_provider: str) -> ConfirmingScheduler:
        return ConfirmingScheduler()

    monkeypatch.setattr(
        "clio_relay.cli.provider_for_scheduler",
        confirming_provider,
    )

    result = CliRunner().invoke(
        app,
        [
            "session",
            "teardown",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--cancel-jobs",
            "--cancel-scheduler-jobs",
            "--preserve-scheduler-job-id",
            "unrelated-sentinel-1",
            "--preserve-scheduler-job-id",
            "unrelated-sentinel-2",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert canceled_scheduler_ids == ["validation-123"]
    refreshed = ClioCoreQueue(core_dir)
    assert refreshed.get_job(owned.job_id).state is JobState.CANCELED
    assert refreshed.get_job(unrelated.job_id).state is JobState.QUEUED
    canonical = json.loads(report_path.read_text(encoding="utf-8"))
    checks = {check["check_id"]: check["status"] for check in canonical["checks"]}
    assert checks["cleanup.explicit-job-cancel"] == "passed"
    scheduler_resources = [
        resource for resource in canonical["resources"] if resource["kind"] == "scheduler_job"
    ]
    by_scheduler_id = {resource["resource_id"]: resource for resource in scheduler_resources}
    assert by_scheduler_id["validation-123"]["provider"] == "slurm"
    assert by_scheduler_id["validation-123"]["state"] == "canceled"
    assert by_scheduler_id["unrelated-sentinel-1"]["role"] == "scheduler_sentinel:retain"
    assert by_scheduler_id["unrelated-sentinel-1"]["state"] == "retained"
    assert by_scheduler_id["unrelated-sentinel-1"]["metadata"] == {
        "ownership_verified": False,
        "cleanup_kind": "scheduler_sentinel",
        "provider": "slurm",
        "verified_after_operation": True,
        "observed_state": "pending",
        "residual": False,
        "detail": "unrelated scheduler sentinel remained active after owned cancellation",
        "unowned_sentinel": True,
        "active_before_operation": True,
        "preservation_verified": True,
        "pre_phase": "pending",
        "post_phase": "pending",
    }
    assert by_scheduler_id["unrelated-sentinel-2"]["state"] == "retained"
    assert by_scheduler_id["unrelated-sentinel-2"]["metadata"]["pre_phase"] == "running"
    assert by_scheduler_id["unrelated-sentinel-2"]["metadata"]["post_phase"] == "completed"
    assert sentinel_poll_counts == {
        "unrelated-sentinel-1": 2,
        "unrelated-sentinel-2": 2,
    }


def test_scheduler_sentinel_conflict_fails_before_provider_poll(
    monkeypatch: MonkeyPatch,
) -> None:
    provider_calls: list[str] = []

    class ForbiddenScheduler:
        name = "slurm"

        def poll(self, scheduler_job_id: str) -> SchedulerStatus:
            provider_calls.append(scheduler_job_id)
            raise AssertionError("conflicting sentinel must fail before scheduler polling")

    def forbidden_provider(_provider: str | None) -> ForbiddenScheduler:
        return ForbiddenScheduler()

    monkeypatch.setattr(cli, "provider_for_scheduler", forbidden_provider)
    jobs = [
        cli._OwnedRelayJob(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            job_id="relay-owned",
            relay_state=JobState.RUNNING,
            scheduler_job_ids=("owned-123",),
            scheduler_provider="slurm",
            owner_session_generation_id="generation-1",
            unowned_scheduler_job_ids=("untrusted-456",),
        )
    ]

    for conflicting_id in ("owned-123", "untrusted-456"):
        with pytest.raises(RelayError, match="no scheduler cancellation was attempted"):
            cli._preflight_scheduler_sentinels(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                ClusterDefinition(
                    name="ares",
                    ssh_host="ares",
                    scheduler_provider="slurm",
                ),
                (conflicting_id,),
                jobs,
            )

    assert provider_calls == []


def test_gateway_scheduler_sentinel_conflict_fails_before_any_destructive_cleanup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, scheduler_provider="slurm")
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    queue = ClioCoreQueue(core_dir)
    _activate_owner_session(queue)
    gateway = queue.create_gateway_session(
        GatewaySession(
            cluster="ares",
            name="owned-gateway",
            state=GatewaySessionState.READY,
            scheduler="slurm",
            scheduler_job_id="sentinel-123",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "session-1",
                "owner_session_generation_id": "generation-1",
            },
        )
    )
    destructive_calls: list[str] = []

    def forbidden_provider(_provider: str | None) -> SchedulerProvider:
        destructive_calls.append("provider")
        raise AssertionError("gateway sentinel conflict must fail before provider access")

    def forbidden_teardown(**_kwargs: object) -> SessionLifecycleReport:
        destructive_calls.append("api")
        raise AssertionError("gateway sentinel conflict must fail before API teardown")

    def forbidden_stop(
        _self: ServiceRuntimeSupervisor,
        **_kwargs: object,
    ) -> object:
        destructive_calls.append("gateway")
        raise AssertionError("gateway sentinel conflict must fail before gateway stop")

    monkeypatch.setattr(cli, "status_remote_session", _fake_owned_session_status)
    monkeypatch.setattr(cli, "provider_for_scheduler", forbidden_provider)
    monkeypatch.setattr(cli, "teardown_remote_session", forbidden_teardown)
    monkeypatch.setattr(ServiceRuntimeSupervisor, "stop", forbidden_stop)
    report_path = tmp_path / "gateway-sentinel-conflict.json"
    result = CliRunner().invoke(
        app,
        [
            "session",
            "teardown",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--cancel-jobs",
            "--cancel-scheduler-jobs",
            "--preserve-scheduler-job-id",
            "sentinel-123",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    assert "no scheduler cancellation was attempted" in result.output
    assert destructive_calls == []
    assert queue.get_gateway_session(gateway.session_id).state is GatewaySessionState.READY
    assert queue.owner_session_is_closing("session-1") is True
    local_admission_id = cli._desktop_owner_session_admission_id(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster="ares",
        session_id="session-1",
    )
    assert queue.owner_session_is_closing(local_admission_id) is True
    assert (
        queue.get_owner_session_closed(
            "session-1",
            session_generation_id="generation-1",
        )
        is None
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["cleanup"]["cancel_scheduler_jobs"] is True
    assert report["cleanup"]["operation_id"].startswith("cleanup_")


@pytest.mark.parametrize(
    "post_phase",
    [SchedulerPhase.CANCELED, SchedulerPhase.FAILED, SchedulerPhase.UNKNOWN],
)
def test_scheduler_sentinel_rejects_unsafe_post_cancel_phase(
    monkeypatch: MonkeyPatch,
    post_phase: SchedulerPhase,
) -> None:
    class UnsafeScheduler:
        name = "slurm"

        def poll(self, scheduler_job_id: str) -> SchedulerStatus:
            return SchedulerStatus(
                scheduler="slurm",
                scheduler_job_id=scheduler_job_id,
                phase=post_phase,
            )

    def unsafe_provider(_provider: str | None) -> UnsafeScheduler:
        return UnsafeScheduler()

    monkeypatch.setattr(cli, "provider_for_scheduler", unsafe_provider)
    resources, errors = cli._scheduler_sentinel_preservation_resources(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        ClusterDefinition(name="ares", ssh_host="ares", scheduler_provider="slurm"),
        {"unrelated-sentinel": "running"},
    )

    assert len(resources) == 1
    assert resources[0].kind == "scheduler_sentinel"
    assert resources[0].outcome == "failed"
    assert resources[0].verified_after_operation is False
    assert resources[0].residual is True
    assert resources[0].metadata == {
        "unowned_sentinel": True,
        "active_before_operation": True,
        "preservation_verified": False,
        "pre_phase": "running",
        "post_phase": post_phase.value,
    }
    assert errors


def test_cli_session_cancels_owned_scheduler_after_relay_job_is_terminal(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, scheduler_provider="slurm")
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(server="jarvis-mcp", tool="jarvis_run"),
            idempotency_key="terminal-relay-owned-scheduler",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "session-1",
                "owner_session_generation_id": "generation-1",
            },
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="mcp.execution"))
    queue.update_task_metadata(
        task.task_id,
        {
            "scheduler": "slurm",
            "runtime_metadata_source": "jarvis_mcp",
            "scheduler_job_ids": ["validation-789"],
            "scheduler_job_ownership": [
                {
                    "scheduler_job_id": "validation-789",
                    "scheduler_provider": "slurm",
                    "relay_job_id": job.job_id,
                    "task_id": task.task_id,
                    "execution_id": "execution-validation-789",
                    "runtime_metadata_source": "jarvis_mcp",
                    "ownership_verified": True,
                    "proof": "owned_jarvis_run_mcp_result",
                }
            ],
        },
    )
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)
    canceled_scheduler_ids: list[str] = []

    class ConfirmingScheduler:
        name = "slurm"

        def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
            canceled_scheduler_ids.append(scheduler_job_id)
            return subprocess.CompletedProcess(["scancel", scheduler_job_id], 0, "", "")

        def poll(self, scheduler_job_id: str) -> SchedulerStatus:
            return SchedulerStatus(
                scheduler="slurm",
                scheduler_job_id=scheduler_job_id,
                phase=SchedulerPhase.CANCELED,
            )

    def fake_teardown(**_kwargs: object) -> SessionLifecycleReport:
        return _verified_teardown_report()

    def fake_provider_for_scheduler(_provider: str | None) -> ConfirmingScheduler:
        return ConfirmingScheduler()

    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    _activate_owner_session(queue)
    monkeypatch.setattr(
        "clio_relay.cli.status_remote_session",
        _fake_owned_session_status,
    )
    monkeypatch.setattr(
        "clio_relay.cli.teardown_remote_session",
        fake_teardown,
    )
    monkeypatch.setattr(
        "clio_relay.cli.provider_for_scheduler",
        fake_provider_for_scheduler,
    )

    result = CliRunner().invoke(
        app,
        [
            "session",
            "teardown",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--cancel-jobs",
            "--cancel-scheduler-jobs",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["relay_jobs"]["canceled_job_ids"] == []
    assert ClioCoreQueue(core_dir).get_job(job.job_id).state is JobState.SUCCEEDED
    assert canceled_scheduler_ids == ["validation-789"]
    relay_resource = next(
        resource for resource in payload["resources"] if resource["kind"] == "relay_job"
    )
    assert relay_resource["outcome"] == "terminal"
    scheduler_resource = next(
        resource for resource in payload["resources"] if resource["kind"] == "scheduler_job"
    )
    assert scheduler_resource["outcome"] == "canceled"


def test_scheduler_natural_completion_during_cancel_allows_cleanup_without_false_claim(
    monkeypatch: MonkeyPatch,
) -> None:
    class CompletingScheduler:
        name = "slurm"

        def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(["scancel", scheduler_job_id], 0, "", "")

        def poll(self, scheduler_job_id: str) -> SchedulerStatus:
            return SchedulerStatus(
                scheduler="slurm",
                scheduler_job_id=scheduler_job_id,
                phase=SchedulerPhase.COMPLETED,
            )

    def completing_provider(_provider: str | None) -> CompletingScheduler:
        return CompletingScheduler()

    monkeypatch.setattr(cli, "provider_for_scheduler", completing_provider)
    scheduler_resource, error = cli._cancel_owned_scheduler_job(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        ClusterDefinition(name="local", ssh_host="localhost", scheduler_provider="slurm"),
        "scheduler-1",
        relay_job_id="relay-1",
        provider="slurm",
        timeout_seconds=0.1,
        poll_seconds=0.01,
    )
    report = _verified_teardown_report(
        cluster="local",
        resources=[
            *_verified_teardown_report(cluster="local").resources,
            CleanupResource(
                kind="relay_job",
                resource_id="relay-1",
                location="local",
                action="cancel",
                ownership_verified=True,
                outcome="canceled",
                verified_after_operation=True,
                metadata={"scheduler_job_ids": ["scheduler-1"]},
            ),
            scheduler_resource,
        ],
    )
    report.relay_cancel_requested = True
    report.scheduler_cancel_requested = True

    cli._verify_owner_session_teardown(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        report,
        session_id="session-1",
        session_generation_id="generation-1",
        stop_worker=False,
    )
    checks = {
        check.check_id: check.status.value for check in report.to_live_validation_report().checks
    }

    assert error is None
    assert scheduler_resource.action == "cancel"
    assert scheduler_resource.outcome == "terminal"
    assert scheduler_resource.observed_state == "completed"
    assert scheduler_resource.verified_after_operation is True
    assert scheduler_resource.residual is False
    assert checks["cleanup.explicit-job-cancel"] == "failed"


def test_owned_relay_job_refuses_scheduler_identity_without_bound_proof() -> None:
    job: dict[str, object] = {
        "job_id": "relay-job",
        "state": "succeeded",
        "metadata": {},
    }
    task: dict[str, object] = {
        "task_id": "relay-task",
        "metadata": {
            "scheduler": "slurm",
            "scheduler_job_ids": ["untrusted-123"],
            "runtime_metadata": {
                "scheduler_provider": "slurm",
                "scheduler_job_id": "untrusted-123",
            },
        },
    }

    owned = cli._owned_relay_job(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        job,
        [task],
        scheduler_provider="slurm",
    )

    assert owned.scheduler_job_ids == ()
    assert owned.unowned_scheduler_job_ids == ("untrusted-123",)
    resources = cli._owned_job_cleanup_resources(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        [owned],
        definition=ClusterDefinition(
            name="ares",
            ssh_host="ares",
            scheduler_provider="slurm",
        ),
        location="ares",
        cancel_jobs=True,
        cancel_scheduler_jobs=True,
    )
    refused = next(resource for resource in resources if resource.kind == "scheduler_job")
    assert refused.ownership_verified is False
    assert refused.outcome == "refused"
    assert refused.residual is True


def test_owner_session_teardown_keeps_missing_scheduler_job_without_residual(
    monkeypatch: MonkeyPatch,
) -> None:
    """Default teardown succeeds when an owned job naturally leaves the active queue."""

    owned = cli._OwnedRelayJob(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        job_id="relay-1",
        relay_state=JobState.SUCCEEDED,
        scheduler_job_ids=("21947",),
        scheduler_provider="slurm",
        owner_session_generation_id="generation-1",
    )

    def missing_phase(
        *_args: object,
        **_kwargs: object,
    ) -> tuple[str | None, str | None]:
        return "missing", None

    monkeypatch.setattr(
        cli,
        "_scheduler_phase_after_operation",
        missing_phase,
    )
    resources = cli._owned_job_cleanup_resources(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        [owned],
        definition=ClusterDefinition(
            name="ares",
            ssh_host="ares",
            scheduler_provider="slurm",
        ),
        location="ares",
        cancel_jobs=False,
        cancel_scheduler_jobs=False,
    )
    report = _verified_teardown_report(
        resources=[*_verified_teardown_report().resources, *resources]
    )

    cli._verify_owner_session_teardown(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        report,
        session_id="session-1",
        session_generation_id="generation-1",
        stop_worker=False,
    )

    scheduler = next(resource for resource in resources if resource.kind == "scheduler_job")
    checks = {
        check.check_id: check.status.value for check in report.to_live_validation_report().checks
    }
    assert scheduler.action == "retain"
    assert scheduler.outcome == "missing"
    assert scheduler.observed_state == "missing"
    assert scheduler.verified_after_operation is True
    assert scheduler.residual is False
    assert report.residual_resources == []
    assert checks["cleanup.jobs-preserved-default"] == "passed"


def test_cli_session_rejects_scheduler_cancel_without_relay_cancel(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, scheduler_provider="slurm")

    result = CliRunner().invoke(
        app,
        [
            "session",
            "teardown",
            "--cluster",
            "ares",
            "--session-id",
            "session-1",
            "--keep-jobs",
            "--cancel-scheduler-jobs",
        ],
    )

    assert result.exit_code == 2


def test_cli_session_rejects_scheduler_sentinel_without_both_cancel_flags(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, scheduler_provider="slurm")

    for flags in ([], ["--cancel-jobs"]):
        result = CliRunner().invoke(
            app,
            [
                "session",
                "teardown",
                "--cluster",
                "ares",
                "--session-id",
                "session-1",
                *flags,
                "--preserve-scheduler-job-id",
                "unrelated-sentinel",
                "--validation-report",
                str(tmp_path / f"sentinel-preflight-{len(flags)}.json"),
            ],
            color=False,
            terminal_width=200,
        )

        assert result.exit_code == 2
        output = unstyle(result.output)
        assert "--preserve-scheduler-job-id requires both --cancel-jobs" in output
        assert "--cancel-scheduler-jobs" in output


def test_remote_owned_job_discovery_never_cancels_unrelated_session(
    monkeypatch: MonkeyPatch,
) -> None:
    definition = ClusterDefinition(name="ares", ssh_host="ares", scheduler_provider="slurm")
    calls: list[list[str]] = []

    def fake_remote(_definition: ClusterDefinition, args: list[str]) -> str:
        calls.append(args)
        if args[:2] == ["queue", "owner-jobs"]:
            generation_selected = "--owner-session-generation-id" in args
            return json.dumps(
                {
                    "jobs": (
                        [
                            {
                                "job_id": "owned-job",
                                "state": "queued",
                                "metadata": {
                                    "owner": "clio-relay",
                                    "owner_session_id": "session-1",
                                    "owner_session_generation_id": "generation-1",
                                },
                            }
                        ]
                        if generation_selected
                        else []
                    ),
                    "source_cursor": None,
                    "source_limit": 500,
                    "source_next_cursor": None,
                    "source_total": 1 if generation_selected else 0,
                    "source_window_count": 1 if generation_selected else 0,
                }
            )
        if args == [
            "job",
            "tasks",
            "owned-job",
            "--cursor",
            "1",
            "--limit",
            "500",
        ]:
            return json.dumps(
                {
                    "tasks": [
                        {
                            "task_id": "owned-task",
                            "metadata": {
                                "scheduler": "slurm",
                                "runtime_metadata_source": "jarvis_mcp",
                                "scheduler_job_ids": ["validation-456"],
                                "scheduler_job_ownership": [
                                    {
                                        "scheduler_job_id": "validation-456",
                                        "scheduler_provider": "slurm",
                                        "relay_job_id": "owned-job",
                                        "task_id": "owned-task",
                                        "execution_id": "execution-validation-456",
                                        "runtime_metadata_source": "jarvis_mcp",
                                        "ownership_verified": True,
                                        "proof": "owned_jarvis_run_mcp_result",
                                    }
                                ],
                            },
                        }
                    ],
                    "cursor": 1,
                    "limit": 500,
                    "next_cursor": None,
                    "total": 1,
                }
            )
        if args[:3] == ["queue", "cancel", "owned-job"]:
            acknowledged_at = datetime.now(UTC).isoformat()
            return json.dumps(
                {
                    "cancellation_requested": True,
                    "job": {
                        "job_id": "owned-job",
                        "state": "canceled",
                        "metadata": {
                            "owner": "clio-relay",
                            "owner_session_id": "session-1",
                            "owner_session_generation_id": "generation-1",
                            "cancellation_request": {
                                "schema_version": "clio-relay.cancellation-request.v1",
                                "requested_at": acknowledged_at,
                                "previous_state": "queued",
                                "cancel_scheduler": False,
                                "acknowledged_at": acknowledged_at,
                                "cleanup_acknowledged": True,
                            },
                        },
                    },
                }
            )
        raise AssertionError(args)

    monkeypatch.setattr("clio_relay.cli.run_remote_clio", fake_remote)

    jobs = cli._list_remote_owned_active_cluster_jobs(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        definition,
        "ares",
        owner_session_id="session-1",
        owner_session_generation_id="generation-1",
    )
    canceled = cli._cancel_remote_owned_jobs(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        definition,
        "ares",
        jobs,
    )

    assert canceled == ["owned-job"]
    assert jobs[0].scheduler_job_ids == ("validation-456",)
    assert [
        "queue",
        "owner-jobs",
        "--cluster",
        "ares",
        "--owner-session-id",
        "session-1",
        "--limit",
        "500",
        "--owner-session-generation-id",
        "generation-1",
    ] in calls
    assert not any("unrelated-job" in command for command in calls)
    assert not any("newer-generation-job" in command for command in calls)


def test_remote_owner_session_discovery_refuses_truncated_legacy_coverage(
    monkeypatch: MonkeyPatch,
) -> None:
    def fake_remote(_definition: ClusterDefinition, _args: list[str]) -> str:
        return json.dumps(
            {
                "jobs": [],
                "source_cursor": 1,
                "source_limit": 500,
                "source_next_cursor": 501,
                "source_total": 10_001,
            }
        )

    monkeypatch.setattr(cli, "run_remote_clio", fake_remote)

    with pytest.raises(RelayError, match="bounded source limit"):
        cli._list_remote_owned_active_cluster_jobs(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            ClusterDefinition(name="ares", ssh_host="ares"),
            "ares",
            owner_session_id="session-1",
            owner_session_generation_id="generation-1",
            include_terminal=True,
        )


def test_owned_runtime_cleanup_scans_remote_gateway_core(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "desktop-core"))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    definition = ClusterDefinition(name="ares", ssh_host="ares")
    calls: list[list[str]] = []
    owned_gateway_state = ["ready"]

    def fake_remote(_definition: ClusterDefinition, args: list[str]) -> str:
        calls.append(args)
        if args[:2] == ["gateway", "list"]:
            return json.dumps(
                {
                    "gateway_sessions": [
                        {
                            "session_id": "owned-gateway",
                            "state": owned_gateway_state[0],
                            "metadata": {
                                "owner": "clio-relay",
                                "owner_session_id": "session-1",
                                "owner_session_generation_id": "generation-1",
                            },
                        },
                        {
                            "session_id": "newer-generation-gateway",
                            "state": "ready",
                            "metadata": {
                                "owner": "clio-relay",
                                "owner_session_id": "session-1",
                                "owner_session_generation_id": "generation-2",
                            },
                        },
                        {
                            "session_id": "unrelated-gateway",
                            "state": "ready",
                            "metadata": {
                                "owner": "clio-relay",
                                "owner_session_id": "session-2",
                            },
                        },
                    ],
                    "source_cursor": 1,
                    "source_limit": 500,
                    "source_next_cursor": None,
                    "source_total": 3,
                }
            )
        if args[:3] == ["gateway", "stop-runtime", "owned-gateway"]:
            owned_gateway_state[0] = "closed"
            return json.dumps(
                {
                    "resources": [
                        {
                            "kind": "gateway_record",
                            "resource_id": "owned-gateway",
                            "location": "ares",
                            "action": "close",
                            "ownership_verified": True,
                            "outcome": "closed",
                            "residual": False,
                        }
                    ],
                    "errors": [],
                }
            )
        raise AssertionError(args)

    monkeypatch.setattr("clio_relay.cli.run_remote_clio", fake_remote)

    reports = cli._cleanup_owned_runtime_sessions(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster="ares",
        definition=definition,
        owner_session_id="session-1",
        owner_session_generation_id="generation-1",
        mode="teardown",
        cancel_scheduler_jobs=False,
    )

    assert len(reports) == 1
    resources = reports[0]["resources"]
    assert isinstance(resources, list)
    first_resource = cast(object, resources[0])
    assert isinstance(first_resource, dict)
    assert first_resource["resource_id"] == "owned-gateway"
    assert not any("unrelated-gateway" in command for command in calls)
    assert not any("newer-generation-gateway" in command for command in calls)


def test_owned_runtime_cleanup_refuses_gateway_without_exact_generation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    gateway = GatewaySession(
        cluster="ares",
        name="legacy-ambiguous-gateway",
        state=GatewaySessionState.READY,
        metadata={
            "owner": "clio-relay",
            "owner_session_id": "session-1",
        },
    )

    def scan_ambiguous_gateways(
        _self: ClioCoreQueue,
        *,
        limit: int,
        cluster: str | None = None,
        state: GatewaySessionState | None = None,
    ) -> tuple[list[GatewaySession], bool]:
        del limit, cluster, state
        return [gateway], False

    monkeypatch.setattr(
        ClioCoreQueue,
        "scan_gateway_sessions",
        scan_ambiguous_gateways,
    )

    def forbidden_stop(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("ambiguous gateway ownership must not authorize cleanup")

    monkeypatch.setattr(ServiceRuntimeSupervisor, "stop", forbidden_stop)

    reports = cli._cleanup_owned_runtime_sessions(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster="ares",
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        owner_session_id="session-1",
        owner_session_generation_id="generation-1",
        mode="teardown",
        cancel_scheduler_jobs=False,
    )

    assert len(reports) == 1
    assert reports[0]["ok"] is False
    resources = cast(list[dict[str, object]], reports[0]["residual_resources"])
    assert len(resources) == 1
    assert resources[0]["resource_id"] == gateway.session_id
    assert resources[0]["action"] == "close"
    assert resources[0]["outcome"] == "refused"
    assert resources[0]["ownership_verified"] is False
    assert resources[0]["residual"] is True
    assert resources[0]["metadata"] == {
        "expected_owner_session_generation_id": "generation-1",
        "observed_owner_session_generation_id": None,
    }


def test_owned_runtime_cleanup_rescans_for_late_exact_generation_gateway(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "desktop-core"))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    definition = ClusterDefinition(name="ares", ssh_host="ares")
    states = {"gateway-1": "ready", "gateway-2": "hidden"}
    stop_calls: list[str] = []
    list_calls = 0

    def gateway_document(session_id: str, state: str) -> dict[str, object]:
        return {
            "session_id": session_id,
            "state": state,
            "metadata": {
                "owner": "clio-relay",
                "owner_session_id": "session-1",
                "owner_session_generation_id": "generation-1",
            },
        }

    def fake_remote(_definition: ClusterDefinition, args: list[str]) -> str:
        nonlocal list_calls
        if args[:2] == ["gateway", "list"]:
            list_calls += 1
            records = [gateway_document("gateway-1", states["gateway-1"])]
            if states["gateway-2"] != "hidden":
                records.append(gateway_document("gateway-2", states["gateway-2"]))
            return json.dumps(
                {
                    "gateway_sessions": records,
                    "source_cursor": 1,
                    "source_limit": 500,
                    "source_next_cursor": None,
                    "source_total": len(records),
                }
            )
        if args[:2] == ["gateway", "stop-runtime"]:
            session_id = args[2]
            stop_calls.append(session_id)
            states[session_id] = "closed"
            if session_id == "gateway-1":
                states["gateway-2"] = "ready"
            resource = CleanupResource(
                kind="gateway_record",
                resource_id=session_id,
                location="ares",
                action="close",
                ownership_verified=True,
                outcome="closed",
                verified_after_operation=True,
            )
            return json.dumps(
                {
                    "resources": [resource.model_dump(mode="json")],
                    "residual_resources": [],
                    "errors": [],
                    "ok": True,
                }
            )
        raise AssertionError(args)

    monkeypatch.setattr(cli, "run_remote_clio", fake_remote)
    reports = cli._cleanup_owned_runtime_sessions(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster="ares",
        definition=definition,
        owner_session_id="session-1",
        owner_session_generation_id="generation-1",
        mode="teardown",
        cancel_scheduler_jobs=False,
    )

    assert stop_calls == ["gateway-1", "gateway-2"]
    assert len(reports) == 2
    assert states == {"gateway-1": "closed", "gateway-2": "closed"}
    assert list_calls >= 5


def test_cli_render_frpc_uses_configured_secret_env(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "env-frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "env-stcp-secret")
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "relay-host",
            "render-frpc-config",
            "--cluster",
            "ares",
            "--local-port",
            "8848",
        ],
    )

    assert result.exit_code == 0
    assert 'auth.token = "env-frp-token"' in result.output
    assert 'secretKey = "env-stcp-secret"' in result.output


def test_cli_render_frpc_uses_local_secret_file(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    secret_dir = tmp_path / ".clio-relay"
    secret_dir.mkdir(exist_ok=True)
    (secret_dir / "secrets.json").write_text(
        json.dumps(
            {
                "CLIO_RELAY_FRP_TOKEN": "file-frp-token",
                "CLIO_RELAY_STCP_SECRET": "file-stcp-secret",
            }
        ),
        encoding="utf-8-sig",
    )
    monkeypatch.delenv("CLIO_RELAY_FRP_TOKEN", raising=False)
    monkeypatch.delenv("CLIO_RELAY_STCP_SECRET", raising=False)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "relay-host",
            "render-frpc-config",
            "--cluster",
            "ares",
            "--local-port",
            "8848",
        ],
    )

    assert result.exit_code == 0
    assert 'auth.token = "file-frp-token"' in result.output
    assert 'secretKey = "file-stcp-secret"' in result.output


def test_cli_secret_file_rejects_non_string_secret(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    secret_dir = tmp_path / ".clio-relay"
    secret_dir.mkdir()
    (secret_dir / "secrets.json").write_text(
        json.dumps({"CLIO_RELAY_FRP_TOKEN": 123}),
        encoding="utf-8",
    )
    monkeypatch.delenv("CLIO_RELAY_FRP_TOKEN", raising=False)
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["relay-host", "render-frps-config"],
    )

    assert result.exit_code == 1
    assert "non-empty string" in result.output


def test_cli_transport_reports_missing_configured_secret_env(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.delenv("CLIO_RELAY_FRP_TOKEN", raising=False)
    monkeypatch.delenv("CLIO_RELAY_STCP_SECRET", raising=False)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "relay-host",
            "render-frpc-config",
            "--cluster",
            "ares",
            "--local-port",
            "8848",
        ],
    )

    assert result.exit_code == 1
    assert "CLIO_RELAY_FRP_TOKEN" in result.output


def test_cli_transport_reports_missing_frp_server_addr(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, frp_server_addr="")
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret-key")

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "render-frpc-config",
            "--cluster",
            "ares",
            "--local-port",
            "8848",
        ],
    )

    assert result.exit_code == 1
    assert "frp server address is not configured" in result.output


def test_cli_direct_transport_is_strict_xtcp_by_default(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret-key")
    calls: list[dict[str, object]] = []

    def fake_direct_probe(**kwargs: object) -> list[str]:
        calls.append(kwargs)
        return ["direct_transport.result=xtcp", "transport.cleanup=passed"]

    monkeypatch.setattr("clio_relay.cli.run_frp_direct_http_probe", fake_direct_probe)

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-direct-transport",
            "--cluster",
            "ares",
            "--local-bind-port",
            "19000",
        ],
    )

    assert result.exit_code == 0
    assert calls[0]["allow_stcp_fallback"] is False


def test_cli_init_creates_empty_cluster_registry(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))

    result = CliRunner().invoke(app, ["init"])

    assert result.exit_code == 0
    assert "clusters=" in result.output
    assert "ares" not in result.output
    registry = ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json")
    assert registry.clusters == {}


def test_cli_init_threads_explicit_legacy_output_migration_authorization(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    observed: list[bool] = []

    def capture_authorization(
        _settings: object,
        *,
        migrate_legacy_output: bool = False,
    ) -> object:
        observed.append(migrate_legacy_output)
        return object()

    monkeypatch.setattr(cli, "storage_managed_queue", capture_authorization)

    result = CliRunner().invoke(app, ["init", "--migrate-legacy-output"])

    assert result.exit_code == 0
    assert observed == [True]


def test_cli_cluster_add_writes_explicit_definition(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "add",
            "--name",
            "delta",
            "--ssh-host",
            "delta-login",
            "--scheduler-provider",
            "slurm",
            "--spack-executable",
            "/opt/site/spack/bin/spack",
            "--target-hostname",
            "delta-login-1",
            "--target-hostname",
            "delta-login-1.example.edu",
            "--ssh-host-key-sha256",
            "SHA256:operator-pinned-fingerprint",
            "--scheduler-cluster-name",
            "delta",
            "--site-marker-sha256",
            "a" * 64,
            "--agent-adapter",
            "exec",
            "--agent-npm-package",
            "",
            "--agent-npm-bin",
            "clio",
            "--frp-server-addr",
            "relay.example.edu",
            "--frp-protocol",
            "tcp",
            "--frp-server-port",
            "7000",
        ],
    )

    assert result.exit_code == 0
    registry = ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json")
    definition = registry.require("delta")
    assert definition.ssh_host == "delta-login"
    assert definition.scheduler_provider == "slurm"
    assert definition.spack_executable == "/opt/site/spack/bin/spack"
    assert definition.target_identity == ClusterTargetIdentity(
        hostnames=["delta-login-1", "delta-login-1.example.edu"],
        ssh_host_key_sha256=["SHA256:operator-pinned-fingerprint"],
        scheduler_cluster_name="delta",
        site_marker_sha256="a" * 64,
    )
    assert definition.agent_adapter == "exec"
    assert definition.agent_npm_package is None
    assert definition.agent_npm_bin == "clio"
    assert definition.frp_transport.server_addr == "relay.example.edu"
    assert definition.frp_transport.protocol == "tcp"
    assert definition.frp_transport.server_port == 7000
    assert definition.frp_transport.direct.enabled is False
    assert definition.frp_transport.direct.fallback_order == ["frp_stcp", "queue"]


def test_cli_cluster_add_requires_hostname_and_host_key_pins_together(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "add",
            "--name",
            "delta",
            "--ssh-host",
            "delta-login",
            "--target-hostname",
            "delta-login-1",
        ],
        terminal_width=200,
    )

    assert result.exit_code == 2
    assert not (tmp_path / ".clio-relay" / "clusters.json").exists()


def test_cli_cluster_pin_target_preserves_every_unrelated_cluster_setting(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    registry_path = tmp_path / ".clio-relay" / "clusters.json"
    definition = ClusterDefinition.model_validate(
        {
            "name": "delta",
            "ssh_host": "delta-login",
            "bootstrap_profile": "site-profile",
            "core_dir": "/srv/clio/core",
            "spool_dir": "/scratch/clio/spool",
            "jarvis_bin": "/opt/jarvis/bin/jarvis",
            "spack_executable": "/opt/site/spack/bin/spack",
            "frpc_bin": "/opt/frp/frpc",
            "agent_bin": "/opt/agent/bin/agent",
            "agent_adapter": "exec",
            "agent_npm_package": "@example/agent",
            "agent_npm_bin": "example-agent",
            "agent_args": ["--profile", "science"],
            "scheduler_provider": "slurm",
            "remote_mcp_servers": {
                "spack": {
                    "command": "uvx",
                    "args": [
                        "--from",
                        "/opt/clio/clio_kit-2.3.1-py3-none-any.whl",
                        "clio-kit",
                        "mcp-server",
                        "spack",
                    ],
                    "namespace": "software",
                    "allow_tools": ["spack_find", "spack_install"],
                    "profiles": ["user"],
                }
            },
            "frp_transport": {
                "protocol": "tcp",
                "server_addr": "relay.example.edu",
                "server_port": 7000,
                "token_env": "SITE_FRP_TOKEN",
                "stcp_secret_env": "SITE_STCP_SECRET",
                "direct": {
                    "enabled": True,
                    "mode": "xtcp",
                    "fallback_order": ["xtcp", "frp_stcp", "queue"],
                    "probe_timeout_seconds": 14,
                },
            },
            "live_test": {
                "jarvis_yaml": "site/pipeline.yaml",
                "monitor_pattern": "iteration",
                "progress_pattern": "progress",
                "verify_transport": True,
                "transport_local_bind_port": 19001,
                "agent_prompt": "validate the site",
            },
            "target_identity": {
                "hostnames": ["old-login.example.edu"],
                "ssh_host_key_sha256": ["SHA256:old-key"],
            },
        }
    )
    ClusterRegistry(clusters={"delta": definition}).save(registry_path)
    expected_unrelated = definition.model_dump(mode="json")
    expected_unrelated.pop("target_identity")

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "pin-target",
            "--cluster",
            "delta",
            "--target-hostname",
            "delta-login-1",
            "--target-hostname",
            "delta-login-1.example.edu",
            "--ssh-host-key-sha256",
            "SHA256:new-key-a",
            "--ssh-host-key-sha256",
            "SHA256:new-key-b",
            "--scheduler-cluster-name",
            "delta-production",
            "--site-marker-sha256",
            "b" * 64,
        ],
    )

    assert result.exit_code == 0, result.output
    updated = ClusterRegistry.load(registry_path).require("delta")
    actual_unrelated = updated.model_dump(mode="json")
    actual_unrelated.pop("target_identity")
    assert actual_unrelated == expected_unrelated
    assert updated.target_identity == ClusterTargetIdentity(
        hostnames=["delta-login-1", "delta-login-1.example.edu"],
        ssh_host_key_sha256=["SHA256:new-key-a", "SHA256:new-key-b"],
        scheduler_cluster_name="delta-production",
        site_marker_sha256="b" * 64,
    )


def test_cli_cluster_pin_target_clear_is_exclusive_and_preserves_cluster_config(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    registry_path = tmp_path / ".clio-relay" / "clusters.json"
    definition = ClusterDefinition(
        name="frontier",
        ssh_host="frontier-login",
        agent_args=["--keep-this"],
        target_identity=ClusterTargetIdentity(
            hostnames=["frontier-login.example.edu"],
            ssh_host_key_sha256=["SHA256:old-key"],
        ),
    )
    ClusterRegistry(clusters={"frontier": definition}).save(registry_path)

    rejected = CliRunner().invoke(
        app,
        [
            "cluster",
            "pin-target",
            "--cluster",
            "frontier",
            "--clear",
            "--target-hostname",
            "unexpected.example.edu",
        ],
    )
    assert rejected.exit_code == 2
    assert ClusterRegistry.load(registry_path).require("frontier") == definition

    cleared = CliRunner().invoke(
        app,
        ["cluster", "pin-target", "--cluster", "frontier", "--clear"],
    )
    assert cleared.exit_code == 0, cleared.output
    updated = ClusterRegistry.load(registry_path).require("frontier")
    assert updated.target_identity is None
    assert updated.model_copy(update={"target_identity": definition.target_identity}) == definition


def test_ssh_host_key_fingerprints_prefers_all_configured_known_hosts_files(
    monkeypatch: MonkeyPatch,
) -> None:
    first_key_bytes = b"operator-pinned-host-key-a"
    second_key_bytes = b"operator-pinned-host-key-b"
    first_encoded = base64.b64encode(first_key_bytes).decode()
    second_encoded = base64.b64encode(second_key_bytes).decode()
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        assert capture_output is True
        assert text is True
        assert check is False
        assert timeout == 10
        commands.append(command)
        if command[:2] == ["ssh", "-G"]:
            return subprocess.CompletedProcess(
                command,
                0,
                "\n".join(
                    [
                        "hostname login.example.edu",
                        "port 2222",
                        "hostkeyalias physical-target",
                        (
                            'userknownhostsfile "C:\\Operator Files\\known hosts" '
                            "D:\\site\\known_hosts"
                        ),
                    ]
                ),
                "",
            )
        if command[-1] == "C:\\Operator Files\\known hosts":
            return subprocess.CompletedProcess(
                command,
                0,
                f"|1|salt|hashed ssh-ed25519 {first_encoded}\n",
                "",
            )
        if command[-1] == "D:\\site\\known_hosts":
            return subprocess.CompletedProcess(
                command,
                0,
                f"@cert-authority *.example.edu ssh-ed25519 {second_encoded}\n",
                "",
            )
        raise AssertionError(f"ssh-keyscan must not run when configured keys exist: {command}")

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    fingerprints = cli._ssh_host_key_fingerprints(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "operator-alias"
    )

    expected = {
        "SHA256:" + base64.b64encode(hashlib.sha256(key).digest()).decode().rstrip("=")
        for key in (first_key_bytes, second_key_bytes)
    }
    assert set(fingerprints) == expected
    assert commands == [
        ["ssh", "-G", "operator-alias"],
        [
            "ssh-keygen",
            "-F",
            "[physical-target]:2222",
            "-f",
            "C:\\Operator Files\\known hosts",
        ],
        [
            "ssh-keygen",
            "-F",
            "[physical-target]:2222",
            "-f",
            "D:\\site\\known_hosts",
        ],
    ]


def test_ssh_host_key_fingerprints_resolves_alias_and_falls_back_to_keyscan(
    monkeypatch: MonkeyPatch,
) -> None:
    key_bytes = b"operator-pinned-host-key"
    encoded_key = base64.b64encode(key_bytes).decode()
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        assert capture_output is True
        assert text is True
        assert check is False
        assert timeout in {10, 15}
        commands.append(command)
        if command[:2] == ["ssh", "-G"]:
            return subprocess.CompletedProcess(
                command,
                0,
                "hostname ares.example.edu\nport 2222\n",
                "",
            )
        return subprocess.CompletedProcess(
            command,
            0,
            f"ares.example.edu ssh-ed25519 {encoded_key}\n",
            "",
        )

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    fingerprints = cli._ssh_host_key_fingerprints("ares")  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    digest = base64.b64encode(hashlib.sha256(key_bytes).digest()).decode().rstrip("=")
    assert fingerprints == [f"SHA256:{digest}"]
    assert commands == [
        ["ssh", "-G", "ares"],
        ["ssh-keyscan", "-T", "10", "-p", "2222", "ares.example.edu"],
    ]


def test_ssh_host_key_fingerprints_bounds_known_hosts_and_scan_timeouts(
    monkeypatch: MonkeyPatch,
) -> None:
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        del capture_output, text, check
        commands.append(command)
        if command[:2] == ["ssh", "-G"]:
            return subprocess.CompletedProcess(
                command,
                0,
                "hostname generic.example.edu\nuserknownhostsfile /operator/known_hosts\n",
                "",
            )
        if command[0] == "ssh-keygen":
            raise subprocess.TimeoutExpired(command, timeout)
        raise subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    with pytest.raises(ConfigurationError, match="ssh-keygen timed out") as captured:
        cli._ssh_host_key_fingerprints(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            "generic-target"
        )

    assert "ssh-keyscan timed out" in str(captured.value)
    assert commands == [
        ["ssh", "-G", "generic-target"],
        [
            "ssh-keygen",
            "-F",
            "generic.example.edu",
            "-f",
            str(Path("/operator/known_hosts")),
        ],
        ["ssh-keyscan", "-T", "10", "-p", "22", "generic.example.edu"],
    ]


def test_ssh_host_key_fingerprint_parser_rejects_revoked_and_malformed_records() -> None:
    revoked = base64.b64encode(b"revoked-key").decode()
    trusted_key = b"trusted-ca-key"
    trusted = base64.b64encode(trusted_key).decode()

    fingerprints = cli._ssh_fingerprints_from_key_lines(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "\n".join(
            [
                f"@revoked host.example ssh-ed25519 {revoked}",
                "host.example ssh-ed25519 !!!not-base64!!!",
                f"@cert-authority *.example ssh-ed25519 {trusted}",
            ]
        )
    )

    expected = base64.b64encode(hashlib.sha256(trusted_key).digest()).decode().rstrip("=")
    assert fingerprints == {f"SHA256:{expected}"}


def test_endpoint_target_info_hashes_raw_machine_id_bytes(
    tmp_path: Path,
) -> None:
    machine_id = tmp_path / "machine-id"
    marker = b"production-site-id\n"
    machine_id.write_bytes(marker)

    observed = cli._physical_site_marker_sha256(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        machine_id
    )

    assert observed == hashlib.sha256(marker).hexdigest()
    assert observed != hashlib.sha256(marker.strip()).hexdigest()

    machine_id.write_bytes(b"\n")
    with pytest.raises(ConfigurationError, match="physical site marker is empty"):
        cli._physical_site_marker_sha256(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            machine_id
        )


def test_remote_worker_info_binds_worker_to_operator_pinned_physical_target(
    monkeypatch: MonkeyPatch,
) -> None:
    definition = ClusterDefinition(
        name="ares",
        ssh_host="ares",
        scheduler_provider="slurm",
        target_identity=ClusterTargetIdentity(
            hostnames=["ares-login-1.example.edu"],
            ssh_host_key_sha256=["SHA256:operator-pinned-fingerprint"],
            scheduler_cluster_name="ares",
            site_marker_sha256="a" * 64,
        ),
    )
    target_scheduler_provider = ["slurm"]

    def fake_run_remote_clio(
        configured: ClusterDefinition,
        arguments: list[str],
    ) -> str:
        assert configured is definition
        if arguments[1] == "worker-info":
            return json.dumps(
                {
                    "schema_version": "clio-relay.worker-runtime-info.v1",
                    "cluster": "ares",
                    "scheduler_provider": "slurm",
                }
            )
        assert arguments == [
            "endpoint",
            "target-info",
            "--scheduler-provider",
            "slurm",
        ]
        return json.dumps(
            {
                "schema_version": "clio-relay.cluster-target-info.v1",
                "hostname": "ares-login-1",
                "fqdn": "ares-login-1.example.edu",
                "site_marker_sha256": "a" * 64,
                "scheduler_provider": target_scheduler_provider[0],
                "scheduler_cluster_name": "ares",
            }
        )

    monkeypatch.setattr(cli, "run_remote_clio", fake_run_remote_clio)

    def fake_host_key_fingerprints(_host: str) -> list[str]:
        return ["SHA256:operator-pinned-fingerprint"]

    monkeypatch.setattr(cli, "_ssh_host_key_fingerprints", fake_host_key_fingerprints)

    info = cli._remote_worker_info(definition)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    target = cast(dict[str, object], info["target_identity"])
    assert target["verified"] is True
    assert target["scheduler_provider"] == "slurm"
    assert target["scheduler_cluster_name"] == "ares"
    assert target["ssh_host_key_sha256"] == ["SHA256:operator-pinned-fingerprint"]

    target_scheduler_provider[0] = "external"
    with pytest.raises(ConfigurationError, match="physical target scheduler provider"):
        cli._remote_worker_info(definition)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def test_remote_worker_info_uses_one_total_observation_deadline(
    monkeypatch: MonkeyPatch,
) -> None:
    definition = ClusterDefinition(
        name="ares",
        ssh_host="ares",
        scheduler_provider="slurm",
        target_identity=ClusterTargetIdentity(
            hostnames=["ares.example.test"],
            ssh_host_key_sha256=["SHA256:operator-pinned-fingerprint"],
            scheduler_cluster_name="ares",
            site_marker_sha256="a" * 64,
        ),
    )
    clock = iter((100.0, 101.0, 105.0))
    observed_timeouts: list[float] = []
    observed_deadlines: list[float | None] = []

    def fake_remote(_definition: ClusterDefinition, arguments: list[str]) -> str:
        if arguments[1] == "worker-info":
            return json.dumps({"scheduler_provider": "slurm"})
        return json.dumps(
            {
                "schema_version": "clio-relay.cluster-target-info.v1",
                "hostname": "ares.example.test",
                "fqdn": "ares.example.test",
                "site_marker_sha256": "a" * 64,
                "scheduler_provider": "slurm",
                "scheduler_cluster_name": "ares",
            }
        )

    def fake_timeout(timeout_seconds: float) -> object:
        observed_timeouts.append(timeout_seconds)
        return nullcontext()

    def fake_fingerprints(
        _ssh_host: str,
        *,
        deadline: float | None = None,
    ) -> list[str]:
        observed_deadlines.append(deadline)
        return ["SHA256:operator-pinned-fingerprint"]

    monkeypatch.setattr(cli, "monotonic", lambda: next(clock))
    monkeypatch.setattr(cli, "run_remote_clio", fake_remote)
    monkeypatch.setattr(cli, "remote_command_timeout", fake_timeout)
    monkeypatch.setattr(cli, "_ssh_host_key_fingerprints", fake_fingerprints)

    info = cli._remote_worker_info(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        definition,
        timeout_seconds=20,
    )

    assert info["scheduler_provider"] == "slurm"
    assert observed_timeouts == [19.0, 15.0]
    assert observed_deadlines == [120.0]


def test_cleanup_worker_observation_is_bounded_and_never_retried_after_cleanup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    definition = ClusterDefinition(name="ares", ssh_host="ares")
    observations: list[float | None] = []

    def timed_out_worker_info(
        _definition: ClusterDefinition,
        *,
        timeout_seconds: float | None = None,
    ) -> dict[str, object]:
        observations.append(timeout_seconds)
        raise RelayError("remote worker identity observation timed out")

    monkeypatch.setattr(cli, "_remote_worker_info", timed_out_worker_info)
    observed_info, observation_error = cli._observe_worker_before_cleanup(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        definition
    )
    report = new_live_validation_report(scenario="cleanup", cluster="ares")
    recorder = ValidationRecorder(report)
    with recorder.check("cleanup.relay-session", "remote API stopped") as evidence:
        evidence.append(EvidenceReference(kind="cleanup", excerpt="remote API stopped"))
    recorder.finish()
    report_path = tmp_path / "bounded-cleanup-report.json"

    with pytest.raises(RelayError, match="identity observation timed out"):
        cli._write_remote_verified_report(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            report,
            definition,
            report_path,
            observed_worker_info=observed_info,
            worker_observation_error=observation_error,
        )

    assert observations == [cli.REMOTE_CLEANUP_WORKER_INFO_TIMEOUT_SECONDS]
    saved = json.loads(report_path.read_text(encoding="utf-8"))
    assert saved["status"] == "failed"
    assert saved["completed_at"] is not None
    assert all(check["completed_at"] is not None for check in saved["checks"])
    assert saved["checks"][-1]["check_id"] == "worker.installation-info"


def test_cleanup_with_artifact_digest_keeps_worker_identity_verification_strict(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    report = new_live_validation_report(
        scenario="cleanup",
        cluster="ares",
        artifact_sha256="a" * 64,
    )
    recorder = ValidationRecorder(report)
    with recorder.check("cleanup.relay-session", "remote API stopped") as evidence:
        evidence.append(EvidenceReference(kind="cleanup", excerpt="remote API stopped"))
    recorder.finish()
    report_path = tmp_path / "strict-cleanup-report.json"

    def reject_worker_identity(
        _report: LiveValidationReport,
        _definition: ClusterDefinition,
        *,
        observed_worker_info: dict[str, object] | None = None,
    ) -> None:
        del observed_worker_info
        raise ConfigurationError("remote worker wheel SHA-256 does not match")

    monkeypatch.setattr(cli, "_attach_verified_remote_worker", reject_worker_identity)

    with pytest.raises(ConfigurationError, match="wheel SHA-256 does not match"):
        cli._write_cleanup_validation_report(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            report,
            ClusterDefinition(name="ares", ssh_host="ares"),
            report_path,
            observed_worker_info={"running": True},
        )

    saved = json.loads(report_path.read_text(encoding="utf-8"))
    assert saved["status"] == "failed"
    assert saved["install_source"]["artifact_sha256"] == "a" * 64
    assert saved["checks"][-1]["check_id"] == "worker.installation-info"


def test_cli_cluster_add_persists_direct_transport_optimization(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "add",
            "--name",
            "homelab",
            "--ssh-host",
            "homelab",
            "--direct-transport",
            "--direct-transport-mode",
            "xtcp",
            "--direct-transport-fallback",
            "xtcp,frp_stcp,queue",
        ],
    )

    assert result.exit_code == 0
    definition = ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json").require("homelab")
    assert definition.frp_transport.direct.enabled is True
    assert definition.frp_transport.direct.mode == "xtcp"
    assert definition.frp_transport.direct.fallback_order == ["xtcp", "frp_stcp", "queue"]
    assert definition.frp_transport.server_addr == ""


def test_cli_cluster_add_rejects_direct_transport_without_queue_fallback(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "add",
            "--name",
            "homelab",
            "--ssh-host",
            "homelab",
            "--direct-transport",
            "--direct-transport-fallback",
            "xtcp,frp_stcp",
        ],
    )

    assert result.exit_code != 0
    assert "fallback_order must end with queue" in result.output


def test_cli_cluster_install_app_uses_explicit_app_installer(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, name="delta")
    calls: list[tuple[str, str]] = []

    def fake_install_cluster_app_over_ssh(*, ssh_host: str, app_name: str) -> list[str]:
        calls.append((ssh_host, app_name))
        return ["site_stack=ready"]

    monkeypatch.setattr(cli, "install_cluster_app_over_ssh", fake_install_cluster_app_over_ssh)

    result = CliRunner().invoke(
        app,
        ["cluster", "install-app", "--cluster", "delta", "--app", "site-stack"],
    )

    assert result.exit_code == 0
    assert calls == [("delta", "site-stack")]
    assert "site_stack=ready" in result.output


def test_cli_endpoint_service_requires_persistence_unless_explicitly_opted_out(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The operator-facing install defaults to persistent and names the diagnostic escape."""
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, name="delta")
    persistence_requests: list[bool] = []

    def fake_install_endpoint_user_service_over_ssh(
        *,
        cluster: str,
        ssh_host: str,
        service_text: str,
        start: bool,
        enable: bool,
        require_persistent: bool,
        timeout_seconds: float = 120.0,
    ) -> list[str]:
        del service_text, timeout_seconds
        assert cluster == "delta"
        assert ssh_host == "delta"
        assert start is True
        assert enable is True
        persistence_requests.append(require_persistent)
        return [
            "endpoint_service.persistence="
            + ("systemd-user-linger" if require_persistent else "login-scoped")
        ]

    monkeypatch.setattr(
        cli,
        "install_endpoint_user_service_over_ssh",
        fake_install_endpoint_user_service_over_ssh,
    )

    persistent = CliRunner().invoke(
        app,
        ["cluster", "install-endpoint-service", "--cluster", "delta"],
    )
    login_scoped = CliRunner().invoke(
        app,
        [
            "cluster",
            "install-endpoint-service",
            "--cluster",
            "delta",
            "--allow-login-scoped",
        ],
    )

    assert persistent.exit_code == 0, persistent.output
    assert login_scoped.exit_code == 0, login_scoped.output
    assert persistence_requests == [True, False]
    assert "endpoint_service.persistence=systemd-user-linger" in persistent.output
    assert "endpoint_service.persistence=login-scoped" in login_scoped.output


def test_cli_cluster_install_app_rejects_option_like_ssh_override(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, name="delta")

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "install-app",
            "--cluster",
            "delta",
            "--app",
            "site-stack",
            "--ssh-host=-oProxyCommand=malicious-command",
        ],
    )

    assert result.exit_code == 1
    assert "ssh host must be one non-option destination" in result.output


def test_cli_mcp_call_preserves_arguments(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "mcp-call",
            "--cluster",
            "ares",
            "--server",
            "remote-server",
            "--server-arg",
            "--stdio",
            "--env-from",
            "SCIENCE_TOKEN=SITE_SCIENCE_TOKEN",
            "--tool",
            "simulate",
            "--arguments-json",
            '{"steps": 100, "case": "site-simulation"}',
            "--timeout-seconds",
            "90",
            "--idempotency-key",
            "cli-mcp-call-args",
        ],
    )

    assert result.exit_code == 0
    job_id = result.output.strip()
    job = ClioCoreQueue(core_dir).get_job(job_id)
    assert job.kind == JobKind.MCP_CALL
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.server == "remote-server"
    assert job.spec.server_args == ["--stdio"]
    assert job.spec.env_from == {"SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"}
    assert job.spec.arguments == {"steps": 100, "case": "site-simulation"}
    assert job.spec.timeout_seconds == 90


def test_cli_jarvis_mcp_call_uses_builtin_cluster_command(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    monkeypatch.setenv("JARVIS_MCP_SPACK_COMMAND", "/opt/site/spack/bin/spack")

    result = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-call",
            "--cluster",
            "ares",
            "--tool",
            "jarvis_describe",
            "--arguments-json",
            '{"target":"packages"}',
            "--idempotency-key",
            "cli-jarvis-mcp",
        ],
    )

    assert result.exit_code == 0
    job = ClioCoreQueue(core_dir).get_job(result.output.strip())
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.server == "clio-kit"
    assert job.spec.server_args == ["mcp-server", "jarvis"]
    assert job.spec.env_from == {"JARVIS_MCP_SPACK_COMMAND": "JARVIS_MCP_SPACK_COMMAND"}
    assert job.spec.tool == "jarvis_describe"
    assert job.spec.expected_jarvis_cd_lock_binding == jarvis_cd_lock_binding_expectation()
    assert job.spec.arguments == {"target": "packages"}


def test_jarvis_package_search_query_uses_bounded_virtual_call(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []
    session = SimpleNamespace(
        tools_list_response={"result": {"tools": []}},
        tools_call_response={"result": {"job_id": "job-search"}},
        initialize_response={"result": {"protocolVersion": "2024-11-05"}},
        evidence=lambda: {"transport": "stdio"},
    )

    def run_session(**kwargs: object) -> SimpleNamespace:
        calls.append(dict(kwargs))
        return session

    def response_job_id(_response: object) -> str:
        return "job-search"

    def execute_locally(_definition: ClusterDefinition) -> bool:
        return False

    def wait_for_local_terminal(
        _queue: ClioCoreQueue,
        _job_id: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> dict[str, object]:
        assert timeout_seconds == 30
        assert poll_seconds == 0.1
        return {"job": {"job_id": "job-search"}, "terminal": True}

    def complete_artifacts(
        _queue: ClioCoreQueue,
        _job_id: str,
    ) -> list[dict[str, object]]:
        return artifacts

    monkeypatch.setattr(cli, "run_packaged_mcp_stdio_session", run_session)
    monkeypatch.setattr(cli, "_mcp_response_job_id", response_job_id)
    monkeypatch.setattr(cli, "should_execute_on_cluster", execute_locally)
    monkeypatch.setattr(
        cli,
        "_wait_for_local_job_terminal",
        wait_for_local_terminal,
    )
    artifacts: list[dict[str, object]] = [
        {"artifact_id": "artifact-result", "kind": "mcp_result"},
        {"artifact_id": "artifact-provenance", "kind": "provenance"},
    ]
    monkeypatch.setattr(cli, "_complete_local_artifact_records", complete_artifacts)

    def read_artifact(
        _queue: ClioCoreQueue,
        _artifacts: list[dict[str, object]],
        *,
        kind: str,
    ) -> dict[str, object]:
        return {"kind": kind}

    monkeypatch.setattr(cli, "_read_local_json_artifact_kind", read_artifact)

    result = cli._run_jarvis_package_search_query(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster="ares",
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        queue=ClioCoreQueue(tmp_path / "core"),
        profile="user",
        query="parallel visualization",
        wait_timeout_seconds=30,
        poll_seconds=0.1,
    )

    assert calls == [
        {
            "profile": "user",
            "tool": "jarvis_describe",
            "arguments": {
                "cluster": "ares",
                "target": "package_search",
                "query": "parallel visualization",
                "page_size": 5,
            },
        }
    ]
    assert result.call_job_id == "job-search"
    assert result.artifacts == artifacts
    assert result.mcp_result == {"kind": "mcp_result"}
    assert result.provenance == {"kind": "provenance"}


def test_jarvis_discovery_persists_exact_durable_artifact_bytes(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Keep the discovery hash bound to stored bytes, not a JSON reserialization."""
    monkeypatch.chdir(tmp_path)
    contract = jarvis_user_contract()
    server_artifact: dict[str, object] = {
        "verified": True,
        "server_process_artifact_verified": True,
        "install_source": "uv-tool",
        "install_artifact_sha256": CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
        "executable": {
            "path": "/opt/clio-kit/bin/clio-kit",
            "sha256": "a" * 64,
        },
        "python_distribution_runtime": {
            "distribution": "clio-kit",
            "distribution_version": CLIO_KIT_JARVIS_MCP_VERSION,
            "entry_point": "clio-kit",
            "runtime_closure_verified": True,
        },
        "nested_runtime": _verified_jarvis_nested_runtime(),
    }
    result: dict[str, object] = {
        "server": "clio-kit",
        "server_args": ["mcp-server", "jarvis"],
        "env_from": {},
        "operation": "tools/list",
        "tool": None,
        "arguments": {},
        "protocol_result": {
            "tools": [
                {
                    "name": name,
                    "description": definition["description"],
                    "inputSchema": definition["inputSchema"],
                    "outputSchema": definition["outputSchema"],
                    "annotations": definition["annotations"],
                }
                for name, definition in contract.items()
            ]
        },
        "structured_result": None,
        "protocol_version": "2024-11-05",
        "server_info": {"name": "clio-kit", "version": "2.5.0"},
        "server_artifact": server_artifact,
        "expected_jarvis_cd_lock_binding": jarvis_cd_lock_binding_expectation(),
        "returncode": 0,
        "stdout": "",
        "stderr": "",
        "timed_out": False,
        "protocol_error": None,
    }
    artifact_payload = (json.dumps(result, indent=2) + "\n").encode()
    compact_payload = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
    artifact_sha256 = hashlib.sha256(artifact_payload).hexdigest()

    assert artifact_payload != compact_payload

    entry, binding = cli._persist_jarvis_remote_contract_discovery(  # pyright: ignore[reportPrivateUsage]
        cluster="ares",
        discovery_job_id="job_discovery",
        result=result,
        artifacts=[
            {
                "artifact_id": "artifact_discovery",
                "kind": "mcp_result",
                "sha256": artifact_sha256,
            }
        ],
        artifact_payload=artifact_payload,
    )

    assert entry.schema_digest == CLIO_KIT_JARVIS_USER_CONTRACT_SHA256
    assert entry.provenance.artifact_sha256 == artifact_sha256
    assert binding

    unmarked = dict(result)
    unmarked.pop("expected_jarvis_cd_lock_binding")
    unmarked_payload = (json.dumps(unmarked, indent=2) + "\n").encode()
    with pytest.raises(RelayError, match="did not enforce the relay JARVIS-CD lock pin"):
        cli._persist_jarvis_remote_contract_discovery(  # pyright: ignore[reportPrivateUsage]
            cluster="ares",
            discovery_job_id="job_unmarked",
            result=unmarked,
            artifacts=[
                {
                    "artifact_id": "artifact_unmarked",
                    "kind": "mcp_result",
                    "sha256": hashlib.sha256(unmarked_payload).hexdigest(),
                }
            ],
            artifact_payload=unmarked_payload,
        )

    mismatched_payload = unmarked_payload
    with pytest.raises(RelayError, match="did not match its durable mcp_result artifact"):
        cli._persist_jarvis_remote_contract_discovery(  # pyright: ignore[reportPrivateUsage]
            cluster="ares",
            discovery_job_id="job_mismatched_payload",
            result=result,
            artifacts=[
                {
                    "artifact_id": "artifact_mismatched_payload",
                    "kind": "mcp_result",
                    "sha256": hashlib.sha256(mismatched_payload).hexdigest(),
                }
            ],
            artifact_payload=mismatched_payload,
        )

    stale = entry.model_copy(deep=True)
    nested = cast(
        dict[str, object],
        stale.provenance.server_artifact["nested_runtime"],
    )
    nested.pop("jarvis_cd_lock_binding")
    with pytest.raises(ValueError, match="run jarvis-mcp-refresh"):
        cli.jarvis_mcp_artifact_binding_from_entry(stale)

    wrong_outer_version = entry.model_copy(deep=True)
    python_runtime = cast(
        dict[str, object],
        wrong_outer_version.provenance.server_artifact["python_distribution_runtime"],
    )
    python_runtime["distribution_version"] = "0.0.0"
    with pytest.raises(ValueError, match="run jarvis-mcp-refresh"):
        cli.jarvis_mcp_artifact_binding_from_entry(wrong_outer_version)

    wrong_outer_hash = entry.model_copy(deep=True)
    wrong_outer_hash.provenance.server_artifact["install_artifact_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="run jarvis-mcp-refresh"):
        cli.jarvis_mcp_artifact_binding_from_entry(wrong_outer_hash)


def test_jarvis_discovery_rejects_ambiguous_mcp_result_artifacts(
    monkeypatch: MonkeyPatch,
) -> None:
    """A retry cannot make an earlier MCP result the implicit discovery authority."""
    definition = ClusterDefinition(name="configured-target", ssh_host="cluster.example")

    def duplicate_results(
        _definition: ClusterDefinition,
        _job_id: str,
    ) -> list[dict[str, object]]:
        return [
            {"artifact_id": "artifact-first", "kind": "mcp_result"},
            {"artifact_id": "artifact-retry", "kind": "mcp_result"},
        ]

    monkeypatch.setattr(cli, "_remote_artifact_records", duplicate_results)

    with pytest.raises(RelayError, match="durable artifact authority is ambiguous"):
        cli._read_remote_mcp_result_artifact(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            definition,
            "job-retried-discovery",
        )


def test_local_jarvis_discovery_rejects_ambiguous_mcp_result_artifacts(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Local mode applies the same unique durable authority as remote mode."""
    queue = ClioCoreQueue(tmp_path / "core")

    def duplicate_results(
        _queue: ClioCoreQueue,
        _job_id: str,
    ) -> list[dict[str, object]]:
        return [
            {"artifact_id": "artifact-first", "kind": "mcp_result"},
            {"artifact_id": "artifact-retry", "kind": "mcp_result"},
        ]

    monkeypatch.setattr(cli, "_complete_local_artifact_records", duplicate_results)

    with pytest.raises(RelayError, match="durable artifact authority is ambiguous"):
        cli._read_local_mcp_result_artifact(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue,
            "job-retried-local-discovery",
        )


def test_cli_remote_jarvis_call_defers_artifact_selection_to_target(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    _write_test_cluster(tmp_path)
    writes: list[tuple[str, bytes]] = []
    removals: list[tuple[str, bool]] = []
    commands: list[list[str]] = []

    def write_remote(_definition: ClusterDefinition, path: str, data: bytes) -> None:
        writes.append((path, data))

    def fail_local_resolution() -> str:
        raise AssertionError("desktop resolved JARVIS artifact")

    monkeypatch.setattr(
        "clio_relay.cli.write_remote_file",
        write_remote,
    )

    def remove_remote(
        _definition: ClusterDefinition,
        path: str,
        *,
        remove_empty_parent: bool,
    ) -> None:
        removals.append((path, remove_empty_parent))

    monkeypatch.setattr("clio_relay.cli.remove_remote_file", remove_remote)

    def run_remote(_definition: ClusterDefinition, args: list[str]) -> str:
        commands.append(args)
        return "job_remote_jarvis\n"

    monkeypatch.setattr("clio_relay.cli.run_remote_clio", run_remote)
    monkeypatch.setattr(
        "clio_relay.cli.jarvis_mcp_server",
        fail_local_resolution,
    )

    result = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-call",
            "--cluster",
            "ares",
            "--tool",
            "jarvis_describe",
            "--arguments-json",
            '{"target":"packages"}',
        ],
    )

    assert result.exit_code == 0
    assert result.output.strip() == "job_remote_jarvis"
    assert writes and json.loads(writes[0][1]) == {"target": "packages"}
    assert removals == [(writes[0][0], True)]
    assert commands[0][0] == "jarvis-mcp-call"
    assert "--server" not in commands[0]


def test_target_side_jarvis_discovery_uses_receipt_without_cluster_registry(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv("CLIO_RELAY_REMOTE_CLUSTER", "ares")

    result = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-call",
            "--cluster",
            "ares",
            "--operation",
            "tools/list",
        ],
    )

    assert result.exit_code == 0
    job = ClioCoreQueue(core_dir).get_job(result.output.strip())
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.operation.value == "tools/list"
    assert job.spec.tool is None
    assert job.spec.expected_jarvis_cd_lock_binding == jarvis_cd_lock_binding_expectation()


def test_cli_mcp_call_reads_arguments_json_file(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    arguments_path = tmp_path / "arguments.json"
    arguments_path.write_text('\ufeff{"steps": 150, "sample": "ares-live"}', encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "mcp-call",
            "--cluster",
            "ares",
            "--server",
            "remote-server",
            "--tool",
            "echo",
            "--arguments-json-file",
            str(arguments_path),
            "--idempotency-key",
            "cli-mcp-call-file-args",
        ],
    )

    assert result.exit_code == 0
    job_id = result.output.strip()
    job = ClioCoreQueue(core_dir).get_job(job_id)
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.arguments == {"steps": 150, "sample": "ares-live"}


def test_cli_remote_job_submit_stages_yaml_and_uses_cluster_core(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    ClusterRegistry(
        clusters={
            "ares": ClusterDefinition(
                name="ares",
                ssh_host="test-host",
                core_dir="/remote/core",
                spool_dir="/remote/spool",
            )
        }
    ).save(tmp_path / ".clio-relay" / "clusters.json")
    (tmp_path / "input.in").write_text("run 150\n", encoding="utf-8")
    yaml_path = tmp_path / "pipeline.yaml"
    yaml_path.write_text(
        """
name: remote-submit
x_clio_relay:
  stage_files:
    - local_path: input.in
      remote_path: .local/share/clio-relay/live-tests/{run_id}/input.in
pkgs:
  - pkg_type: site.simulation
    input: $HOME/.local/share/clio-relay/live-tests/{run_id}/input.in
""".lstrip(),
        encoding="utf-8",
    )
    writes: dict[str, bytes] = {}
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        input: bytes | None = None,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        assert capture_output is True
        assert check is False
        if "cat > " in command[2]:
            remote_path = command[2].split("cat > ", maxsplit=1)[1].split(" &&", maxsplit=1)[0]
            writes[remote_path.strip("'")] = input or b""
            return subprocess.CompletedProcess(command, 0, b"", b"")
        if "clio-relay job submit" in command[2]:
            assert "CLIO_RELAY_CLI_MODE=local" in command[2]
            assert 'CLIO_RELAY_CORE_DIR="/remote/core"' in command[2]
            return subprocess.CompletedProcess(command, 0, b"job_remote\n", b"")
        return subprocess.CompletedProcess(command, 0, b"", b"")

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)

    result = CliRunner().invoke(
        app,
        [
            "job",
            "submit",
            "--cluster",
            "ares",
            "--jarvis-yaml",
            str(yaml_path),
            "--idempotency-key",
            "desktop-submit",
        ],
    )

    assert result.exit_code == 0
    assert result.output.strip() == "job_remote"
    assert ClioCoreQueue(tmp_path / ".clio-relay" / "core").list_jobs() == []
    assert any(path.endswith("/input.in") for path in writes)
    staged_yaml = next(
        data.decode("utf-8") for path, data in writes.items() if path.endswith("/pipeline.yaml")
    )
    assert "x_clio_relay" not in staged_yaml
    assert ".local/share/clio-relay/live-tests/pipeline-" in staged_yaml
    assert any("clio-relay job submit" in command[2] for command in commands)


def test_cli_remote_wait_passthrough_uses_cluster_core(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    _write_test_cluster(tmp_path)
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        assert capture_output is True
        assert check is False
        return subprocess.CompletedProcess(command, 0, b'{"job_id":"job_remote"}\n', b"")

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)

    result = CliRunner().invoke(
        app,
        [
            "job",
            "wait",
            "job_remote",
            "--cluster",
            "ares",
            "--timeout-seconds",
            "1",
            "--poll-seconds",
            "0.1",
        ],
    )

    assert result.exit_code == 0
    assert json.loads(result.output)["job_id"] == "job_remote"
    assert len(commands) == 1
    assert "clio-relay job wait job_remote" in commands[0][2]


def test_cli_cluster_bootstrap_uses_package_source_root(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    package_root = tmp_path / "package-root"
    wheel = tmp_path / "clio_relay-0.0.0-py3-none-any.whl"
    wheel.write_bytes(b"wheel")
    captured: dict[str, object] = {}

    def fake_package_source_root() -> Path:
        return package_root

    def fake_bootstrap_cluster_over_ssh(**kwargs: object) -> list[str]:
        captured.update(kwargs)
        receipt = {
            "schema_version": "clio-relay.bootstrap-receipt.v1",
            "invocation_id": "bootstrap_test",
            "bootstrap_profile": "linux-user",
            "relay_install_spec": "clio-relay==1.0.0",
            "install_receipt_sha256": "a" * 64,
            "completed_at": "2026-07-11T00:00:00Z",
        }
        return [
            "bootstrapped",
            "bootstrap_receipt=/home/test/.local/share/clio-relay/bootstrap-receipt.json",
            "bootstrap_invocation_id=bootstrap_test",
            "bootstrap_install_receipt_sha256=" + "a" * 64,
            "bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True),
        ]

    def fake_remote_target_identity(
        definition: ClusterDefinition,
    ) -> dict[str, object]:
        assert definition.name == "ares"
        assert definition.ssh_host == "ares"
        return {
            "schema_version": "clio-relay.cluster-target-info.v1",
            "hostname": "ares",
            "fqdn": "ares.example.test",
            "scheduler_provider": "external",
            "scheduler_cluster_name": None,
            "site_marker_sha256": "b" * 64,
            "ssh_host": "ares",
            "ssh_host_key_sha256": ["SHA256:test"],
            "expected_hostnames": ["ares.example.test"],
            "expected_ssh_host_key_sha256": ["SHA256:test"],
            "expected_scheduler_cluster_name": None,
            "expected_site_marker_sha256": "b" * 64,
            "verified": True,
        }

    monkeypatch.setattr(cli, "package_source_root", fake_package_source_root)
    monkeypatch.setattr(cli, "bootstrap_cluster_over_ssh", fake_bootstrap_cluster_over_ssh)
    monkeypatch.setattr(cli, "_remote_target_identity", fake_remote_target_identity)

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "bootstrap",
            "--cluster",
            "ares",
            "--relay-wheel",
            str(wheel),
        ],
    )

    assert result.exit_code == 0
    output_lines = result.output.splitlines()
    assert output_lines[:-1] == [
        "bootstrapped",
        "bootstrap_receipt=/home/test/.local/share/clio-relay/bootstrap-receipt.json",
        "bootstrap_invocation_id=bootstrap_test",
        "bootstrap_install_receipt_sha256=" + "a" * 64,
        "bootstrap_receipt_json="
        + json.dumps(
            {
                "schema_version": "clio-relay.bootstrap-receipt.v1",
                "invocation_id": "bootstrap_test",
                "bootstrap_profile": "linux-user",
                "relay_install_spec": "clio-relay==1.0.0",
                "install_receipt_sha256": "a" * 64,
                "completed_at": "2026-07-11T00:00:00Z",
            },
            sort_keys=True,
        ),
    ]
    assert output_lines[-1].startswith("validation.report=")
    report_path = Path(output_lines[-1].partition("=")[2])
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "passed"
    assert report["checks"][0]["check_id"] == "cluster.bootstrap"
    assert report["checks"][1]["check_id"] == "worker.target-identity"
    assert report["checks"][1]["evidence"][0]["kind"] == "cluster_target"
    cluster_target = next(
        resource for resource in report["resources"] if resource["kind"] == "cluster_target"
    )
    assert cluster_target["resource_id"] == "target:ares"
    assert cluster_target["role"] == "physical_cluster_target"
    assert cluster_target["metadata"]["verified"] is True
    assert captured["ssh_host"] == "ares"
    assert captured["source_root"] == package_root
    assert captured["source_root"] != tmp_path
    assert captured["relay_artifact_sha256"] == hashlib.sha256(b"wheel").hexdigest()
    assert captured["relay_wheel"] == wheel


def test_cli_remote_task_event_passthrough_uses_cluster_core(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    _write_test_cluster(tmp_path)
    metadata_json = tmp_path / "metadata.json"
    metadata_json.write_text('{"surface":"cli-file"}', encoding="utf-8")
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        assert capture_output is True
        assert check is False
        return subprocess.CompletedProcess(command, 0, b'{"seq":1}\n', b"")

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)

    result = CliRunner().invoke(
        app,
        [
            "job",
            "record-task-event",
            "task_remote",
            "--cluster",
            "ares",
            "--event-type",
            "dataset_found",
            "--label",
            "dataset",
            "--summary",
            "Found staged dataset",
            "--status",
            "succeeded",
            "--path-ref",
            "/mnt/common/datasets/example_001",
            "--metadata-json-file",
            str(metadata_json),
        ],
    )

    assert result.exit_code == 0
    assert json.loads(result.output)["seq"] == 1
    assert len(commands) == 1
    assert "CLIO_RELAY_CLI_MODE=local" in commands[0][2]
    assert "clio-relay job record-task-event task_remote" in commands[0][2]
    assert "--path-ref /mnt/common/datasets/example_001" in commands[0][2]
    assert "cli-file" in commands[0][2]


def test_cli_remote_gateway_passthrough_uses_cluster_core(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    _write_test_cluster(tmp_path)
    gateway_json = tmp_path / "gateway.json"
    gateway_json.write_text('{"strategy":"ssh_forward","remote_port":11111}', encoding="utf-8")
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        assert capture_output is True
        assert check is False
        return subprocess.CompletedProcess(
            command,
            0,
            (b'{"session_id":"gateway_remote","cluster":"ares","name":"live-service-example"}\n'),
            b"",
        )

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)

    created = CliRunner().invoke(
        app,
        [
            "gateway",
            "create",
            "--cluster",
            "ares",
            "--name",
            "live-service-example",
            "--gateway-json-file",
            str(gateway_json),
        ],
    )
    updated = CliRunner().invoke(
        app,
        [
            "gateway",
            "update",
            "gateway_remote",
            "--cluster",
            "ares",
            "--state",
            "ready",
            "--node",
            "ares-comp-01",
        ],
    )
    closed = CliRunner().invoke(app, ["gateway", "close", "gateway_remote", "--cluster", "ares"])

    assert created.exit_code == 0
    assert updated.exit_code == 0
    assert closed.exit_code == 0
    assert [json.loads(item.output)["session_id"] for item in [created, updated, closed]] == [
        "gateway_remote",
        "gateway_remote",
        "gateway_remote",
    ]
    assert "clio-relay gateway create" in commands[0][2]
    assert "remote_port" in commands[0][2]
    assert "clio-relay gateway update gateway_remote" in commands[1][2]
    assert "clio-relay gateway close gateway_remote" in commands[2][2]


@pytest.mark.parametrize(
    "command",
    [
        [
            "gateway",
            "create",
            "--cluster",
            "ares",
            "--name",
            "forged-runtime",
            "--scheduler",
            "slurm",
        ],
        [
            "gateway",
            "update",
            "gateway_target",
            "--scheduler-job-id",
            "12345",
        ],
    ],
)
def test_cli_generic_gateway_commands_have_no_scheduler_identity_arguments(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    command: list[str],
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))

    result = CliRunner().invoke(app, command)

    assert result.exit_code != 0
    assert "No such option" in result.output
    assert ClioCoreQueue(tmp_path / "core").list_gateway_sessions() == []


@pytest.mark.parametrize(
    ("option", "payload", "protected_field"),
    [
        ("--gateway-json", '{"runtime_spec":{"kind":"forged"}}', "gateway.runtime_spec"),
        (
            "--gateway-json",
            '{"jarvis_runtime_binding":{"schema_version":"forged"}}',
            "gateway.jarvis_runtime_binding",
        ),
        (
            "--gateway-json",
            '{"transport":{"remote_connector":{"pid":42}}}',
            "gateway.transport.remote_connector",
        ),
        ("--metadata-json", '{"owner":"clio-relay"}', "metadata.owner"),
    ],
)
def test_cli_generic_gateway_create_rejects_runtime_owned_json(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    option: str,
    payload: str,
    protected_field: str,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))

    result = CliRunner().invoke(
        app,
        [
            "gateway",
            "create",
            "--cluster",
            "ares",
            "--name",
            "forged-runtime",
            option,
            payload,
        ],
    )

    assert result.exit_code != 0
    assert protected_field in result.output
    assert ClioCoreQueue(tmp_path / "core").list_gateway_sessions() == []


def test_cli_generic_gateway_update_cannot_replace_relay_runtime_state(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    queue = ClioCoreQueue(core_dir)
    runtime = queue.create_gateway_session(
        GatewaySession(
            cluster="ares",
            name="relay-runtime",
            gateway={
                "runtime_spec": {"kind": "image-service"},
                "ownership_intents": {"scheduler_submission": {"state": "recorded"}},
            },
            metadata={"owner": "clio-relay", "runtime_kind": "image-service"},
        )
    )

    result = CliRunner().invoke(
        app,
        [
            "gateway",
            "update",
            runtime.session_id,
            "--gateway-json",
            '{"strategy":"ssh_forward"}',
        ],
    )

    assert result.exit_code == 1
    assert "cannot replace relay-managed runtime state" in result.stderr
    assert queue.get_gateway_session(runtime.session_id).gateway == runtime.gateway


def test_cli_generic_gateway_update_preserves_ordinary_gateway_mutations(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    created = CliRunner().invoke(
        app,
        ["gateway", "create", "--cluster", "ares", "--name", "ordinary-gateway"],
    )
    session_id = json.loads(created.output)["session_id"]

    updated = CliRunner().invoke(
        app,
        [
            "gateway",
            "update",
            session_id,
            "--gateway-json",
            '{"strategy":"ssh_forward","local_port":5900}',
            "--metadata-json",
            '{"dataset":"example"}',
        ],
    )

    assert created.exit_code == 0
    assert updated.exit_code == 0
    payload = json.loads(updated.output)
    assert payload["gateway"] == {"strategy": "ssh_forward", "local_port": 5900}
    assert payload["metadata"] == {"dataset": "example"}


def test_cli_gateway_update_closed_session_reports_clean_error(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    created = CliRunner().invoke(
        app,
        ["gateway", "create", "--cluster", "ares", "--name", "live-service-example"],
    )
    assert created.exit_code == 0
    session_id = json.loads(created.output)["session_id"]

    closed = CliRunner().invoke(app, ["gateway", "close", session_id])
    updated = CliRunner().invoke(app, ["gateway", "update", session_id, "--state", "ready"])

    assert closed.exit_code == 0
    assert updated.exit_code == 1
    assert "error: cannot reopen closed gateway session" in updated.stderr
    assert "Traceback" not in updated.output
    assert "Traceback" not in updated.stderr


def test_cli_remote_agent_run_preserves_posix_prompt_path(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    _write_test_cluster(tmp_path)
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        assert capture_output is True
        assert check is False
        return subprocess.CompletedProcess(command, 0, b"job_agent\n", b"")

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)

    result = CliRunner().invoke(
        app,
        [
            "agent",
            "run",
            "--cluster",
            "ares",
            "--prompt",
            "/home/user/prompt.md",
            "--mcp-config",
            "/home/user/mcp.toml",
            "--idempotency-key",
            "agent-posix-path",
        ],
    )

    assert result.exit_code == 0
    assert result.output.strip() == "job_agent"
    assert "--prompt /home/user/prompt.md" in commands[0][2]
    assert "--mcp-config /home/user/mcp.toml" in commands[0][2]
    assert "\\home\\user" not in commands[0][2]
