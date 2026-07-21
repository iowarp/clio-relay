"""Acceptance-capable CLI commands persist canonical reports by default."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Literal

import pytest
from typer import Typer
from typer.testing import CliRunner

from clio_relay import cli
from clio_relay.cli import app
from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    FrpTransportConfig,
    RemoteMcpServerConfig,
)
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import RelayError
from clio_relay.identifiers import DurableRecordId
from clio_relay.live_acceptance import LiveAcceptanceOptions
from clio_relay.models import GatewaySession, GatewaySessionState
from clio_relay.release_validation import LocalReleaseValidationOptions
from clio_relay.remote_mcp import (
    RemoteMcpRoute,
    RemoteMcpToolSchema,
    VirtualRemoteMcpCatalog,
    VirtualRemoteMcpTool,
)
from clio_relay.service_runtime import (
    ServiceRuntimeStartResult,
    ServiceRuntimeStopResult,
    ServiceRuntimeSupervisor,
)
from clio_relay.session_lifecycle import (
    CleanupResource,
    OwnedSessionCleanupReportReference,
    OwnedSessionRecoveryStatus,
    RemoteSessionStateEvidence,
    SessionLifecycleReport,
    session_lifecycle_report_bytes,
)
from clio_relay.validation_report import (
    EvidenceReference,
    LiveValidationReport,
    ValidationRecorder,
    load_validation_report,
    new_live_validation_report,
    write_validation_report,
)


@pytest.fixture(autouse=True)
def _local_cli(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")


def _write_cluster(root: Path, name: str = "test-cluster") -> None:
    ClusterRegistry(
        clusters={
            name: ClusterDefinition(
                name=name,
                ssh_host=name,
                frp_transport=FrpTransportConfig(server_addr="relay.example.test"),
                remote_mcp_servers={
                    "science": RemoteMcpServerConfig(
                        command="science-mcp",
                        allow_tools=["inspect"],
                        profiles=["user"],
                    )
                },
            )
        }
    ).save(root / ".clio-relay" / "clusters.json")


def _report_paths(root: Path) -> list[Path]:
    return sorted((root / ".clio-relay" / "validation-reports").glob("*.json"))


def _passed_report(
    *,
    scenario: str,
    cluster: str,
    report_id: DurableRecordId | None = None,
) -> LiveValidationReport:
    report = new_live_validation_report(
        scenario=scenario,
        cluster=cluster,
        report_id=report_id,
    )
    recorder = ValidationRecorder(report)
    with recorder.check("acceptance.completed", "complete acceptance command") as evidence:
        evidence.append(EvidenceReference(kind="test", excerpt="acceptance completed"))
    recorder.finish()
    return report


@dataclass(frozen=True)
class AcceptanceCommandCase:
    """One report-producing acceptance command and its canonical scenario."""

    name: str
    success_command: tuple[str, ...]
    failure_command: tuple[str, ...]
    scenario: str


ACCEPTANCE_COMMANDS = (
    AcceptanceCommandCase(
        name="release-validate-local",
        success_command=("release", "validate-local"),
        failure_command=("release", "validate-local", "--project-root", "missing-checkout"),
        scenario="local-release",
    ),
    AcceptanceCommandCase(
        name="relay-host-test-frpc-connection",
        success_command=(
            "relay-host",
            "test-frpc-connection",
            "--cluster",
            "test-cluster",
            "--local-port",
            "18848",
        ),
        failure_command=(
            "relay-host",
            "test-frpc-connection",
            "--cluster",
            "missing",
            "--local-port",
            "18848",
        ),
        scenario="transport",
    ),
    AcceptanceCommandCase(
        name="relay-host-test-http-transport",
        success_command=(
            "relay-host",
            "test-http-transport",
            "--cluster",
            "test-cluster",
            "--local-bind-port",
            "19001",
        ),
        failure_command=(
            "relay-host",
            "test-http-transport",
            "--cluster",
            "missing",
            "--local-bind-port",
            "19001",
        ),
        scenario="transport",
    ),
    AcceptanceCommandCase(
        name="relay-host-test-direct-transport",
        success_command=(
            "relay-host",
            "test-direct-transport",
            "--cluster",
            "test-cluster",
            "--local-bind-port",
            "19002",
        ),
        failure_command=(
            "relay-host",
            "test-direct-transport",
            "--cluster",
            "missing",
            "--local-bind-port",
            "19002",
        ),
        scenario="transport",
    ),
    AcceptanceCommandCase(
        name="relay-host-test-ssh-transport",
        success_command=(
            "relay-host",
            "test-ssh-transport",
            "--cluster",
            "test-cluster",
            "--local-bind-port",
            "19003",
        ),
        failure_command=(
            "relay-host",
            "test-ssh-transport",
            "--cluster",
            "missing",
            "--local-bind-port",
            "19003",
        ),
        scenario="transport",
    ),
    AcceptanceCommandCase(
        name="remote-mcp-validate",
        success_command=(
            "remote-mcp",
            "validate",
            "--cluster",
            "test-cluster",
            "--name",
            "science",
            "--tool",
            "inspect",
        ),
        failure_command=(
            "remote-mcp",
            "validate",
            "--cluster",
            "missing",
            "--name",
            "science",
            "--tool",
            "inspect",
        ),
        scenario="remote-mcp",
    ),
    AcceptanceCommandCase(
        name="cluster-bootstrap",
        success_command=("cluster", "bootstrap", "--cluster", "test-cluster"),
        failure_command=("cluster", "bootstrap", "--cluster", "missing"),
        scenario="cluster-bootstrap",
    ),
    AcceptanceCommandCase(
        name="session-detach",
        success_command=(
            "session",
            "detach",
            "--cluster",
            "test-cluster",
            "--session-id",
            "owned",
        ),
        failure_command=(
            "session",
            "detach",
            "--cluster",
            "missing",
            "--session-id",
            "owned",
        ),
        scenario="cleanup",
    ),
    AcceptanceCommandCase(
        name="session-teardown",
        success_command=(
            "session",
            "teardown",
            "--cluster",
            "test-cluster",
            "--session-id",
            "owned",
        ),
        failure_command=(
            "session",
            "teardown",
            "--cluster",
            "missing",
            "--session-id",
            "owned",
        ),
        scenario="cleanup",
    ),
    AcceptanceCommandCase(
        name="queue-validate",
        success_command=("queue", "validate", "--cluster", "test-cluster"),
        failure_command=("queue", "validate", "--cluster", "missing"),
        scenario="queue-management",
    ),
    AcceptanceCommandCase(
        name="scheduler-validate-lifecycle",
        success_command=("scheduler", "validate-lifecycle", "--cluster", "test-cluster"),
        failure_command=("scheduler", "validate-lifecycle", "--cluster", "missing"),
        scenario="scheduler-lifecycle",
    ),
    AcceptanceCommandCase(
        name="gateway-start-runtime",
        success_command=(
            "gateway",
            "start-runtime",
            "--cluster",
            "test-cluster",
            "--name",
            "runtime",
            "--runtime-json-file",
            "runtime.json",
        ),
        failure_command=(
            "gateway",
            "start-runtime",
            "--cluster",
            "missing",
            "--name",
            "runtime",
            "--runtime-json-file",
            "missing-runtime.json",
        ),
        scenario="gateway-runtime",
    ),
    AcceptanceCommandCase(
        name="gateway-detach-runtime",
        success_command=(
            "gateway",
            "detach-runtime",
            "gateway-owned",
            "--cluster",
            "test-cluster",
        ),
        failure_command=(
            "gateway",
            "detach-runtime",
            "gateway-owned",
            "--cluster",
            "missing",
        ),
        scenario="gateway-runtime",
    ),
    AcceptanceCommandCase(
        name="gateway-stop-runtime",
        success_command=(
            "gateway",
            "stop-runtime",
            "gateway-owned",
            "--cluster",
            "test-cluster",
        ),
        failure_command=(
            "gateway",
            "stop-runtime",
            "gateway-owned",
            "--cluster",
            "missing",
        ),
        scenario="gateway-runtime",
    ),
    AcceptanceCommandCase(
        name="jarvis-mcp-validate",
        success_command=(
            "jarvis-mcp-validate",
            "--cluster",
            "test-cluster",
            "--package-search-query",
            "lammps",
            "--arguments-json",
            '{"pipeline_id":"pipeline"}',
        ),
        failure_command=(
            "jarvis-mcp-validate",
            "--cluster",
            "missing",
            "--package-search-query",
            "lammps",
            "--arguments-json",
            '{"pipeline_id":"pipeline"}',
        ),
        scenario="remote-mcp",
    ),
    AcceptanceCommandCase(
        name="live-test",
        success_command=("live-test", "--cluster", "test-cluster"),
        failure_command=("live-test", "--cluster", "missing"),
        scenario="live-test",
    ),
)


def _validation_report_command_names(
    typer_app: Typer,
    *,
    prefix: tuple[str, ...] = (),
) -> set[str]:
    """Return CLI paths whose callbacks expose the canonical report option."""
    discovered: set[str] = set()
    for command in typer_app.registered_commands:
        callback = command.callback
        if callback is None or not getattr(
            callback,
            "__clio_relay_acceptance_report_command__",
            False,
        ):
            continue
        command_name = command.name or callback.__name__.replace("_", "-")
        discovered.add("-".join((*prefix, command_name)))
    for group in typer_app.registered_groups:
        if group.name is None:
            raise AssertionError("acceptance command groups require explicit names")
        if group.typer_instance is None:
            raise AssertionError("acceptance command groups require a Typer instance")
        discovered.update(
            _validation_report_command_names(
                group.typer_instance,
                prefix=(*prefix, group.name),
            )
        )
    return discovered


def test_acceptance_inventory_matches_cli_report_producers() -> None:
    """Make new canonical-report commands update the exhaustive acceptance matrix."""
    assert _validation_report_command_names(app) == {case.name for case in ACCEPTANCE_COMMANDS}


class _PackagedMcpSession:
    """Minimal successful packaged stdio session used at the CLI boundary."""

    initialize_response: dict[str, object] = {}
    tools_list_response: dict[str, object] = {}
    tools_call_response: dict[str, object] = {}

    def evidence(self) -> dict[str, object]:
        return {"boundary": "packaged_clio_relay_mcp_server_stdio", "returncode": 0}


class _RemoteAcceptanceResult:
    """Minimal domain report whose canonical conversion remains real."""

    passed = True

    def __init__(self, *, cluster: str) -> None:
        self.cluster = cluster

    def to_live_validation_report(
        self,
        *,
        launcher: str | None = None,
        install_source: str | None = None,
        artifact_sha256: str | None = None,
    ) -> LiveValidationReport:
        del launcher, install_source, artifact_sha256
        return _passed_report(scenario="remote-mcp", cluster=self.cluster)

    def model_dump_json(self, *, indent: int | None = None) -> str:
        return json.dumps({"passed": True}, indent=indent)


def _activate_owner_session(root: Path) -> None:
    queue = ClioCoreQueue(root)
    selected = queue.prepare_owner_session_start(
        "owned",
        recorded_generation_id=None,
        candidate_generation_id="generation-1",
    )
    assert selected == "generation-1"


def _owned_session_status(**_kwargs: object) -> dict[str, object]:
    return {
        "owner": "clio-relay",
        "session_id": "owned",
        "session_generation_id": "generation-1",
        "running": True,
        "ownership_verified": True,
    }


def _session_report(*, mode: Literal["detach", "teardown"]) -> SessionLifecycleReport:
    observed_at = datetime.now(UTC)
    action: Literal["retain", "stop"] = "retain" if mode == "detach" else "stop"
    outcome: Literal["retained", "stopped"] = "retained" if mode == "detach" else "stopped"
    return SessionLifecycleReport(
        cluster="test-cluster",
        session_id="owned",
        session_generation_id="generation-1",
        mode=mode,
        prior_session_status=(
            None
            if mode == "detach"
            else RemoteSessionStateEvidence(
                api_pid=123,
                session_generation_id="generation-1",
                process_start_marker="start-123",
                running=True,
                ownership_verified=True,
                observed_at=observed_at,
                started_at=observed_at,
            )
        ),
        post_session_status=(
            None
            if mode == "detach"
            else RemoteSessionStateEvidence(
                api_pid=123,
                session_generation_id="generation-1",
                process_start_marker="start-123",
                running=False,
                ownership_verified=True,
                observed_at=observed_at,
                started_at=observed_at,
            )
        ),
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id="123",
                location="test-cluster",
                action=action,
                ownership_verified=True,
                outcome=outcome,
                verified_after_operation=True,
            ),
            *(
                [
                    CleanupResource(
                        kind="remote_session_files",
                        resource_id="owned:generation-1",
                        location="test-cluster",
                        action="close",
                        ownership_verified=True,
                        outcome="closed",
                        verified_after_operation=True,
                    )
                ]
                if mode == "teardown"
                else []
            ),
        ],
    )


def _gateway_session(*, state: GatewaySessionState) -> GatewaySession:
    return GatewaySession(
        session_id="gateway-owned",
        cluster="test-cluster",
        name="runtime",
        state=state,
        scheduler="external",
        queue_state="running" if state is not GatewaySessionState.CLOSED else "completed",
        gateway={"runtime_kind": "test-runtime"},
    )


def _gateway_start_result() -> ServiceRuntimeStartResult:
    return ServiceRuntimeStartResult(
        session=_gateway_session(state=GatewaySessionState.READY),
        connect_url="http://127.0.0.1:19010",
        health_url="http://127.0.0.1:19010/healthz",
        stream_url=None,
        compatibility_urls={},
        events_url=None,
    )


def _gateway_stop_result(*, mode: Literal["detach", "teardown"]) -> ServiceRuntimeStopResult:
    detached = mode == "detach"
    session = _gateway_session(
        state=GatewaySessionState.DEGRADED if detached else GatewaySessionState.CLOSED
    )
    if not detached:
        session = session.model_copy(
            update={
                "gateway": {
                    **session.gateway,
                    "teardown_intent": {
                        "schema_version": "clio-relay.gateway-teardown-intent.v1",
                        "operation_id": "gateway_cleanup_acceptance_default",
                        "gateway_session_id": session.session_id,
                        "cancel_scheduler_job": False,
                        "created_at": "2026-07-19T00:00:00Z",
                    },
                }
            }
        )
    return ServiceRuntimeStopResult(
        session=session,
        mode=mode,
        stopped_local_pid=555,
        stopped_remote_pid=None if detached else 444,
        canceled_scheduler_job=None,
        resources=[
            CleanupResource(
                kind="desktop_connector",
                resource_id="555",
                location="desktop",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
                metadata={"gateway_session_id": "gateway-owned"},
            ),
            CleanupResource(
                kind="remote_connector",
                resource_id="444",
                location="test-cluster",
                action="retain" if detached else "stop",
                ownership_verified=True,
                outcome="retained" if detached else "stopped",
                verified_after_operation=True,
                metadata={"gateway_session_id": "gateway-owned"},
            ),
            CleanupResource(
                kind="gateway_record",
                resource_id="gateway-owned",
                location="test-cluster",
                action="retain" if detached else "close",
                ownership_verified=True,
                outcome="retained" if detached else "closed",
                verified_after_operation=True,
            ),
        ],
        errors=[],
    )


def _remote_mcp_catalog() -> VirtualRemoteMcpCatalog:
    remote_tool = RemoteMcpToolSchema(
        name="inspect",
        input_schema={"type": "object", "properties": {}},
    )
    route = RemoteMcpRoute(
        cluster="test-cluster",
        server_name="science",
        command="science-mcp",
        args=(),
        env_from=(),
        expected_server_artifact_digest=None,
        remote_tool_name="inspect",
        timeout_seconds=30,
        contract=None,
        cluster_route_revision="r" * 64,
        registration_revision="g" * 64,
    )
    virtual = VirtualRemoteMcpTool(
        alias="remote_science_inspect",
        namespace="science",
        remote_tool=remote_tool,
        routes={"test-cluster": route},
        arguments_wrapped=False,
    )
    return VirtualRemoteMcpCatalog(
        revision="test-revision",
        tools={virtual.alias: virtual},
        issues=(),
    )


def _write_runtime_spec(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "kind": "test-runtime",
                "submit_command": ["runtime-submit"],
                "service_port": 19010,
                "desktop_bind_port": 19010,
            }
        ),
        encoding="utf-8",
    )


def _successful_frpc_probe(**_kwargs: object) -> list[str]:
    return ["frpc stayed connected", "login to server success"]


def _successful_http_probe(**_kwargs: object) -> list[str]:
    return [
        "transport.protocol=wss",
        "transport.healthz=ok",
        "transport.cleanup=passed",
    ]


def _successful_direct_probe(**_kwargs: object) -> list[str]:
    return [
        "direct_transport.result=xtcp",
        "transport.protocol=wss",
        "transport.proxy_type=xtcp",
        "transport.healthz=ok",
        "transport.cleanup=passed",
    ]


def _successful_ssh_probe(**_kwargs: object) -> list[str]:
    return [
        "transport.protocol=ssh_forward",
        "transport.healthz=ok",
        "transport.cleanup=passed",
    ]


def _load_remote_mcp_catalog(_profile: str) -> VirtualRemoteMcpCatalog:
    return _remote_mcp_catalog()


def _packaged_mcp_session(**_kwargs: object) -> _PackagedMcpSession:
    return _PackagedMcpSession()


def _relay_job_id(_response: object) -> str:
    return "relay-job"


def _wait_for_terminal(*_args: object, **_kwargs: object) -> None:
    return None


def _terminal_job_status(*_args: object, **_kwargs: object) -> dict[str, object]:
    return {"terminal": True}


def _empty_artifacts(*_args: object, **_kwargs: object) -> list[dict[str, object]]:
    return []


def _empty_json_artifact(*_args: object, **_kwargs: object) -> dict[str, object]:
    return {}


def _remote_acceptance_result(**_kwargs: object) -> _RemoteAcceptanceResult:
    return _RemoteAcceptanceResult(cluster="test-cluster")


def _successful_bootstrap(**_kwargs: object) -> list[str]:
    return [
        'bootstrap_receipt_json={"invocation_id":"bootstrap_test"}',
        "bootstrap_receipt=/tmp/bootstrap-test.json",
    ]


def _verified_target_identity(_definition: ClusterDefinition) -> dict[str, object]:
    return {
        "schema_version": "clio-relay.cluster-target-info.v1",
        "hostname": "test-cluster",
        "fqdn": "test-cluster.example.test",
        "scheduler_provider": "external",
        "scheduler_cluster_name": None,
        "site_marker_sha256": "b" * 64,
        "ssh_host": "test-cluster",
        "ssh_host_key_sha256": ["SHA256:test"],
        "expected_hostnames": ["test-cluster.example.test"],
        "expected_ssh_host_key_sha256": ["SHA256:test"],
        "expected_scheduler_cluster_name": None,
        "expected_site_marker_sha256": "b" * 64,
        "verified": True,
    }


def _empty_runtime_cleanup(**_kwargs: object) -> list[dict[str, object]]:
    return []


def _detached_session(**_kwargs: object) -> SessionLifecycleReport:
    return _session_report(mode="detach")


def _torn_down_session(**_kwargs: object) -> SessionLifecycleReport:
    return _session_report(mode="teardown")


def _unused_validation_provider(_provider: str | None) -> object:
    return object()


def _successful_queue_validation(
    *_args: object,
    **_kwargs: object,
) -> LiveValidationReport:
    return _passed_report(scenario="queue-management", cluster="test-cluster")


def _successful_scheduler_validation(**_kwargs: object) -> LiveValidationReport:
    return _passed_report(scenario="scheduler-lifecycle", cluster="test-cluster")


def _start_gateway(
    _self: ServiceRuntimeSupervisor,
    **_kwargs: object,
) -> ServiceRuntimeStartResult:
    return _gateway_start_result()


def _detach_gateway(
    _self: ServiceRuntimeSupervisor,
    **_kwargs: object,
) -> ServiceRuntimeStopResult:
    return _gateway_stop_result(mode="detach")


def _stop_gateway(
    _self: ServiceRuntimeSupervisor,
    **_kwargs: object,
) -> ServiceRuntimeStopResult:
    return _gateway_stop_result(mode="teardown")


def _jarvis_contract_discovery(
    **_kwargs: object,
) -> tuple[str, dict[str, object], list[dict[str, object]], bytes]:
    return "discovery-job", {}, [], b"{}"


def _persist_jarvis_contract(**_kwargs: object) -> None:
    return None


def _post_run_jarvis_query(**_kwargs: object) -> SimpleNamespace:
    return SimpleNamespace(
        tools_list_response={},
        call_response={},
        call_job_id="query-job",
        call_status={"terminal": True},
        artifacts=[],
        mcp_result={},
        provenance={},
        initialize_response={},
        stdio_evidence={},
    )


def _jarvis_package_search(**_kwargs: object) -> SimpleNamespace:
    return SimpleNamespace(
        tools_list_response={},
        call_response={},
        call_job_id="package-search-job",
        call_status={"terminal": True},
        artifacts=[],
        mcp_result={},
        provenance={},
        initialize_response={},
        stdio_evidence={},
    )


def _successful_jarvis_validation(**_kwargs: object) -> LiveValidationReport:
    return _passed_report(scenario="remote-mcp", cluster="test-cluster")


def _install_success_fakes(
    case: AcceptanceCommandCase,
    *,
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(root / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(root / "spool"))
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret")
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "api-token")

    if case.name == "release-validate-local":

        def run_local(options: LocalReleaseValidationOptions) -> LiveValidationReport:
            report = _passed_report(
                scenario="local-release",
                cluster="local",
                report_id=options.report_id,
            )
            write_validation_report(report, options.report_path)
            return report

        monkeypatch.setattr(cli, "run_local_release_validation", run_local)
        return
    if case.name == "relay-host-test-frpc-connection":
        monkeypatch.setattr(cli, "run_frpc_connection_check", _successful_frpc_probe)
        return
    if case.name == "relay-host-test-http-transport":
        monkeypatch.setattr(cli, "run_frp_http_probe", _successful_http_probe)
        return
    if case.name == "relay-host-test-direct-transport":
        monkeypatch.setattr(cli, "run_frp_direct_http_probe", _successful_direct_probe)
        return
    if case.name == "relay-host-test-ssh-transport":
        monkeypatch.setattr(cli, "run_ssh_forward_http_probe", _successful_ssh_probe)
        return
    if case.name == "remote-mcp-validate":
        monkeypatch.setattr(cli, "load_registered_remote_mcp_catalog", _load_remote_mcp_catalog)
        monkeypatch.setattr(cli, "run_packaged_mcp_stdio_session", _packaged_mcp_session)
        monkeypatch.setattr(cli, "_mcp_response_job_id", _relay_job_id)
        monkeypatch.setattr(cli, "wait_for_terminal", _wait_for_terminal)
        monkeypatch.setattr(cli, "get_job_status", _terminal_job_status)
        monkeypatch.setattr(cli, "_complete_local_artifact_records", _empty_artifacts)
        monkeypatch.setattr(cli, "_read_local_json_artifact_kind", _empty_json_artifact)
        monkeypatch.setattr(
            cli,
            "build_remote_mcp_acceptance_report",
            _remote_acceptance_result,
        )
        return
    if case.name == "cluster-bootstrap":
        monkeypatch.setattr(cli, "bootstrap_cluster_over_ssh", _successful_bootstrap)
        monkeypatch.setattr(cli, "_remote_target_identity", _verified_target_identity)
        return
    if case.name in {"session-detach", "session-teardown"}:
        _activate_owner_session(root / "core")

        def execute_locally(_definition: ClusterDefinition) -> bool:
            return False

        monkeypatch.setattr(cli, "should_execute_on_cluster", execute_locally)
        monkeypatch.setattr(cli, "_cleanup_owned_runtime_sessions", _empty_runtime_cleanup)
        if case.name == "session-detach":
            monkeypatch.setattr(cli, "detach_remote_session", _detached_session)
        else:
            finalized_status: list[OwnedSessionRecoveryStatus] = []

            def persist_cleanup_report(
                *,
                definition: ClusterDefinition,
                cluster: str,
                session_id: str,
                session_generation_id: str,
                report: SessionLifecycleReport,
            ) -> tuple[SessionLifecycleReport, OwnedSessionRecoveryStatus]:
                del definition
                payload = session_lifecycle_report_bytes(report)
                digest = hashlib.sha256(payload).hexdigest()
                reference = OwnedSessionCleanupReportReference(
                    name=f"coordinator-cleanup-report-{digest}.json",
                    size=len(payload),
                    sha256=digest,
                )
                status = OwnedSessionRecoveryStatus(
                    cluster=cluster,
                    session_id=session_id,
                    session_generation_id=session_generation_id,
                    owner="clio-relay",
                    process_state="already_closed",
                    process_absence_verified=True,
                    generation_process_absence_verified=True,
                    metadata_verified=True,
                    cluster_registry_verified=True,
                    durable_generation_verified=True,
                    cleanup_receipt=True,
                    cleanup_paths_pending=False,
                    coordinator_report_ref=reference,
                    coordinator_report_sha256=digest,
                    coordinator_report_bound=True,
                    ownership_verified=True,
                    recovery_verified=True,
                    admission_status={"closed": True},
                )
                finalized_status[:] = [status]
                return report, status

            def closed_recovery_status(**_kwargs: object) -> OwnedSessionRecoveryStatus:
                assert finalized_status
                return finalized_status[0]

            def verified_cleanup_report(
                _status: OwnedSessionRecoveryStatus,
                *,
                report: SessionLifecycleReport,
                **_kwargs: object,
            ) -> SessionLifecycleReport:
                return report

            def mark_closed(**_kwargs: object) -> None:
                return None

            monkeypatch.setattr(cli, "status_remote_session", _owned_session_status)
            monkeypatch.setattr(cli, "teardown_remote_session", _torn_down_session)
            monkeypatch.setattr(
                cli,
                "_persist_verified_cleanup_report_before_closure",
                persist_cleanup_report,
            )
            monkeypatch.setattr(cli, "_mark_owner_session_closed", mark_closed)
            monkeypatch.setattr(
                cli,
                "_owned_session_recovery_status",
                closed_recovery_status,
            )
            monkeypatch.setattr(
                cli,
                "_verified_finalized_cleanup_report",
                verified_cleanup_report,
            )
        return
    if case.name == "queue-validate":
        monkeypatch.setattr(
            cli,
            "validation_provider_for_scheduler",
            _unused_validation_provider,
        )
        monkeypatch.setattr(
            cli,
            "run_queue_management_validation",
            _successful_queue_validation,
        )
        return
    if case.name == "scheduler-validate-lifecycle":
        monkeypatch.setattr(
            cli,
            "run_scheduler_lifecycle_validation",
            _successful_scheduler_validation,
        )
        return
    if case.name == "gateway-start-runtime":
        _write_runtime_spec(root / "runtime.json")
        monkeypatch.setattr(ServiceRuntimeSupervisor, "start", _start_gateway)
        return
    if case.name == "gateway-detach-runtime":
        monkeypatch.setattr(ServiceRuntimeSupervisor, "detach", _detach_gateway)
        return
    if case.name == "gateway-stop-runtime":
        monkeypatch.setattr(ServiceRuntimeSupervisor, "stop", _stop_gateway)
        return
    if case.name == "jarvis-mcp-validate":
        monkeypatch.setattr(
            cli,
            "_run_jarvis_remote_contract_discovery",
            _jarvis_contract_discovery,
        )
        monkeypatch.setattr(
            cli,
            "_persist_jarvis_remote_contract_discovery",
            _persist_jarvis_contract,
        )
        monkeypatch.setattr(cli, "run_packaged_mcp_stdio_session", _packaged_mcp_session)
        monkeypatch.setattr(cli, "_mcp_response_job_id", _relay_job_id)
        monkeypatch.setattr(cli, "_wait_for_local_job_terminal", _terminal_job_status)
        monkeypatch.setattr(cli, "_complete_local_progress_records", _empty_artifacts)
        monkeypatch.setattr(cli, "_complete_local_artifact_records", _empty_artifacts)

        def read_jarvis_artifact(
            *_args: object,
            kind: str,
        ) -> dict[str, object]:
            if kind == "runtime_metadata":
                return {"pipeline_id": "pipeline", "execution_id": "execution"}
            return {}

        monkeypatch.setattr(cli, "_read_local_json_artifact_kind", read_jarvis_artifact)
        monkeypatch.setattr(
            cli,
            "_run_post_run_jarvis_execution_query",
            _post_run_jarvis_query,
        )
        monkeypatch.setattr(
            cli,
            "_run_jarvis_package_search_query",
            _jarvis_package_search,
        )
        monkeypatch.setattr(
            cli,
            "build_jarvis_mcp_validation_report",
            _successful_jarvis_validation,
        )
        return
    if case.name == "live-test":

        def run_live(options: LiveAcceptanceOptions) -> list[str]:
            report = _passed_report(
                scenario=options.validation_scenario,
                cluster=options.cluster,
                report_id=options.report_id,
            )
            report_path = options.report_path
            assert report_path is not None
            write_validation_report(report, report_path)
            return [f"validation.report={report_path.resolve()}"]

        monkeypatch.setattr(cli, "run_live_acceptance", run_live)
        return
    raise AssertionError(f"success fake is missing for acceptance command: {case.name}")


@pytest.mark.parametrize(
    "case",
    ACCEPTANCE_COMMANDS,
    ids=lambda case: case.name,
)
def test_every_acceptance_command_writes_successful_default_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: AcceptanceCommandCase,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_cluster(tmp_path)
    _install_success_fakes(case, root=tmp_path, monkeypatch=monkeypatch)

    result = CliRunner().invoke(app, list(case.success_command))

    assert result.exit_code == 0, (
        f"command={case.name}\noutput={result.output}\nexception={result.exception!r}"
    )
    reports = _report_paths(tmp_path)
    assert len(reports) == 1
    report = load_validation_report(reports[0])
    assert report.scenario == case.scenario
    assert report.status.value == "passed"
    assert report.completed_at is not None
    assert report.checks
    assert all(check.status.value == "passed" for check in report.checks)
    assert all(check.evidence for check in report.checks)
    assert report.cleanup.remaining_resources == []


@pytest.mark.parametrize(
    "case",
    ACCEPTANCE_COMMANDS,
    ids=lambda case: case.name,
)
def test_every_acceptance_command_writes_default_preflight_failure_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: AcceptanceCommandCase,
) -> None:
    monkeypatch.chdir(tmp_path)
    ClusterRegistry.default().save(tmp_path / ".clio-relay" / "clusters.json")
    if case.name == "release-validate-local":

        def fail_local(_options: LocalReleaseValidationOptions) -> LiveValidationReport:
            raise RelayError("release checkout preflight failed")

        monkeypatch.setattr(cli, "run_local_release_validation", fail_local)

    result = CliRunner().invoke(app, list(case.failure_command))

    assert result.exit_code != 0
    reports = _report_paths(tmp_path)
    assert len(reports) == 1
    report = load_validation_report(reports[0])
    assert report.scenario == case.scenario
    assert report.status.value == "failed"
    assert report.completed_at is not None
    assert report.checks[-1].status.value == "failed"
    assert report.checks[-1].error


def test_live_test_writes_default_report_when_backend_fails_before_persisting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_cluster(tmp_path)

    def fail_live(_options: LiveAcceptanceOptions) -> list[str]:
        raise RelayError("live acceptance backend failed before writing its report")

    monkeypatch.setattr(cli, "run_live_acceptance", fail_live)

    result = CliRunner().invoke(app, ["live-test", "--cluster", "test-cluster"])

    assert result.exit_code == 1
    reports = _report_paths(tmp_path)
    assert len(reports) == 1
    report = load_validation_report(reports[0])
    assert report.status.value == "failed"
    assert report.checks[-1].check_id == "live.completed"
    assert report.checks[-1].error == (
        "RelayError: live acceptance backend failed before writing its report"
    )


def test_frpc_connection_writes_distinct_default_reports_on_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret")

    def successful_probe(**_kwargs: object) -> list[str]:
        return ["frpc stayed connected until timeout", "login to server success"]

    monkeypatch.setattr(cli, "run_frpc_connection_check", successful_probe)
    command = [
        "relay-host",
        "test-frpc-connection",
        "--cluster",
        "test-cluster",
        "--local-port",
        "8848",
        "--validation-launcher",
        "uvx",
        "--validation-install-source",
        "wheel:clio_relay-1.0.0-py3-none-any.whl",
    ]

    first = CliRunner().invoke(app, command)
    second = CliRunner().invoke(app, command)

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    reports = _report_paths(tmp_path)
    assert len(reports) == 2
    assert reports[0] != reports[1]
    parsed = [load_validation_report(path) for path in reports]
    assert len({report.report_id for report in parsed}) == 2
    for report in parsed:
        assert report.status.value == "passed"
        assert [check.check_id for check in report.checks] == ["transport.frpc-connection"]
        assert report.resources[0].state == "stopped"
        assert report.resources[0].metadata["cleanup_verified"] is True
        assert report.cleanup.remaining_resources == []
    assert "validation.report=" in first.output


def test_frpc_connection_writes_default_failure_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret")

    def failed_probe(**_kwargs: object) -> list[str]:
        raise RelayError("frpc exited before the connection interval")

    monkeypatch.setattr(cli, "run_frpc_connection_check", failed_probe)

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-frpc-connection",
            "--cluster",
            "test-cluster",
            "--local-port",
            "8848",
        ],
    )

    assert result.exit_code == 1
    reports = _report_paths(tmp_path)
    assert len(reports) == 1
    report = load_validation_report(reports[0])
    assert report.status.value == "failed"
    assert report.checks[-1].check_id == "transport.frpc-connection"
    assert report.checks[-1].status.value == "failed"
    assert report.cleanup.remaining_resources == []
