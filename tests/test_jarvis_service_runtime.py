from __future__ import annotations

import base64
import hashlib
import json
import socket
import urllib.parse
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest
from typer.testing import CliRunner

import clio_relay.jarvis_service_runtime as runtime_binding
import clio_relay.service_runtime as service_runtime_module
from clio_relay.browser_gateway import BrowserAttachmentGrant, BrowserDetachmentResult
from clio_relay.cli import app
from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    FrpTransportConfig,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError
from clio_relay.jarvis_mcp import jarvis_cd_lock_binding_expectation
from clio_relay.jarvis_service_runtime import (
    RELAY_JARVIS_RUNTIME_BINDING_SCHEMA,
    JarvisServiceRuntime,
    resolve_jarvis_service_runtime,
    reverify_jarvis_service_runtime,
)
from clio_relay.mcp_server import handle_request
from clio_relay.models import (
    ArtifactRef,
    GatewaySession,
    JobKind,
    JobState,
    McpCallSpec,
    RelayJob,
    SchedulerConnectorStepIdentity,
    ServiceRuntimeSpec,
)
from clio_relay.remote_mcp import remote_mcp_server_artifact_digest
from clio_relay.service_runtime import ServiceRuntimeSupervisor
from clio_relay.session_lifecycle import CleanupResource
from tests.jarvis_mcp_fakes import verified_jarvis_server_artifact


def test_resolve_jarvis_service_runtime_binds_only_exact_durable_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, definition, job, artifact, envelope = _source_result(tmp_path)
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))

    verified = resolve_jarvis_service_runtime(
        queue=queue,
        definition=definition,
        source_job_id=job.job_id,
        source_artifact_id=artifact.artifact_id,
        package_id="paraview-1",
        package_name="builtin.paraview",
    )

    assert verified.binding.schema_version == RELAY_JARVIS_RUNTIME_BINDING_SCHEMA
    assert verified.binding.source_relay_job_id == job.job_id
    assert verified.binding.source_relay_artifact_id == artifact.artifact_id
    assert verified.binding.source_relay_artifact_sha256 == artifact.sha256
    assert verified.binding.jarvis_execution_id == "execution-1"
    assert verified.binding.scheduler_provider == "slurm"
    assert verified.binding.scheduler_native_id == "12345"
    assert verified.binding.service_instance_id == "paraview-live-1"
    assert verified.binding.service_revision == 3
    assert verified.runtime.host == "127.0.0.1"
    assert verified.runtime.command_path == "/commands"
    assert [member.location for member in verified.binding.dataset_descriptor.members] == [
        "/datasets/asteroid"
    ]
    assert len(verified.binding.service_report_sha256) == 64
    assert len(verified.binding.dataset_descriptor_sha256) == 64


def test_owned_remote_source_uses_identity_bound_api_for_every_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, local_definition, job, artifact, envelope = _source_result(tmp_path)
    definition = local_definition.model_copy(update={"ssh_host": "relay.example.test"})
    owner_settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster=definition.name,
    )
    requests: list[tuple[str, str]] = []
    lifecycle: list[str] = []

    class FakeOwnedSessionApiClient:
        def __init__(
            self,
            *,
            definition: ClusterDefinition,
            settings: RelaySettings,
        ) -> None:
            assert definition == local_definition.model_copy(
                update={"ssh_host": "relay.example.test"}
            )
            assert settings == owner_settings

        def __enter__(self) -> FakeOwnedSessionApiClient:
            lifecycle.append("entered")
            return self

        def __exit__(self, *_args: object) -> None:
            lifecycle.append("exited")

        def request_json(self, *, method: str, path: str) -> object:
            requests.append((method, path))
            if path == f"/jobs/{job.job_id}/status":
                return {"job": job.model_dump(mode="json")}
            if path == f"/artifacts/{artifact.artifact_id}/content":
                return envelope
            raise AssertionError(f"unexpected owned session API path: {path}")

    def direct_ssh_forbidden(
        _definition: ClusterDefinition,
        _arguments: list[str],
    ) -> str:
        raise AssertionError("owned source must not use direct SSH")

    def local_artifact_forbidden(_queue: ClioCoreQueue, _artifact_id: str) -> dict[str, object]:
        raise AssertionError("owned remote source must not read desktop artifact storage")

    def source_is_remote(_definition: ClusterDefinition) -> bool:
        return True

    monkeypatch.setattr(runtime_binding, "should_execute_on_cluster", source_is_remote)
    monkeypatch.setattr(runtime_binding, "OwnedSessionApiClient", FakeOwnedSessionApiClient)
    monkeypatch.setattr(runtime_binding, "run_remote_clio", direct_ssh_forbidden)
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", local_artifact_forbidden)

    verified = resolve_jarvis_service_runtime(
        queue=queue,
        definition=definition,
        settings=owner_settings,
        source_job_id=job.job_id,
        source_artifact_id=artifact.artifact_id,
        package_id="paraview-1",
        package_name="builtin.paraview",
    )
    reverified = reverify_jarvis_service_runtime(
        queue=queue,
        definition=definition,
        settings=owner_settings,
        binding_document=verified.binding.model_dump(mode="json"),
    )

    assert verified.binding.source_relay_artifact_sha256 == artifact.sha256
    assert reverified.binding == verified.binding
    assert lifecycle == ["entered", "exited", "entered", "exited"]
    assert requests == [
        ("GET", f"/jobs/{job.job_id}/status"),
        ("GET", f"/artifacts/{artifact.artifact_id}/content"),
        ("GET", f"/jobs/{job.job_id}/status"),
        ("GET", f"/artifacts/{artifact.artifact_id}/content"),
    ]


def test_owned_remote_source_fails_closed_without_session_api_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, local_definition, job, artifact, _envelope = _source_result(tmp_path)
    definition = local_definition.model_copy(update={"ssh_host": "relay.example.test"})
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster=definition.name,
    )

    def direct_ssh_forbidden(
        _definition: ClusterDefinition,
        _arguments: list[str],
    ) -> str:
        raise AssertionError("owned source must not fall back to direct SSH")

    def source_is_remote(_definition: ClusterDefinition) -> bool:
        return True

    monkeypatch.setattr(runtime_binding, "should_execute_on_cluster", source_is_remote)
    monkeypatch.setattr(runtime_binding, "run_remote_clio", direct_ssh_forbidden)

    with pytest.raises(ConfigurationError, match="CLIO_RELAY_API_TOKEN"):
        resolve_jarvis_service_runtime(
            queue=queue,
            definition=definition,
            settings=settings,
            source_job_id=job.job_id,
            source_artifact_id=artifact.artifact_id,
            package_id="paraview-1",
            package_name="builtin.paraview",
        )


def test_resolve_jarvis_service_runtime_rejects_unbound_or_ambiguous_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, definition, job, _artifact, envelope = _source_result(tmp_path)
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))
    duplicate = json.loads(base64.b64decode(envelope["data"], validate=True).decode("utf-8"))
    services = duplicate["structured_result"]["service_runtimes"]["service_runtimes"]
    repeated = dict(services[0])
    repeated["service_instance_id"] = "paraview-live-2"
    services.append(repeated)
    duplicate["protocol_result"]["structuredContent"] = duplicate["structured_result"]
    _replace_envelope_document(envelope, duplicate)

    with pytest.raises(ValueError, match="exactly one service instance"):
        resolve_jarvis_service_runtime(
            queue=queue,
            definition=definition,
            source_job_id=job.job_id,
            source_artifact_id="artifact-result",
            package_id="paraview-1",
            package_name="builtin.paraview",
        )


def test_reverify_jarvis_service_runtime_rejects_revision_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, definition, job, artifact, envelope = _source_result(tmp_path)
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))
    verified = resolve_jarvis_service_runtime(
        queue=queue,
        definition=definition,
        source_job_id=job.job_id,
        source_artifact_id=artifact.artifact_id,
        package_id="paraview-1",
        package_name="builtin.paraview",
    )
    changed = verified.binding.model_copy(update={"service_revision": 4})

    with pytest.raises(ValueError, match="no longer matches"):
        reverify_jarvis_service_runtime(
            queue=queue,
            definition=definition,
            binding_document=changed.model_dump(mode="json"),
        )


def test_resolve_jarvis_service_runtime_rejects_caller_invented_schema_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, definition, job, artifact, envelope = _source_result(tmp_path)
    document = json.loads(base64.b64decode(envelope["data"], validate=True).decode("utf-8"))
    runtime = document["structured_result"]["service_runtimes"]["service_runtimes"][0]
    runtime["submit_command"] = ["sbatch", "invented.sh"]
    document["protocol_result"]["structuredContent"] = document["structured_result"]
    _replace_envelope_document(envelope, document)
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))

    with pytest.raises(ValueError, match="submit_command"):
        resolve_jarvis_service_runtime(
            queue=queue,
            definition=definition,
            source_job_id=job.job_id,
            source_artifact_id=artifact.artifact_id,
            package_id="paraview-1",
            package_name="builtin.paraview",
        )


def test_get_execution_v1_is_not_treated_as_service_runtime_shape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, definition, job, artifact, envelope = _source_result(tmp_path)
    document = json.loads(base64.b64decode(envelope["data"], validate=True).decode("utf-8"))
    document["structured_result"]["schema_version"] = "clio-kit.jarvis-execution.v1"
    document["protocol_result"]["structuredContent"] = document["structured_result"]
    _replace_envelope_document(envelope, document)
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))

    with pytest.raises(ValueError, match="clio-kit.jarvis-execution.v2"):
        resolve_jarvis_service_runtime(
            queue=queue,
            definition=definition,
            source_job_id=job.job_id,
            source_artifact_id=artifact.artifact_id,
            package_id="paraview-1",
            package_name="builtin.paraview",
        )


def test_execution_v2_source_requires_the_complete_clio_kit_query_envelope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, definition, job, artifact, envelope = _source_result(tmp_path)
    document = json.loads(base64.b64decode(envelope["data"], validate=True).decode("utf-8"))
    document["structured_result"].pop("runtime_metadata")
    document["protocol_result"]["structuredContent"] = document["structured_result"]
    _replace_envelope_document(envelope, document)
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))

    with pytest.raises(ValueError, match="runtime_metadata"):
        resolve_jarvis_service_runtime(
            queue=queue,
            definition=definition,
            source_job_id=job.job_id,
            source_artifact_id=artifact.artifact_id,
            package_id="paraview-1",
            package_name="builtin.paraview",
        )


def test_service_runtime_uses_exact_jarvis_epoch_observation_contract(
    tmp_path: Path,
) -> None:
    _queue, _definition, _job, _artifact, envelope = _source_result(tmp_path)
    document = json.loads(base64.b64decode(envelope["data"], validate=True).decode("utf-8"))
    report = document["structured_result"]["service_runtimes"]["service_runtimes"][0]

    validated = JarvisServiceRuntime.model_validate(report)

    assert validated.observed_at_epoch == 1_784_080_860.125
    assert "observed_at" not in report
    fictional = {**report, "observed_at": "2026-07-15T02:01:00Z"}
    fictional.pop("observed_at_epoch")
    with pytest.raises(ValueError, match="observed_at"):
        JarvisServiceRuntime.model_validate(fictional)


@pytest.mark.parametrize("include_service_runtimes", [None, False])
def test_service_runtime_source_requires_explicit_execution_query_view(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    include_service_runtimes: bool | None,
) -> None:
    queue, definition, job, artifact, envelope = _source_result(
        tmp_path,
        include_service_runtimes=include_service_runtimes,
    )
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))

    with pytest.raises(ValueError, match="include_service_runtimes=true"):
        resolve_jarvis_service_runtime(
            queue=queue,
            definition=definition,
            source_job_id=job.job_id,
            source_artifact_id=artifact.artifact_id,
            package_id="paraview-1",
            package_name="builtin.paraview",
        )


def test_service_runtime_source_rejects_jarvis_run_and_unconfigured_mcp_route(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_queue, definition, run_job, run_artifact, run_envelope = _source_result(
        tmp_path / "run",
        tool="jarvis_run",
    )
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(run_envelope))
    with pytest.raises(ValueError, match="must be jarvis_get_execution"):
        resolve_jarvis_service_runtime(
            queue=run_queue,
            definition=definition,
            source_job_id=run_job.job_id,
            source_artifact_id=run_artifact.artifact_id,
            package_id="paraview-1",
            package_name="builtin.paraview",
        )

    route_queue, definition, route_job, route_artifact, route_envelope = _source_result(
        tmp_path / "route",
        server="not-clio-kit",
    )
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(route_envelope))
    with pytest.raises(ValueError, match="configured clio-kit JARVIS MCP"):
        resolve_jarvis_service_runtime(
            queue=route_queue,
            definition=definition,
            source_job_id=route_job.job_id,
            source_artifact_id=route_artifact.artifact_id,
            package_id="paraview-1",
            package_name="builtin.paraview",
        )


def test_service_runtime_source_rejects_generic_unmarked_jarvis_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operator JARVIS calls cannot authorize the built-in gateway binding path."""
    queue, definition, job, artifact, envelope = _source_result(
        tmp_path,
        bind_relay_jarvis=False,
    )
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))

    with pytest.raises(ValueError, match="JARVIS-CD lock pin"):
        resolve_jarvis_service_runtime(
            queue=queue,
            definition=definition,
            source_job_id=job.job_id,
            source_artifact_id=artifact.artifact_id,
            package_id="paraview-1",
            package_name="builtin.paraview",
        )


def test_service_runtime_source_rejects_result_lock_marker_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persisted result evidence must carry the same built-in lock marker as its job."""
    queue, definition, job, artifact, envelope = _source_result(tmp_path)
    document = json.loads(base64.b64decode(envelope["data"], validate=True).decode("utf-8"))
    document["expected_jarvis_cd_lock_binding"] = None
    _replace_envelope_document(envelope, document)
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))

    with pytest.raises(ValueError, match="result JARVIS-CD lock pin"):
        resolve_jarvis_service_runtime(
            queue=queue,
            definition=definition,
            source_job_id=job.job_id,
            source_artifact_id=artifact.artifact_id,
            package_id="paraview-1",
            package_name="builtin.paraview",
        )


def test_service_runtime_rejects_unverified_outer_clio_kit_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A self-consistent digest cannot authorize a different clio-kit release."""
    server_artifact = verified_jarvis_server_artifact()
    python_runtime = cast(
        dict[str, object],
        server_artifact["python_distribution_runtime"],
    )
    python_runtime["distribution_version"] = "0.0.0"
    queue, definition, job, artifact, envelope = _source_result(
        tmp_path,
        server_artifact=server_artifact,
    )
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))

    with pytest.raises(ValueError, match="not the exact release pin"):
        resolve_jarvis_service_runtime(
            queue=queue,
            definition=definition,
            source_job_id=job.job_id,
            source_artifact_id=artifact.artifact_id,
            package_id="paraview-1",
            package_name="builtin.paraview",
        )


def test_agent_bind_persists_urls_and_rejects_runtime_commands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, definition, job, artifact, envelope = _source_result(tmp_path)
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={definition.name: definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret")
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))
    _patch_connector_start(monkeypatch)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "relay_bind_jarvis_runtime",
                "arguments": {
                    "cluster": definition.name,
                    "source_job_id": job.job_id,
                    "source_artifact_id": artifact.artifact_id,
                    "package_id": "paraview-1",
                    "package_name": "builtin.paraview",
                },
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
    )

    assert response is not None and "error" not in response
    result = cast(dict[str, Any], response["result"]["structuredContent"])
    connect_url = cast(str, result["connect_url"])
    local_port = urllib.parse.urlparse(connect_url).port
    assert local_port is not None
    assert local_port != 18777
    assert result["connect_url"] == f"http://127.0.0.1:{local_port}"
    assert result["health_url"] == f"http://127.0.0.1:{local_port}/healthz"
    assert result["stream_url"] == f"http://127.0.0.1:{local_port}/live-data"
    assert result["events_url"] == f"http://127.0.0.1:{local_port}/events"
    assert result["state_url"] == f"http://127.0.0.1:{local_port}/state"
    assert result["command_url"] == f"http://127.0.0.1:{local_port}/commands"
    assert result["scheduler_cancel_requested"] is False
    gateway = result["gateway_session"]
    assert gateway["state"] == "ready"
    assert gateway["gateway"]["jarvis_runtime_binding"]["source_relay_job_id"] == job.job_id
    assert (
        gateway["gateway"]["jarvis_runtime_binding"]["dataset_descriptor"] == _dataset_descriptor()
    )
    assert gateway["gateway"]["state_url"] == result["state_url"]
    assert gateway["gateway"]["command_url"] == result["command_url"]
    public_document = json.dumps(response, sort_keys=True)
    persisted = queue.get_gateway_session(gateway["session_id"])
    owner_tokens = _owner_token_values(persisted.model_dump(mode="json"))
    assert owner_tokens
    assert all(token not in public_document for token in owner_tokens)
    assert "?capability=" not in public_document
    runtime_spec = ServiceRuntimeSpec.model_validate(persisted.gateway["runtime_spec"])
    assert runtime_spec.desktop_bind_port == local_port
    assert runtime_spec.service_port == 18777
    persisted_transport = cast(dict[str, Any], persisted.gateway["transport"])
    for connector_name in ("remote_connector", "desktop_connector"):
        persisted_connector = cast(dict[str, Any], persisted_transport[connector_name])
        owner_token = cast(str, persisted_connector["owner_token"])
        assert gateway["gateway"]["transport"][connector_name]["owner_token"] == "<redacted>"
        persisted_intent = cast(
            dict[str, Any],
            cast(dict[str, Any], persisted.gateway["ownership_intents"])[connector_name],
        )
        intent_token = cast(str, persisted_intent["owner_token"])
        assert intent_token == owner_token
        assert (
            gateway["gateway"]["ownership_intents"][connector_name]["owner_token"] == "<redacted>"
        )

    port_refused = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "relay_bind_jarvis_runtime",
                "arguments": {
                    "cluster": definition.name,
                    "source_job_id": job.job_id,
                    "source_artifact_id": artifact.artifact_id,
                    "package_id": "paraview-1",
                    "package_name": "builtin.paraview",
                    "desktop_bind_port": 28777,
                },
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
    )
    assert port_refused is not None
    assert "does not accept caller-supplied runtime metadata" in port_refused["error"]["message"]

    refused = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "relay_bind_jarvis_runtime",
                "arguments": {
                    "cluster": definition.name,
                    "source_job_id": job.job_id,
                    "source_artifact_id": artifact.artifact_id,
                    "package_id": "paraview-1",
                    "package_name": "builtin.paraview",
                    "cancel_command": ["scancel", "12345"],
                },
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
    )
    assert refused is not None
    assert "does not accept caller-supplied runtime metadata" in refused["error"]["message"]

    replaced = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "relay_update_gateway_session",
                "arguments": {
                    "session_id": gateway["session_id"],
                    "gateway": {"strategy": "forged"},
                },
            },
        },
        queue=queue,
        settings=settings,
        profile="admin",
    )
    closed = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "relay_close_gateway_session",
                "arguments": {"session_id": gateway["session_id"]},
            },
        },
        queue=queue,
        settings=settings,
        profile="admin",
    )
    assert replaced is not None
    assert "cannot replace relay-managed runtime state" in replaced["error"]["message"]
    assert closed is not None
    assert "must be closed with stop-runtime" in closed["error"]["message"]
    assert queue.get_gateway_session(gateway["session_id"]).gateway["jarvis_runtime_binding"]


def test_internal_bind_override_rejects_occupied_loopback_port(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An internal/operator override must fail before creating connector state on collision."""
    queue, definition, job, artifact, envelope = _source_result(tmp_path)
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))
    verified = resolve_jarvis_service_runtime(
        queue=queue,
        definition=definition,
        source_job_id=job.job_id,
        source_artifact_id=artifact.artifact_id,
        package_id="paraview-1",
        package_name="builtin.paraview",
    )
    supervisor = ServiceRuntimeSupervisor(
        settings=RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"),
        queue=queue,
        cluster=definition.name,
        definition=definition,
        token="token",
        secret_key="secret",
    )

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        occupied_port = cast(int, listener.getsockname()[1])
        with pytest.raises(
            ConfigurationError,
            match=f"desktop bind port is already occupied: {occupied_port}",
        ):
            supervisor.bind_verified_jarvis_runtime(
                name="paraview-live",
                verified=verified,
                desktop_bind_port=occupied_port,
            )


def test_bound_runtime_detach_and_teardown_preserve_scheduler_by_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, definition, job, artifact, envelope = _source_result(tmp_path)
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))
    _patch_connector_start(monkeypatch)
    verified = resolve_jarvis_service_runtime(
        queue=queue,
        definition=definition,
        source_job_id=job.job_id,
        source_artifact_id=artifact.artifact_id,
        package_id="paraview-1",
        package_name="builtin.paraview",
    )
    supervisor = ServiceRuntimeSupervisor(
        settings=RelaySettings(
            core_dir=tmp_path / "core",
            spool_dir=tmp_path / "spool",
            frpc_bin="frpc-test",
        ),
        queue=queue,
        cluster=definition.name,
        definition=definition,
        token="token",
        secret_key="secret",
        sleep=lambda _seconds: None,
    )
    monkeypatch.setattr(supervisor, "_stop_local_connector", _fake_local_connector_stop)
    monkeypatch.setattr(supervisor, "_ssh", _fake_connector_ssh)

    def retained_scheduler_resource(
        *,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
    ) -> CleanupResource:
        return CleanupResource(
            kind="scheduler_job",
            resource_id=str(session.scheduler_job_id),
            location=definition.ssh_host,
            provider=spec.scheduler,
            action="retain",
            ownership_verified=True,
            outcome="retained",
            verified_after_operation=True,
            observed_state="running",
        )

    monkeypatch.setattr(
        supervisor,
        "_retained_scheduler_resource",
        retained_scheduler_resource,
    )
    reverified: list[str] = []
    reverified_settings: list[RelaySettings | None] = []
    original_reverify = service_runtime_module.reverify_jarvis_service_runtime

    def tracked_reverify(**kwargs: Any) -> Any:
        observed = original_reverify(**kwargs)
        reverified.append(observed.binding.service_instance_id)
        reverified_settings.append(kwargs.get("settings"))
        return observed

    monkeypatch.setattr(
        service_runtime_module,
        "reverify_jarvis_service_runtime",
        tracked_reverify,
    )
    started = supervisor.bind_verified_jarvis_runtime(
        name="paraview-live",
        verified=verified,
        desktop_bind_port=28777,
    )
    generation_id = str(
        cast(
            dict[str, object],
            cast(dict[str, object], started.session.gateway["transport"])["remote_connector"],
        )["connector_generation_id"]
    )
    step_marker = (
        "clio-relay-connector-"
        + hashlib.sha256(f"{started.session.session_id}\x00{generation_id}".encode()).hexdigest()[
            :32
        ]
    )
    placement = {
        "schema_version": "clio-relay.scheduler-connector-placement.v1",
        "scheduler": "slurm",
        "scheduler_job_id": "12345",
        "placement_host": "compute-07",
        "allocation_node_count": 1,
        "source": "slurm-scontrol-batch-host",
        "verified": True,
        "observed_at": "2026-07-15T02:00:00Z",
    }
    step = {
        "schema_version": "clio-relay.scheduler-connector-step.v1",
        "scheduler": "slurm",
        "scheduler_job_id": "12345",
        "scheduler_step_id": "12345.7",
        "step_marker": step_marker,
        "placement_host": "compute-07",
        "source": "slurm-srun-detached-marker",
        "verified": True,
        "observed_at": "2026-07-15T02:00:01Z",
    }
    allocation_connector: dict[str, object] = {
        "owner": "clio-relay",
        "session_id": started.session.session_id,
        "execution_scope": "scheduler_allocation",
        "scheduler_provider": "slurm",
        "scheduler_native_id": "12345",
        "scheduler_step_id": "12345.7",
        "scheduler_step_marker": step_marker,
        "scheduler_step": step,
        "connector_generation_id": generation_id,
        "owner_token": "owner-token",
        "config_path": "/runtime/remote-frpc.toml",
        "log_path": "/runtime/remote-frpc.log",
        "placement": placement,
    }
    gateway = dict(started.session.gateway)
    gateway["transport"] = {
        **cast(dict[str, object], gateway["transport"]),
        "remote_connector": allocation_connector,
    }
    queue.update_gateway_session(started.session.session_id, gateway=gateway)
    step_statuses = iter(["active", "active", "absent"])
    allocation_commands: list[str] = []

    def allocation_ssh(script: str) -> str:
        allocation_commands.append(script)
        if "connector-step-status" in script:
            state = next(step_statuses)
            return json.dumps(
                {
                    "schema_version": "clio-relay.scheduler-connector-step-status.v1",
                    "scheduler": "slurm",
                    "scheduler_job_id": "12345",
                    "scheduler_step_id": "12345.7",
                    "placement_host": "compute-07",
                    "record_found": state == "active",
                    "state": state,
                    "observed_host": "compute-07" if state == "active" else None,
                    "source": "slurm-squeue-steps",
                    "verified": True,
                    "observed_at": "2026-07-15T02:00:02Z",
                }
            )
        if "connector-step-cancel" in script:
            return json.dumps(
                {
                    "scheduler": "slurm",
                    "scheduler_job_id": "12345",
                    "scheduler_step_id": "12345.7",
                    "cancel_requested": True,
                    "accepted": True,
                    "returncode": 0,
                    "stdout": "",
                    "stderr": "",
                }
            )
        raise AssertionError(f"unexpected allocation connector script: {script}")

    monkeypatch.setattr(supervisor, "_ssh", allocation_ssh)

    detached = supervisor.detach(session_id=started.session.session_id)
    assert detached.stopped_local_pid == 555
    assert detached.stopped_remote_pid is None
    assert detached.canceled_scheduler_job is None
    assert next(item for item in detached.resources if item.kind == "scheduler_job").action == (
        "retain"
    )
    detached_remote = next(item for item in detached.resources if item.kind == "remote_connector")
    assert detached_remote.resource_id == "12345.7"
    assert detached_remote.location == "compute-07"
    assert detached_remote.outcome == "retained"

    stopped = supervisor.stop(session_id=started.session.session_id)
    assert stopped.canceled_scheduler_job is None
    scheduler = next(item for item in stopped.resources if item.kind == "scheduler_job")
    assert scheduler.action == "retain"
    assert scheduler.outcome == "retained"
    stopped_remote = next(item for item in stopped.resources if item.kind == "remote_connector")
    assert stopped_remote.resource_id == "12345.7"
    assert stopped_remote.outcome == "stopped"
    assert stopped_remote.metadata["parent_scheduler_job_retained"] is True
    assert sum("connector-step-status" in command for command in allocation_commands) == 3
    assert sum("connector-step-cancel" in command for command in allocation_commands) == 1
    assert reverified == ["paraview-live-1", "paraview-live-1"]
    assert reverified_settings == [supervisor.settings, supervisor.settings]


def test_bound_runtime_scheduler_cancel_requires_fresh_binding_reverification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, definition, job, artifact, envelope = _source_result(tmp_path)
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))
    _patch_connector_start(monkeypatch)
    verified = resolve_jarvis_service_runtime(
        queue=queue,
        definition=definition,
        source_job_id=job.job_id,
        source_artifact_id=artifact.artifact_id,
        package_id="paraview-1",
        package_name="builtin.paraview",
    )
    supervisor = ServiceRuntimeSupervisor(
        settings=RelaySettings(
            core_dir=tmp_path / "core",
            spool_dir=tmp_path / "spool",
            frpc_bin="frpc-test",
        ),
        queue=queue,
        cluster=definition.name,
        definition=definition,
        token="token",
        secret_key="secret",
        sleep=lambda _seconds: None,
    )
    monkeypatch.setattr(supervisor, "_stop_local_connector", _fake_local_connector_stop)
    monkeypatch.setattr(supervisor, "_ssh", _fake_connector_ssh)
    canceled: list[str] = []

    def request_cancel(*, provider: str, scheduler_job_id: str) -> None:
        canceled.append(f"{provider}:{scheduler_job_id}")

    def scheduler_terminal(**_kwargs: object) -> str:
        return "canceled"

    monkeypatch.setattr(supervisor, "_request_scheduler_provider_cancel", request_cancel)
    monkeypatch.setattr(
        supervisor,
        "_wait_for_scheduler_terminal",
        scheduler_terminal,
    )
    started = supervisor.bind_verified_jarvis_runtime(
        name="paraview-live",
        verified=verified,
        desktop_bind_port=28777,
    )
    binding = dict(started.session.gateway["jarvis_runtime_binding"])
    binding["service_revision"] = 4
    queue.update_gateway_session(
        started.session.session_id,
        gateway={**started.session.gateway, "jarvis_runtime_binding": binding},
    )

    stopped = supervisor.stop(
        session_id=started.session.session_id,
        cancel_scheduler_job=True,
    )

    assert canceled == []
    scheduler = next(item for item in stopped.resources if item.kind == "scheduler_job")
    assert scheduler.action == "cancel"
    assert scheduler.outcome == "refused"
    assert scheduler.residual is True
    assert "re-verification failed" in str(scheduler.detail)


def test_browser_attachment_is_one_time_safe_and_idempotently_revoked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, definition, job, artifact, envelope = _source_result(tmp_path)
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))
    _patch_connector_start(monkeypatch)
    verified = resolve_jarvis_service_runtime(
        queue=queue,
        definition=definition,
        source_job_id=job.job_id,
        source_artifact_id=artifact.artifact_id,
        package_id="paraview-1",
        package_name="builtin.paraview",
    )
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster=definition.name,
        definition=definition,
        token="token",
        secret_key="secret",
        sleep=lambda _seconds: None,
    )
    started = supervisor.bind_verified_jarvis_runtime(
        name="paraview-live",
        verified=verified,
        desktop_bind_port=28777,
    )

    def start_browser_proxy(
        *,
        session: GatewaySession,
        config: Any,
        capability: str,
        ownership_intent: dict[str, object],
    ) -> dict[str, object]:
        assert len(capability) >= 43
        return {
            "owner": "clio-relay",
            "session_id": session.session_id,
            "attachment_id": config.attachment_id,
            "pid": 777,
            "process_group_id": 777,
            "process_start_marker": "start-777",
            "owner_token": ownership_intent["owner_token"],
            "connector_generation_id": ownership_intent["connector_generation_id"],
            "config_path": ownership_intent["config_path"],
            "stdout_path": ownership_intent["stdout_path"],
            "stderr_path": ownership_intent["stderr_path"],
            "metadata_path": ownership_intent["metadata_path"],
        }

    monkeypatch.setattr(supervisor, "_start_browser_proxy", start_browser_proxy)

    def browser_health(*_args: object, **_kwargs: object) -> int:
        return 7

    monkeypatch.setattr(supervisor, "_wait_for_browser_health", browser_health)
    marker_seen: list[Path] = []

    def stop_browser_proxy(
        *,
        session_id: str,
        connector: dict[str, object],
        require_record: bool = False,
        absence_verified: bool = False,
    ) -> tuple[int | None, CleanupResource]:
        del require_record, absence_verified
        record = queue.get_gateway_session(session_id).gateway["browser_attachment"]
        marker = Path(str(cast(dict[str, object], record)["revocation_path"]))
        assert marker.exists()
        marker_seen.append(marker)
        return 777, CleanupResource(
            kind="desktop_connector",
            resource_id="777",
            location="desktop",
            action="stop",
            ownership_verified=True,
            outcome="stopped",
            verified_after_operation=True,
        )

    monkeypatch.setattr(supervisor, "_stop_local_connector", stop_browser_proxy)

    grant = supervisor.browser_attach(
        session_id=started.session.session_id,
        ttl_seconds=300,
    )
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(grant.state_url).query)
    capability = query["capability"][0]
    persisted = queue.get_gateway_session(started.session.session_id)
    serialized_gateway = json.dumps(persisted.gateway, sort_keys=True)
    assert capability not in serialized_gateway
    assert persisted.gateway["browser_attachment"]["state"] == "active"
    assert (
        persisted.gateway["browser_attachment"]["token_sha256"]
        == hashlib.sha256(capability.encode("utf-8")).hexdigest()
    )

    with pytest.raises(ConfigurationError, match="does not match"):
        supervisor.browser_detach(
            session_id=started.session.session_id,
            attachment_id="browser-wrong",
        )
    first = supervisor.browser_detach(
        session_id=started.session.session_id,
        attachment_id=grant.attachment_id,
    )
    repeated = supervisor.browser_detach(
        session_id=started.session.session_id,
        attachment_id=grant.attachment_id,
    )

    assert first.state == "revoked"
    assert first.already_revoked is False
    assert first.proxy_stopped is True
    assert first.capability_revoked is True
    assert repeated.revoked_at == first.revoked_at
    assert repeated.already_revoked is True
    assert repeated.proxy_stopped is False
    assert marker_seen


def test_bound_remote_connector_persists_verified_slurm_placement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = ClusterDefinition(
        name="test-cluster",
        ssh_host="localhost",
        scheduler_provider="slurm",
        frp_transport=FrpTransportConfig(
            protocol="wss",
            server_addr="frps.example.org",
            server_port=443,
        ),
    )
    supervisor = ServiceRuntimeSupervisor(
        settings=RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"),
        queue=ClioCoreQueue(tmp_path / "core"),
        cluster=definition.name,
        definition=definition,
        token="token",
        secret_key="secret",
    )
    commands: list[str] = []
    session = GatewaySession(cluster=definition.name, name="paraview")
    step_marker = (
        "clio-relay-connector-"
        + hashlib.sha256(f"{session.session_id}\x00generation-1".encode()).hexdigest()[:32]
    )

    def keep_intent(
        current: GatewaySession,
        _role: str,
        _intent: dict[str, object],
    ) -> GatewaySession:
        return current

    monkeypatch.setattr(
        supervisor,
        "_set_ownership_intent",
        keep_intent,
    )

    def ssh(script: str) -> str:
        commands.append(script)
        if "clio-relay scheduler connector-placement" in script:
            return json.dumps(
                {
                    "schema_version": "clio-relay.scheduler-connector-placement.v1",
                    "scheduler": "slurm",
                    "scheduler_job_id": "12345",
                    "placement_host": "compute-07",
                    "allocation_node_count": 1,
                    "source": "slurm-scontrol-batch-host",
                    "verified": True,
                    "observed_at": "2026-07-15T02:00:00Z",
                }
            )
        assert "connector-step-start" in script
        assert "connector-step-reconcile" in script
        assert "nohup setsid" not in script
        return json.dumps(
            {
                "schema_version": "clio-relay.allocation-connector-start.v1",
                "session_id": session.session_id,
                "connector_generation_id": "generation-1",
                "config_path": "/runtime/remote.toml",
                "log_path": "/runtime/remote.log",
                "step_identity": {
                    "schema_version": "clio-relay.scheduler-connector-step.v1",
                    "scheduler": "slurm",
                    "scheduler_job_id": "12345",
                    "scheduler_step_id": "12345.7",
                    "step_marker": step_marker,
                    "placement_host": "compute-07",
                    "source": "slurm-srun-detached-marker",
                    "verified": True,
                    "observed_at": "2026-07-15T02:00:01Z",
                },
            }
        )

    monkeypatch.setattr(supervisor, "_ssh", ssh)
    connector = supervisor._start_remote_connector(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        session=session,
        spec=ServiceRuntimeSpec(
            kind="jarvis-service-runtime",
            submit_command=None,
            deployment_driver="jarvis-bound",
            service_port=18777,
            desktop_bind_port=28777,
            scheduler="slurm",
        ),
        node="127.0.0.1",
        proxy_name="paraview-proxy",
        ownership_intent={
            "owner_token": "owner-token",
            "connector_generation_id": "generation-1",
        },
        allocation_provider="slurm",
        allocation_job_id="12345",
    )

    assert len(commands) == 2
    assert connector["execution_scope"] == "scheduler_allocation"
    assert connector["scheduler_step_id"] == "12345.7"
    assert "pid" not in connector
    placement = cast(dict[str, object], connector["placement"])
    assert placement["placement_host"] == "compute-07"
    start_script = commands[1]
    assert start_script.index("clio-relay.allocation-connector-sidecar.v1") < (
        start_script.index("connector-step-reconcile")
    )
    assert start_script.index("connector-step-reconcile") < start_script.index(
        "connector-step-start"
    )
    assert "scheduler-connector-step.pending.json" in start_script
    assert "remote_frpc_pid" not in start_script


def test_allocation_connector_reconciliation_recovers_crash_interrupted_step(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = ClusterDefinition(
        name="test-cluster",
        ssh_host="localhost",
        scheduler_provider="slurm",
        frp_transport=FrpTransportConfig(
            protocol="wss",
            server_addr="frps.example.org",
            server_port=443,
        ),
    )
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    session = GatewaySession(
        cluster=definition.name,
        name="crash-recovery",
        scheduler="slurm",
        scheduler_job_id="12345",
        metadata={"owner": "clio-relay"},
    )
    generation_id = "generation-1"
    step_marker = (
        "clio-relay-connector-"
        + hashlib.sha256(f"{session.session_id}\x00{generation_id}".encode()).hexdigest()[:32]
    )
    placement: dict[str, object] = {
        "schema_version": "clio-relay.scheduler-connector-placement.v1",
        "scheduler": "slurm",
        "scheduler_job_id": "12345",
        "placement_host": "compute-07",
        "allocation_node_count": 1,
        "source": "slurm-scontrol-batch-host",
        "verified": True,
        "observed_at": "2026-07-15T02:00:00Z",
    }
    intent: dict[str, object] = {
        "schema_version": "clio-relay.gateway-ownership-intent.v1",
        "state": "starting",
        "updated_at": "2026-07-15T02:00:00Z",
        "owner_token": "owner-token",
        "connector_generation_id": generation_id,
        "execution_scope": "scheduler_allocation",
        "scheduler_provider": "slurm",
        "scheduler_native_id": "12345",
        "scheduler_step_marker": step_marker,
        "placement": placement,
    }
    session = queue.create_gateway_session(
        session.model_copy(
            update={
                "gateway": {
                    "transport": {},
                    "ownership_intents": {"remote_connector": intent},
                }
            }
        )
    )
    supervisor = ServiceRuntimeSupervisor(
        settings=RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"),
        queue=queue,
        cluster=definition.name,
        definition=definition,
        token="token",
        secret_key="secret",
    )
    connector_base: dict[str, object] = {
        "owner": "clio-relay",
        "session_id": session.session_id,
        "execution_scope": "scheduler_allocation",
        "scheduler_provider": "slurm",
        "scheduler_native_id": "12345",
        "scheduler_step_marker": step_marker,
        "connector_generation_id": generation_id,
        "owner_token": "owner-token",
        "config_path": "/runtime/remote-frpc.toml",
        "log_path": "/runtime/remote-frpc.log",
        "placement": placement,
    }
    step: dict[str, object] = {
        "schema_version": "clio-relay.scheduler-connector-step.v1",
        "scheduler": "slurm",
        "scheduler_job_id": "12345",
        "scheduler_step_id": "12345.7",
        "step_marker": step_marker,
        "placement_host": "compute-07",
        "source": "slurm-squeue-step-marker",
        "verified": True,
        "observed_at": "2026-07-15T02:00:01Z",
    }

    def ssh(script: str) -> str:
        if "__CLIO_DISCOVER_CONNECTOR__" in script:
            return json.dumps(
                {
                    "present": False,
                    "ownership_verified": True,
                    "reconciliation_required": True,
                    "connector": connector_base,
                }
            )
        if "connector-step-reconcile" in script:
            return json.dumps(
                {
                    "schema_version": ("clio-relay.scheduler-connector-step-reconciliation.v1"),
                    "scheduler": "slurm",
                    "scheduler_job_id": "12345",
                    "step_marker": step_marker,
                    "placement_host": "compute-07",
                    "found": True,
                    "step": step,
                }
            )
        if "connector-step-status" in script:
            return json.dumps(
                {
                    "schema_version": "clio-relay.scheduler-connector-step-status.v1",
                    "scheduler": "slurm",
                    "scheduler_job_id": "12345",
                    "scheduler_step_id": "12345.7",
                    "placement_host": "compute-07",
                    "record_found": True,
                    "state": "active",
                    "observed_host": "compute-07",
                    "source": "slurm-squeue-steps",
                    "verified": True,
                    "observed_at": "2026-07-15T02:00:02Z",
                }
            )
        raise AssertionError(script)

    monkeypatch.setattr(supervisor, "_ssh", ssh)

    recovered = supervisor._reconcile_ownership_intents(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        session
    )

    connector = cast(
        dict[str, object],
        cast(dict[str, object], recovered.gateway["transport"])["remote_connector"],
    )
    assert connector["scheduler_step_id"] == "12345.7"
    assert "pid" not in connector
    recovered_intent = cast(
        dict[str, object],
        cast(dict[str, object], recovered.gateway["ownership_intents"])["remote_connector"],
    )
    assert recovered_intent["state"] == "recorded"


def test_internal_scheduler_step_start_cli_preserves_connector_argv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = ClusterDefinition(
        name="test-cluster",
        ssh_host="localhost",
        scheduler_provider="slurm",
    )
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={definition.name: definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    marker = "clio-relay-connector-0123456789abcdef0123456789abcdef"
    observed_command: list[str] = []

    class Provider:
        def launch_connector_step(
            self,
            scheduler_job_id: str,
            *,
            placement_host: str,
            step_marker: str,
            command: list[str],
            output_path: str,
        ) -> SchedulerConnectorStepIdentity:
            assert scheduler_job_id == "12345"
            assert placement_host == "compute-07"
            assert step_marker == marker
            assert output_path == "/runtime/remote-frpc.log"
            observed_command.extend(command)
            return SchedulerConnectorStepIdentity(
                scheduler="slurm",
                scheduler_job_id="12345",
                scheduler_step_id="12345.7",
                step_marker=marker,
                placement_host="compute-07",
                source="slurm-srun-detached-marker",
                verified=True,
            )

    provider = Provider()

    def allocation_provider(_name: str) -> Provider:
        return provider

    monkeypatch.setattr(
        "clio_relay.cli.allocation_connector_provider_for_scheduler",
        allocation_provider,
    )

    result = CliRunner().invoke(
        app,
        [
            "scheduler",
            "connector-step-start",
            "12345",
            "--cluster",
            "test-cluster",
            "--provider",
            "slurm",
            "--placement-host",
            "compute-07",
            "--step-marker",
            marker,
            "--output-path",
            "/runtime/remote-frpc.log",
            "--",
            "/opt/frpc",
            "-c",
            "/runtime/remote-frpc.toml",
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["scheduler_step_id"] == "12345.7"
    assert observed_command == [
        "/opt/frpc",
        "-c",
        "/runtime/remote-frpc.toml",
    ]


def test_paraview_state_admission_binds_health_execution_and_dataset() -> None:
    descriptor = _dataset_descriptor()
    descriptor_sha256 = hashlib.sha256(
        json.dumps(
            descriptor,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    state: dict[str, object] = {
        "schema_version": "jarvis.paraview.service-state.v1",
        "service_instance_id": "paraview-live-1",
        "revision": 7,
        "execution_id": "execution-1",
        "dataset": {
            "descriptor": descriptor,
            "discovery": {"arrays": [], "bounds": None, "timestep_values": []},
        },
        "pipeline": {
            "timestep": {"index": 0, "value": None, "count": 1},
            "active_field": None,
            "filters": [],
            "colormap": None,
            "camera": None,
            "selection": None,
            "artifacts": [],
        },
    }

    service_runtime_module._validate_jarvis_service_state(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        state,
        service_instance_id="paraview-live-1",
        execution_id="execution-1",
        health_revision=7,
        dataset_descriptor_sha256=descriptor_sha256,
    )

    with pytest.raises(ValueError, match="health or binding"):
        service_runtime_module._validate_jarvis_service_state(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            {**state, "revision": 8},
            service_instance_id="paraview-live-1",
            execution_id="execution-1",
            health_revision=7,
            dataset_descriptor_sha256=descriptor_sha256,
        )
    with pytest.raises(ValueError, match="descriptor disagrees"):
        service_runtime_module._validate_jarvis_service_state(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            state,
            service_instance_id="paraview-live-1",
            execution_id="execution-1",
            health_revision=7,
            dataset_descriptor_sha256="0" * 64,
        )


def test_internal_browser_cli_emits_pinned_one_time_and_detach_schemas(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = ClusterDefinition(name="test-cluster", ssh_host="localhost")
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={definition.name: definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))

    def attach(
        _self: ServiceRuntimeSupervisor,
        *,
        session_id: str,
        ttl_seconds: int,
        bind_port: int | None,
    ) -> BrowserAttachmentGrant:
        assert session_id == "gateway-test"
        assert ttl_seconds == 600
        assert bind_port == 28788
        return BrowserAttachmentGrant(
            attachment_id="browser-test",
            expires_at="2026-07-15T20:00:00Z",
            connect_url="http://127.0.0.1:28788/?capability=secret",
            health_url="http://127.0.0.1:28788/healthz?capability=secret",
            stream_url="http://127.0.0.1:28788/live-data?capability=secret",
            events_url="http://127.0.0.1:28788/events?capability=secret",
            state_url="http://127.0.0.1:28788/state?capability=secret",
            command_url="http://127.0.0.1:28788/commands?capability=secret",
        )

    def detach(
        _self: ServiceRuntimeSupervisor,
        *,
        session_id: str,
        attachment_id: str,
    ) -> BrowserDetachmentResult:
        assert session_id == "gateway-test"
        assert attachment_id == "browser-test"
        return BrowserDetachmentResult(
            attachment_id=attachment_id,
            revoked_at="2026-07-15T19:45:00Z",
            already_revoked=False,
            proxy_process_id=777,
            proxy_stopped=True,
        )

    monkeypatch.setattr(ServiceRuntimeSupervisor, "browser_attach", attach)
    monkeypatch.setattr(ServiceRuntimeSupervisor, "browser_detach", detach)
    runner = CliRunner()
    attached = runner.invoke(
        app,
        [
            "gateway",
            "browser-attach",
            "gateway-test",
            "--cluster",
            definition.name,
            "--ttl-seconds",
            "600",
            "--bind-port",
            "28788",
        ],
    )
    detached = runner.invoke(
        app,
        [
            "gateway",
            "browser-detach",
            "gateway-test",
            "--cluster",
            definition.name,
            "--attachment-id",
            "browser-test",
        ],
    )

    assert attached.exit_code == 0, attached.output
    assert json.loads(attached.output)["schema_version"] == "clio-relay.browser-attachment.v1"
    assert "capability=secret" in json.loads(attached.output)["events_url"]
    assert detached.exit_code == 0, detached.output
    assert json.loads(detached.output) == {
        "schema_version": "clio-relay.browser-detachment.v1",
        "attachment_id": "browser-test",
        "state": "revoked",
        "revoked_at": "2026-07-15T19:45:00Z",
        "already_revoked": False,
        "proxy_process_id": 777,
        "proxy_stopped": True,
        "capability_revoked": True,
    }


def _source_result(
    tmp_path: Path,
    *,
    tool: str = "jarvis_get_execution",
    include_service_runtimes: bool | None = True,
    server: str = "/home/cluster/.local/bin/clio-kit",
    bind_relay_jarvis: bool = True,
    server_artifact: dict[str, Any] | None = None,
) -> tuple[ClioCoreQueue, ClusterDefinition, RelayJob, ArtifactRef, dict[str, Any]]:
    queue = ClioCoreQueue(tmp_path / "core")
    resolved_server_artifact = (
        verified_jarvis_server_artifact() if server_artifact is None else server_artifact
    )
    digest = remote_mcp_server_artifact_digest(resolved_server_artifact)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=server,
                server_args=["mcp-server", "jarvis"],
                expected_server_artifact_digest=digest,
                expected_jarvis_cd_lock_binding=(
                    jarvis_cd_lock_binding_expectation() if bind_relay_jarvis else None
                ),
                tool=tool,
                arguments={
                    "pipeline_id": "pipeline-1",
                    "execution_id": "execution-1",
                    **(
                        {"include_service_runtimes": include_service_runtimes}
                        if include_service_runtimes is not None
                        else {}
                    ),
                },
            ),
            idempotency_key="jarvis-service-source",
        )
    )
    job = queue.update_job_state(job.job_id, JobState.SUCCEEDED)
    document = _mcp_result_document(
        digest=digest,
        spec=cast(McpCallSpec, job.spec),
        server_artifact=resolved_server_artifact,
    )
    payload = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
    artifact = ArtifactRef(
        artifact_id="artifact-result",
        job_id=job.job_id,
        uri=(tmp_path / "mcp-result.json").as_uri(),
        kind="mcp_result",
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )
    envelope: dict[str, Any] = {
        "artifact": artifact.model_dump(mode="json"),
        "encoding": "base64",
        "data": base64.b64encode(payload).decode("ascii"),
    }
    definition = ClusterDefinition(
        name="test-cluster",
        ssh_host="localhost",
        scheduler_provider="slurm",
        frp_transport=FrpTransportConfig(
            protocol="wss",
            server_addr="frps.example.org",
            server_port=443,
        ),
    )
    return queue, definition, job, artifact, envelope


def _mcp_result_document(
    *,
    digest: str,
    spec: McpCallSpec,
    server_artifact: dict[str, Any],
) -> dict[str, Any]:
    submission = {
        "schema_version": "jarvis.scheduler.submission.v1",
        "execution_id": "execution-1",
        "provider": "slurm",
        "scheduler_job_id": "12345",
        "scheduler_cluster": "test-cluster",
        "submitted": True,
        "identity_source": "scheduler_submit_api",
    }
    handle = {
        "schema_version": "jarvis.execution.handle.v1",
        "execution_id": "execution-1",
        "pipeline_id": "pipeline-1",
        "mode": "scheduler",
        "scheduler_provider": "slurm",
        "scheduler_native_id": "12345",
        "cluster": "test-cluster",
    }
    record = {
        "schema_version": "jarvis.execution.record.v1",
        "execution_id": "execution-1",
        "pipeline_id": "pipeline-1",
        "pipeline_name": "pipeline-1",
        "mode": "scheduler",
        "scheduler_provider": "slurm",
        "scheduler_native_id": "12345",
        "cluster": "test-cluster",
        "state": "running",
        "submitted": True,
        "terminal": False,
        "created_at": "2026-07-15T02:00:00Z",
        "updated_at": "2026-07-15T02:01:00Z",
        "return_code": None,
        "error": None,
        "metadata": {"submission": submission},
    }
    progress = {
        "schema_version": "jarvis.execution.progress.v1",
        "execution_id": "execution-1",
        "pipeline_id": "pipeline-1",
        "execution_state": "running",
        "terminal": False,
        "packages": [
            {
                "package_id": "paraview-1",
                "package_name": "builtin.paraview",
                "event_count": 0,
                "latest": None,
            }
        ],
    }
    service = {
        "schema_version": "jarvis.service-runtime.v1",
        "execution_id": "execution-1",
        "package_name": "builtin.paraview",
        "package_id": "paraview-1",
        "service_instance_id": "paraview-live-1",
        "revision": 3,
        "lifecycle": "ready",
        "host": "127.0.0.1",
        "port": 18777,
        "protocol": "http",
        "health_path": "/healthz",
        "live_data_path": "/live-data",
        "events_path": "/events",
        "state_path": "/state",
        "command_path": "/commands",
        "delivery_mode": "push",
        "dataset_descriptor": _dataset_descriptor(),
        "message": "service ready",
        "observed_at_epoch": 1_784_080_860.125,
    }
    services = {
        "schema_version": "jarvis.execution.service-runtimes.v1",
        "execution_id": "execution-1",
        "pipeline_id": "pipeline-1",
        "execution_state": "running",
        "terminal": False,
        "service_runtimes": [service],
    }
    structured = {
        "schema_version": "clio-kit.jarvis-execution.v2",
        "pipeline_id": "pipeline-1",
        "execution_id": "execution-1",
        "execution_handle": handle,
        "execution_record": record,
        "runtime_metadata": {},
        "progress": progress,
        "artifact_page": None,
        "service_runtimes": services,
    }
    return {
        "server": spec.server,
        "server_args": spec.server_args,
        "env_from": spec.env_from,
        "expected_server_artifact_digest": digest,
        "expected_jarvis_cd_lock_binding": spec.expected_jarvis_cd_lock_binding,
        "observed_server_artifact_digest": digest,
        "server_artifact": server_artifact,
        "operation": "tools/call",
        "tool": spec.tool,
        "arguments": spec.arguments,
        "returncode": 0,
        "timed_out": False,
        "protocol_error": None,
        "structured_result": structured,
        "protocol_result": {"structuredContent": structured, "isError": False},
    }


def _replace_envelope_document(envelope: dict[str, Any], document: dict[str, Any]) -> None:
    payload = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
    envelope["data"] = base64.b64encode(payload).decode("ascii")
    envelope["artifact"]["size_bytes"] = len(payload)
    envelope["artifact"]["sha256"] = hashlib.sha256(payload).hexdigest()


def _owner_token_values(value: object) -> set[str]:
    """Collect every nonempty bearer token persisted below a gateway record."""
    if isinstance(value, dict):
        tokens: set[str] = set()
        for key, nested in cast(dict[object, object], value).items():
            if (
                isinstance(key, str)
                and key.casefold().replace("-", "_").endswith("_token")
                and isinstance(nested, str)
                and nested
            ):
                tokens.add(nested)
            else:
                tokens.update(_owner_token_values(nested))
        return tokens
    if isinstance(value, (list, tuple)):
        tokens: set[str] = set()
        for nested in cast(list[object] | tuple[object, ...], value):
            tokens.update(_owner_token_values(nested))
        return tokens
    return set()


def _dataset_descriptor() -> dict[str, Any]:
    descriptor: dict[str, Any] = {
        "schema_version": "jarvis.dataset-descriptor.v1",
        "dataset_id": "asteroid-2018",
        "kind": "temporal-volume",
        "format": "vti-series",
        "members": [{"index": 0, "location": "/datasets/asteroid"}],
        "arrays": [],
        "bounds": None,
        "source_artifact": None,
    }
    digest = hashlib.sha256(
        json.dumps(
            descriptor,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    return {
        **descriptor,
        "fingerprint": {"algorithm": "sha256", "digest": digest},
    }


def _envelope_reader(
    envelope: dict[str, Any],
) -> Callable[[ClioCoreQueue, str], dict[str, object]]:
    def read(_queue: ClioCoreQueue, _artifact_id: str) -> dict[str, object]:
        return envelope

    return read


def _patch_connector_start(monkeypatch: pytest.MonkeyPatch) -> None:
    def start_remote(
        _self: ServiceRuntimeSupervisor,
        *,
        session: Any,
        spec: Any,
        node: str,
        proxy_name: str,
        ownership_intent: dict[str, object],
        allocation_provider: str | None = None,
        allocation_job_id: str | None = None,
    ) -> dict[str, object]:
        del spec, node, proxy_name, allocation_provider, allocation_job_id
        return {
            "owner": "clio-relay",
            "session_id": session.session_id,
            "pid": 444,
            "process_group_id": 444,
            "connector_generation_id": ownership_intent["connector_generation_id"],
            "owner_token": ownership_intent["owner_token"],
            "config_path": "/runtime/remote-frpc.toml",
            "log_path": "/runtime/remote-frpc.log",
        }

    def start_local(
        _self: ServiceRuntimeSupervisor,
        *,
        session: Any,
        spec: Any,
        proxy_name: str,
        ownership_intent: dict[str, object],
    ) -> dict[str, object]:
        del spec, proxy_name
        return {
            "owner": "clio-relay",
            "session_id": session.session_id,
            "pid": 555,
            "process_group_id": 555,
            "process_start_marker": "start-555",
            "connector_generation_id": ownership_intent["connector_generation_id"],
            "owner_token": ownership_intent["owner_token"],
            "config_path": "/runtime/desktop-frpc.toml",
            "stdout_path": "/runtime/desktop-frpc.out",
            "stderr_path": "/runtime/desktop-frpc.err",
        }

    monkeypatch.setattr(ServiceRuntimeSupervisor, "_start_remote_connector", start_remote)
    monkeypatch.setattr(ServiceRuntimeSupervisor, "_start_local_visitor", start_local)

    def health_ready(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(
        ServiceRuntimeSupervisor,
        "_wait_for_local_health",
        health_ready,
    )

    def jarvis_health(*_args: object, **_kwargs: object) -> int:
        return 1

    monkeypatch.setattr(
        ServiceRuntimeSupervisor,
        "_wait_for_jarvis_health",
        jarvis_health,
    )
    monkeypatch.setattr(
        ServiceRuntimeSupervisor,
        "_wait_for_jarvis_state",
        health_ready,
    )


def _fake_local_connector_stop(
    *,
    session_id: str,
    connector: dict[str, object],
    require_record: bool = False,
    absence_verified: bool = False,
) -> tuple[int | None, CleanupResource]:
    del require_record, absence_verified
    raw_pid = connector.get("pid", 555)
    if not isinstance(raw_pid, int):
        raise AssertionError("test connector pid is not an integer")
    pid = raw_pid
    return pid, CleanupResource(
        kind="desktop_connector",
        resource_id=str(pid),
        location="desktop",
        action="stop",
        ownership_verified=True,
        outcome="stopped",
        verified_after_operation=True,
        metadata={"gateway_session_id": session_id},
    )


def _fake_connector_ssh(script: str) -> str:
    if "__CLIO_CONNECTOR_STATUS__" in script:
        return json.dumps(
            {
                "pid": 444,
                "ownership_verified": True,
                "running": True,
                "matching_pids": [444],
            }
        )
    if "__CLIO_STOP_CONNECTOR__" in script:
        return json.dumps(
            {
                "pid": 444,
                "outcome": "stopped",
                "ownership_verified": True,
                "verified_after_operation": True,
                "residual": False,
                "remaining_pids": [],
            }
        )
    raise AssertionError(f"unexpected remote connector script: {script}")
