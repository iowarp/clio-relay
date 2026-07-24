from __future__ import annotations

import base64
import hashlib
import json
import socket
import subprocess
import sys
import threading
import urllib.parse
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

import clio_relay.cli as cli_module
import clio_relay.http_api as http_api_module
import clio_relay.jarvis_service_runtime as runtime_binding
import clio_relay.mcp_server as mcp_server_module
import clio_relay.owner_session_admission as owner_session_admission_module
import clio_relay.remote_cli as remote_cli_module
import clio_relay.service_runtime as service_runtime_module
import clio_relay.session_api as session_api_module
from clio_relay.browser_gateway import BrowserAttachmentGrant, BrowserDetachmentResult
from clio_relay.cli import app
from clio_relay.cluster_config import (
    CLUSTER_REGISTRY_ENV,
    ClusterDefinition,
    ClusterRegistry,
    FrpTransportConfig,
    cluster_route_revision,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, QueueConflictError, RelayError
from clio_relay.http_api import create_app
from clio_relay.jarvis_mcp import jarvis_cd_lock_binding_expectation
from clio_relay.jarvis_service_runtime import (
    RELAY_JARVIS_RUNTIME_BINDING_SCHEMA,
    JarvisDatasetDescriptor,
    JarvisServiceRuntime,
    resolve_jarvis_service_runtime,
    reverify_jarvis_service_runtime,
)
from clio_relay.mcp_server import handle_request
from clio_relay.models import (
    REGISTERED_JARVIS_USER_CONTRACT,
    ArtifactRef,
    GatewaySession,
    GatewaySessionState,
    JobKind,
    JobState,
    McpCallSpec,
    RelayJob,
    SchedulerConnectorStepIdentity,
    SchedulerPhase,
    SchedulerStatus,
    ServiceRuntimeSpec,
)
from clio_relay.remote_mcp import remote_mcp_server_artifact_digest
from clio_relay.service_runtime import (
    ServiceRuntimePendingResult,
    ServiceRuntimeStopResult,
    ServiceRuntimeSupervisor,
)
from clio_relay.session_lifecycle import CleanupResource
from tests.jarvis_mcp_fakes import verified_jarvis_server_artifact


@pytest.fixture(autouse=True)
def _private_authority_resolver(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep supervisor tests focused while authority transport has dedicated coverage."""

    def resolve_for_test(
        *,
        definition: ClusterDefinition,
        settings: RelaySettings | None,
        verified: Any,
    ) -> str | None:
        del definition, settings
        if verified.runtime.schema_version == "jarvis.service-runtime.v1":
            return None
        return f"Bearer {'a' * 64}"

    monkeypatch.setattr(
        service_runtime_module,
        "resolve_jarvis_service_runtime_authorization",
        resolve_for_test,
    )


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
    assert verified.binding.service_runtime_schema_version == "jarvis.service-runtime.v2"
    assert (
        verified.binding.authorization_sha256
        == hashlib.sha256(("a" * 64).encode("ascii")).hexdigest()
    )
    assert len(verified.binding.dataset_descriptor_sha256) == 64


def test_service_runtime_binding_defaults_to_authenticated_v2() -> None:
    """New binding construction must fail closed toward authenticated provenance."""

    assert (
        runtime_binding.JarvisServiceRuntimeBinding.model_fields["schema_version"].default
        == "clio-relay.jarvis-service-runtime-binding.v2"
    )


def test_service_runtime_v1_remains_readable_but_cannot_smuggle_authorization() -> None:
    """Previously released executions remain queryable without weakening v2."""
    runtime = _service_runtime_document()
    runtime["schema_version"] = "jarvis.service-runtime.v1"
    runtime.pop("authorization")
    legacy = JarvisServiceRuntime.model_validate(runtime)
    assert legacy.authorization is None

    runtime["authorization"] = {
        "scheme": "bearer",
        "token_sha256": hashlib.sha256(("a" * 64).encode("ascii")).hexdigest(),
    }
    with pytest.raises(ValueError, match="v1 cannot contain authorization"):
        JarvisServiceRuntime.model_validate(runtime)

    runtime["schema_version"] = "jarvis.service-runtime.v2"
    runtime.pop("authorization")
    with pytest.raises(ValueError, match="v2 requires authorization"):
        JarvisServiceRuntime.model_validate(runtime)

    runtime["authorization"] = {"scheme": "bearer", "token": "a" * 64}
    with pytest.raises(ValueError, match="token"):
        JarvisServiceRuntime.model_validate(runtime)


def test_legacy_runtime_cannot_create_new_handoff_but_existing_binding_reverifies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Released v1 receipts remain readable without admitting new unauthenticated binds."""
    queue, definition, job, artifact, envelope = _source_result(tmp_path)
    document = json.loads(base64.b64decode(envelope["data"], validate=True).decode("utf-8"))
    runtime = document["structured_result"]["service_runtimes"]["service_runtimes"][0]
    runtime["schema_version"] = "jarvis.service-runtime.v1"
    runtime.pop("authorization")
    document["protocol_result"]["structuredContent"] = document["structured_result"]
    _replace_envelope_document(envelope, document)
    artifact = ArtifactRef.model_validate(envelope["artifact"])
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))

    with pytest.raises(ValueError, match="cannot create new relay bindings"):
        resolve_jarvis_service_runtime(
            queue=queue,
            definition=definition,
            source_job_id=job.job_id,
            source_artifact_id=artifact.artifact_id,
            package_id="paraview-1",
            package_name="builtin.paraview",
        )
    assert (
        runtime_binding.derive_jarvis_service_runtime_handoffs(
            cluster=definition.name,
            source_job=job,
            source_artifact=artifact,
            document=document,
        )
        == []
    )

    legacy = runtime_binding._resolve_jarvis_service_runtime(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        queue=queue,
        definition=definition,
        settings=None,
        source_job_id=job.job_id,
        source_artifact_id=artifact.artifact_id,
        package_id="paraview-1",
        package_name="builtin.paraview",
        service_instance_id=None,
        allow_legacy_v1=True,
    )
    assert legacy.binding.schema_version == "clio-relay.jarvis-service-runtime-binding.v1"
    assert (
        reverify_jarvis_service_runtime(
            queue=queue,
            definition=definition,
            binding_document=legacy.binding.model_dump(mode="json"),
        ).binding
        == legacy.binding
    )


def test_private_authority_resolver_uses_exact_remote_identity_and_never_persists_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Relay resolves the bearer only after binding every durable public identity."""
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
    token = "a" * 64
    digest = hashlib.sha256(token.encode("ascii")).hexdigest()
    observed: list[str] = []

    def remote_authority(
        selected: ClusterDefinition,
        arguments: list[str],
        *,
        timeout_seconds: float,
        maximum_stdout_bytes: int,
    ) -> str:
        assert selected == definition
        assert timeout_seconds == 30
        assert maximum_stdout_bytes == 32 * 1024
        observed.extend(arguments)
        return json.dumps(_service_runtime_authority_document(token=token))

    def executes_remotely(_definition: ClusterDefinition) -> bool:
        return True

    monkeypatch.setattr(runtime_binding, "should_execute_on_cluster", executes_remotely)
    monkeypatch.setattr(
        runtime_binding,
        "run_remote_jarvis_runtime_authority",
        remote_authority,
    )

    header = runtime_binding.resolve_jarvis_service_runtime_authorization(
        definition=definition,
        settings=None,
        verified=verified,
    )

    assert header == f"Bearer {token}"
    assert observed == [
        "execution-1",
        "--pipeline-id",
        "pipeline-1",
        "--package-id",
        "paraview-1",
        "--service-instance-id",
        "paraview-live-1",
        "--revision",
        "3",
        "--token-sha256",
        digest,
    ]
    assert token not in verified.runtime.model_dump_json()
    assert token not in verified.binding.model_dump_json()
    persisted_payloads = [path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()]
    assert all(token.encode("ascii") not in payload for payload in persisted_payloads)


def test_owned_session_authority_uses_identity_bound_api_when_browser_attach_is_local(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Desktop-local browser attach must resolve authority on the receipt-owning cluster."""
    queue, local_definition, job, artifact, envelope = _source_result(tmp_path)
    definition = local_definition.model_copy(update={"ssh_host": "relay.example.test"})
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))
    verified = resolve_jarvis_service_runtime(
        queue=queue,
        definition=local_definition,
        source_job_id=job.job_id,
        source_artifact_id=artifact.artifact_id,
        package_id="paraview-1",
        package_name="builtin.paraview",
    )
    settings = RelaySettings(
        core_dir=tmp_path / "desktop-core",
        spool_dir=tmp_path / "desktop-spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster=definition.name,
    )
    token = "a" * 64
    requests: list[dict[str, object]] = []
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
            assert settings == settings_for_assertion

        def __enter__(self) -> FakeOwnedSessionApiClient:
            lifecycle.append("entered")
            return self

        def __exit__(self, *_args: object) -> None:
            lifecycle.append("exited")

        def request_json(
            self,
            *,
            method: str,
            path: str,
            body: dict[str, object] | None = None,
        ) -> object:
            requests.append({"method": method, "path": path, "body": body})
            return _service_runtime_authority_document(token=token)

    settings_for_assertion = settings

    def forbidden(*_args: object, **_kwargs: object) -> Any:
        raise AssertionError("owned-session authority must not use desktop JARVIS or direct SSH")

    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setattr(runtime_binding, "OwnedSessionApiClient", FakeOwnedSessionApiClient)
    monkeypatch.setattr(runtime_binding, "should_execute_on_cluster", forbidden)
    monkeypatch.setattr(runtime_binding, "run_remote_jarvis_runtime_authority", forbidden)
    monkeypatch.setattr(
        runtime_binding, "resolve_local_jarvis_service_runtime_authority", forbidden
    )

    assert (
        session_api_module._validate_request(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            method="POST",
            path=runtime_binding.OWNED_SESSION_JARVIS_RUNTIME_AUTHORITY_PATH,
        )
        == "POST"
    )

    header = runtime_binding.resolve_jarvis_service_runtime_authorization(
        definition=definition,
        settings=settings,
        verified=verified,
    )

    assert header == f"Bearer {token}"
    assert lifecycle == ["entered", "exited"]
    assert requests == [
        {
            "method": "POST",
            "path": runtime_binding.OWNED_SESSION_JARVIS_RUNTIME_AUTHORITY_PATH,
            "body": {"binding": verified.binding.model_dump(mode="json")},
        }
    ]


def test_owned_session_authority_endpoint_reverifies_owned_artifact_before_resolving(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The private endpoint must bind its answer to this session's durable receipt."""
    queue, local_definition, job, artifact, envelope = _source_result(tmp_path)
    definition = local_definition.model_copy(update={"ssh_host": "relay.example.test"})
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    source_root = tmp_path / "spool" / job.job_id
    source_root.mkdir(parents=True)
    source_path = source_root / "mcp-result.json"
    source_path.write_bytes(base64.b64decode(cast(str, envelope["data"]), validate=True))
    artifact = artifact.model_copy(update={"uri": source_path.as_uri()})
    envelope["artifact"] = artifact.model_dump(mode="json")
    queue.append_artifact(artifact)
    queue.update_job_metadata(
        job.job_id,
        {
            "owner": "clio-relay",
            "owner_session_id": "desktop-session-1",
            "owner_session_generation_id": "generation-1",
        },
    )
    verified = resolve_jarvis_service_runtime(
        queue=queue,
        definition=definition,
        source_job_id=job.job_id,
        source_artifact_id=artifact.artifact_id,
        package_id="paraview-1",
        package_name="builtin.paraview",
    )
    foreign_job = queue.submit_job(
        RelayJob(
            cluster=definition.name,
            kind=JobKind.MCP_CALL,
            spec=job.spec,
            idempotency_key="foreign-runtime-authority-source",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "another-desktop-session",
                "owner_session_generation_id": "another-generation",
            },
        )
    )
    foreign_payload = b"foreign-session-artifact"
    foreign_root = tmp_path / "spool" / foreign_job.job_id
    foreign_root.mkdir(parents=True)
    foreign_path = foreign_root / "foreign-mcp-result.json"
    foreign_path.write_bytes(foreign_payload)
    foreign_artifact = queue.append_artifact(
        ArtifactRef(
            job_id=foreign_job.job_id,
            uri=foreign_path.as_uri(),
            kind="mcp_result",
            size_bytes=len(foreign_payload),
            sha256=hashlib.sha256(foreign_payload).hexdigest(),
        )
    )
    registry_path = tmp_path / "session-registry.json"
    ClusterRegistry(clusters={definition.name: definition}).save(registry_path)
    registry_payload = registry_path.read_bytes()
    monkeypatch.setenv(CLUSTER_REGISTRY_ENV, str(registry_path))
    monkeypatch.setenv(
        "CLIO_RELAY_SESSION_REGISTRY_SHA256",
        hashlib.sha256(registry_payload).hexdigest(),
    )
    monkeypatch.setenv(
        "CLIO_RELAY_SESSION_ROUTE_REVISION",
        cluster_route_revision(definition),
    )
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster=definition.name,
        session_owner_token="o" * 32,
        jarvis_bin="/released/bin/jarvis",
    )
    authority = runtime_binding.JarvisServiceRuntimeAuthority.model_validate(
        _service_runtime_authority_document(token="a" * 64)
    )
    observed: dict[str, object] = {}

    def forbidden(*_args: object, **_kwargs: object) -> Any:
        raise AssertionError("cluster-local API must not recurse through owner API or direct SSH")

    def resolve_local_verified(**kwargs: object) -> runtime_binding.JarvisServiceRuntimeAuthority:
        observed.update(kwargs)
        return authority

    monkeypatch.setattr(
        http_api_module,
        "resolve_local_verified_jarvis_service_runtime_authority",
        resolve_local_verified,
    )
    monkeypatch.setattr(runtime_binding, "OwnedSessionApiClient", forbidden)
    monkeypatch.setattr(runtime_binding, "run_remote_clio", forbidden)
    headers = {
        "Authorization": "Bearer session-api-token",
        "X-Clio-Relay-Owner-Session-Id": "desktop-session-1",
        "X-Clio-Relay-Session-Generation-Id": "generation-1",
    }
    request = {"binding": verified.binding.model_dump(mode="json")}

    client = cast(Any, TestClient(create_app(settings)))
    with client:
        assert (
            client.post(
                runtime_binding.OWNED_SESSION_JARVIS_RUNTIME_AUTHORITY_PATH,
                json=request,
            ).status_code
            == 401
        )
        foreign_job_response = client.post(
            runtime_binding.OWNED_SESSION_JARVIS_RUNTIME_AUTHORITY_PATH,
            headers=headers,
            json={
                "binding": verified.binding.model_copy(
                    update={"source_relay_job_id": foreign_job.job_id}
                ).model_dump(mode="json")
            },
        )
        foreign_artifact_response = client.post(
            runtime_binding.OWNED_SESSION_JARVIS_RUNTIME_AUTHORITY_PATH,
            headers=headers,
            json={
                "binding": verified.binding.model_copy(
                    update={
                        "source_relay_artifact_id": foreign_artifact.artifact_id,
                        "source_relay_artifact_sha256": foreign_artifact.sha256,
                    }
                ).model_dump(mode="json")
            },
        )
        drift_response = client.post(
            runtime_binding.OWNED_SESSION_JARVIS_RUNTIME_AUTHORITY_PATH,
            headers=headers,
            json={
                "binding": verified.binding.model_copy(
                    update={"service_revision": verified.binding.service_revision + 1}
                ).model_dump(mode="json")
            },
        )
        response = client.post(
            runtime_binding.OWNED_SESSION_JARVIS_RUNTIME_AUTHORITY_PATH,
            headers=headers,
            json=request,
        )

    assert foreign_job_response.status_code == 403
    assert foreign_artifact_response.status_code == 403
    assert drift_response.status_code == 409
    assert "no longer matches its durable source" in drift_response.json()["detail"]
    assert response.status_code == 200, response.text
    assert response.json() == _service_runtime_authority_document(token="a" * 64)
    assert observed["jarvis_bin"] == "/released/bin/jarvis"
    resolved = cast(runtime_binding.VerifiedJarvisServiceRuntime, observed["verified"])
    assert resolved.binding == verified.binding
    token = ("a" * 64).encode("ascii")
    assert token.decode("ascii") not in caplog.text
    assert token not in verified.model_dump_json().encode("utf-8")
    persisted = [path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()]
    private_field = b'"token":"' + token + b'"'
    assert all(private_field not in b"".join(payload.split()) for payload in persisted)


def test_private_runtime_authority_endpoint_is_unavailable_on_non_owned_api(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ordinary relay API must never expose the private authority transport."""
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

    def forbidden(*_args: object, **_kwargs: object) -> Any:
        raise AssertionError("ordinary API must not invoke a private authority resolver")

    monkeypatch.setattr(
        http_api_module,
        "resolve_local_verified_jarvis_service_runtime_authority",
        forbidden,
    )
    settings = RelaySettings(
        core_dir=tmp_path / "ordinary-core",
        spool_dir=tmp_path / "ordinary-spool",
    )

    client = cast(Any, TestClient(create_app(settings)))
    with client:
        response = client.post(
            runtime_binding.OWNED_SESSION_JARVIS_RUNTIME_AUTHORITY_PATH,
            json={"binding": verified.binding.model_dump(mode="json")},
        )
        assert (
            runtime_binding.OWNED_SESSION_JARVIS_RUNTIME_AUTHORITY_PATH
            not in (client.get("/openapi.json").json()["paths"])
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "owned JARVIS runtime authority resolver is unavailable"


def test_owned_private_authority_api_cannot_start_without_api_token(tmp_path: Path) -> None:
    """A secret-returning owned API must fail before serving without authentication."""
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster="test-cluster",
        session_owner_token="o" * 32,
    )

    with pytest.raises(ConfigurationError, match="CLIO_RELAY_API_TOKEN"):
        create_app(settings)


def test_private_authority_resolver_rejects_private_token_digest_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A validly shaped but different private bearer cannot cross the relay boundary."""
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
    document = _service_runtime_authority_document(token="b" * 64)
    document["token_sha256"] = verified.binding.authorization_sha256

    def executes_remotely(_definition: ClusterDefinition) -> bool:
        return True

    def mismatched_authority(
        _definition: ClusterDefinition,
        _arguments: list[str],
        *,
        timeout_seconds: float,
        maximum_stdout_bytes: int,
    ) -> str:
        assert timeout_seconds == 30
        assert maximum_stdout_bytes == 32 * 1024
        return json.dumps(document)

    monkeypatch.setattr(runtime_binding, "should_execute_on_cluster", executes_remotely)
    monkeypatch.setattr(
        runtime_binding,
        "run_remote_jarvis_runtime_authority",
        mismatched_authority,
    )

    with pytest.raises(ValueError, match="token did not match token_sha256"):
        runtime_binding.resolve_jarvis_service_runtime_authorization(
            definition=definition,
            settings=None,
            verified=verified,
        )


def test_private_authority_transport_rejects_duplicate_keys_without_disclosing_bearer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ambiguous private JSON is rejected without copying any raw bearer into errors."""
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
    token = "a" * 64
    digest = hashlib.sha256(token.encode("ascii")).hexdigest()
    duplicate_document = (
        '{"authorization":{"scheme":"bearer","token":"'
        + token
        + '"},"execution_id":"execution-1","package_id":"paraview-1",'
        '"pipeline_id":"pipeline-1","revision":3,'
        '"schema_version":"jarvis.execution.service-runtime-authority.v1",'
        '"service_instance_id":"paraview-live-1","token_sha256":"'
        + digest
        + '","token_sha256":"'
        + digest
        + '"}'
    )

    def executes_remotely(_definition: ClusterDefinition) -> bool:
        return True

    def duplicate_authority(
        _definition: ClusterDefinition,
        _arguments: list[str],
        *,
        timeout_seconds: float,
        maximum_stdout_bytes: int,
    ) -> str:
        assert timeout_seconds == 30
        assert maximum_stdout_bytes == 32 * 1024
        return duplicate_document

    monkeypatch.setattr(runtime_binding, "should_execute_on_cluster", executes_remotely)
    monkeypatch.setattr(
        runtime_binding,
        "run_remote_jarvis_runtime_authority",
        duplicate_authority,
    )

    with pytest.raises(ValueError, match="duplicate JSON key: token_sha256") as caught:
        runtime_binding.resolve_jarvis_service_runtime_authorization(
            definition=definition,
            settings=None,
            verified=verified,
        )

    assert token not in str(caught.value)
    assert token not in verified.runtime.model_dump_json()
    assert token not in verified.binding.model_dump_json()


def test_private_authority_transport_sanitizes_invalid_json_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed private stdout cannot survive in the raised error or its cause chain."""
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
    token = "a" * 64

    def executes_remotely(_definition: ClusterDefinition) -> bool:
        return True

    def malformed_authority(
        _definition: ClusterDefinition,
        _arguments: list[str],
        *,
        timeout_seconds: float,
        maximum_stdout_bytes: int,
    ) -> str:
        assert timeout_seconds == 30
        assert maximum_stdout_bytes == 32 * 1024
        return f"{token}\nnot-json"

    monkeypatch.setattr(runtime_binding, "should_execute_on_cluster", executes_remotely)
    monkeypatch.setattr(
        runtime_binding,
        "run_remote_jarvis_runtime_authority",
        malformed_authority,
    )

    with pytest.raises(RelayError, match="returned invalid JSON") as caught:
        runtime_binding.resolve_jarvis_service_runtime_authorization(
            definition=definition,
            settings=None,
            verified=verified,
        )

    assert token not in str(caught.value)
    assert caught.value.__cause__ is None


@pytest.mark.parametrize(
    ("script", "maximum_stdout_bytes", "timeout_seconds", "expected_error"),
    [
        (
            "import sys; "
            "sys.stdout.write('a' * 64); sys.stdout.flush(); "
            "sys.stderr.write('a' * 64); sys.exit(19)",
            1024,
            2.0,
            "failed with exit code 19",
        ),
        (
            "import sys; sys.stdout.write('a' * 4096); sys.stdout.flush()",
            128,
            2.0,
            "response exceeded its byte limit",
        ),
        (
            "import sys, time; sys.stdout.write('a' * 64); sys.stdout.flush(); time.sleep(5)",
            1024,
            0.05,
            "timed out after 0.05 seconds",
        ),
    ],
)
def test_private_command_transport_bounds_and_redacts_all_failure_output(
    script: str,
    maximum_stdout_bytes: int,
    timeout_seconds: float,
    expected_error: str,
) -> None:
    """Exit, byte-limit, and timeout failures disclose no captured private output."""
    token = "a" * 64

    with pytest.raises(RelayError, match=expected_error) as caught:
        remote_cli_module._run_bounded_private_command(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            [sys.executable, "-c", script],
            timeout_seconds=timeout_seconds,
            maximum_stdout_bytes=maximum_stdout_bytes,
            label="private resolver",
        )

    assert token not in str(caught.value)


def test_local_authority_invocation_is_bounded_and_exact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cluster-side wrapper invokes only JARVIS's identity-complete JSON resolver."""
    commands: list[tuple[list[str], int | None]] = []
    payload = json.dumps(_service_runtime_authority_document(token="a" * 64))

    class FakeProvider:
        def __init__(self, *, jarvis_bin: str) -> None:
            assert jarvis_bin == "/released/bin/jarvis"

        def require_available(self) -> None:
            return None

        def run_command_streaming(
            self,
            command: list[str],
            *,
            timeout_seconds: int | None = None,
        ) -> subprocess.CompletedProcess[str]:
            commands.append((command, timeout_seconds))
            return subprocess.CompletedProcess(command, 0, stdout=payload, stderr="")

    monkeypatch.setattr(runtime_binding, "JarvisCdProvider", FakeProvider)
    digest = hashlib.sha256(("a" * 64).encode("ascii")).hexdigest()
    authority = runtime_binding.resolve_local_jarvis_service_runtime_authority(
        jarvis_bin="/released/bin/jarvis",
        execution_id="execution-1",
        pipeline_id="pipeline-1",
        package_id="paraview-1",
        service_instance_id="paraview-live-1",
        revision=3,
        token_sha256=digest,
    )

    assert authority.token_sha256 == digest
    assert commands == [
        (
            [
                "/released/bin/jarvis",
                "execution",
                "resolve-service-runtime-authority",
                "execution-1",
                "--pipeline-id",
                "pipeline-1",
                "--package-id",
                "paraview-1",
                "--service-instance-id",
                "paraview-live-1",
                "--revision",
                "3",
                "--token-sha256",
                digest,
                "+json",
            ],
            30,
        )
    ]


def test_hidden_relay_authority_command_emits_one_private_json_document(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The remote relay wrapper preserves the exact JARVIS authority wire document."""
    authority = runtime_binding.JarvisServiceRuntimeAuthority.model_validate(
        _service_runtime_authority_document(token="a" * 64)
    )
    captured: dict[str, object] = {}

    def resolve_local(**kwargs: object) -> runtime_binding.JarvisServiceRuntimeAuthority:
        captured.update(kwargs)
        return authority

    monkeypatch.setattr(cli_module, "resolve_local_jarvis_service_runtime_authority", resolve_local)
    monkeypatch.setenv("CLIO_RELAY_JARVIS_BIN", "/released/bin/jarvis")
    digest = authority.token_sha256
    result = CliRunner().invoke(
        app,
        [
            "jarvis-runtime-authority",
            "execution-1",
            "--pipeline-id",
            "pipeline-1",
            "--package-id",
            "paraview-1",
            "--service-instance-id",
            "paraview-live-1",
            "--revision",
            "3",
            "--token-sha256",
            digest,
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == _service_runtime_authority_document(token="a" * 64)
    assert captured == {
        "jarvis_bin": "/released/bin/jarvis",
        "execution_id": "execution-1",
        "pipeline_id": "pipeline-1",
        "package_id": "paraview-1",
        "service_instance_id": "paraview-live-1",
        "revision": 3,
        "token_sha256": digest,
    }


def test_model_facing_mcp_projection_redacts_runtime_bearer_token() -> None:
    """Every nested capability occurrence is absent from ordinary tool output."""
    token = "a" * 64
    projected = mcp_server_module._bounded_mcp_result(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        {
            "structured_result": {
                "service_runtimes": [
                    {
                        "authorization": {
                            "scheme": "bearer",
                            "token": token,
                            "audience": "paraview",
                        },
                        "message": f"capability={token}",
                    }
                ],
                "api_token": token,
            }
        }
    )
    assert projected["sensitive_values_redacted"] is True
    structured = cast(dict[str, Any], projected["structured_result"])
    services = cast(list[dict[str, Any]], structured["service_runtimes"])
    assert services[0]["authorization"] == "<redacted>"
    assert services[0]["message"] == "capability=<redacted>"
    assert structured["api_token"] == "<redacted>"
    assert token not in json.dumps(projected, sort_keys=True)


@pytest.mark.parametrize(
    "expected_registered_contract",
    [None, REGISTERED_JARVIS_USER_CONTRACT],
    ids=["built-in", "registered"],
)
def test_verified_jarvis_wait_preserves_public_bearer_fingerprint(
    tmp_path: Path,
    expected_registered_contract: str | None,
) -> None:
    """Exact built-in and registered JARVIS waits preserve the public runtime hash."""

    _queue, _definition, job, _artifact, envelope = _source_result(
        tmp_path,
        expected_registered_contract=expected_registered_contract,
    )
    artifact = ArtifactRef.model_validate(envelope["artifact"])
    parsed = mcp_server_module._decode_verified_mcp_result(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        envelope,
        artifact=artifact.model_dump(mode="json"),
        job_id=job.job_id,
    )
    receipt: dict[str, Any] = {}

    mcp_server_module._attach_terminal_mcp_evidence(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        receipt,
        source_job=job,
        last_error=None,
        artifacts=[artifact.model_dump(mode="json")],
        parsed_result=parsed,
    )

    original = cast(
        dict[str, Any],
        parsed.document["structured_result"]["service_runtimes"]["service_runtimes"][0],
    )
    projected = cast(
        dict[str, Any],
        receipt["mcp_result"]["structured_result"]["service_runtimes"]["service_runtimes"][0],
    )
    assert (
        projected["authorization"]
        == original["authorization"]
        == {
            "scheme": "bearer",
            "token_sha256": hashlib.sha256(("a" * 64).encode("ascii")).hexdigest(),
        }
    )
    assert projected == original
    assert (
        hashlib.sha256(
            json.dumps(projected, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        == hashlib.sha256(
            json.dumps(original, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
    )
    assert (
        len(
            json.dumps(
                receipt["mcp_result"],
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        <= mcp_server_module.MAX_INLINE_MCP_RESULT_BYTES
    )


@pytest.mark.parametrize(
    "authorization",
    [
        {"scheme": "bearer", "token": "a" * 64},
        {
            "scheme": "bearer",
            "token_sha256": "b" * 64,
            "audience": "paraview",
        },
        {"scheme": "bearer", "token_sha256": "not-a-canonical-sha256"},
        {"scheme": "basic", "token_sha256": "b" * 64},
    ],
    ids=["raw-token", "extra-key", "wrong-hash", "wrong-scheme"],
)
def test_jarvis_authorization_restore_rejects_non_public_descriptors(
    authorization: dict[str, str],
) -> None:
    """Even a trusted projection flag cannot restore a secret or widened shape."""

    projected = mcp_server_module._bounded_mcp_result(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        {
            "structured_result": {
                "service_runtimes": {
                    "service_runtimes": [
                        {
                            "schema_version": "jarvis.service-runtime.v2",
                            "authorization": authorization,
                        }
                    ]
                }
            }
        },
        preserve_verified_jarvis_authorization_descriptors=True,
    )

    runtime = projected["structured_result"]["service_runtimes"]["service_runtimes"][0]
    assert runtime["authorization"] == "<redacted>"
    assert projected["sensitive_values_redacted"] is True
    if "token" in authorization:
        assert authorization["token"] not in json.dumps(projected, sort_keys=True)


def test_arbitrary_mcp_cannot_expose_jarvis_shaped_authorization() -> None:
    """A lookalike authorization stays redacted without exact JARVIS source proof."""

    projected = mcp_server_module._bounded_mcp_result(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        {
            "structured_result": {
                "service_runtimes": {
                    "service_runtimes": [
                        {
                            "schema_version": "jarvis.service-runtime.v2",
                            "authorization": {
                                "scheme": "bearer",
                                "token_sha256": "b" * 64,
                            },
                        }
                    ]
                }
            }
        }
    )

    runtime = projected["structured_result"]["service_runtimes"]["service_runtimes"][0]
    assert runtime["authorization"] == "<redacted>"
    assert projected["sensitive_values_redacted"] is True


def test_waited_execution_exposes_compact_verified_binding_before_large_result(
    tmp_path: Path,
) -> None:
    """A waited query exposes the exact bind input before its bounded MCP payload."""
    _queue, _definition, job, _artifact, envelope = _source_result(tmp_path)
    document = json.loads(base64.b64decode(envelope["data"], validate=True).decode("utf-8"))
    document["structured_result"]["runtime_metadata"] = {"padding": "x" * 90_000}
    document["protocol_result"]["structuredContent"] = document["structured_result"]
    _replace_envelope_document(envelope, document)
    artifact = ArtifactRef.model_validate(envelope["artifact"])
    parsed = mcp_server_module._decode_verified_mcp_result(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        envelope,
        artifact=artifact.model_dump(mode="json"),
        job_id=job.job_id,
    )
    receipt: dict[str, Any] = {
        "cluster": job.cluster,
        "job_id": job.job_id,
        "state": "succeeded",
    }

    mcp_server_module._attach_terminal_mcp_evidence(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        receipt,
        source_job=job,
        last_error=None,
        artifacts=[artifact.model_dump(mode="json")],
        parsed_result=parsed,
    )

    expected = {
        "cluster": job.cluster,
        "source_job_id": job.job_id,
        "source_artifact_id": artifact.artifact_id,
        "package_id": "paraview-1",
        "package_name": "builtin.paraview",
        "service_instance_id": "paraview-live-1",
    }
    assert receipt["service_runtime_bindings"] == [expected]
    keys = list(receipt)
    assert keys.index("mcp_result_artifact") < keys.index("service_runtime_bindings")
    assert keys.index("service_runtime_bindings") < keys.index("mcp_result")
    serialized = mcp_server_module._serialize_tool_result(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        receipt
    )
    assert "a" * 64 not in serialized
    assert serialized.index('"service_runtime_bindings":') < serialized.index(
        '"mcp_result_artifact":'
    )
    assert serialized.index('"mcp_result_artifact":') < serialized.index('"mcp_result":')


def test_generic_mcp_wait_serializes_verified_result_before_bulk_evidence() -> None:
    """An agent sees the remote tool result before large logs and artifact inventories."""

    result: dict[str, Any] = {
        "job": {"job_id": "job_result_order", "metadata": {"padding": "j" * 20_000}},
        "terminal": True,
        "cluster": "ares",
        "route_revision": "a" * 64,
        "mcp_result_artifact": {
            "artifact_id": "artifact_result_order",
            "job_id": "job_result_order",
            "kind": "mcp_result",
            "sha256": "b" * 64,
        },
        "mcp_result": {
            "operation": "tools/call",
            "tool": "package_describe",
            "structured_result": {
                "agent_contract": {
                    "config": {"dataset_descriptor": "JSON string", "mode": "service"}
                }
            },
        },
        "logs": {"stdout": {"text": "l" * 20_000}, "stderr": {"text": ""}},
        "artifacts": [{"metadata": {"padding": "a" * 20_000}}],
    }

    serialized = mcp_server_module._serialize_tool_result(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        result
    )

    assert serialized.index('"mcp_result_artifact":') < serialized.index('"mcp_result":')
    assert serialized.index('"mcp_result":') < serialized.index('"job":')
    assert serialized.index('"mcp_result":') < serialized.index('"logs":')
    assert serialized.index('"mcp_result":') < serialized.index('"artifacts":')
    assert '"dataset_descriptor": "JSON string"' in serialized[:12_000]


def test_async_relay_wait_exposes_same_compact_verified_service_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later relay_wait preserves the same artifact-bound bind handoff as an inline wait."""
    queue, _definition, job, _artifact, envelope = _source_result(tmp_path)
    queue.update_job_metadata(job.job_id, {"padding": "j" * 90_000})
    document = json.loads(base64.b64decode(envelope["data"], validate=True).decode("utf-8"))
    document["structured_result"]["runtime_metadata"] = {"padding": "x" * 90_000}
    document["protocol_result"]["structuredContent"] = document["structured_result"]
    _replace_envelope_document(envelope, document)
    artifact = ArtifactRef.model_validate(envelope["artifact"])
    artifact_record = artifact.model_dump(mode="json")
    parsed = mcp_server_module._decode_verified_mcp_result(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        envelope,
        artifact=artifact_record,
        job_id=job.job_id,
    )

    def complete_artifacts(
        _queue: ClioCoreQueue,
        _job_id: str,
    ) -> list[dict[str, Any]]:
        return [artifact_record]

    def verified_result(
        _queue: ClioCoreQueue,
        _job_id: str,
        *,
        artifacts: list[dict[str, Any]] | None = None,
    ) -> mcp_server_module._VerifiedMcpResult:  # pyright: ignore[reportPrivateUsage]
        del artifacts
        return parsed

    def large_logs(
        _queue: ClioCoreQueue,
        _settings: RelaySettings,
        _job_id: str,
        *,
        limit: int,
    ) -> dict[str, Any]:
        assert limit == 32_768
        return {
            "stdout": {"text": "l" * limit, "eof": True},
            "stderr": {"text": "", "eof": True},
        }

    monkeypatch.setattr(
        mcp_server_module,
        "_complete_local_artifacts",
        complete_artifacts,
    )
    monkeypatch.setattr(
        mcp_server_module,
        "_verified_local_mcp_result",
        verified_result,
    )
    monkeypatch.setattr(mcp_server_module, "_job_logs", large_logs)

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "relay_wait",
                "arguments": {"job_id": job.job_id, "include_logs": True},
            },
        },
        queue=queue,
        settings=RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"),
        profile="user",
    )

    assert response is not None and "error" not in response, response
    receipt = response["result"]["structuredContent"]
    expected = {
        "cluster": job.cluster,
        "source_job_id": job.job_id,
        "source_artifact_id": artifact.artifact_id,
        "package_id": "paraview-1",
        "package_name": "builtin.paraview",
        "service_instance_id": "paraview-live-1",
    }
    assert receipt["service_runtime_bindings"] == [expected]
    assert receipt["mcp_result_artifact"]["artifact_id"] == artifact.artifact_id
    assert receipt["artifacts"] == [artifact_record]
    keys = list(receipt)
    assert keys.index("service_runtime_bindings") < keys.index("mcp_result")
    assert keys.index("mcp_result") < keys.index("artifacts")
    serialized = response["result"]["content"][0]["text"]
    assert serialized.count('"mcp_result":') == 1
    assert serialized.startswith('{"service_runtime_bindings":')
    assert serialized.index('"service_runtime_bindings":') < serialized.index('"job":')
    assert serialized.index('"service_runtime_bindings":') < serialized.index('"mcp_result":')
    assert serialized.index('"service_runtime_bindings":') < serialized.index('"logs":')
    assert serialized.index('"service_runtime_bindings":') < serialized.index('"artifacts":')
    assert serialized.index('"mcp_result_artifact":') < serialized.index('"job":')
    assert serialized.index('"mcp_result":') < serialized.index('"artifacts":')


def test_ssh_remote_async_relay_wait_exposes_verified_service_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The SSH transport returns the same exact bind handoff from a later relay_wait."""
    queue, source_definition, job, _artifact, envelope = _source_result(tmp_path)
    definition = source_definition.model_copy(update={"ssh_host": "cluster-login"})
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={definition.name: definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    artifact = ArtifactRef.model_validate(envelope["artifact"])
    artifact_record = artifact.model_dump(mode="json")
    parsed = mcp_server_module._decode_verified_mcp_result(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        envelope,
        artifact=artifact_record,
        job_id=job.job_id,
    )
    wait_commands: list[list[str]] = []

    def run_remote(_definition: ClusterDefinition, command: list[str]) -> str:
        wait_commands.append(command)
        return ""

    def remote_json(
        _definition: ClusterDefinition,
        command: list[str],
        _label: str,
    ) -> dict[str, Any]:
        assert command == ["job", "status", job.job_id]
        return {
            "job": job.model_dump(mode="json"),
            "relay_queue": {},
            "scheduler": [],
            "terminal": True,
        }

    def complete_remote_collection(
        _definition: ClusterDefinition,
        command: list[str],
        *,
        record_key: str,
        label: str,
    ) -> list[dict[str, Any]]:
        assert command == ["job", "list-artifacts", job.job_id]
        assert record_key == "artifacts"
        assert label == f"remote artifacts for {job.job_id}"
        return [artifact_record]

    def verified_result(
        _definition: ClusterDefinition,
        selected_job_id: str,
        artifacts: list[dict[str, Any]],
    ) -> mcp_server_module._VerifiedMcpResult:  # pyright: ignore[reportPrivateUsage]
        assert selected_job_id == job.job_id
        assert artifacts == [artifact_record]
        return parsed

    monkeypatch.setattr(mcp_server_module, "run_remote_clio", run_remote)
    monkeypatch.setattr(mcp_server_module, "_remote_json", remote_json)
    monkeypatch.setattr(
        mcp_server_module,
        "_complete_remote_collection",
        complete_remote_collection,
    )
    monkeypatch.setattr(mcp_server_module, "_verified_mcp_result", verified_result)

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "relay_wait",
                "arguments": {
                    "cluster": definition.name,
                    "job_id": job.job_id,
                    "route_revision": mcp_server_module._route_revision(definition),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    "include_logs": False,
                },
            },
        },
        queue=queue,
        settings=RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"),
        profile="user",
    )

    assert response is not None and "error" not in response, response
    receipt = response["result"]["structuredContent"]
    assert receipt["service_runtime_bindings"][0]["source_job_id"] == job.job_id
    assert receipt["service_runtime_bindings"][0]["source_artifact_id"] == artifact.artifact_id
    assert receipt["mcp_result_artifact"]["artifact_id"] == artifact.artifact_id
    assert list(receipt).index("service_runtime_bindings") < list(receipt).index("artifacts")
    assert wait_commands == [
        [
            "job",
            "wait",
            job.job_id,
            "--timeout-seconds",
            "600.0",
            "--poll-seconds",
            "2.0",
        ]
    ]


def test_owned_remote_async_relay_wait_exposes_verified_service_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The owned-session transport returns the same exact bind handoff from relay_wait."""
    queue, source_definition, job, _artifact, envelope = _source_result(tmp_path)
    definition = source_definition.model_copy(update={"ssh_host": "cluster-login"})
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={definition.name: definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    artifact = ArtifactRef.model_validate(envelope["artifact"])
    artifact_record = artifact.model_dump(mode="json")
    parsed = mcp_server_module._decode_verified_mcp_result(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        envelope,
        artifact=artifact_record,
        job_id=job.job_id,
    )
    requests: list[tuple[str, str]] = []

    class FakeOwnedSessionApiClient:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def __enter__(self) -> FakeOwnedSessionApiClient:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def request_json(
            self,
            *,
            method: str,
            path: str,
            query: dict[str, object] | None = None,
            body: dict[str, object] | None = None,
            response_timeout_seconds: float | None = None,
        ) -> object:
            del query, body, response_timeout_seconds
            requests.append((method, path))
            if method == "POST" and path == f"/jobs/{job.job_id}/wait":
                return job.model_dump(mode="json")
            if method == "GET" and path == f"/jobs/{job.job_id}/status":
                return {
                    "job": job.model_dump(mode="json"),
                    "relay_queue": {},
                    "scheduler": [],
                    "terminal": True,
                }
            raise AssertionError(f"unexpected owned request: {method} {path}")

    def complete_owned_collection(
        _client: object,
        *,
        path: str,
        record_key: str,
        label: str,
    ) -> list[dict[str, Any]]:
        assert path == f"/jobs/{job.job_id}/artifacts"
        assert record_key == "artifacts"
        assert label == f"owned remote artifacts for {job.job_id}"
        return [artifact_record]

    def verified_result(
        _client: object,
        selected_job_id: str,
        artifacts: list[dict[str, Any]],
    ) -> mcp_server_module._VerifiedMcpResult:  # pyright: ignore[reportPrivateUsage]
        assert selected_job_id == job.job_id
        assert artifacts == [artifact_record]
        return parsed

    monkeypatch.setattr(mcp_server_module, "OwnedSessionApiClient", FakeOwnedSessionApiClient)
    monkeypatch.setattr(
        mcp_server_module,
        "_complete_owned_collection",
        complete_owned_collection,
    )
    monkeypatch.setattr(mcp_server_module, "_verified_owned_mcp_result", verified_result)

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "relay_wait",
                "arguments": {
                    "cluster": definition.name,
                    "job_id": job.job_id,
                    "route_revision": mcp_server_module._route_revision(definition),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    "include_logs": False,
                },
            },
        },
        queue=queue,
        settings=RelaySettings(
            core_dir=tmp_path / "core",
            spool_dir=tmp_path / "spool",
            api_token="owner-token",
            owner_session_id="desktop-session",
            owner_session_generation_id="generation-1",
        ),
        profile="user",
    )

    assert response is not None and "error" not in response, response
    receipt = response["result"]["structuredContent"]
    assert receipt["service_runtime_bindings"][0]["source_job_id"] == job.job_id
    assert receipt["service_runtime_bindings"][0]["source_artifact_id"] == artifact.artifact_id
    assert receipt["mcp_result_artifact"]["artifact_id"] == artifact.artifact_id
    assert list(receipt).index("service_runtime_bindings") < list(receipt).index("artifacts")
    assert requests == [
        ("POST", f"/jobs/{job.job_id}/wait"),
        ("GET", f"/jobs/{job.job_id}/status"),
    ]


def test_multiple_ready_services_have_exact_unchanged_bindings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated package services remain individually selectable by their handoff."""
    queue, definition, job, _artifact, envelope = _source_result(tmp_path)
    document = json.loads(base64.b64decode(envelope["data"], validate=True).decode("utf-8"))
    services = document["structured_result"]["service_runtimes"]["service_runtimes"]
    second = dict(services[0])
    second.update(
        {
            "service_instance_id": "paraview-live-2",
            "revision": 1,
            "port": 18778,
        }
    )
    services.append(second)
    document["protocol_result"]["structuredContent"] = document["structured_result"]
    _replace_envelope_document(envelope, document)
    artifact = ArtifactRef.model_validate(envelope["artifact"])
    parsed = mcp_server_module._decode_verified_mcp_result(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        envelope,
        artifact=artifact.model_dump(mode="json"),
        job_id=job.job_id,
    )
    receipt: dict[str, Any] = {}
    mcp_server_module._attach_terminal_mcp_evidence(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        receipt,
        source_job=job,
        last_error=None,
        artifacts=[artifact.model_dump(mode="json")],
        parsed_result=parsed,
    )
    bindings = cast(list[dict[str, Any]], receipt["service_runtime_bindings"])
    assert [item["service_instance_id"] for item in bindings] == [
        "paraview-live-1",
        "paraview-live-2",
    ]

    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={definition.name: definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret")
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))
    _patch_connector_start(monkeypatch)
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "relay_bind_jarvis_runtime",
                "arguments": {"binding": bindings[1]},
            },
        },
        queue=queue,
        settings=RelaySettings(
            core_dir=tmp_path / "core",
            spool_dir=tmp_path / "spool",
            frpc_bin="frpc-test",
        ),
        profile="user",
    )

    assert response is not None and "error" not in response, response
    gateway = response["result"]["structuredContent"]["gateway_session"]
    persisted_binding = gateway["gateway"]["jarvis_runtime_binding"]
    assert persisted_binding["service_instance_id"] == "paraview-live-2"
    assert persisted_binding["source_relay_artifact_id"] == artifact.artifact_id


def test_bind_rejects_mixed_or_invalid_handoff_forms(tmp_path: Path) -> None:
    binding = {
        "cluster": "test-cluster",
        "source_job_id": "job-source",
        "source_artifact_id": "artifact-result",
        "package_id": "paraview-1",
        "package_name": "builtin.paraview",
        "service_instance_id": "paraview-live-1",
    }
    queue = ClioCoreQueue(tmp_path / "core")
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")

    mixed = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "relay_bind_jarvis_runtime",
                "arguments": {"binding": binding, "cluster": "other-cluster"},
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
    )
    invalid = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "relay_bind_jarvis_runtime",
                "arguments": {"binding": {**binding, "host": "caller.example"}},
            },
        },
        queue=queue,
        settings=settings,
        profile="user",
    )

    assert mixed is not None
    assert "cannot be mixed with legacy selectors: cluster" in mixed["error"]["message"]
    assert invalid is not None
    assert "binding is invalid" in invalid["error"]["message"]
    assert "host" in invalid["error"]["message"]


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
    # Browser attachment is intentionally desktop-local. Its immutable source
    # receipt still belongs to the remote owner session and must not be looked up
    # in the desktop queue merely because command placement is local.
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
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

    def locality_must_not_select_provenance(_definition: ClusterDefinition) -> bool:
        raise AssertionError("owner-session provenance must take precedence over CLI locality")

    monkeypatch.setattr(
        runtime_binding,
        "should_execute_on_cluster",
        locality_must_not_select_provenance,
    )
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


def test_dataset_descriptor_accepts_producer_null_optionals_but_canonicalizes_them() -> None:
    """clio-kit may serialize optional timestep and units as null on the wire."""
    canonical = {
        "schema_version": "jarvis.dataset-descriptor.v1",
        "dataset_id": "asteroid-first-five",
        "kind": "temporal-volume",
        "format": "vti-series",
        "members": [{"index": 0, "location": "/datasets/asteroid/frame-000.vti"}],
        "arrays": [
            {
                "name": "density",
                "association": "point",
                "components": 1,
            }
        ],
        "bounds": None,
        "source_artifact": None,
    }
    digest = hashlib.sha256(
        json.dumps(
            canonical,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    wire = json.loads(json.dumps(canonical))
    wire["members"][0]["timestep"] = None
    wire["arrays"][0]["units"] = None
    wire["fingerprint"] = {"algorithm": "sha256", "digest": digest}

    descriptor = JarvisDatasetDescriptor.model_validate(wire)
    dumped = descriptor.model_dump(mode="json")

    assert "timestep" not in dumped["members"][0]
    assert "units" not in dumped["arrays"][0]
    assert dumped["fingerprint"]["digest"] == digest


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


def test_registered_service_runtime_source_binds_exact_durable_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An operator-registered JARVIS route uses its contract and artifact proof."""
    queue, definition, job, artifact, envelope = _source_result(
        tmp_path,
        server="/opt/operator/bin/registered-jarvis-mcp",
        expected_registered_contract=REGISTERED_JARVIS_USER_CONTRACT,
    )
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))

    verified = resolve_jarvis_service_runtime(
        queue=queue,
        definition=definition,
        source_job_id=job.job_id,
        source_artifact_id=artifact.artifact_id,
        package_id="paraview-1",
        package_name="builtin.paraview",
    )

    assert verified.binding.source_relay_job_id == job.job_id
    assert verified.binding.source_relay_artifact_id == artifact.artifact_id
    assert verified.binding.jarvis_execution_id == "execution-1"
    assert verified.runtime.service_instance_id == "paraview-live-1"


def test_registered_service_runtime_source_rejects_wrong_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, definition, job, artifact, envelope = _source_result(
        tmp_path,
        expected_registered_contract="clio-kit-jarvis-user-v3.5",
    )
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))

    with pytest.raises(ValueError, match="supported JARVIS contract"):
        resolve_jarvis_service_runtime(
            queue=queue,
            definition=definition,
            source_job_id=job.job_id,
            source_artifact_id=artifact.artifact_id,
            package_id="paraview-1",
            package_name="builtin.paraview",
        )


def test_registered_service_runtime_source_rejects_mixed_builtin_lock_pin(
    tmp_path: Path,
) -> None:
    _queue, definition, job, _artifact, _envelope = _source_result(
        tmp_path,
        expected_registered_contract=REGISTERED_JARVIS_USER_CONTRACT,
    )
    spec = cast(McpCallSpec, job.spec).model_copy(
        update={"expected_jarvis_cd_lock_binding": jarvis_cd_lock_binding_expectation()}
    )
    mixed_job = job.model_copy(update={"spec": spec})

    with pytest.raises(ValueError, match="also supplied a built-in JARVIS-CD lock pin"):
        runtime_binding._validate_source_job(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            mixed_job,
            cluster=definition.name,
        )


def test_registered_service_runtime_source_rejects_result_contract_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, definition, job, artifact, envelope = _source_result(
        tmp_path,
        expected_registered_contract=REGISTERED_JARVIS_USER_CONTRACT,
    )
    document = json.loads(base64.b64decode(envelope["data"], validate=True).decode("utf-8"))
    document["expected_registered_contract"] = None
    _replace_envelope_document(envelope, document)
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))

    with pytest.raises(ValueError, match="result registered contract"):
        resolve_jarvis_service_runtime(
            queue=queue,
            definition=definition,
            source_job_id=job.job_id,
            source_artifact_id=artifact.artifact_id,
            package_id="paraview-1",
            package_name="builtin.paraview",
        )


def test_registered_service_runtime_source_rejects_mismatched_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, definition, job, artifact, envelope = _source_result(
        tmp_path,
        expected_registered_contract=REGISTERED_JARVIS_USER_CONTRACT,
    )
    document = json.loads(base64.b64decode(envelope["data"], validate=True).decode("utf-8"))
    document["server_artifact"]["install_artifact_sha256"] = "c" * 64
    _replace_envelope_document(envelope, document)
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))

    with pytest.raises(ValueError, match="not the immutable registered route"):
        resolve_jarvis_service_runtime(
            queue=queue,
            definition=definition,
            source_job_id=job.job_id,
            source_artifact_id=artifact.artifact_id,
            package_id="paraview-1",
            package_name="builtin.paraview",
        )


def test_registered_service_runtime_source_rejects_unverified_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server_artifact = _registered_jarvis_server_artifact()
    server_artifact["verified"] = False
    queue, definition, job, artifact, envelope = _source_result(
        tmp_path,
        expected_registered_contract=REGISTERED_JARVIS_USER_CONTRACT,
        server_artifact=server_artifact,
    )
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))

    with pytest.raises(ValueError, match="not the immutable registered route"):
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
    actual_jarvis_health = (
        ServiceRuntimeSupervisor._wait_for_jarvis_health  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    )
    _patch_connector_start(monkeypatch)
    monkeypatch.setattr(
        ServiceRuntimeSupervisor,
        "_wait_for_jarvis_health",
        actual_jarvis_health,
    )
    readiness_requests: list[tuple[str, dict[str, str] | None]] = []

    def read_readiness(
        url: str,
        *,
        headers: dict[str, str] | None,
        maximum_bytes: int | None,
        deadline: float | None = None,
    ) -> service_runtime_module._BoundedHttpResponse:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        del deadline
        readiness_requests.append((url, headers))
        assert maximum_bytes is None
        return service_runtime_module._BoundedHttpResponse(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            status_code=401 if headers is None else 200,
            headers=httpx.Headers(),
            content=b"",
        )

    monkeypatch.setattr(service_runtime_module, "_read_bounded_http_response", read_readiness)
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
    assert result["outcome"] == "ready"
    assert result["retry_selector"] is None
    assert result["scheduler_action"] == "none"
    assert result["relay_action"] == "none"
    connect_url = cast(str, result["connect_url"])
    local_port = urllib.parse.urlparse(connect_url).port
    assert local_port is not None
    assert local_port != 18777
    assert result["connect_url"] == f"http://127.0.0.1:{local_port}"
    assert result["health_url"] == f"http://127.0.0.1:{local_port}/healthz"
    assert readiness_requests == [
        (result["health_url"], None),
        (result["health_url"], {"Authorization": f"Bearer {'a' * 64}"}),
    ]
    assert result["stream_url"] == f"http://127.0.0.1:{local_port}/live-data"
    assert result["events_url"] == f"http://127.0.0.1:{local_port}/events"
    assert result["state_url"] == f"http://127.0.0.1:{local_port}/state"
    assert result["command_url"] == f"http://127.0.0.1:{local_port}/commands"
    assert result["scheduler_cancel_requested"] is False
    gateway = result["gateway_session"]
    assert result["gateway_session_id"] == gateway["session_id"]
    agent_visible = json.loads(response["result"]["content"][0]["text"])
    assert agent_visible["gateway_session_id"] == gateway["session_id"]
    assert agent_visible["gateway_session_id"] == agent_visible["gateway_session"]["session_id"]
    assert gateway["state"] == "ready"
    public_binding = gateway["gateway"]["jarvis_runtime_binding"]
    assert public_binding["source_relay_job_id"] == job.job_id
    assert public_binding["schema_version"] == "clio-relay.jarvis-service-runtime-binding.v2"
    assert public_binding["service_runtime_schema_version"] == "jarvis.service-runtime.v2"
    assert (
        public_binding["authorization_sha256"]
        == hashlib.sha256(("a" * 64).encode("ascii")).hexdigest()
    )
    assert (
        gateway["gateway"]["jarvis_runtime_binding"]["dataset_descriptor"] == _dataset_descriptor()
    )
    assert gateway["gateway"]["state_url"] == result["state_url"]
    assert gateway["gateway"]["command_url"] == result["command_url"]
    public_document = json.dumps(response, sort_keys=True)
    assert "a" * 64 not in public_document
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


def test_agent_bind_pending_replays_same_gateway_and_connector_intents(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A health observation miss is a successful checkpoint, not another bind."""
    queue, definition, job, artifact, envelope = _source_result(tmp_path)
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={definition.name: definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret")
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))
    launches = {"remote": 0, "desktop": 0}

    def start_remote(
        _self: ServiceRuntimeSupervisor,
        *,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
        node: str,
        proxy_name: str,
        ownership_intent: dict[str, object],
        allocation_provider: str | None = None,
        allocation_job_id: str | None = None,
    ) -> dict[str, object]:
        del spec, node, proxy_name, allocation_provider, allocation_job_id
        launches["remote"] += 1
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
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
        proxy_name: str,
        ownership_intent: dict[str, object],
    ) -> dict[str, object]:
        del spec, proxy_name
        launches["desktop"] += 1
        return {
            "owner": "clio-relay",
            "session_id": session.session_id,
            "pid": 555,
            "process_group_id": 555,
            "process_start_marker": "start-555",
            "connector_generation_id": ownership_intent["connector_generation_id"],
            "owner_token": ownership_intent["owner_token"],
            "config_path": ownership_intent["config_path"],
            "stdout_path": ownership_intent["stdout_path"],
            "stderr_path": ownership_intent["stderr_path"],
            "metadata_path": ownership_intent["metadata_path"],
        }

    health_observations = 0

    def observe_health(*_args: object, **_kwargs: object) -> None:
        nonlocal health_observations
        health_observations += 1
        if health_observations == 1:
            raise RelayError("bounded health observation did not become ready")

    def reconcile(
        self: ServiceRuntimeSupervisor,
        session: GatewaySession,
    ) -> GatewaySession:
        transport = cast(dict[str, object], session.gateway.get("transport", {}))
        roles = ("remote_connector", "desktop_connector")
        if not all(isinstance(transport.get(role), dict) for role in roles):
            return session
        gateway = dict(session.gateway)
        intents = dict(cast(dict[str, object], gateway["ownership_intents"]))
        for role in roles:
            intent = dict(cast(dict[str, object], intents[role]))
            intent["live_identity_verified"] = True
            intent.pop("reconciliation_error", None)
            intents[role] = intent
        gateway["ownership_intents"] = intents
        return self.queue.update_gateway_session(
            session.session_id,
            gateway=gateway,
            expected_updated_at=session.updated_at,
        )

    monkeypatch.setattr(ServiceRuntimeSupervisor, "_start_remote_connector", start_remote)
    monkeypatch.setattr(ServiceRuntimeSupervisor, "_start_local_visitor", start_local)
    monkeypatch.setattr(ServiceRuntimeSupervisor, "_wait_for_jarvis_health", observe_health)
    monkeypatch.setattr(ServiceRuntimeSupervisor, "_reconcile_ownership_intents", reconcile)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
    )
    arguments = {
        "cluster": definition.name,
        "source_job_id": job.job_id,
        "source_artifact_id": artifact.artifact_id,
        "package_id": "paraview-1",
        "package_name": "builtin.paraview",
    }

    first_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "relay_bind_jarvis_runtime", "arguments": arguments},
        },
        queue=queue,
        settings=settings,
        profile="user",
    )
    second_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "relay_bind_jarvis_runtime", "arguments": arguments},
        },
        queue=queue,
        settings=settings,
        profile="user",
    )

    assert first_response is not None and "error" not in first_response, first_response
    assert second_response is not None and "error" not in second_response, second_response
    first = cast(dict[str, Any], first_response["result"]["structuredContent"])
    second = cast(dict[str, Any], second_response["result"]["structuredContent"])
    assert first["outcome"] == "pending"
    assert first["scheduler_action"] == "none"
    assert first["relay_action"] == "none"
    assert first["scheduler_cancel_requested"] is False
    assert all(
        first[key] is None
        for key in (
            "connect_url",
            "health_url",
            "stream_url",
            "events_url",
            "state_url",
            "command_url",
        )
    )
    retry = cast(dict[str, Any], first["retry_selector"])
    assert retry["resume_tool"] == "relay_bind_jarvis_runtime"
    assert retry["binding"]["source_job_id"] == job.job_id
    pending_report = ServiceRuntimePendingResult(
        session=GatewaySession.model_validate(first["gateway_session"])
    ).to_live_validation_report()
    assert pending_report.status.value == "pending"
    assert {resource.kind for resource in pending_report.resources} == {
        "gateway_session",
        "jarvis_service_runtime",
        "scheduler_job",
    }
    assert second["outcome"] == "ready"
    assert second["retry_selector"] is None
    assert second["gateway_session_id"] == first["gateway_session_id"]
    assert launches == {"remote": 1, "desktop": 1}
    assert health_observations == 2
    assert len(queue.list_gateway_sessions(cluster=definition.name)) == 1


def test_pending_direct_jarvis_binding_does_not_invent_scheduler_submission() -> None:
    """A direct JARVIS service resumes by binding identity with no fake job handle."""
    session = GatewaySession(
        session_id="gateway_direct_jarvis_pending",
        cluster="direct-cluster",
        name="direct-paraview",
        state=GatewaySessionState.STARTING,
        scheduler="external",
        scheduler_job_id=None,
        queue_state="ready",
        gateway={
            "jarvis_runtime_binding": {
                "source_relay_job_id": "job_source",
                "source_relay_artifact_id": "artifact_source",
                "jarvis_execution_id": "execution-1",
                "package_id": "paraview-1",
                "package_name": "builtin.paraview",
                "service_instance_id": "paraview-live-1",
            }
        },
        metadata={"owner": "clio-relay"},
    )
    pending = ServiceRuntimePendingResult(session=session)

    selector = pending.retry_selector()
    report = pending.to_live_validation_report()

    assert selector["scheduler_job_id"] is None
    assert selector["resume_tool"] == "relay_bind_jarvis_runtime"
    assert selector["binding"] == {
        "cluster": "direct-cluster",
        "source_job_id": "job_source",
        "source_artifact_id": "artifact_source",
        "package_id": "paraview-1",
        "package_name": "builtin.paraview",
        "service_instance_id": "paraview-live-1",
    }
    assert [resource.kind for resource in report.resources] == [
        "gateway_session",
        "jarvis_service_runtime",
    ]
    assert all(resource.kind != "scheduler_submission" for resource in report.resources)


def test_owned_agent_bind_uses_shared_cluster_scoped_gateway_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The MCP bind path mirrors owner intake and locks through connector setup."""
    queue, definition, job, artifact, envelope = _source_result(tmp_path)
    remote_definition = definition.model_copy(update={"ssh_host": "relay.example.test"})
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={definition.name: remote_definition}).save(registry_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret")
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))
    verified = resolve_jarvis_service_runtime(
        queue=queue,
        definition=definition,
        source_job_id=job.job_id,
        source_artifact_id=artifact.artifact_id,
        package_id="paraview-1",
        package_name="builtin.paraview",
    )
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")

    with pytest.raises(
        QueueConflictError,
        match="owner session generation has no active admission state",
    ):
        queue.create_gateway_session(
            GatewaySession(
                cluster=definition.name,
                name="pre-fix-owned-bind",
                metadata={
                    "owner": "clio-relay",
                    "owner_session_id": "desktop-session",
                    "owner_session_generation_id": "generation-1",
                },
            )
        )

    def resolved_runtime(**_kwargs: object) -> Any:
        return verified

    monkeypatch.setattr(mcp_server_module, "resolve_jarvis_service_runtime", resolved_runtime)
    _patch_connector_start(monkeypatch)
    events: list[str] = []
    lock_held = False

    class RecordingLock:
        def __enter__(self) -> RecordingLock:
            nonlocal lock_held
            assert lock_held is False
            lock_held = True
            events.append("enter")
            return self

        def __exit__(self, *_args: object) -> None:
            nonlocal lock_held
            assert lock_held is True
            lock_held = False
            events.append("exit")

    def transition_lock(*, cluster: str, session_id: str) -> RecordingLock:
        assert (cluster, session_id) == (definition.name, "desktop-session")
        return RecordingLock()

    def remote_session_status(**_kwargs: object) -> dict[str, object]:
        assert lock_held is True
        events.append("session")
        return {
            "owner": "clio-relay",
            "session_id": "desktop-session",
            "session_generation_id": "generation-1",
            "running": True,
            "ownership_verified": True,
        }

    def remote_admission_status(
        _definition: ClusterDefinition,
        args: list[str],
    ) -> str:
        assert lock_held is True
        assert args[:2] == ["session", "admission-status"]
        events.append("admission")
        return json.dumps(
            {
                "schema_version": "clio-relay.owner-session-admission-status.v1",
                "owner_session_id": "desktop-session",
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

    monkeypatch.setattr(
        owner_session_admission_module,
        "owner_session_transition_lock",
        transition_lock,
    )
    monkeypatch.setattr(
        owner_session_admission_module,
        "status_remote_session",
        remote_session_status,
    )
    monkeypatch.setattr(
        owner_session_admission_module,
        "run_remote_clio",
        remote_admission_status,
    )
    original_bind = ServiceRuntimeSupervisor.bind_verified_jarvis_runtime

    def bind_while_locked(self: ServiceRuntimeSupervisor, **kwargs: Any) -> Any:
        assert lock_held is True
        assert kwargs["owner_session_admission_id"].startswith("desktop_")
        events.append("bind")
        return original_bind(self, **kwargs)

    monkeypatch.setattr(
        ServiceRuntimeSupervisor,
        "bind_verified_jarvis_runtime",
        bind_while_locked,
    )
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="frpc-test",
        api_token="api-token",
        owner_session_id="desktop-session",
        owner_session_generation_id="generation-1",
        owner_session_cluster=definition.name,
        remote_cluster=definition.name,
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

    assert response is not None and "error" not in response, response
    result = cast(dict[str, Any], response["result"]["structuredContent"])
    gateway = cast(dict[str, Any], result["gateway_session"])
    persisted = queue.get_gateway_session(cast(str, gateway["session_id"]))
    admission_id = cast(str, persisted.metadata["owner_session_admission_id"])
    assert admission_id.startswith("desktop_")
    assert persisted.metadata["owner_session_id"] == "desktop-session"
    assert persisted.metadata["owner_session_generation_id"] == "generation-1"
    assert gateway["metadata"]["owner_session_admission_id"] == admission_id
    assert result["scheduler_cancel_requested"] is False
    assert events == [
        "enter",
        "session",
        "admission",
        "session",
        "admission",
        "bind",
        "exit",
    ]


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


def test_owned_bind_rejects_missing_cluster_scoped_admission_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct callers cannot regress an owned bind to the raw session key."""
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

    with pytest.raises(
        ConfigurationError,
        match="owned JARVIS runtime binding requires owner_session_admission_id",
    ):
        supervisor.bind_verified_jarvis_runtime(
            name="paraview-live",
            verified=verified,
            owner_session_id="desktop-session",
            owner_session_generation_id="generation-1",
        )

    assert queue.list_gateway_sessions() == []


@pytest.mark.parametrize("wrong_admission", ["raw", "other-cluster"])
def test_owned_bind_rejects_wrong_cluster_scoped_admission_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    wrong_admission: str,
) -> None:
    """Direct callers cannot substitute raw or cross-cluster admission state."""
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
    admission_id = (
        "desktop-session"
        if wrong_admission == "raw"
        else owner_session_admission_module.desktop_owner_session_admission_id(
            cluster="another-cluster",
            session_id="desktop-session",
        )
    )

    with pytest.raises(
        ConfigurationError,
        match="admission id does not match its cluster/session identity",
    ):
        supervisor.bind_verified_jarvis_runtime(
            name="paraview-live",
            verified=verified,
            owner_session_id="desktop-session",
            owner_session_generation_id="generation-1",
            owner_session_admission_id=admission_id,
        )

    assert queue.list_gateway_sessions() == []


def test_jarvis_bind_serializes_connector_creation_against_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bound-runtime stop cannot close before its connector producer publishes."""
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
    connector_start_entered = threading.Event()
    stop_calling = threading.Event()
    teardown_prepared = threading.Event()
    original_remote_start = supervisor._start_remote_connector  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    original_prepare = supervisor._prepare_teardown_intent  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def pause_remote_start(**kwargs: object) -> dict[str, object]:
        connector_start_entered.set()
        assert stop_calling.wait(timeout=5)
        assert not teardown_prepared.wait(timeout=0.1)
        return original_remote_start(**kwargs)  # pyright: ignore[reportArgumentType]

    def observe_teardown_prepare(
        session: GatewaySession,
        *,
        cancel_scheduler_job: bool,
    ) -> GatewaySession:
        teardown_prepared.set()
        return original_prepare(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
        )

    monkeypatch.setattr(supervisor, "_start_remote_connector", pause_remote_start)
    monkeypatch.setattr(supervisor, "_prepare_teardown_intent", observe_teardown_prepare)

    with ThreadPoolExecutor(max_workers=2) as pool:
        bind_future = pool.submit(
            supervisor.bind_verified_jarvis_runtime,
            name="serialized-paraview-bind",
            verified=verified,
            desktop_bind_port=28777,
        )
        assert connector_start_entered.wait(timeout=5)
        sessions = queue.list_gateway_sessions(cluster=definition.name)
        assert len(sessions) == 1
        session_id = sessions[0].session_id

        def stop_while_binding() -> ServiceRuntimeStopResult:
            stop_calling.set()
            return supervisor.stop(session_id=session_id)

        stop_future = pool.submit(stop_while_binding)
        bound = bind_future.result(timeout=15)
        stopped = stop_future.result(timeout=15)

    assert bound.session.state is GatewaySessionState.READY
    assert stopped.session.state is GatewaySessionState.CLOSED
    assert teardown_prepared.is_set()
    assert stopped.canceled_scheduler_job is None


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


def test_quiesced_owner_keep_scheduler_recovery_reverifies_without_stopped_api(
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
    owner_session_id = "desktop-session"
    generation_id = "generation-1"
    admission_id = owner_session_admission_module.desktop_owner_session_admission_id(
        cluster=definition.name,
        session_id=owner_session_id,
    )
    queue.mirror_owner_session_generation_open(
        admission_id,
        session_generation_id=generation_id,
    )
    spec = ServiceRuntimeSpec(
        kind="jarvis-service-runtime",
        submit_command=None,
        deployment_driver="jarvis-bound",
        service_port=verified.runtime.port,
        desktop_bind_port=28777,
        scheduler="slurm",
    )
    session = queue.create_gateway_session(
        GatewaySession(
            cluster=definition.name,
            name="quiesced-owner-recovery",
            scheduler="slurm",
            scheduler_job_id="12345",
            gateway={
                "runtime_spec": spec.model_dump(mode="json"),
                "jarvis_runtime_binding": verified.binding.model_dump(mode="json"),
                "teardown_intent": {
                    "schema_version": "clio-relay.gateway-teardown-intent.v1",
                    "operation_id": "gateway_cleanup_00000000000000000000000000000000",
                    "gateway_session_id": "gateway_recovery",
                    "cancel_scheduler_job": False,
                    "created_at": "2026-07-18T03:00:00Z",
                },
            },
            metadata={
                "owner": "clio-relay",
                "owner_session_id": owner_session_id,
                "owner_session_generation_id": generation_id,
                "owner_session_admission_id": admission_id,
            },
            session_id="gateway_recovery",
        )
    )
    queue.set_owner_session_closing(
        admission_id,
        session_generation_id=generation_id,
        stop_worker=False,
        cancel_jobs=False,
        cancel_scheduler_jobs=False,
    )
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="api-token",
        owner_session_id=owner_session_id,
        owner_session_generation_id=generation_id,
        owner_session_cluster=definition.name,
    )
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster=definition.name,
        definition=definition,
        token="token",
        secret_key="secret",
    )
    observed_settings: list[RelaySettings | None] = []

    def stopped_api_then_direct_source(**kwargs: Any) -> Any:
        source_settings = kwargs.get("settings")
        observed_settings.append(source_settings)
        if source_settings is not None:
            raise RelayError("owned API generation is already stopped")
        return verified

    monkeypatch.setattr(
        service_runtime_module,
        "reverify_jarvis_service_runtime",
        stopped_api_then_direct_source,
    )

    submission = supervisor._verified_scheduler_submission(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        session,
        allow_quiesced_owner_source_recovery=True,
    )

    assert submission.provider == "slurm"
    assert submission.scheduler_job_id == "12345"
    assert observed_settings == [settings, None]
    observed_settings.clear()
    with pytest.raises(RelayError, match="already stopped"):
        supervisor._verified_scheduler_submission(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            session,
            allow_quiesced_owner_source_recovery=False,
        )
    assert observed_settings == [settings]


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
        upstream_authorization: str | None,
        ownership_intent: dict[str, object],
    ) -> dict[str, object]:
        assert len(capability) >= 43
        assert upstream_authorization == f"Bearer {'a' * 64}"
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

    def browser_health(*_args: object, **_kwargs: object) -> None:
        return None

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


def test_browser_attach_serializes_its_launch_against_runtime_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stop cannot commit absence while a browser proxy launch is in flight."""
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
        settings=RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"),
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
    launch_entered = threading.Event()
    allow_launch_return = threading.Event()
    stop_entered = threading.Event()

    def start_browser_proxy(
        *,
        session: GatewaySession,
        config: Any,
        capability: str,
        upstream_authorization: str | None,
        ownership_intent: dict[str, object],
    ) -> dict[str, object]:
        del capability, upstream_authorization
        launch_entered.set()
        assert allow_launch_return.wait(timeout=5)
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

    def stop_serialized(**_kwargs: object) -> ServiceRuntimeStopResult:
        stop_entered.set()
        raise RuntimeError("stop entered after browser launch transition")

    def browser_health_ready(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(supervisor, "_start_browser_proxy", start_browser_proxy)
    monkeypatch.setattr(supervisor, "_wait_for_browser_health", browser_health_ready)
    monkeypatch.setattr(supervisor, "_stop_serialized", stop_serialized)

    with ThreadPoolExecutor(max_workers=2) as executor:
        attach_future = executor.submit(
            supervisor.browser_attach,
            session_id=started.session.session_id,
            ttl_seconds=300,
        )
        assert launch_entered.wait(timeout=5)
        stop_future = executor.submit(
            supervisor.stop,
            session_id=started.session.session_id,
        )
        assert not stop_entered.wait(timeout=0.2)
        allow_launch_return.set()
        grant = attach_future.result(timeout=5)
        with pytest.raises(RuntimeError, match="stop entered after browser launch transition"):
            stop_future.result(timeout=5)

    assert grant.attachment_id.startswith("browser-")
    assert stop_entered.is_set()


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


def test_public_jarvis_bind_uses_the_durable_allocation_connector_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real allocation-scoped bind must not publish through a stale queue revision."""
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
        sleep=lambda _seconds: None,
    )

    def ssh(script: str) -> str:
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
        session = queue.list_gateway_sessions(cluster=definition.name)[0]
        remote_intent = cast(
            dict[str, object],
            cast(dict[str, object], session.gateway["ownership_intents"])["remote_connector"],
        )
        generation = str(remote_intent["connector_generation_id"])
        step_marker = (
            "clio-relay-connector-"
            + hashlib.sha256(f"{session.session_id}\x00{generation}".encode()).hexdigest()[:32]
        )
        return json.dumps(
            {
                "schema_version": "clio-relay.allocation-connector-start.v1",
                "session_id": session.session_id,
                "connector_generation_id": generation,
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

    def start_local(
        *,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
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

    def jarvis_health_ready(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(supervisor, "_ssh", ssh)
    monkeypatch.setattr(supervisor, "_start_local_visitor", start_local)
    monkeypatch.setattr(supervisor, "_wait_for_jarvis_health", jarvis_health_ready)

    started = supervisor.bind_verified_jarvis_runtime(
        name="paraview-live",
        verified=verified,
        desktop_bind_port=28777,
    )

    assert started.session.state is GatewaySessionState.READY
    remote = cast(
        dict[str, object],
        cast(dict[str, object], started.session.gateway["transport"])["remote_connector"],
    )
    assert remote["execution_scope"] == "scheduler_allocation"
    assert remote["scheduler_step_id"] == "12345.7"
    intents = cast(dict[str, object], started.session.gateway["ownership_intents"])
    remote_intent = cast(dict[str, object], intents["remote_connector"])
    assert remote_intent["state"] == "recorded"


def test_jarvis_bind_preserves_and_resumes_allocation_connector_after_lost_start_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lost SSH response stays pending and adopts the exact scheduler step on replay."""
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
        sleep=lambda _seconds: None,
    )
    events: list[str] = []

    def allocation_context() -> tuple[
        GatewaySession,
        dict[str, object],
        dict[str, object],
        dict[str, object],
    ]:
        session = queue.list_gateway_sessions(cluster=definition.name)[0]
        intent = cast(
            dict[str, object],
            cast(dict[str, object], session.gateway["ownership_intents"])["remote_connector"],
        )
        placement = cast(dict[str, object], intent["placement"])
        connector = {
            "owner": "clio-relay",
            "session_id": session.session_id,
            "execution_scope": "scheduler_allocation",
            "scheduler_provider": "slurm",
            "scheduler_native_id": "12345",
            "scheduler_step_marker": intent["scheduler_step_marker"],
            "connector_generation_id": intent["connector_generation_id"],
            "owner_token": intent["owner_token"],
            "config_path": "/runtime/remote.toml",
            "log_path": "/runtime/remote.log",
            "placement": placement,
        }
        step = {
            "schema_version": "clio-relay.scheduler-connector-step.v1",
            "scheduler": "slurm",
            "scheduler_job_id": "12345",
            "scheduler_step_id": "12345.7",
            "step_marker": intent["scheduler_step_marker"],
            "placement_host": "compute-07",
            "source": "slurm-squeue-step-marker",
            "verified": True,
            "observed_at": "2026-07-15T02:00:01Z",
        }
        return session, intent, connector, step

    def ssh(script: str) -> str:
        if "clio-relay scheduler connector-placement" in script:
            events.append("placement")
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
        if "__CLIO_WRITE_ALLOCATION_FRPC__" in script:
            events.append("start-side-effect")
            raise service_runtime_module._AmbiguousRemoteSideEffectError(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                "lost allocation connector start response"
            )
        _session, intent, connector, step = allocation_context()
        if "__CLIO_DISCOVER_CONNECTOR__" in script:
            events.append("discover")
            return json.dumps(
                {
                    "present": True,
                    "ownership_verified": True,
                    "connector": connector,
                }
            )
        if "connector-step-reconcile" in script:
            events.append("reconcile-step")
            return json.dumps(
                {
                    "schema_version": ("clio-relay.scheduler-connector-step-reconciliation.v1"),
                    "scheduler": "slurm",
                    "scheduler_job_id": "12345",
                    "step_marker": intent["scheduler_step_marker"],
                    "placement_host": "compute-07",
                    "found": True,
                    "step": step,
                }
            )
        if "connector-step-cancel" in script:
            events.append("cancel-step")
            return json.dumps(
                {
                    "scheduler": "slurm",
                    "scheduler_job_id": "12345",
                    "scheduler_step_id": "12345.7",
                    "cancel_requested": True,
                    "accepted": True,
                    "returncode": 0,
                }
            )
        if "connector-step-status" in script:
            status_count = sum(event.startswith("status-") for event in events)
            state = "active" if status_count < 2 else "absent"
            events.append(f"status-{state}")
            return json.dumps(
                {
                    "schema_version": "clio-relay.scheduler-connector-step-status.v1",
                    "scheduler": "slurm",
                    "scheduler_job_id": "12345",
                    "scheduler_step_id": "12345.7",
                    "placement_host": "compute-07",
                    "record_found": state != "absent",
                    "state": state,
                    "observed_host": "compute-07" if state != "absent" else None,
                    "source": "slurm-squeue-steps",
                    "verified": True,
                    "observed_at": "2026-07-15T02:00:02Z",
                }
            )
        raise AssertionError("unexpected remote connector script")

    monkeypatch.setattr(supervisor, "_ssh", ssh)
    first = supervisor.bind_verified_jarvis_runtime(
        name="paraview-lost-allocation-response",
        verified=verified,
        desktop_bind_port=28777,
    )

    assert isinstance(first, ServiceRuntimePendingResult)
    assert first.session.state is GatewaySessionState.STARTING
    assert first.scheduler_action == "none"
    assert first.relay_action == "none"
    assert events == ["placement", "start-side-effect"]
    assert "cancel-step" not in events
    remote_intent = cast(
        dict[str, object],
        cast(dict[str, object], first.session.gateway["ownership_intents"])["remote_connector"],
    )
    assert remote_intent["state"] == "starting"
    assert remote_intent["scheduler_native_id"] == "12345"

    def start_local(
        *,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
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
            "config_path": ownership_intent["config_path"],
            "stdout_path": ownership_intent["stdout_path"],
            "stderr_path": ownership_intent["stderr_path"],
            "metadata_path": ownership_intent["metadata_path"],
        }

    monkeypatch.setattr(supervisor, "_start_local_visitor", start_local)

    def health_ready(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(
        supervisor,
        "_wait_for_jarvis_health",
        health_ready,
    )
    resumed = supervisor.bind_verified_jarvis_runtime(
        name="paraview-lost-allocation-response",
        verified=verified,
        desktop_bind_port=28777,
    )

    assert not isinstance(resumed, ServiceRuntimePendingResult)
    assert resumed.session.session_id == first.session.session_id
    assert resumed.session.state is GatewaySessionState.READY
    assert len(queue.list_gateway_sessions(cluster=definition.name)) == 1
    assert events == [
        "placement",
        "start-side-effect",
        "discover",
        "reconcile-step",
        "status-active",
    ]
    assert "cancel-step" not in events


def test_jarvis_bind_preserves_local_connector_intent_after_lost_start_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A local sidecar and intent survive ambiguity without cleanup or replacement."""
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
        settings=RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"),
        queue=queue,
        cluster=definition.name,
        definition=definition,
        token="token",
        secret_key="secret",
        sleep=lambda _seconds: None,
    )
    stopped: list[int] = []

    def start_local_then_lose_response(
        *,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
        proxy_name: str,
        ownership_intent: dict[str, object],
    ) -> dict[str, object]:
        del spec, proxy_name
        connector = {
            "owner": "clio-relay",
            "session_id": session.session_id,
            "pid": 555,
            "process_group_id": 555,
            "process_start_marker": "start-555",
            "connector_generation_id": ownership_intent["connector_generation_id"],
            "owner_token": ownership_intent["owner_token"],
            "config_path": ownership_intent["config_path"],
            "stdout_path": ownership_intent["stdout_path"],
            "stderr_path": ownership_intent["stderr_path"],
            "metadata_path": ownership_intent["metadata_path"],
        }
        service_runtime_module._write_local_connector_sidecar(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            Path(str(ownership_intent["metadata_path"])),
            connector,
        )
        raise RelayError("lost desktop connector start response")

    def connector_owned(
        _connector: dict[str, object],
    ) -> tuple[str, str | None]:
        return "owned", None

    def stop_local(
        *,
        session_id: str,
        connector: dict[str, object],
        require_record: bool = False,
        absence_verified: bool = False,
    ) -> tuple[int | None, CleanupResource]:
        del session_id, require_record, absence_verified
        pid = int(cast(int, connector["pid"]))
        stopped.append(pid)
        return pid, CleanupResource(
            kind="desktop_connector",
            resource_id=str(pid),
            location="desktop",
            action="stop",
            ownership_verified=True,
            outcome="stopped",
            verified_after_operation=True,
        )

    monkeypatch.setattr(supervisor, "_start_local_visitor", start_local_then_lose_response)
    monkeypatch.setattr(service_runtime_module, "_local_connector_identity_status", connector_owned)
    monkeypatch.setattr(supervisor, "_stop_local_connector", stop_local)
    monkeypatch.setattr(supervisor, "_ssh", _fake_connector_ssh)

    pending = supervisor.bind_verified_jarvis_runtime(
        name="paraview-lost-desktop-response",
        verified=verified,
        desktop_bind_port=28777,
    )

    assert isinstance(pending, ServiceRuntimePendingResult)
    persisted = queue.get_gateway_session(pending.session.session_id)
    assert persisted.state is GatewaySessionState.STARTING
    assert stopped == []
    local_intent = cast(
        dict[str, object],
        cast(dict[str, object], persisted.gateway["ownership_intents"])["desktop_connector"],
    )
    assert local_intent["state"] == "starting"
    assert Path(str(local_intent["metadata_path"])).exists()
    assert persisted.metadata["runtime_observation_error"] == (
        "lost desktop connector start response"
    )


def test_jarvis_bind_rejects_policy_change_without_creating_another_gateway(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retry may change observation bounds, but not connector side-effect policy."""
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
        settings=RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"),
        queue=queue,
        cluster=definition.name,
        definition=definition,
        token="token",
        secret_key="secret",
        sleep=lambda _seconds: None,
    )

    def not_ready(*_args: object, **_kwargs: object) -> None:
        raise RelayError("health observation expired")

    monkeypatch.setattr(supervisor, "_wait_for_jarvis_health", not_ready)
    first = supervisor.bind_verified_jarvis_runtime(
        name="paraview-live",
        verified=verified,
        desktop_bind_port=28777,
        readiness_timeout_seconds=1,
    )
    assert isinstance(first, ServiceRuntimePendingResult)

    with pytest.raises(
        ConfigurationError,
        match="already bound with a different immutable policy",
    ):
        supervisor.bind_verified_jarvis_runtime(
            name="renamed-paraview-live",
            verified=verified,
            desktop_bind_port=28777,
            # Observation policy is deliberately different and remains mutable.
            readiness_timeout_seconds=5,
        )

    sessions = queue.list_gateway_sessions(cluster=definition.name)
    assert [session.session_id for session in sessions] == [first.session.session_id]
    assert sessions[0].name == "paraview-live"
    assert sessions[0].state is GatewaySessionState.STARTING


def test_jarvis_bind_fails_closed_on_definitive_scheduler_terminal_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A proven terminal allocation is failure evidence, not a pending observation."""
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
        sleep=lambda _seconds: None,
    )

    def fail_connector(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise RelayError("scheduler connector placement is unavailable")

    def completed_status(**_kwargs: object) -> SchedulerStatus:
        return SchedulerStatus(
            scheduler="slurm",
            scheduler_job_id="12345",
            phase=SchedulerPhase.COMPLETED,
            record_found=True,
            active_record_found=False,
        )

    def no_connectors(session: GatewaySession) -> GatewaySession:
        return session

    monkeypatch.setattr(supervisor, "_start_remote_connector", fail_connector)
    monkeypatch.setattr(supervisor, "_poll_scheduler_provider", completed_status)
    monkeypatch.setattr(supervisor, "_reconcile_ownership_intents", no_connectors)

    with pytest.raises(
        RelayError,
        match="scheduler job reached a terminal state before its verified JARVIS service",
    ):
        supervisor.bind_verified_jarvis_runtime(
            name="paraview-terminal",
            verified=verified,
            desktop_bind_port=28777,
        )

    persisted = queue.list_gateway_sessions(cluster=definition.name)[0]
    assert persisted.state is GatewaySessionState.FAILED
    assert persisted.scheduler_job_id == "12345"
    assert "terminal state" in str(persisted.metadata["last_error"])


@pytest.mark.parametrize("operation", ["stop", "detach"])
def test_cleanup_refuses_a_missing_scheduler_id_that_remains_in_the_jarvis_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    """Cleanup cannot close or detach without an exact scheduler disposition."""
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
        settings=RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"),
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
    queue.update_gateway_session(
        started.session.session_id,
        scheduler_job_id=None,
    )
    local_cleanup_calls: list[str] = []
    ssh_calls: list[str] = []

    def local_stop(**_kwargs: object) -> tuple[int | None, CleanupResource]:
        local_cleanup_calls.append("called")
        raise AssertionError("connector cleanup ran before scheduler identity validation")

    def ssh(script: str) -> str:
        ssh_calls.append(script)
        raise AssertionError("remote side effect ran before scheduler identity validation")

    monkeypatch.setattr(supervisor, "_stop_local_connector", local_stop)
    monkeypatch.setattr(supervisor, "_ssh", ssh)
    cleanup = supervisor.stop if operation == "stop" else supervisor.detach

    with pytest.raises(RelayError, match="scheduler job identity disagrees"):
        cleanup(session_id=started.session.session_id)

    persisted = queue.get_gateway_session(started.session.session_id)
    assert persisted.state is GatewaySessionState.READY
    assert local_cleanup_calls == []
    assert ssh_calls == []


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
    expected_registered_contract: str | None = None,
    server_artifact: dict[str, Any] | None = None,
) -> tuple[ClioCoreQueue, ClusterDefinition, RelayJob, ArtifactRef, dict[str, Any]]:
    queue = ClioCoreQueue(tmp_path / "core")
    resolved_server_artifact = (
        (
            _registered_jarvis_server_artifact()
            if expected_registered_contract is not None
            else verified_jarvis_server_artifact()
        )
        if server_artifact is None
        else server_artifact
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
                expected_registered_contract=expected_registered_contract,
                expected_jarvis_cd_lock_binding=(
                    jarvis_cd_lock_binding_expectation()
                    if bind_relay_jarvis and expected_registered_contract is None
                    else None
                ),
                tool=tool,
                arguments={
                    "pipeline_id": "pipeline-1",
                    **({} if tool == "jarvis_run" else {"execution_id": "execution-1"}),
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
    service = _service_runtime_document()
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
        "expected_registered_contract": spec.expected_registered_contract,
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


def _registered_jarvis_server_artifact() -> dict[str, Any]:
    """Return an immutable discovered artifact for an operator-registered route."""
    return {
        **verified_jarvis_server_artifact(),
        "install_spec": "/releases/clio_kit-2.6.2-py3-none-any.whl",
    }


def _service_runtime_document() -> dict[str, Any]:
    """Return one authenticated service-runtime v2 test document."""
    return {
        "schema_version": "jarvis.service-runtime.v2",
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
        "authorization": {
            "scheme": "bearer",
            "token_sha256": hashlib.sha256(("a" * 64).encode("ascii")).hexdigest(),
        },
        "dataset_descriptor": _dataset_descriptor(),
        "message": "service ready",
        "observed_at_epoch": 1_784_080_860.125,
    }


def _service_runtime_authority_document(*, token: str) -> dict[str, Any]:
    """Return one exact private JARVIS authority response for the test runtime."""
    return {
        "authorization": {"scheme": "bearer", "token": token},
        "execution_id": "execution-1",
        "package_id": "paraview-1",
        "pipeline_id": "pipeline-1",
        "revision": 3,
        "schema_version": "jarvis.execution.service-runtime-authority.v1",
        "service_instance_id": "paraview-live-1",
        "token_sha256": hashlib.sha256(token.encode("ascii")).hexdigest(),
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

    def jarvis_health(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(
        ServiceRuntimeSupervisor,
        "_wait_for_jarvis_health",
        jarvis_health,
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
