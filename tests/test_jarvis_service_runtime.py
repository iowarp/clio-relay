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
from typing import Any, Literal, cast

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

import clio_relay.adapters.worker.http_api_routes_session as http_api_routes_session_module
import clio_relay.jarvis_service_runtime as runtime_binding
import clio_relay.owner_session_admission as owner_session_admission_module
import clio_relay.remote_cli as remote_cli_module
import clio_relay.service_runtime_connector_identity as service_runtime_connector_identity_module
import clio_relay.service_runtime_core as service_runtime_core_module
import clio_relay.service_runtime_observation as service_runtime_observation_module
import clio_relay.service_runtime_types as service_runtime_types_module
from clio_relay.adapters.worker.cli import worker_app as app
from clio_relay.adapters.worker.core_queue import ClioCoreQueue
from clio_relay.adapters.worker.http_api import create_app
from clio_relay.adapters.worker.remote_connection import validate_channel_request
from clio_relay.adapters.worker.session_lifecycle import CleanupResource
from clio_relay.cluster_config import (
    CLUSTER_REGISTRY_ENV,
    ClusterDefinition,
    ClusterRegistry,
    FrpTransportConfig,
    cluster_route_revision,
)
from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_mcp import jarvis_cd_lock_binding_expectation, server_artifact_digest
from clio_relay.jarvis_service_runtime import (
    RELAY_JARVIS_RUNTIME_BINDING_SCHEMA,
    JarvisDatasetDescriptor,
    JarvisServiceRuntime,
    resolve_jarvis_service_runtime,
    reverify_jarvis_service_runtime,
)
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
from clio_relay.service_runtime import (
    ServiceRuntimePendingResult,
    ServiceRuntimeStopResult,
    ServiceRuntimeSupervisor,
)
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
        service_runtime_core_module,
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
        validate_channel_request(
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

    # split/http-api-w3 (iowarp/clio-relay#231):
    # resolve_owned_jarvis_runtime_authority moved to
    # http_api_routes_session.py -- the patch target follows the move.
    monkeypatch.setattr(
        http_api_routes_session_module,
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

    # split/http-api-w3 (iowarp/clio-relay#231):
    # resolve_owned_jarvis_runtime_authority moved to
    # http_api_routes_session.py -- the patch target follows the move.
    monkeypatch.setattr(
        http_api_routes_session_module,
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


def test_registered_service_runtime_source_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Relay no longer binds a result that came through a registered remote MCP route."""
    queue, definition, job, artifact, envelope = _source_result(
        tmp_path,
        server="/opt/operator/bin/registered-jarvis-mcp",
        expected_registered_contract=REGISTERED_JARVIS_USER_CONTRACT,
    )
    monkeypatch.setattr(runtime_binding, "read_artifact_bytes", _envelope_reader(envelope))

    with pytest.raises(ValueError, match="registered remote MCP route"):
        resolve_jarvis_service_runtime(
            queue=queue,
            definition=definition,
            source_job_id=job.job_id,
            source_artifact_id=artifact.artifact_id,
            package_id="paraview-1",
            package_name="builtin.paraview",
        )


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


def test_internal_bind_override_rejects_occupied_loopback_port(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An internal/operator override must fail before creating connector state on collision."""
    queue, definition, job, artifact, envelope = _source_result(tmp_path, execution_mode="direct")
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
    queue, definition, job, artifact, envelope = _source_result(tmp_path, execution_mode="direct")
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


def test_retired_allocation_connector_cleanup_reports_the_step_to_cancel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue, definition, job, artifact, envelope = _source_result(tmp_path, execution_mode="direct")
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
    # #231 class-mixin split: _verified_scheduler_submission (the caller, via
    # detach/stop) now lives in service_runtime_observation.py, not
    # service_runtime.py -- patch the symbol where it is looked up.
    original_reverify = service_runtime_observation_module.reverify_jarvis_service_runtime

    def tracked_reverify(**kwargs: Any) -> Any:
        observed = original_reverify(**kwargs)
        reverified.append(observed.binding.service_instance_id)
        reverified_settings.append(kwargs.get("settings"))
        return observed

    monkeypatch.setattr(
        service_runtime_observation_module,
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
    allocation_commands: list[str] = []

    def allocation_ssh(script: str) -> str:
        allocation_commands.append(script)
        raise AssertionError(f"unexpected allocation connector script: {script}")

    monkeypatch.setattr(supervisor, "_ssh", allocation_ssh)

    detached = supervisor.detach(session_id=started.session.session_id)
    detached_remote = next(item for item in detached.resources if item.kind == "remote_connector")
    assert detached_remote.resource_id == "12345.7"
    assert detached_remote.outcome == "refused"
    assert detached_remote.residual is True
    assert "scancel 12345.7" in str(detached_remote.detail)
    assert any("scancel 12345.7" in error for error in detached.errors)

    stopped = supervisor.stop(session_id=started.session.session_id)
    assert stopped.canceled_scheduler_job is None
    stopped_remote = next(item for item in stopped.resources if item.kind == "remote_connector")
    assert stopped_remote.resource_id == "12345.7"
    assert stopped_remote.outcome == "refused"
    assert stopped_remote.residual is True
    assert "scancel 12345.7" in str(stopped_remote.detail)
    assert any("scancel 12345.7" in error for error in stopped.errors)
    assert allocation_commands == []


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

    # #231 class-mixin split: _verified_scheduler_submission lives in
    # service_runtime_observation.py, not service_runtime.py -- patch the
    # symbol where it is looked up.
    monkeypatch.setattr(
        service_runtime_observation_module,
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


def test_jarvis_bind_refuses_a_scheduler_backed_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Slurm-backed JARVIS service is served with serve, not an allocation connector."""
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
    assert verified.binding.scheduler_native_id == "12345"
    supervisor = ServiceRuntimeSupervisor(
        settings=RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"),
        queue=queue,
        cluster=definition.name,
        definition=definition,
        token="token",
        secret_key="secret",
        sleep=lambda _seconds: None,
    )

    def no_ssh(script: str) -> str:
        raise AssertionError(f"a refused bind ran a remote command: {script}")

    monkeypatch.setattr(supervisor, "_ssh", no_ssh)

    with pytest.raises(ConfigurationError, match="serve the service with the serve tool"):
        supervisor.bind_verified_jarvis_runtime(
            name="paraview-live",
            verified=verified,
            desktop_bind_port=28777,
        )

    assert queue.list_gateway_sessions(cluster=definition.name) == []


def test_jarvis_bind_preserves_local_connector_intent_after_lost_start_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A local sidecar and intent survive ambiguity without cleanup or replacement."""
    queue, definition, job, artifact, envelope = _source_result(tmp_path, execution_mode="direct")
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
        service_runtime_connector_identity_module._write_local_connector_sidecar(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
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
    monkeypatch.setattr(
        service_runtime_connector_identity_module,
        "_local_connector_identity_status",
        connector_owned,
    )
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
    queue, definition, job, artifact, envelope = _source_result(tmp_path, execution_mode="direct")
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


@pytest.mark.parametrize("operation", ["stop", "detach"])
def test_cleanup_refuses_a_missing_scheduler_id_that_remains_in_the_jarvis_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    """Cleanup cannot close or detach without an exact scheduler disposition."""
    queue, definition, job, artifact, envelope = _source_result(tmp_path, execution_mode="direct")
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
    binding = {
        **cast(dict[str, object], started.session.gateway["jarvis_runtime_binding"]),
        "scheduler_native_id": "12345",
    }
    queue.update_gateway_session(
        started.session.session_id,
        scheduler_job_id=None,
        gateway={**started.session.gateway, "jarvis_runtime_binding": binding},
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


def _source_result(
    tmp_path: Path,
    *,
    tool: str = "jarvis_get_execution",
    include_service_runtimes: bool | None = True,
    server: str = "/home/cluster/.local/bin/clio-kit",
    bind_relay_jarvis: bool = True,
    expected_registered_contract: str | None = None,
    server_artifact: dict[str, Any] | None = None,
    execution_mode: Literal["direct", "scheduler"] = "scheduler",
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
    digest = server_artifact_digest(resolved_server_artifact)
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
        execution_mode=execution_mode,
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
    execution_mode: Literal["direct", "scheduler"] = "scheduler",
) -> dict[str, Any]:
    scheduled = execution_mode == "scheduler"
    submission = {
        "schema_version": "jarvis.scheduler.submission.v1",
        "execution_id": "execution-1",
        "provider": "slurm",
        "scheduler_job_id": "12345",
        "scheduler_cluster": "test-cluster",
        "submitted": True,
        "identity_source": "scheduler_submit_api",
    }
    ownership: dict[str, Any] = {
        "mode": execution_mode,
        "scheduler_provider": "slurm" if scheduled else None,
        "scheduler_native_id": "12345" if scheduled else None,
        "cluster": "test-cluster" if scheduled else None,
    }
    handle = {
        "schema_version": "jarvis.execution.handle.v1",
        "execution_id": "execution-1",
        "pipeline_id": "pipeline-1",
        **ownership,
    }
    record = {
        "schema_version": "jarvis.execution.record.v1",
        "execution_id": "execution-1",
        "pipeline_id": "pipeline-1",
        "pipeline_name": "pipeline-1",
        **ownership,
        "state": "running",
        "submitted": scheduled,
        "terminal": False,
        "created_at": "2026-07-15T02:00:00Z",
        "updated_at": "2026-07-15T02:01:00Z",
        "return_code": None,
        "error": None,
        "metadata": {"submission": submission} if scheduled else {},
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
    ) -> dict[str, object]:
        del spec, node, proxy_name
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
