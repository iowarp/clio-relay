"""JARVIS service runtime models and the retired binding.

Relay no longer binds JARVIS service runtimes (their ``mcp_call`` source job
kind is retired). These tests keep the stored-record models readable, prove a
stored binding is refused with what a person must stop.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, cast

import pytest

import clio_relay.jarvis_service_runtime as runtime_binding
from clio_relay.adapters.worker.core_queue import ClioCoreQueue
from clio_relay.adapters.worker.http_api import create_app
from clio_relay.cluster_config import (
    ClusterDefinition,
    FrpTransportConfig,
)
from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_service_runtime import (
    JarvisDatasetDescriptor,
    JarvisServiceRuntime,
    JarvisServiceRuntimeBinding,
    retired_jarvis_binding_message,
    reverify_jarvis_service_runtime,
)
from clio_relay.models import (
    GatewaySession,
    GatewaySessionState,
)
from clio_relay.service_runtime import ServiceRuntimePendingResult, ServiceRuntimeSupervisor
from tests.test_service_runtime import (
    FakeRunner,
    _started_session,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

_RETIRED = "relay no longer binds JARVIS service runtimes"


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


def test_owned_session_api_cannot_start_without_api_token(tmp_path: Path) -> None:
    """An owned session API must fail before serving without authentication."""
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


def test_retired_binding_names_the_scheduler_job_to_cancel() -> None:
    """A stored scheduler-backed binding tells the person which job to cancel."""
    message = retired_jarvis_binding_message(
        _binding_document(scheduler_provider="slurm", scheduler_native_id="12345")
    )

    assert message.startswith(_RETIRED)
    assert "Cancel slurm job 12345 (JARVIS execution execution-1) yourself." in message


def test_retired_binding_names_a_direct_execution_to_stop() -> None:
    """A stored direct binding names the JARVIS execution and its service."""
    message = retired_jarvis_binding_message(_binding_document())

    assert "Stop JARVIS execution execution-1 (service paraview-live-1) yourself." in message


@pytest.mark.parametrize("document", [None, "binding", {}, {"jarvis_execution_id": ""}])
def test_retired_binding_refuses_an_unreadable_record_plainly(document: object) -> None:
    """A damaged binding still refuses, and says it names no execution."""
    message = retired_jarvis_binding_message(document)

    assert message.startswith(_RETIRED)
    assert "names no JARVIS execution" in message


def test_reverify_refuses_every_stored_binding(tmp_path: Path) -> None:
    """Re-verification has no source job left to read, so it always refuses."""
    with pytest.raises(ValueError, match="Cancel slurm job 12345") as caught:
        reverify_jarvis_service_runtime(
            queue=ClioCoreQueue(tmp_path / "core"),
            definition=_definition(),
            binding_document=_binding_document(
                scheduler_provider="slurm", scheduler_native_id="12345"
            ),
        )

    assert str(caught.value).startswith(_RETIRED)


def _bound_session(tmp_path: Path) -> tuple[ServiceRuntimeSupervisor, ClioCoreQueue, str, str]:
    """Start a scheduler-backed runtime, then give it a stored JARVIS binding."""
    queue, settings, definition, runner, session_id = _started_session(tmp_path)
    session = queue.get_gateway_session(session_id)
    job_id = str(session.scheduler_job_id)
    binding = _binding_document(
        scheduler_provider=session.scheduler,
        scheduler_native_id=job_id,
    )
    queue.update_gateway_session(
        session_id,
        gateway={**session.gateway, "jarvis_runtime_binding": binding},
    )
    runner.inputs.clear()
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster=definition.name,
        definition=definition,
        token="token",
        secret_key="secret",
        runner=runner,
        sleep=lambda _seconds: None,
    )
    return supervisor, queue, session_id, job_id


@pytest.mark.parametrize("cancel_scheduler_job", [False, True])
def test_stop_reports_a_stored_binding_job_as_a_residual(
    tmp_path: Path,
    cancel_scheduler_job: bool,
) -> None:
    """Stop never cancels a JARVIS-bound job; it names the job for the person."""
    supervisor, _queue, session_id, job_id = _bound_session(tmp_path)
    runner = cast(FakeRunner, supervisor.runner)

    result = supervisor.stop(session_id=session_id, cancel_scheduler_job=cancel_scheduler_job)

    scheduler = next(item for item in result.resources if item.kind == "scheduler_job")
    assert result.canceled_scheduler_job is None
    assert scheduler.resource_id == job_id
    assert scheduler.outcome == "refused"
    assert scheduler.residual is True
    assert _RETIRED in (scheduler.detail or "")
    assert f"job {job_id} (JARVIS execution execution-1)" in (scheduler.detail or "")
    assert runner.canceled_jobs == []
    assert runner.provider_canceled_jobs == []


def test_detach_reports_a_stored_binding_job_as_a_residual(tmp_path: Path) -> None:
    """Detach keeps the job and names it, rather than re-verifying a dead source."""
    supervisor, _queue, session_id, job_id = _bound_session(tmp_path)

    result = supervisor.detach(session_id=session_id)

    scheduler = next(item for item in result.resources if item.kind == "scheduler_job")
    assert scheduler.resource_id == job_id
    assert scheduler.residual is True
    assert _RETIRED in (scheduler.detail or "")


def test_attach_refuses_a_stored_binding(tmp_path: Path) -> None:
    """Attach cannot recreate a connector for a binding relay no longer verifies."""
    supervisor, queue, session_id, job_id = _bound_session(tmp_path)

    with pytest.raises(RelayError, match=_RETIRED) as caught:
        supervisor.attach(session_id=session_id)

    assert f"job {job_id}" in str(caught.value)
    assert queue.get_gateway_session(session_id).state is GatewaySessionState.READY


def test_pending_result_refuses_a_stored_binding() -> None:
    """A pending binding has no resume tool now; its selector refuses plainly."""
    session = GatewaySession(
        session_id="gateway_direct_jarvis_pending",
        cluster="direct-cluster",
        name="direct-paraview",
        state=GatewaySessionState.STARTING,
        scheduler="external",
        scheduler_job_id=None,
        queue_state="ready",
        gateway={"jarvis_runtime_binding": _binding_document()},
        metadata={"owner": "clio-relay"},
    )
    pending = ServiceRuntimePendingResult(session=session)

    with pytest.raises(RelayError, match="Stop JARVIS execution execution-1"):
        pending.retry_selector()


def _definition() -> ClusterDefinition:
    return ClusterDefinition(
        name="test-cluster",
        ssh_host="relay.example.test",
        scheduler_provider="slurm",
        frp_transport=FrpTransportConfig(
            protocol="wss",
            server_addr="frps.example.org",
            server_port=443,
        ),
    )


def _binding_document(**updates: object) -> dict[str, Any]:
    """Return one stored v2 binding record, as an old session holds it."""
    descriptor = JarvisDatasetDescriptor.model_validate(_dataset_descriptor())
    token_digest = hashlib.sha256(("a" * 64).encode("ascii")).hexdigest()
    binding = JarvisServiceRuntimeBinding(
        source_relay_job_id="job_source",
        source_relay_artifact_id="artifact_source",
        source_relay_artifact_sha256="b" * 64,
        source_tool="jarvis_get_execution",
        jarvis_execution_id="execution-1",
        package_id="paraview-1",
        package_name="builtin.paraview",
        service_instance_id="paraview-live-1",
        service_revision=3,
        service_report_sha256="c" * 64,
        service_runtime_schema_version="jarvis.service-runtime.v2",
        authorization_sha256=token_digest,
        dataset_descriptor_sha256=descriptor.fingerprint.digest,
        dataset_descriptor=descriptor,
    )
    return {**binding.model_dump(mode="json"), **updates}


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
