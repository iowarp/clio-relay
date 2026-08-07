"""MCP-flow tests for JARVIS local-input staging through the built-in JARVIS door."""

from __future__ import annotations

import base64
import copy
import hashlib
from pathlib import Path
from typing import Any, cast

import pytest

from clio_relay import mcp_server as mcp_server_module
from clio_relay.cluster_config import ClusterDefinition, cluster_route_revision
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.input_staging import (
    JarvisPackageInputContract,
    jarvis_package_input_contract_record,
    jarvis_package_input_route,
)
from clio_relay.jarvis_input_plane import builtin_jarvis_staging_route, prepare_jarvis_inputs
from clio_relay.mcp_server import McpSessionState, handle_request
from clio_relay.models import (
    ArtifactRef,
    ArtifactUse,
    InputArtifactSpec,
    JobKind,
    JobState,
    McpCallSpec,
    RelayJob,
    deterministic_input_artifact_id,
)
from clio_relay.remote_mcp import VirtualRemoteMcpCatalog

JSON = dict[str, Any]
BUILTIN_ARTIFACT_DIGEST = "a" * 64


class _BuiltinJarvisHarness:
    """Deterministic owned-session boundary for built-in JARVIS door dispatch."""

    def __init__(self, *, settings: RelaySettings) -> None:
        self.settings = settings
        self.submitted_payloads: list[JSON] = []
        self.ingest_bodies: list[JSON] = []
        self.nonterminal_tools: set[str] = set()
        self.declare_input_binding = True
        self._submission_by_key: dict[str, RelayJob] = {}

    def submit_owned(self, **kwargs: object) -> RelayJob:
        """Record the exact submitted payload and return one durable owned job."""
        payload = copy.deepcopy(cast(JSON, kwargs["payload"]))
        assert kwargs["path"] == "/jobs/jarvis-mcp-call"
        self.submitted_payloads.append(payload)
        idempotency_key = cast(str, payload["idempotency_key"])
        existing = self._submission_by_key.get(idempotency_key)
        if existing is not None:
            return existing
        raw_uses = cast(list[object], payload.get("used_artifact_refs", []))
        job = RelayJob(
            job_id=_durable_id("job", idempotency_key),
            cluster=cast(str, payload["cluster"]),
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server="clio-kit",
                server_args=["mcp-server", "jarvis"],
                expected_server_artifact_digest=cast(
                    str | None,
                    payload.get("expected_server_artifact_digest"),
                ),
                tool=cast(str, payload["tool"]),
                arguments=copy.deepcopy(cast(JSON, payload["arguments"])),
                timeout_seconds=cast(int | None, payload.get("timeout_seconds")),
            ),
            idempotency_key=idempotency_key,
            used_artifact_refs=[ArtifactUse.model_validate(item) for item in raw_uses],
            metadata={
                "owner": "clio-relay",
                "owner_session_id": self.settings.owner_session_id,
                "owner_session_generation_id": self.settings.owner_session_generation_id,
            },
        )
        self._submission_by_key[idempotency_key] = job
        return job

    def submission_result(
        self,
        job: RelayJob,
        *,
        definition: ClusterDefinition,
        wait_for_terminal_result: bool,
        **_kwargs: object,
    ) -> JSON:
        """Return a terminal, artifact-verified result for the requested JARVIS call."""
        assert isinstance(job.spec, McpCallSpec)
        if job.spec.tool == "jarvis_describe":
            assert wait_for_terminal_result is True
        if job.spec.tool in {"jarvis_add_step", "jarvis_edit_step"} and job.used_artifact_refs:
            assert wait_for_terminal_result is True
        if job.spec.tool in self.nonterminal_tools:
            return {
                "cluster": definition.name,
                "job_id": job.job_id,
                "state": "queued",
                "kind": "mcp_call",
                "terminal": False,
                "remote": True,
                "route_revision": cluster_route_revision(definition),
            }
        result: JSON = {
            "cluster": definition.name,
            "job_id": job.job_id,
            "state": "succeeded",
            "kind": "mcp_call",
            "terminal": True,
            "remote": True,
            "route_revision": cluster_route_revision(definition),
            "last_error": None,
        }
        if job.spec.tool == "jarvis_describe":
            result["mcp_result"] = _describe_mcp_result(
                declare_input_binding=self.declare_input_binding
            )
        elif job.spec.tool == "jarvis_add_step":
            package_name = cast(str, job.spec.arguments["package_name"])
            result["mcp_result"] = {
                "tool": "jarvis_add_step",
                "structured_result": {
                    "pipeline_id": job.spec.arguments["pipeline_id"],
                    "appended": package_name,
                    "step_id": job.spec.arguments.get("step_id") or package_name.rsplit(".", 1)[-1],
                    "configured": True,
                    "config": job.spec.arguments.get("config", {}),
                },
            }
        return result

    def ingest(self, body: JSON) -> JSON:
        """Return the exact hidden-ingest response for one bounded content snapshot."""
        self.ingest_bodies.append(copy.deepcopy(body))
        data = base64.b64decode(cast(str, body["data_base64"]), validate=True)
        logical_name = cast(str, body["logical_name"])
        sha256 = cast(str, body["sha256"])
        assert len(data) == body["size_bytes"]
        assert hashlib.sha256(data).hexdigest() == sha256
        idempotency_key = cast(str, body["idempotency_key"])
        producer = RelayJob(
            job_id=_durable_id("job", idempotency_key),
            cluster="ares",
            kind=JobKind.INPUT_INGEST,
            state=JobState.SUCCEEDED,
            spec=InputArtifactSpec(
                logical_name=logical_name,
                size_bytes=len(data),
                sha256=sha256,
            ),
            idempotency_key=idempotency_key,
            metadata={
                "owner": "clio-relay",
                "owner_session_id": self.settings.owner_session_id,
                "owner_session_generation_id": self.settings.owner_session_generation_id,
            },
        )
        artifact = ArtifactRef(
            artifact_id=deterministic_input_artifact_id(producer.job_id),
            job_id=producer.job_id,
            uri=f"file:///srv/clio-relay/{producer.job_id}/inputs/{logical_name}",
            kind="input",
            size_bytes=len(data),
            sha256=sha256,
        )
        return {
            "job": producer.model_dump(mode="json"),
            "artifact": artifact.model_dump(mode="json"),
        }


def test_builtin_jarvis_input_flows_from_describe_through_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The built-in door stages its declared file, rewrites config, and pins the run."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    source = workspace / "in.lj"
    source.write_text("units lj\nrun 5000\n", encoding="utf-8")
    settings, definition, harness = _configured_flow(tmp_path, workspace=workspace)
    _patch_flow(monkeypatch, definition=definition, harness=harness)
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)

    described = _describe_package(queue, settings=settings, session=session)
    assert "error" not in described
    assert len(session.jarvis_package_input_contracts) == 2
    contracts = list(session.jarvis_package_input_contracts.values())
    assert {contract.package_names for contract in contracts} == {("builtin.lammps", "lammps")}
    assert all(contract.local_file_settings[0].canonical_name == "script" for contract in contracts)

    add_arguments: JSON = {
        "cluster": "ares",
        "pipeline_id": "science-run",
        "package_name": "lammps",
        "config": {
            "script": "in.lj",
            "out": ".",
            "restart_path": "researcher-owned/restart.bin",
        },
        "idempotency_key": "builtin-add-lammps",
    }
    added = _call(
        queue,
        settings=settings,
        session=session,
        name="jarvis_add_step",
        arguments=add_arguments,
    )
    assert "error" not in added
    assert add_arguments["config"]["script"] == "in.lj"
    add_payload = harness.submitted_payloads[-1]
    forwarded_config = cast(JSON, cast(JSON, add_payload["arguments"])["config"])
    assert forwarded_config["script"].startswith("/srv/clio-relay/job_")
    assert forwarded_config["script"].endswith("/inputs/in.lj")
    assert forwarded_config["out"] == "."
    assert forwarded_config["restart_path"] == "researcher-owned/restart.bin"
    add_uses = cast(list[JSON], add_payload["used_artifact_refs"])
    source_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    assert len(add_uses) == 1
    assert add_uses[0]["sha256"] == source_sha256
    assert add_uses[0]["provenance"]["evidence"] == "schema-arg"
    assert add_uses[0]["provenance"]["arg"] == "script"
    assert len(harness.ingest_bodies) == 1

    # A restarted MCP process must recover the exact lineage from durable storage.
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)

    ran = _call(
        queue,
        settings=settings,
        session=session,
        name="jarvis_run",
        arguments={
            "cluster": "ares",
            "pipeline_id": "science-run",
            "idempotency_key": "builtin-run-lammps",
        },
    )
    assert "error" not in ran
    run_payload = harness.submitted_payloads[-1]
    assert run_payload["tool"] == "jarvis_run"
    assert run_payload["used_artifact_refs"] == add_uses
    assert len(harness.ingest_bodies) == 1


def test_builtin_edit_step_restages_tracked_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tracked built-in edit stages the newly named file and rewrites its config."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    first = workspace / "first.lj"
    second = workspace / "second.lj"
    first.write_text("run 1\n", encoding="utf-8")
    second.write_text("run 2\n", encoding="utf-8")
    settings, definition, harness = _configured_flow(tmp_path, workspace=workspace)
    _patch_flow(monkeypatch, definition=definition, harness=harness)
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)
    _describe_package(queue, settings=settings, session=session)
    added = _call(
        queue,
        settings=settings,
        session=session,
        name="jarvis_add_step",
        arguments={
            "cluster": "ares",
            "pipeline_id": "edited-pipeline",
            "package_name": "lammps",
            "config": {"script": "first.lj"},
            "idempotency_key": "builtin-edited-add",
        },
    )
    assert "error" not in added

    edited = _call(
        queue,
        settings=settings,
        session=session,
        name="jarvis_edit_step",
        arguments={
            "cluster": "ares",
            "pipeline_id": "edited-pipeline",
            "step_id": "lammps",
            "config": {"script": "second.lj"},
            "operation": "edit",
            "idempotency_key": "builtin-edited-v2",
        },
    )
    assert "error" not in edited
    edit_payload = harness.submitted_payloads[-1]
    edit_config = cast(JSON, cast(JSON, edit_payload["arguments"])["config"])
    assert edit_config["script"].endswith("/inputs/second.lj")
    assert cast(list[JSON], edit_payload["used_artifact_refs"])[0]["sha256"] == (
        hashlib.sha256(second.read_bytes()).hexdigest()
    )
    assert len(harness.ingest_bodies) == 2


def test_builtin_run_refuses_content_that_drifted_after_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A run cannot claim freshly changed bytes the cluster configuration never received."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    source = workspace / "in.lj"
    source.write_text("run 1\n", encoding="utf-8")
    settings, definition, harness = _configured_flow(tmp_path, workspace=workspace)
    _patch_flow(monkeypatch, definition=definition, harness=harness)
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)
    _describe_package(queue, settings=settings, session=session)
    added = _call(
        queue,
        settings=settings,
        session=session,
        name="jarvis_add_step",
        arguments={
            "cluster": "ares",
            "pipeline_id": "drift-pipeline",
            "package_name": "lammps",
            "config": {"script": "in.lj"},
            "idempotency_key": "builtin-drift-add",
        },
    )
    assert "error" not in added
    source.write_text("run 2\n", encoding="utf-8")

    ran = _call(
        queue,
        settings=settings,
        session=session,
        name="jarvis_run",
        arguments={
            "cluster": "ares",
            "pipeline_id": "drift-pipeline",
            "idempotency_key": "builtin-drift-run",
        },
    )

    assert ran["error"]["code"] == -32000
    assert "jarvis_edit_step" in ran["error"]["message"]
    assert "lammps.script" in ran["error"]["message"]
    assert harness.submitted_payloads[-1]["tool"] == "jarvis_add_step"
    assert len(harness.ingest_bodies) == 1


def test_builtin_add_step_without_describe_refuses_before_reading_local_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The built-in door requires route-bound package semantics before configuration."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "in.lj").write_text("run 1\n", encoding="utf-8")
    settings, definition, harness = _configured_flow(tmp_path, workspace=workspace)
    _patch_flow(monkeypatch, definition=definition, harness=harness)

    def forbidden_snapshot(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("local file was read without package schema evidence")

    monkeypatch.setattr(
        "clio_relay.input_staging.snapshot_owned_regular_file",
        forbidden_snapshot,
    )
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)

    response = _call(
        queue,
        settings=settings,
        session=session,
        name="jarvis_add_step",
        arguments={
            "cluster": "ares",
            "pipeline_id": "no-description",
            "package_name": "lammps",
            "config": {"script": "in.lj"},
        },
    )

    assert response["error"]["code"] == -32000
    assert "requires a successful jarvis_describe" in response["error"]["message"]
    assert harness.submitted_payloads == []
    assert harness.ingest_bodies == []


def test_builtin_add_step_refuses_unreadable_declared_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A declared local file that cannot be read never reaches the cluster verbatim."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    settings, definition, harness = _configured_flow(tmp_path, workspace=workspace)
    _patch_flow(monkeypatch, definition=definition, harness=harness)
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)
    _describe_package(queue, settings=settings, session=session)

    response = _call(
        queue,
        settings=settings,
        session=session,
        name="jarvis_add_step",
        arguments={
            "cluster": "ares",
            "pipeline_id": "missing-input",
            "package_name": "lammps",
            "config": {"script": "absent.lj"},
            "idempotency_key": "builtin-missing-add",
        },
    )

    assert response["error"]["code"] == -32000
    assert harness.ingest_bodies == []
    assert [payload["tool"] for payload in harness.submitted_payloads] == ["jarvis_describe"]


def test_builtin_add_step_refuses_without_configured_input_workspace_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Staging a declared file requires the operator-configured private workspace root."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "in.lj").write_text("run 1\n", encoding="utf-8")
    settings, definition, harness = _configured_flow(
        tmp_path,
        workspace=workspace,
        workspace_root_configured=False,
    )
    _patch_flow(monkeypatch, definition=definition, harness=harness)
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)
    _describe_package(queue, settings=settings, session=session)

    response = _call(
        queue,
        settings=settings,
        session=session,
        name="jarvis_add_step",
        arguments={
            "cluster": "ares",
            "pipeline_id": "unrooted",
            "package_name": "lammps",
            "config": {"script": "in.lj"},
            "idempotency_key": "builtin-unrooted-add",
        },
    )

    assert response["error"]["code"] == -32000
    assert "CLIO_RELAY_INPUT_WORKSPACE_ROOT" in response["error"]["message"]
    assert harness.ingest_bodies == []
    assert [payload["tool"] for payload in harness.submitted_payloads] == ["jarvis_describe"]


def test_builtin_add_step_refuses_without_owned_remote_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Byte ingest requires the relay-owned remote session that carries it."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "in.lj").write_text("run 1\n", encoding="utf-8")
    settings, definition, harness = _configured_flow(tmp_path, workspace=workspace)
    _patch_flow(monkeypatch, definition=definition, harness=harness)
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)
    _describe_package(queue, settings=settings, session=session)
    unowned = _settings(
        tmp_path,
        workspace=workspace,
        owner_session_id=None,
    )

    response = _call(
        queue,
        settings=unowned,
        session=session,
        name="jarvis_add_step",
        arguments={
            "cluster": "ares",
            "pipeline_id": "unowned",
            "package_name": "lammps",
            "config": {"script": "in.lj"},
            "idempotency_key": "builtin-unowned-add",
        },
    )

    assert response["error"]["code"] == -32000
    assert "relay-owned remote session" in response["error"]["message"]
    assert harness.ingest_bodies == []
    assert [payload["tool"] for payload in harness.submitted_payloads] == ["jarvis_describe"]


def test_builtin_add_step_forwards_settings_without_input_bindings_verbatim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A package whose settings declare no binding keeps cluster-side paths untouched."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    settings, definition, harness = _configured_flow(tmp_path, workspace=workspace)
    _patch_flow(monkeypatch, definition=definition, harness=harness, declare_input_binding=False)
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)
    _describe_package(queue, settings=settings, session=session)

    added = _call(
        queue,
        settings=settings,
        session=session,
        name="jarvis_add_step",
        arguments={
            "cluster": "ares",
            "pipeline_id": "unbound-pipeline",
            "package_name": "lammps",
            "config": {"script": "/cluster/absolute/in.lj", "out": "."},
            "idempotency_key": "builtin-unbound-add",
        },
    )

    assert "error" not in added
    payload = harness.submitted_payloads[-1]
    assert cast(JSON, payload["arguments"])["config"] == {
        "script": "/cluster/absolute/in.lj",
        "out": ".",
    }
    assert payload.get("used_artifact_refs", []) == []
    assert harness.ingest_bodies == []


def test_builtin_package_without_declared_inputs_configures_without_owned_session(
    tmp_path: Path,
) -> None:
    """A package that stages nothing needs no owned session and no lineage route."""
    settings = _settings(tmp_path, workspace=None, owner_session_id=None)
    queue = ClioCoreQueue(settings.core_dir)
    definition = ClusterDefinition(name="ares", ssh_host="ares-login")
    route = builtin_jarvis_staging_route(
        cluster="ares",
        cluster_route_revision=cluster_route_revision(definition),
        expected_server_artifact_digest=BUILTIN_ARTIFACT_DIGEST,
        remote_tool_name="jarvis_add_step",
    )
    package_route = jarvis_package_input_route(
        cluster=route.cluster,
        server_name=route.server_name,
        cluster_route_revision=route.cluster_route_revision,
        registration_revision=route.registration_revision,
        expected_server_artifact_digest=route.expected_server_artifact_digest,
        package_name="lammps",
    )
    queue.put_jarvis_package_input_contract(
        jarvis_package_input_contract_record(
            route=package_route,
            contract=JarvisPackageInputContract(
                cache_key=package_route.identity_sha256(),
                package_names=("lammps",),
                local_file_settings=(),
                settings_sha256="0" * 64,
            ),
        )
    )

    plan = prepare_jarvis_inputs(
        {
            "pipeline_id": "unbound",
            "package_name": "lammps",
            "config": {"script": "/cluster/absolute/in.lj"},
        },
        route=route,
        queue=queue,
        settings=settings,
        session=None,
        resolve_definition=lambda _cluster: definition,
        requested_idempotency_key=None,
    )

    assert plan.arguments["config"] == {"script": "/cluster/absolute/in.lj"}
    assert plan.automatic_artifact_uses == ()
    assert plan.pipeline_route is None
    assert plan.require_terminal_wait is False


def _settings(
    tmp_path: Path,
    *,
    workspace: Path | None,
    owner_session_id: str | None = "desktop-session",
) -> RelaySettings:
    return RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        owner_session_id=owner_session_id,
        owner_session_generation_id=(
            "generation_0123456789abcdef0123456789abcdef" if owner_session_id else None
        ),
        owner_session_cluster="ares" if owner_session_id else None,
        api_token="session-token",
        input_workspace_root=workspace,
        input_file_max_bytes=1024,
        input_total_max_bytes=4096,
    )


def _configured_flow(
    tmp_path: Path,
    *,
    workspace: Path,
    workspace_root_configured: bool = True,
) -> tuple[RelaySettings, ClusterDefinition, _BuiltinJarvisHarness]:
    definition = ClusterDefinition(name="ares", ssh_host="ares-login")
    settings = _settings(tmp_path, workspace=workspace if workspace_root_configured else None)
    return settings, definition, _BuiltinJarvisHarness(settings=settings)


def _patch_flow(
    monkeypatch: pytest.MonkeyPatch,
    *,
    definition: ClusterDefinition,
    harness: _BuiltinJarvisHarness,
    declare_input_binding: bool = True,
) -> None:
    harness.declare_input_binding = declare_input_binding
    route_revision = cluster_route_revision(definition)

    def catalog(**_kwargs: object) -> VirtualRemoteMcpCatalog:
        return VirtualRemoteMcpCatalog(
            revision="d" * 64,
            tools={},
            issues=(),
            cluster_route_revisions={"ares": route_revision},
            jarvis_artifact_bindings={"ares": BUILTIN_ARTIFACT_DIGEST},
        )

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
            body: JSON,
            **_kwargs: object,
        ) -> object:
            assert method == "POST"
            assert path == "/input-artifacts/ingest"
            return harness.ingest(body)

    def remote_definition(_cluster: str) -> ClusterDefinition:
        return definition

    def artifact_binding(_cluster: str) -> str:
        return BUILTIN_ARTIFACT_DIGEST

    def execute_remotely(_definition: ClusterDefinition) -> bool:
        return True

    monkeypatch.setattr(mcp_server_module, "_remote_mcp_catalog", catalog)
    monkeypatch.setattr(mcp_server_module, "_remote_cluster_definition", remote_definition)
    monkeypatch.setattr(mcp_server_module, "jarvis_mcp_artifact_binding", artifact_binding)
    monkeypatch.setattr(mcp_server_module, "should_execute_on_cluster", execute_remotely)
    monkeypatch.setattr(mcp_server_module, "submit_owned_session_job", harness.submit_owned)
    monkeypatch.setattr(
        mcp_server_module,
        "_owned_session_submission_result",
        harness.submission_result,
    )
    monkeypatch.setattr(
        "clio_relay.input_staging.OwnedSessionApiClient",
        FakeOwnedSessionApiClient,
    )


def _advertise(
    queue: ClioCoreQueue,
    *,
    settings: RelaySettings,
    session: McpSessionState,
) -> None:
    response = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        settings=settings,
        profile="user",
        session=session,
    )
    assert response is not None and "error" not in response


def _call(
    queue: ClioCoreQueue,
    *,
    settings: RelaySettings,
    session: McpSessionState,
    name: str,
    arguments: JSON,
) -> JSON:
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        },
        queue=queue,
        settings=settings,
        profile="user",
        session=session,
    )
    assert response is not None
    return response


def _describe_package(
    queue: ClioCoreQueue,
    *,
    settings: RelaySettings,
    session: McpSessionState,
    idempotency_key: str = "builtin-describe-package",
) -> JSON:
    response = _call(
        queue,
        settings=settings,
        session=session,
        name="jarvis_describe",
        arguments={
            "cluster": "ares",
            "target": "package",
            "package_name": "lammps",
            "idempotency_key": idempotency_key,
        },
    )
    assert "error" not in response
    return response


def _describe_mcp_result(*, declare_input_binding: bool = True) -> JSON:
    script_setting: JSON = {
        "name": "script",
        "aliases": [],
        "type": "str",
        "required": False,
        "nullable": False,
        "default": "",
    }
    if declare_input_binding:
        script_setting["input_binding"] = {
            "schema_version": "jarvis.configuration-input-binding.v1",
            "kind": "local_file",
            "structure": "regular_file",
        }
    return {
        "tool": "jarvis_describe",
        "structured_result": {
            "result": {
                "target": "package",
                "package": {
                    "name": "builtin.lammps",
                    "short_name": "lammps",
                    "settings": [
                        script_setting,
                        {
                            "name": "out",
                            "type": "str",
                            "required": False,
                            "nullable": False,
                            "default": ".",
                        },
                    ],
                },
            }
        },
    }


def _durable_id(prefix: str, value: str) -> str:
    return f"{prefix}_{hashlib.sha256(value.encode('utf-8')).hexdigest()[:32]}"
