"""MCP-flow tests for schema-driven JARVIS local-input staging."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

from clio_relay import mcp_server as mcp_server_module
from clio_relay.cluster_config import (
    ClusterDefinition,
    RemoteMcpServerConfig,
    cluster_route_revision,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.input_staging import REGISTERED_JARVIS_CONTRACT_ID
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
from clio_relay.remote_mcp import (
    RemoteMcpRoute,
    RemoteMcpToolSchema,
    VirtualRemoteMcpCatalog,
    VirtualRemoteMcpTool,
    remote_mcp_registration_revision,
)

JSON = dict[str, Any]
DESCRIBE_ALIAS = "remote_jarvis_demo_jarvis_describe"
ADD_STEP_ALIAS = "remote_jarvis_demo_jarvis_add_step"
RUN_ALIAS = "remote_jarvis_demo_jarvis_run"
GET_EXECUTION_ALIAS = "remote_jarvis_demo_jarvis_get_execution"


class _McpFlowHarness:
    """Deterministic owned-session boundary used by focused MCP dispatch tests."""

    def __init__(self, *, settings: RelaySettings) -> None:
        self.settings = settings
        self.submitted_payloads: list[JSON] = []
        self.ingest_bodies: list[JSON] = []
        self.corrupt_ingest_producer_kind = False
        self.nonterminal_tools: set[str] = set()
        self._submission_by_key: dict[str, tuple[str, RelayJob]] = {}

    def submit_owned(self, **kwargs: object) -> RelayJob:
        """Apply remote idempotency semantics and retain the exact submitted payload."""
        payload = copy.deepcopy(cast(JSON, kwargs["payload"]))
        self.submitted_payloads.append(payload)
        idempotency_key = cast(str, payload["idempotency_key"])
        payload_digest = _digest(payload)
        existing = self._submission_by_key.get(idempotency_key)
        if existing is not None:
            existing_digest, existing_job = existing
            if existing_digest != payload_digest:
                raise QueueConflictError("idempotency key was reused with a different job payload")
            return existing_job
        raw_uses = cast(list[object], payload.get("used_artifact_refs", []))
        job = RelayJob(
            job_id=_durable_id("job", idempotency_key),
            cluster=cast(str, payload["cluster"]),
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=cast(str, payload["server"]),
                server_args=cast(list[str], payload.get("server_args", [])),
                env_from=cast(dict[str, str], payload.get("env_from", {})),
                expected_server_artifact_digest=cast(
                    str | None,
                    payload.get("expected_server_artifact_digest"),
                ),
                expected_registered_contract=cast(
                    str | None,
                    payload.get("expected_registered_contract"),
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
        self._submission_by_key[idempotency_key] = (payload_digest, job)
        return job

    def submission_result(
        self,
        job: RelayJob,
        *,
        definition: ClusterDefinition,
        wait_for_terminal_result: bool,
        **_kwargs: object,
    ) -> JSON:
        """Return a terminal, artifact-verified result for the requested MCP operation."""
        assert isinstance(job.spec, McpCallSpec)
        if (
            job.spec.tool == "jarvis_describe"
            and job.spec.expected_registered_contract == REGISTERED_JARVIS_CONTRACT_ID
        ):
            assert wait_for_terminal_result is True
        if job.spec.tool == "jarvis_add_step" and job.used_artifact_refs:
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
            result["mcp_result"] = _describe_mcp_result()
        return result

    def ingest(self, body: JSON) -> JSON:
        """Return the exact hidden-ingest response for one bounded content snapshot."""
        copied = copy.deepcopy(body)
        self.ingest_bodies.append(copied)
        encoded = cast(str, body["data_base64"])
        data = base64.b64decode(encoded, validate=True)
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
        job_document = producer.model_dump(mode="json")
        if self.corrupt_ingest_producer_kind:
            job_document["kind"] = "mcp_call"
        return {
            "job": job_document,
            "artifact": artifact.model_dump(mode="json"),
        }


def test_registered_jarvis_input_flows_from_describe_through_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A waited package description authorizes only its declared file and pins the run."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    source = workspace / "in.lj"
    source.write_text("units lj\nrun 5000\n", encoding="utf-8")
    settings, definition, catalog, harness = _configured_flow(tmp_path, workspace=workspace)
    current_catalog = {"value": catalog}
    _patch_flow(
        monkeypatch,
        current_catalog=current_catalog,
        definition=definition,
        harness=harness,
    )
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)

    described = _call(
        queue,
        settings=settings,
        session=session,
        name=DESCRIBE_ALIAS,
        arguments={
            "cluster": "ares",
            "target": "package",
            "package_name": "lammps",
            "idempotency_key": "describe-lammps-inputs",
        },
    )
    assert "error" not in described
    assert harness.submitted_payloads[-1]["expected_registered_contract"] == (
        REGISTERED_JARVIS_CONTRACT_ID
    )
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
        "idempotency_key": "add-lammps-input",
    }
    added = _call(
        queue,
        settings=settings,
        session=session,
        name=ADD_STEP_ALIAS,
        arguments=add_arguments,
    )
    assert "error" not in added
    assert add_arguments["config"]["script"] == "in.lj"
    add_payload = harness.submitted_payloads[-1]
    assert add_payload["expected_registered_contract"] == REGISTERED_JARVIS_CONTRACT_ID
    forwarded = cast(JSON, add_payload["arguments"])
    forwarded_config = cast(JSON, forwarded["config"])
    assert forwarded_config["script"].startswith("/srv/clio-relay/job_")
    assert forwarded_config["script"].endswith("/inputs/in.lj")
    assert forwarded_config["out"] == "."
    assert forwarded_config["restart_path"] == "researcher-owned/restart.bin"
    add_uses = cast(list[JSON], add_payload["used_artifact_refs"])
    assert len(add_uses) == 1
    source_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    assert add_uses[0]["sha256"] == source_sha256
    assert add_uses[0]["provenance"] == {
        "schema_version": "clio-relay.artifact-use-provenance.v1",
        "evidence": "schema-arg",
        "authority": "",
        "external_ref": "",
        "arg": "script",
        "note": "",
    }
    assert harness.ingest_bodies[0]["idempotency_key"] == "input-ingest:" + _digest(
        {
            "cluster": "ares",
            "owner_session_id": "desktop-session",
            "owner_session_generation_id": "generation_0123456789abcdef0123456789abcdef",
            "logical_name": "in.lj",
            "size_bytes": source.stat().st_size,
            "sha256": source_sha256,
        }
    )

    # A new local MCP process/session has no in-memory package or pipeline
    # cache. The exact route-bound input lineage must survive in core storage.
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)
    assert session.jarvis_pipeline_input_uses == {}

    ran = _call(
        queue,
        settings=settings,
        session=session,
        name=RUN_ALIAS,
        arguments={
            "cluster": "ares",
            "pipeline_id": "science-run",
            "idempotency_key": "run-lammps-input",
        },
    )
    assert "error" not in ran
    run_payload = harness.submitted_payloads[-1]
    assert run_payload["tool"] == "jarvis_run"
    assert run_payload["expected_registered_contract"] == REGISTERED_JARVIS_CONTRACT_ID
    assert run_payload["used_artifact_refs"] == add_payload["used_artifact_refs"]


def test_legacy_jarvis_contract_does_not_activate_transparent_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A loadable legacy route remains pass-through and cannot opt into v3.6 staging."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "in.lj").write_text("run 1\n", encoding="utf-8")
    settings, definition, catalog, harness = _configured_flow(
        tmp_path,
        workspace=workspace,
        contract_id="clio-kit-jarvis-user-v3.5",
    )
    current_catalog = {"value": catalog}
    _patch_flow(
        monkeypatch,
        current_catalog=current_catalog,
        definition=definition,
        harness=harness,
    )
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)

    _describe_package(queue, settings=settings, session=session)
    added = _call(
        queue,
        settings=settings,
        session=session,
        name=ADD_STEP_ALIAS,
        arguments={
            "cluster": "ares",
            "pipeline_id": "legacy-run",
            "package_name": "lammps",
            "config": {"script": "in.lj"},
            "idempotency_key": "legacy-add",
            "wait_for_terminal": True,
        },
    )

    assert "error" not in added
    assert session.jarvis_package_input_contracts == {}
    assert harness.ingest_bodies == []
    payload = harness.submitted_payloads[-1]
    assert cast(JSON, payload["arguments"])["config"] == {"script": "in.lj"}
    assert payload.get("used_artifact_refs", []) == []


def test_durable_pipeline_inputs_do_not_cross_server_artifact_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A restarted client cannot reuse staged inputs through a changed server route."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "in.lj").write_text("run 1\n", encoding="utf-8")
    settings, definition, catalog, harness = _configured_flow(tmp_path, workspace=workspace)
    current_catalog = {"value": catalog}
    _patch_flow(
        monkeypatch,
        current_catalog=current_catalog,
        definition=definition,
        harness=harness,
    )
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)
    _describe_package(queue, settings=settings, session=session)
    added = _call(
        queue,
        settings=settings,
        session=session,
        name=ADD_STEP_ALIAS,
        arguments={
            "cluster": "ares",
            "pipeline_id": "route-bound-pipeline",
            "package_name": "lammps",
            "config": {"script": "in.lj"},
            "wait_for_terminal": True,
            "idempotency_key": "route-bound-add",
        },
    )
    assert "error" not in added
    assert harness.submitted_payloads[-1]["used_artifact_refs"]

    changed_tools = {
        name: replace(
            tool,
            routes={
                "ares": replace(
                    tool.routes["ares"],
                    expected_server_artifact_digest="b" * 64,
                )
            },
        )
        for name, tool in catalog.tools.items()
    }
    current_catalog["value"] = replace(
        catalog,
        revision="e" * 64,
        tools=changed_tools,
        jarvis_artifact_bindings={"ares": "b" * 64},
    )
    restarted = McpSessionState()
    _advertise(queue, settings=settings, session=restarted)
    ran = _call(
        queue,
        settings=settings,
        session=restarted,
        name=RUN_ALIAS,
        arguments={
            "cluster": "ares",
            "pipeline_id": "route-bound-pipeline",
            "idempotency_key": "route-bound-run",
        },
    )

    assert "error" not in ran
    assert harness.submitted_payloads[-1].get("used_artifact_refs", []) == []


def test_registered_jarvis_query_forwards_contract_marker_to_owned_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The authenticated owner submission retains the accepted JARVIS contract identity."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    settings, definition, catalog, harness = _configured_flow(tmp_path, workspace=workspace)
    _patch_flow(
        monkeypatch,
        current_catalog={"value": catalog},
        definition=definition,
        harness=harness,
    )
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)

    queried = _call(
        queue,
        settings=settings,
        session=session,
        name=GET_EXECUTION_ALIAS,
        arguments={
            "cluster": "ares",
            "pipeline_id": "pipeline-a",
            "execution_id": "jarvis_execution_a",
            "wait_for_terminal": True,
        },
    )

    assert "error" not in queried
    payload = harness.submitted_payloads[-1]
    assert payload["tool"] == "jarvis_get_execution"
    assert payload["expected_registered_contract"] == REGISTERED_JARVIS_CONTRACT_ID
    assert payload["expected_server_artifact_digest"] == "a" * 64


def test_changed_local_content_conflicts_for_same_add_step_idempotency_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller cannot silently retarget one accepted add-step identity to new bytes."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    source = workspace / "in.lj"
    source.write_text("run 1\n", encoding="utf-8")
    settings, definition, catalog, harness = _configured_flow(tmp_path, workspace=workspace)
    current_catalog = {"value": catalog}
    _patch_flow(
        monkeypatch,
        current_catalog=current_catalog,
        definition=definition,
        harness=harness,
    )
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)
    _describe_package(queue, settings=settings, session=session)
    arguments: JSON = {
        "cluster": "ares",
        "pipeline_id": "same-pipeline",
        "package_name": "lammps",
        "config": {"script": "in.lj"},
        "wait_for_terminal": True,
        "idempotency_key": "same-add-step",
    }

    first = _call(
        queue,
        settings=settings,
        session=session,
        name=ADD_STEP_ALIAS,
        arguments=arguments,
    )
    assert "error" not in first
    first_use = copy.deepcopy(harness.submitted_payloads[-1]["used_artifact_refs"])
    source.write_text("run 2\n", encoding="utf-8")

    changed = _call(
        queue,
        settings=settings,
        session=session,
        name=ADD_STEP_ALIAS,
        arguments=arguments,
    )
    assert changed["error"]["code"] == -32000
    assert "idempotency key was reused with a different job payload" in changed["error"]["message"]
    assert harness.submitted_payloads[-1]["used_artifact_refs"] != first_use


def test_add_step_without_describe_fails_before_file_read_or_submission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing route-bound schema evidence stops dispatch before inspecting Host files."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "in.lj").write_text("run 1\n", encoding="utf-8")
    settings, definition, catalog, harness = _configured_flow(tmp_path, workspace=workspace)
    current_catalog = {"value": catalog}
    _patch_flow(
        monkeypatch,
        current_catalog=current_catalog,
        definition=definition,
        harness=harness,
    )

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
        name=ADD_STEP_ALIAS,
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


def test_add_step_rejects_ingest_response_from_wrong_producer_kind(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The trusted client still verifies the exact hidden-ingest producer contract."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "in.lj").write_text("run 1\n", encoding="utf-8")
    settings, definition, catalog, harness = _configured_flow(tmp_path, workspace=workspace)
    current_catalog = {"value": catalog}
    _patch_flow(
        monkeypatch,
        current_catalog=current_catalog,
        definition=definition,
        harness=harness,
    )
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)
    _describe_package(queue, settings=settings, session=session)
    harness.corrupt_ingest_producer_kind = True

    response = _add_step(queue, settings=settings, session=session)

    assert response["error"]["code"] == -32000
    assert "does not match the requested content" in response["error"]["message"]
    assert len(harness.ingest_bodies) == 1
    assert [payload["tool"] for payload in harness.submitted_payloads] == ["jarvis_describe"]


def test_input_contract_survives_session_reset_but_cannot_cross_route_reset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Durable package semantics survive initialize but remain exact to the registered route."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "in.lj").write_text("run 1\n", encoding="utf-8")
    settings, definition, catalog, harness = _configured_flow(tmp_path, workspace=workspace)
    current_catalog = {"value": catalog}
    _patch_flow(
        monkeypatch,
        current_catalog=current_catalog,
        definition=definition,
        harness=harness,
    )
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)
    _describe_package(queue, settings=settings, session=session)

    initialized = handle_request(
        {"jsonrpc": "2.0", "id": 30, "method": "initialize", "params": {}},
        queue=queue,
        settings=settings,
        profile="user",
        session=session,
    )
    assert initialized is not None and "error" not in initialized
    _advertise(queue, settings=settings, session=session)
    after_session_reset = _add_step(queue, settings=settings, session=session)
    assert "error" not in after_session_reset
    assert harness.submitted_payloads[-1]["tool"] == "jarvis_add_step"
    assert harness.submitted_payloads[-1]["used_artifact_refs"]

    _describe_package(
        queue,
        settings=settings,
        session=session,
        idempotency_key="describe-after-reset",
    )
    changed_routes = {
        name: replace(
            tool,
            routes={
                "ares": replace(
                    tool.routes["ares"],
                    registration_revision="f" * 64,
                )
            },
        )
        for name, tool in catalog.tools.items()
    }
    current_catalog["value"] = replace(
        catalog,
        revision="e" * 64,
        tools=changed_routes,
    )
    _advertise(queue, settings=settings, session=session)
    after_route_reset = _add_step(queue, settings=settings, session=session)
    assert "requires a successful jarvis_describe" in after_route_reset["error"]["message"]


def test_nonterminal_staged_add_step_reports_handle_without_accepting_lineage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bounded wait expiry is explicit and cannot silently authorize a later run."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "in.lj").write_text("run 1\n", encoding="utf-8")
    settings, definition, catalog, harness = _configured_flow(tmp_path, workspace=workspace)
    _patch_flow(
        monkeypatch,
        current_catalog={"value": catalog},
        definition=definition,
        harness=harness,
    )
    queue = ClioCoreQueue(settings.core_dir)
    session = McpSessionState()
    _advertise(queue, settings=settings, session=session)
    _describe_package(queue, settings=settings, session=session)
    harness.nonterminal_tools.add("jarvis_add_step")

    response = _add_step(queue, settings=settings, session=session)

    assert response["error"]["code"] == -32000
    assert "did not become terminal" in response["error"]["message"]
    assert "job_id=" in response["error"]["message"]
    assert harness.submitted_payloads[-1]["tool"] == "jarvis_add_step"
    assert not list((settings.core_dir / "jarvis_pipeline_input_lineage").glob("*.json"))

    harness.nonterminal_tools.clear()
    retried = _add_step(queue, settings=settings, session=session)

    assert "error" not in retried
    assert (
        harness.submitted_payloads[-2]["idempotency_key"]
        == harness.submitted_payloads[-1]["idempotency_key"]
    )
    assert list((settings.core_dir / "jarvis_pipeline_input_lineage").glob("*.json"))


def _configured_flow(
    tmp_path: Path,
    *,
    workspace: Path,
    contract_id: str = REGISTERED_JARVIS_CONTRACT_ID,
) -> tuple[RelaySettings, ClusterDefinition, VirtualRemoteMcpCatalog, _McpFlowHarness]:
    registration = RemoteMcpServerConfig(
        command="clio-kit",
        args=["mcp-server", "jarvis"],
        namespace="jarvis-demo",
        contract=cast(Any, contract_id),
        allow_tools=[
            "jarvis_describe",
            "jarvis_add_step",
            "jarvis_run",
            "jarvis_get_execution",
        ],
        profiles=["user"],
    )
    definition = ClusterDefinition(
        name="ares",
        ssh_host="ares-login",
        remote_mcp_servers={"jarvis-demo": registration},
    )
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        owner_session_id="desktop-session",
        owner_session_generation_id="generation_0123456789abcdef0123456789abcdef",
        owner_session_cluster="ares",
        api_token="session-token",
        input_workspace_root=workspace,
        input_file_max_bytes=1024,
        input_total_max_bytes=4096,
    )
    catalog = _catalog(definition, registration)
    return settings, definition, catalog, _McpFlowHarness(settings=settings)


def _catalog(
    definition: ClusterDefinition,
    registration: RemoteMcpServerConfig,
) -> VirtualRemoteMcpCatalog:
    route_revision = cluster_route_revision(definition)
    registration_revision = remote_mcp_registration_revision(registration)
    schemas = {
        DESCRIBE_ALIAS: RemoteMcpToolSchema(
            name="jarvis_describe",
            input_schema={
                "type": "object",
                "properties": {
                    "target": {"type": "string"},
                    "package_name": {"type": "string"},
                },
                "required": ["target"],
                "additionalProperties": False,
            },
        ),
        ADD_STEP_ALIAS: RemoteMcpToolSchema(
            name="jarvis_add_step",
            input_schema={
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string"},
                    "package_name": {"type": "string"},
                    "config": {"type": "object"},
                },
                "required": ["pipeline_id", "package_name"],
                "additionalProperties": False,
            },
        ),
        RUN_ALIAS: RemoteMcpToolSchema(
            name="jarvis_run",
            input_schema={
                "type": "object",
                "properties": {"pipeline_id": {"type": "string"}},
                "required": ["pipeline_id"],
                "additionalProperties": False,
            },
        ),
        GET_EXECUTION_ALIAS: RemoteMcpToolSchema(
            name="jarvis_get_execution",
            input_schema={
                "type": "object",
                "properties": {
                    "pipeline_id": {"type": "string"},
                    "execution_id": {"type": "string"},
                },
                "required": ["pipeline_id", "execution_id"],
                "additionalProperties": False,
            },
            annotations={"readOnlyHint": True, "destructiveHint": False},
        ),
    }
    tools: dict[str, VirtualRemoteMcpTool] = {}
    for alias, schema in schemas.items():
        route = RemoteMcpRoute(
            cluster="ares",
            server_name="jarvis-demo",
            command=registration.command,
            args=tuple(registration.args),
            env_from=tuple(registration.env_from.items()),
            expected_server_artifact_digest="a" * 64,
            remote_tool_name=schema.name,
            timeout_seconds=registration.call_timeout_seconds,
            contract=registration.contract,
            cluster_route_revision=route_revision,
            registration_revision=registration_revision,
        )
        tools[alias] = VirtualRemoteMcpTool(
            alias=alias,
            namespace="remote_jarvis_demo",
            remote_tool=schema,
            routes={"ares": route},
            arguments_wrapped=False,
        )
    return VirtualRemoteMcpCatalog(
        revision="d" * 64,
        tools=tools,
        issues=(),
        cluster_route_revisions={"ares": route_revision},
        jarvis_artifact_bindings={"ares": "a" * 64},
    )


def _patch_flow(
    monkeypatch: pytest.MonkeyPatch,
    *,
    current_catalog: dict[str, VirtualRemoteMcpCatalog],
    definition: ClusterDefinition,
    harness: _McpFlowHarness,
) -> None:
    def catalog(**_kwargs: object) -> VirtualRemoteMcpCatalog:
        return current_catalog["value"]

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

    def execute_remotely(_definition: ClusterDefinition) -> bool:
        return True

    monkeypatch.setattr(mcp_server_module, "_remote_mcp_catalog", catalog)
    monkeypatch.setattr(
        mcp_server_module,
        "_remote_cluster_definition",
        remote_definition,
    )
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
    idempotency_key: str = "describe-package",
) -> None:
    response = _call(
        queue,
        settings=settings,
        session=session,
        name=DESCRIBE_ALIAS,
        arguments={
            "cluster": "ares",
            "target": "package",
            "package_name": "lammps",
            "idempotency_key": idempotency_key,
        },
    )
    assert "error" not in response


def _add_step(
    queue: ClioCoreQueue,
    *,
    settings: RelaySettings,
    session: McpSessionState,
) -> JSON:
    return _call(
        queue,
        settings=settings,
        session=session,
        name=ADD_STEP_ALIAS,
        arguments={
            "cluster": "ares",
            "pipeline_id": "reset-pipeline",
            "package_name": "lammps",
            "config": {"script": "in.lj"},
        },
    )


def _describe_mcp_result() -> JSON:
    return {
        "tool": "jarvis_describe",
        "structured_result": {
            "result": {
                "target": "package",
                "package": {
                    "name": "builtin.lammps",
                    "short_name": "lammps",
                    "settings": [
                        {
                            "name": "script",
                            "aliases": [],
                            "type": "str",
                            "required": False,
                            "nullable": False,
                            "default": "",
                            "input_binding": {
                                "schema_version": "jarvis.configuration-input-binding.v1",
                                "kind": "local_file",
                                "structure": "regular_file",
                            },
                        },
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


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
