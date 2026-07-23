"""Focused tests for schema-declared JARVIS local-input staging."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.input_staging import (
    JarvisPackageInputContract,
    jarvis_package_input_cache_key,
    jarvis_package_input_contract_from_record,
    jarvis_package_input_contract_record,
    jarvis_package_input_route,
    jarvis_pipeline_input_route,
    parse_jarvis_package_input_contract,
    stage_jarvis_add_step_inputs,
)
from clio_relay.models import (
    ArtifactRef,
    ArtifactUse,
    InputArtifactSpec,
    JobKind,
    JobState,
    RelayJob,
    deterministic_input_artifact_id,
)


def _describe_result(*, binding: object) -> dict[str, Any]:
    """Return the verified relay envelope produced by a package description."""
    return {
        "state": "succeeded",
        "terminal": True,
        "mcp_result": {
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
                                "description": "LAMMPS input",
                                "required": False,
                                "nullable": False,
                                "default": "",
                                "type": "str",
                                "input_binding": binding,
                            },
                            {
                                "name": "out",
                                "description": "Output directory",
                                "required": False,
                                "nullable": False,
                                "default": ".",
                                "type": "str",
                            },
                        ],
                    },
                }
            },
        },
    }


def _contract() -> JarvisPackageInputContract:
    result = parse_jarvis_package_input_contract(
        _describe_result(
            binding={
                "schema_version": "jarvis.configuration-input-binding.v1",
                "kind": "local_file",
                "structure": "regular_file",
            }
        ),
        cache_key="route-package-key",
    )
    assert result is not None
    return result


def test_package_description_declares_local_file_without_name_inference() -> None:
    """Only the closed input_binding object, not a path-like setting name, enables staging."""
    contract = _contract()

    assert contract.package_names == ("builtin.lammps", "lammps")
    assert len(contract.local_file_settings) == 1
    assert contract.local_file_settings[0].canonical_name == "script"
    assert contract.local_file_settings[0].accepted_names == ("script",)
    assert len(contract.settings_sha256) == 64

    no_binding = _describe_result(binding=None)
    settings = no_binding["mcp_result"]["structured_result"]["result"]["package"]["settings"]
    settings[0].pop("input_binding")
    settings.append(
        {
            "name": "input_file_path",
            "required": False,
            "nullable": False,
            "default": "",
            "type": "str",
        }
    )
    parsed = parse_jarvis_package_input_contract(no_binding, cache_key="without-binding")
    assert parsed is not None
    assert parsed.local_file_settings == ()


def test_declared_local_file_alias_is_staged_but_duplicate_spelling_is_rejected(
    tmp_path: Path,
) -> None:
    """Aliases are contract data, while simultaneous canonical and alias values are ambiguous."""
    described = _describe_result(
        binding={
            "schema_version": "jarvis.configuration-input-binding.v1",
            "kind": "local_file",
            "structure": "regular_file",
        }
    )
    setting = described["mcp_result"]["structured_result"]["result"]["package"]["settings"][0]
    setting["aliases"] = ["input_script"]
    contract = parse_jarvis_package_input_contract(described, cache_key="alias-contract")
    assert contract is not None
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "in.lj").write_text("units lj\n", encoding="utf-8")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        input_workspace_root=workspace,
    )

    with pytest.raises(ValueError, match="more than one"):
        stage_jarvis_add_step_inputs(
            {
                "pipeline_id": "science-run",
                "package_name": "lammps",
                "config": {"script": "in.lj", "input_script": "in.lj"},
            },
            contract=contract,
            definition=ClusterDefinition(name="ares", ssh_host="ares"),
            settings=settings,
        )


def test_package_description_rejects_open_or_unknown_input_binding() -> None:
    """Unknown binding fields and kinds fail closed before any local path is read."""
    with pytest.raises(ValueError, match="unsupported or malformed"):
        parse_jarvis_package_input_contract(
            _describe_result(
                binding={
                    "schema_version": "jarvis.configuration-input-binding.v1",
                    "kind": "local_file",
                    "structure": "regular_file",
                    "future_option": True,
                }
            ),
            cache_key="malformed-binding",
        )


def test_stage_declared_input_snapshots_ingests_and_rewrites_privately(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The remote call receives a cluster path while the model keeps its Host-relative path."""
    workspace = tmp_path / "workspace"
    local_input = workspace / "session-1" / "in.lj"
    local_input.parent.mkdir(parents=True)
    payload = b"units lj\nrun 5000\n"
    local_input.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster="ares",
        api_token="session-token",
        input_workspace_root=workspace,
        input_file_max_bytes=1_024,
        input_total_max_bytes=2_048,
    )
    definition = ClusterDefinition(name="ares", ssh_host="ares")
    observed: list[dict[str, object]] = []
    producers: list[RelayJob] = []

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
            body: dict[str, object],
        ) -> object:
            assert method == "POST"
            assert path == "/input-artifacts/ingest"
            observed.append(body)
            producer = RelayJob(
                cluster="ares",
                kind=JobKind.INPUT_INGEST,
                state=JobState.SUCCEEDED,
                spec=InputArtifactSpec(
                    logical_name="in.lj",
                    size_bytes=len(payload),
                    sha256=digest,
                ),
                idempotency_key=str(body["idempotency_key"]),
                metadata={
                    "owner": "clio-relay",
                    "owner_session_id": "desktop-session-1",
                    "owner_session_generation_id": "generation-1",
                },
            )
            producers.append(producer)
            artifact = ArtifactRef(
                artifact_id=deterministic_input_artifact_id(producer.job_id),
                job_id=producer.job_id,
                uri=f"file:///srv/clio-relay/{producer.job_id}/inputs/in.lj",
                kind="input",
                size_bytes=len(payload),
                sha256=digest,
            )
            return {
                "job": producer.model_dump(mode="json"),
                "artifact": artifact.model_dump(mode="json"),
            }

    monkeypatch.setattr(
        "clio_relay.input_staging.OwnedSessionApiClient",
        FakeOwnedSessionApiClient,
    )
    model_arguments: dict[str, Any] = {
        "pipeline_id": "science-run",
        "package_name": "lammps",
        "config": {
            "script": "session-1/in.lj",
            "out": ".",
        },
    }

    staged = stage_jarvis_add_step_inputs(
        model_arguments,
        contract=_contract(),
        definition=definition,
        settings=settings,
    )

    assert model_arguments["config"]["script"] == "session-1/in.lj"
    assert staged.arguments["config"]["script"].endswith("/inputs/in.lj")
    assert staged.arguments["config"]["script"].startswith("/srv/clio-relay/job_")
    assert staged.arguments["config"]["out"] == "."
    assert [(use.artifact_id, use.sha256) for use in staged.artifact_uses] == [
        (deterministic_input_artifact_id(producers[0].job_id), digest)
    ]
    assert staged.artifact_uses[0].provenance is not None
    assert staged.artifact_uses[0].provenance.evidence.value == "schema-arg"
    assert staged.artifact_uses[0].provenance.arg == "script"
    assert staged.manifest_sha256 is not None
    assert observed[0]["logical_name"] == "in.lj"
    assert observed[0]["size_bytes"] == len(payload)
    assert observed[0]["sha256"] == digest


def test_stage_rejects_files_outside_workspace_and_oversize(
    tmp_path: Path,
) -> None:
    """Host-relative bindings cannot escape the trusted root or exceed policy."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "outside.lj"
    outside.write_text("run 1\n", encoding="utf-8")
    oversized = workspace / "large.lj"
    oversized.write_bytes(b"12345")
    definition = ClusterDefinition(name="ares", ssh_host="ares")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        owner_session_id="desktop-session",
        owner_session_generation_id="generation_0123456789abcdef0123456789abcdef",
        input_workspace_root=workspace,
        input_file_max_bytes=4,
        input_total_max_bytes=4,
    )

    with pytest.raises(RuntimeError, match="outside|owned root|within"):
        stage_jarvis_add_step_inputs(
            {
                "pipeline_id": "science-run",
                "package_name": "lammps",
                "config": {"script": str(outside)},
            },
            contract=_contract(),
            definition=definition,
            settings=settings,
        )
    with pytest.raises(RuntimeError, match="limit|exceeds"):
        stage_jarvis_add_step_inputs(
            {
                "pipeline_id": "science-run",
                "package_name": "lammps",
                "config": {"script": "large.lj"},
            },
            contract=_contract(),
            definition=definition,
            settings=settings,
        )


def test_route_cache_key_binds_registration_and_package() -> None:
    """Package metadata cannot cross a cluster route, registration, or package identity."""
    common = {
        "cluster": "ares",
        "server_name": "jarvis-demo",
        "cluster_route_revision": "a" * 64,
        "registration_revision": "b" * 64,
        "expected_server_artifact_digest": "c" * 64,
    }
    lammps = jarvis_package_input_cache_key(**common, package_name="lammps")
    paraview = jarvis_package_input_cache_key(**common, package_name="paraview")

    assert len(lammps) == 64
    assert lammps != paraview


def test_package_input_contract_is_durable_bounded_and_route_exact(tmp_path: Path) -> None:
    """Verified package semantics survive restart and reject route or document substitution."""
    route = jarvis_package_input_route(
        cluster="ares",
        server_name="jarvis-demo",
        cluster_route_revision="a" * 64,
        registration_revision="b" * 64,
        expected_server_artifact_digest="c" * 64,
        package_name="lammps",
    )
    described = _contract()
    contract = JarvisPackageInputContract(
        cache_key=route.identity_sha256(),
        package_names=described.package_names,
        local_file_settings=described.local_file_settings,
        settings_sha256=described.settings_sha256,
    )
    queue = ClioCoreQueue(tmp_path / "core")
    saved = queue.put_jarvis_package_input_contract(
        jarvis_package_input_contract_record(route=route, contract=contract)
    )

    restarted = ClioCoreQueue(tmp_path / "core")
    loaded = restarted.get_jarvis_package_input_contract(route)
    assert loaded == saved
    assert loaded is not None
    assert jarvis_package_input_contract_from_record(loaded) == contract
    changed_route = route.model_copy(update={"registration_revision": "d" * 64})
    assert restarted.get_jarvis_package_input_contract(changed_route) is None

    record_path = (
        tmp_path / "core" / "jarvis_package_input_contracts" / f"{route.identity_sha256()}.json"
    )
    document = json.loads(record_path.read_text(encoding="utf-8"))
    document["settings_sha256"] = "e" * 64
    record_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ValueError, match="checksum"):
        restarted.get_jarvis_package_input_contract(route)


def test_pipeline_input_lineage_is_checksum_bound_and_route_exact(tmp_path: Path) -> None:
    """Durable staged inputs survive restart and fail closed after record mutation."""
    queue = ClioCoreQueue(tmp_path / "core")
    route = jarvis_pipeline_input_route(
        cluster="ares",
        server_name="jarvis-demo",
        cluster_route_revision="a" * 64,
        registration_revision="b" * 64,
        expected_server_artifact_digest="c" * 64,
        pipeline_id="science-run",
        owner_session_id="desktop-session",
        owner_session_generation_id="generation_0123456789abcdef0123456789abcdef",
    )
    artifact_use = ArtifactUse(
        artifact_id="artifact_0123456789abcdef0123456789abcdef",
        sha256="d" * 64,
    )
    saved = queue.merge_jarvis_pipeline_input_lineage(
        route,
        (artifact_use,),
        manifest_sha256="e" * 64,
    )

    restarted = ClioCoreQueue(tmp_path / "core")
    assert restarted.get_jarvis_pipeline_input_lineage(route) == saved
    changed_route = route.model_copy(update={"registration_revision": "f" * 64})
    assert restarted.get_jarvis_pipeline_input_lineage(changed_route) is None

    record_path = (
        tmp_path / "core" / "jarvis_pipeline_input_lineage" / f"{route.identity_sha256()}.json"
    )
    document = json.loads(record_path.read_text(encoding="utf-8"))
    document["route"]["pipeline_id"] = "substituted-pipeline"
    record_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ValueError, match="checksum"):
        restarted.get_jarvis_pipeline_input_lineage(route)


def test_pipeline_input_lineage_first_merge_uses_one_mutation_timestamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A first lineage write cannot observe creation after its update timestamp."""
    mutation_at = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    later_model_default = mutation_at + timedelta(microseconds=1)
    monkeypatch.setattr("clio_relay.core_queue.utc_now", lambda: mutation_at)
    monkeypatch.setattr("clio_relay.models.utc_now", lambda: later_model_default)
    route = jarvis_pipeline_input_route(
        cluster="ares",
        server_name="jarvis-demo",
        cluster_route_revision="a" * 64,
        registration_revision="b" * 64,
        expected_server_artifact_digest="c" * 64,
        pipeline_id="science-run",
        owner_session_id="desktop-session",
        owner_session_generation_id="generation_0123456789abcdef0123456789abcdef",
    )

    saved = ClioCoreQueue(tmp_path / "core").merge_jarvis_pipeline_input_lineage(
        route,
        (
            ArtifactUse(
                artifact_id="artifact_0123456789abcdef0123456789abcdef",
                sha256="d" * 64,
            ),
        ),
        manifest_sha256="e" * 64,
    )

    assert saved.created_at == mutation_at
    assert saved.updated_at == mutation_at
