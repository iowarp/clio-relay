"""Schema-driven staging for caller-local JARVIS package inputs."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from pydantic import ValidationError

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.models import (
    REGISTERED_JARVIS_USER_CONTRACT,
    ArtifactRef,
    ArtifactUse,
    ArtifactUseEvidence,
    ArtifactUseProvenance,
    InputArtifactSpec,
    JarvisPackageInputContractRecord,
    JarvisPackageInputRoute,
    JarvisPackageLocalFileInput,
    JarvisPipelineInputBinding,
    JarvisPipelineInputBindings,
    JarvisPipelineInputRoute,
    JarvisRunInputResolution,
    JobKind,
    RelayJob,
    deterministic_input_artifact_id,
    validate_artifact_use_collection,
)
from clio_relay.session_api import OwnedSessionApiClient
from clio_relay.spool import snapshot_owned_regular_file

JSON = dict[str, Any]
JARVIS_INPUT_BINDING_SCHEMA = "jarvis.configuration-input-binding.v1"
INPUT_INGEST_SCHEMA = "clio-relay.input-artifact-ingest.v1"
INPUT_ARTIFACT_KIND = "input"
REGISTERED_JARVIS_CONTRACT_ID = REGISTERED_JARVIS_USER_CONTRACT


@dataclass(frozen=True, slots=True)
class JarvisPackageInputContract:
    """One package description bound to an exact registered JARVIS route."""

    cache_key: str
    package_names: tuple[str, ...]
    local_file_settings: tuple[JarvisPackageLocalFileInput, ...]
    settings_sha256: str


@dataclass(frozen=True, slots=True)
class StagedJarvisInputs:
    """Rewritten remote arguments and their immutable input dependencies."""

    arguments: JSON
    artifact_uses: tuple[ArtifactUse, ...]
    manifest_sha256: str | None
    bindings: tuple[JarvisPipelineInputBinding, ...]
    removed_binding_identities: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class _InputSnapshot:
    """One securely captured workspace input awaiting immutable ingestion."""

    step_id: str
    canonical_setting: str
    accepted_names: tuple[str, ...]
    supplied_setting: str
    workspace_relative_path: str
    logical_name: str
    data: bytes
    size_bytes: int
    sha256: str


def jarvis_package_input_cache_key(
    *,
    cluster: str,
    server_name: str,
    cluster_route_revision: str,
    registration_revision: str,
    expected_server_artifact_digest: str | None,
    package_name: str,
) -> str:
    """Return the exact route-and-package identity for a description cache entry."""
    return jarvis_package_input_route(
        cluster=cluster,
        server_name=server_name,
        cluster_route_revision=cluster_route_revision,
        registration_revision=registration_revision,
        expected_server_artifact_digest=expected_server_artifact_digest,
        package_name=package_name,
    ).identity_sha256()


def jarvis_package_input_route(
    *,
    cluster: str,
    server_name: str,
    cluster_route_revision: str,
    registration_revision: str,
    expected_server_artifact_digest: str | None,
    package_name: str,
) -> JarvisPackageInputRoute:
    """Build the exact route, artifact, contract, and package cache identity."""
    if expected_server_artifact_digest is None:
        raise ValueError("durable JARVIS package semantics require an immutable server artifact")
    return JarvisPackageInputRoute(
        cluster=cluster,
        server_name=server_name,
        cluster_route_revision=cluster_route_revision,
        registration_revision=registration_revision,
        expected_server_artifact_digest=expected_server_artifact_digest,
        package_name=package_name,
    )


def jarvis_package_input_contract_record(
    *,
    route: JarvisPackageInputRoute,
    contract: JarvisPackageInputContract,
) -> JarvisPackageInputContractRecord:
    """Create one checksum-bound durable record from verified package semantics."""
    if contract.cache_key != route.identity_sha256():
        raise ValueError("package input contract cache identity does not match its route")
    return JarvisPackageInputContractRecord.create(
        route=route,
        package_names=contract.package_names,
        local_file_settings=contract.local_file_settings,
        settings_sha256=contract.settings_sha256,
    )


def jarvis_package_input_contract_from_record(
    record: JarvisPackageInputContractRecord,
) -> JarvisPackageInputContract:
    """Restore runtime package semantics from one validated durable record."""
    return JarvisPackageInputContract(
        cache_key=record.route_sha256,
        package_names=record.package_names,
        local_file_settings=record.local_file_settings,
        settings_sha256=record.settings_sha256,
    )


def jarvis_pipeline_input_cache_key(
    *,
    cluster: str,
    server_name: str,
    cluster_route_revision: str,
    registration_revision: str,
    expected_server_artifact_digest: str | None,
    pipeline_id: str,
    owner_session_id: str | None,
    owner_session_generation_id: str | None,
) -> str:
    """Return the exact route-and-pipeline identity for inherited input lineage."""
    return jarvis_pipeline_input_route(
        cluster=cluster,
        server_name=server_name,
        cluster_route_revision=cluster_route_revision,
        registration_revision=registration_revision,
        expected_server_artifact_digest=expected_server_artifact_digest,
        pipeline_id=pipeline_id,
        owner_session_id=owner_session_id,
        owner_session_generation_id=owner_session_generation_id,
    ).identity_sha256()


def jarvis_pipeline_input_route(
    *,
    cluster: str,
    server_name: str,
    cluster_route_revision: str,
    registration_revision: str,
    expected_server_artifact_digest: str | None,
    pipeline_id: str,
    owner_session_id: str | None,
    owner_session_generation_id: str | None,
) -> JarvisPipelineInputRoute:
    """Build the exact route, artifact, pipeline, and session-generation identity."""
    if expected_server_artifact_digest is None:
        raise ValueError("durable JARVIS input lineage requires an immutable server artifact")
    if owner_session_id is None or owner_session_generation_id is None:
        raise ValueError("durable JARVIS input lineage requires an active owned session")
    return JarvisPipelineInputRoute(
        cluster=cluster,
        server_name=server_name,
        cluster_route_revision=cluster_route_revision,
        registration_revision=registration_revision,
        expected_server_artifact_digest=expected_server_artifact_digest,
        pipeline_id=pipeline_id,
        owner_session_id=owner_session_id,
        owner_session_generation_id=owner_session_generation_id,
    )


def parse_jarvis_package_input_contract(
    result: JSON,
    *,
    cache_key: str,
) -> JarvisPackageInputContract | None:
    """Extract declared local-file settings from a successful package description."""
    if result.get("state") != "succeeded" or result.get("terminal") is not True:
        return None
    raw_mcp_result = result.get("mcp_result")
    if not isinstance(raw_mcp_result, dict):
        return None
    mcp_result = cast(JSON, raw_mcp_result)
    if mcp_result.get("tool") != "jarvis_describe":
        return None
    raw_structured = mcp_result.get("structured_result")
    if not isinstance(raw_structured, dict):
        raise ValueError("jarvis_describe returned no structured result")
    raw_result = cast(JSON, raw_structured).get("result")
    if not isinstance(raw_result, dict):
        raise ValueError("jarvis_describe structured result omitted result")
    raw_package = cast(JSON, raw_result).get("package")
    if not isinstance(raw_package, dict):
        return None
    package = cast(JSON, raw_package)
    names = _package_names(package)
    raw_settings = package.get("settings")
    if not isinstance(raw_settings, list):
        raise ValueError("jarvis_describe package omitted its settings array")
    settings = cast(list[object], raw_settings)
    local_file_settings: list[JarvisPackageLocalFileInput] = []
    accepted_local_file_names: set[str] = set()
    normalized_settings: list[JSON] = []
    for raw_setting in settings:
        if not isinstance(raw_setting, dict):
            raise ValueError("jarvis_describe package contains a non-object setting")
        setting = cast(JSON, raw_setting)
        normalized_settings.append(copy.deepcopy(setting))
        raw_binding = setting.get("input_binding")
        if raw_binding is None:
            continue
        if not isinstance(raw_binding, dict):
            raise ValueError("JARVIS package input_binding must be an object")
        binding = cast(JSON, raw_binding)
        if set(binding) != {"schema_version", "kind", "structure"} or binding != {
            "schema_version": JARVIS_INPUT_BINDING_SCHEMA,
            "kind": "local_file",
            "structure": "regular_file",
        }:
            raise ValueError("JARVIS package input_binding is unsupported or malformed")
        name = setting.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError("JARVIS package local-file setting has no name")
        raw_aliases = setting.get("aliases", [])
        if not isinstance(raw_aliases, list):
            raise ValueError(f"JARVIS package local-file setting {name!r} has invalid aliases")
        aliases: list[str] = []
        for raw_alias in cast(list[object], raw_aliases):
            if not isinstance(raw_alias, str) or not raw_alias:
                raise ValueError(f"JARVIS package local-file setting {name!r} has invalid aliases")
            aliases.append(raw_alias)
        accepted_names = tuple(dict.fromkeys([name, *aliases]))
        overlap = accepted_local_file_names.intersection(accepted_names)
        if overlap:
            raise ValueError(
                f"JARVIS package repeats a local-file setting name or alias: {sorted(overlap)[0]!r}"
            )
        accepted_local_file_names.update(accepted_names)
        local_file_settings.append(
            JarvisPackageLocalFileInput(
                canonical_name=name,
                accepted_names=accepted_names,
            )
        )
    return JarvisPackageInputContract(
        cache_key=cache_key,
        package_names=names,
        local_file_settings=tuple(local_file_settings),
        settings_sha256=_stable_digest({"settings": normalized_settings}),
    )


def stage_jarvis_add_step_inputs(
    arguments: JSON,
    *,
    contract: JarvisPackageInputContract,
    definition: ClusterDefinition,
    settings: RelaySettings,
) -> StagedJarvisInputs:
    """Snapshot and ingest only package settings explicitly declared as local files."""
    package_name = _required_string(arguments, "package_name")
    if package_name not in contract.package_names:
        raise ValueError(
            "jarvis_add_step package does not match the package returned by jarvis_describe"
        )
    raw_config = arguments.get("config", {})
    if not isinstance(raw_config, dict):
        raise ValueError("jarvis_add_step config must be an object")
    rewritten = copy.deepcopy(arguments)
    config = copy.deepcopy(cast(JSON, raw_config))
    rewritten["config"] = config
    requested: list[tuple[str, tuple[str, ...], str, Path, str]] = []
    raw_step_id = arguments.get("step_id")
    if raw_step_id is not None and (not isinstance(raw_step_id, str) or not raw_step_id):
        raise ValueError("jarvis_add_step step_id must be a non-empty string when supplied")
    step_id = raw_step_id if raw_step_id is not None else package_name.rsplit(".", 1)[-1]
    for local_file_input in contract.local_file_settings:
        supplied = [
            (setting_name, config[setting_name])
            for setting_name in local_file_input.accepted_names
            if setting_name in config
            and config[setting_name] is not None
            and config[setting_name] != ""
        ]
        if not supplied:
            continue
        if len(supplied) > 1:
            raise ValueError(
                "local-file setting was supplied through more than one canonical name or alias: "
                f"{local_file_input.canonical_name!r}"
            )
        setting_name, raw_value = supplied[0]
        if not isinstance(raw_value, str):
            raise ValueError(f"local-file setting {setting_name!r} must be a path string")
        requested.append(
            (
                local_file_input.canonical_name,
                local_file_input.accepted_names,
                setting_name,
                _workspace_path(raw_value, settings=settings),
                raw_value,
            )
        )
    if not requested:
        return StagedJarvisInputs(
            arguments=rewritten,
            artifact_uses=(),
            manifest_sha256=None,
            bindings=(),
        )
    snapshots = _snapshot_inputs(
        step_id=step_id,
        requested=requested,
        settings=settings,
    )
    bindings: list[JarvisPipelineInputBinding] = []
    with OwnedSessionApiClient(definition=definition, settings=settings) as client:
        for snapshot in snapshots:
            binding = _ingest_input_snapshot(
                snapshot,
                client=client,
                definition=definition,
                settings=settings,
            )
            config[snapshot.supplied_setting] = binding.remote_path
            bindings.append(binding)
    artifact_uses = tuple(
        sorted((item.artifact_use for item in bindings), key=lambda item: item.artifact_id)
    )
    return StagedJarvisInputs(
        arguments=rewritten,
        artifact_uses=artifact_uses,
        manifest_sha256=_stable_digest(
            {"inputs": [item.model_dump(mode="json") for item in bindings]}
        ),
        bindings=tuple(sorted(bindings, key=lambda item: item.identity())),
    )


def stage_jarvis_edit_step_inputs(
    arguments: JSON,
    *,
    current: JarvisPipelineInputBindings,
    definition: ClusterDefinition,
    settings: RelaySettings,
) -> StagedJarvisInputs:
    """Stage changed logical paths for tracked settings on one edited JARVIS step."""
    rewritten = copy.deepcopy(arguments)
    step_id = _required_string(arguments, "step_id")
    operation = arguments.get("operation", "edit")
    tracked = tuple(item for item in current.bindings if item.step_id == step_id)
    if operation == "remove":
        return StagedJarvisInputs(
            arguments=rewritten,
            artifact_uses=(),
            manifest_sha256=None,
            bindings=(),
            removed_binding_identities=tuple(item.identity() for item in tracked),
        )
    if operation != "edit":
        raise ValueError("jarvis_edit_step operation must be edit or remove")
    raw_config = arguments.get("config")
    if not isinstance(raw_config, dict):
        raise ValueError("jarvis_edit_step config must be an object for operation='edit'")
    config = copy.deepcopy(cast(JSON, raw_config))
    rewritten["config"] = config
    requested: list[tuple[str, tuple[str, ...], str, Path, str]] = []
    removed: list[tuple[str, str]] = []
    for binding in tracked:
        supplied = [(name, config[name]) for name in binding.accepted_names if name in config]
        if not supplied:
            continue
        if len(supplied) > 1:
            raise ValueError(
                "local-file setting was supplied through more than one canonical name or alias: "
                f"{binding.canonical_setting!r}"
            )
        setting_name, raw_value = supplied[0]
        if raw_value is None or raw_value == "":
            removed.append(binding.identity())
            continue
        if not isinstance(raw_value, str):
            raise ValueError(f"local-file setting {setting_name!r} must be a path string")
        requested.append(
            (
                binding.canonical_setting,
                binding.accepted_names,
                setting_name,
                _workspace_path(raw_value, settings=settings),
                raw_value,
            )
        )
    snapshots = _snapshot_inputs(
        step_id=step_id,
        requested=requested,
        settings=settings,
    )
    bindings: list[JarvisPipelineInputBinding] = []
    if snapshots:
        with OwnedSessionApiClient(definition=definition, settings=settings) as client:
            for snapshot in snapshots:
                binding = _ingest_input_snapshot(
                    snapshot,
                    client=client,
                    definition=definition,
                    settings=settings,
                )
                config[snapshot.supplied_setting] = binding.remote_path
                bindings.append(binding)
    artifact_uses = tuple(
        sorted((item.artifact_use for item in bindings), key=lambda item: item.artifact_id)
    )
    return StagedJarvisInputs(
        arguments=rewritten,
        artifact_uses=artifact_uses,
        manifest_sha256=(
            _stable_digest({"inputs": [item.model_dump(mode="json") for item in bindings]})
            if bindings
            else None
        ),
        bindings=tuple(sorted(bindings, key=lambda item: item.identity())),
        removed_binding_identities=tuple(sorted(removed)),
    )


def reconcile_jarvis_run_inputs(
    current: JarvisPipelineInputBindings,
    *,
    definition: ClusterDefinition,
    settings: RelaySettings,
) -> tuple[JarvisRunInputResolution, ...]:
    """Resolve every tracked logical path to immutable bytes for one new execution."""
    snapshots = _snapshot_inputs_by_binding(current=current, settings=settings)
    existing_by_identity = {item.identity(): item for item in current.bindings}
    resolutions: list[JarvisRunInputResolution] = []
    changed = [
        snapshot
        for snapshot in snapshots
        if existing_by_identity[(snapshot.step_id, snapshot.canonical_setting)].sha256
        != snapshot.sha256
    ]
    updated_by_identity: dict[tuple[str, str], JarvisPipelineInputBinding] = {}
    if changed:
        with OwnedSessionApiClient(definition=definition, settings=settings) as client:
            for snapshot in changed:
                updated = _ingest_input_snapshot(
                    snapshot,
                    client=client,
                    definition=definition,
                    settings=settings,
                )
                updated_by_identity[updated.identity()] = updated
    for snapshot in snapshots:
        identity = (snapshot.step_id, snapshot.canonical_setting)
        previous = existing_by_identity[identity]
        updated = updated_by_identity.get(identity, previous)
        resolutions.append(
            JarvisRunInputResolution(
                binding=updated,
                disposition=("reused" if updated.sha256 == previous.sha256 else "updated"),
                previous_sha256=previous.sha256,
            )
        )
    return tuple(sorted(resolutions, key=lambda item: item.binding.identity()))


def _snapshot_inputs(
    *,
    step_id: str,
    requested: list[tuple[str, tuple[str, ...], str, Path, str]],
    settings: RelaySettings,
) -> tuple[_InputSnapshot, ...]:
    """Capture a bounded set of requested settings for one pipeline step."""
    return _snapshot_input_requests(
        [
            (step_id, canonical, accepted, supplied, local_path, model_path)
            for canonical, accepted, supplied, local_path, model_path in requested
        ],
        settings=settings,
    )


def _snapshot_inputs_by_binding(
    *,
    current: JarvisPipelineInputBindings,
    settings: RelaySettings,
) -> tuple[_InputSnapshot, ...]:
    """Capture the stable logical path held by every current pipeline binding."""
    return _snapshot_input_requests(
        [
            (
                binding.step_id,
                binding.canonical_setting,
                binding.accepted_names,
                binding.canonical_setting,
                _workspace_path(binding.workspace_relative_path, settings=settings),
                binding.workspace_relative_path,
            )
            for binding in current.bindings
        ],
        settings=settings,
    )


def _snapshot_input_requests(
    requested: list[tuple[str, str, tuple[str, ...], str, Path, str]],
    *,
    settings: RelaySettings,
) -> tuple[_InputSnapshot, ...]:
    """Securely snapshot exact workspace-relative requests with aggregate limits."""
    root = settings.input_workspace_root
    if root is None:
        raise ValueError(
            "local JARVIS inputs require CLIO_RELAY_INPUT_WORKSPACE_ROOT to be configured"
        )
    if len(requested) > settings.input_file_max_count:
        raise ValueError(
            f"JARVIS input count exceeds the configured limit of {settings.input_file_max_count}"
        )
    if settings.owner_session_id is None or settings.owner_session_generation_id is None:
        raise ValueError("local JARVIS inputs require an active relay-owned remote session")
    snapshots: list[_InputSnapshot] = []
    total_bytes = 0
    for step_id, canonical, accepted, supplied, local_path, model_path in requested:
        snapshot = snapshot_owned_regular_file(
            local_path,
            owned_root=root,
            max_bytes=settings.input_file_max_bytes,
            capture_data=True,
        )
        if snapshot.data is None:
            raise RuntimeError("local input snapshot did not retain its bounded content")
        total_bytes += snapshot.size_bytes
        if total_bytes > settings.input_total_max_bytes:
            raise ValueError(
                "JARVIS input bytes exceed the configured aggregate limit of "
                f"{settings.input_total_max_bytes}"
            )
        snapshots.append(
            _InputSnapshot(
                step_id=step_id,
                canonical_setting=canonical,
                accepted_names=accepted,
                supplied_setting=supplied,
                workspace_relative_path=_workspace_relative_path(local_path, root=root),
                logical_name=_logical_name(model_path),
                data=snapshot.data,
                size_bytes=snapshot.size_bytes,
                sha256=snapshot.sha256,
            )
        )
    return tuple(
        sorted(
            snapshots,
            key=lambda item: (item.step_id, item.canonical_setting),
        )
    )


def _ingest_input_snapshot(
    snapshot: _InputSnapshot,
    *,
    client: OwnedSessionApiClient,
    definition: ClusterDefinition,
    settings: RelaySettings,
) -> JarvisPipelineInputBinding:
    """Ingest one immutable snapshot and return its exact durable binding."""
    owner_session_id = settings.owner_session_id
    owner_session_generation_id = settings.owner_session_generation_id
    if owner_session_id is None or owner_session_generation_id is None:
        raise ValueError("local JARVIS inputs require an active relay-owned remote session")
    ingest_key = "input-ingest:" + _stable_digest(
        {
            "cluster": definition.name,
            "owner_session_id": owner_session_id,
            "owner_session_generation_id": str(owner_session_generation_id),
            "step_id": snapshot.step_id,
            "canonical_setting": snapshot.canonical_setting,
            "workspace_relative_path": snapshot.workspace_relative_path,
            "logical_name": snapshot.logical_name,
            "size_bytes": snapshot.size_bytes,
            "sha256": snapshot.sha256,
        }
    )
    raw_response = client.request_json(
        method="POST",
        path="/input-artifacts/ingest",
        body={
            "schema_version": INPUT_INGEST_SCHEMA,
            "cluster": definition.name,
            "logical_name": snapshot.logical_name,
            "size_bytes": snapshot.size_bytes,
            "sha256": snapshot.sha256,
            "data_base64": base64.b64encode(snapshot.data).decode("ascii"),
            "idempotency_key": ingest_key,
        },
    )
    artifact, _producer = _validated_ingest_response(
        raw_response,
        definition=definition,
        settings=settings,
        logical_name=snapshot.logical_name,
        size_bytes=snapshot.size_bytes,
        sha256=snapshot.sha256,
        idempotency_key=ingest_key,
    )
    assert artifact.sha256 is not None
    return JarvisPipelineInputBinding(
        step_id=snapshot.step_id,
        canonical_setting=snapshot.canonical_setting,
        accepted_names=snapshot.accepted_names,
        workspace_relative_path=snapshot.workspace_relative_path,
        logical_name=snapshot.logical_name,
        size_bytes=snapshot.size_bytes,
        sha256=artifact.sha256,
        remote_path=_cluster_path_from_file_uri(artifact.uri),
        artifact_use=ArtifactUse(
            artifact_id=artifact.artifact_id,
            sha256=artifact.sha256,
            provenance=ArtifactUseProvenance(
                evidence=ArtifactUseEvidence.SCHEMA_ARG,
                arg=snapshot.canonical_setting,
            ),
        ),
    )


def merge_artifact_uses(
    explicit: list[ArtifactUse],
    automatic: tuple[ArtifactUse, ...],
) -> list[ArtifactUse]:
    """Merge explicit and automatic dependencies without ambiguous duplicate identities."""
    by_id = {item.artifact_id: item for item in explicit}
    for item in automatic:
        existing = by_id.get(item.artifact_id)
        if existing is not None and existing != item:
            raise ValueError(f"artifact dependency identity changed: {item.artifact_id}")
        by_id[item.artifact_id] = item
    merged = sorted(by_id.values(), key=lambda item: item.artifact_id)
    validate_artifact_use_collection(merged)
    return merged


def _validated_ingest_response(
    raw_response: object,
    *,
    definition: ClusterDefinition,
    settings: RelaySettings,
    logical_name: str,
    size_bytes: int,
    sha256: str,
    idempotency_key: str,
) -> tuple[ArtifactRef, RelayJob]:
    if not isinstance(raw_response, dict):
        raise ValueError("owned input ingest returned a non-object response")
    response = cast(JSON, raw_response)
    try:
        producer = RelayJob.model_validate(response.get("job"))
        artifact = ArtifactRef.model_validate(response.get("artifact"))
    except ValidationError as exc:
        raise ValueError("owned input ingest returned an invalid durable record") from exc
    if (
        producer.cluster != definition.name
        or producer.kind is not JobKind.INPUT_INGEST
        or producer.state.value != "succeeded"
        or not isinstance(producer.spec, InputArtifactSpec)
        or producer.spec.logical_name != logical_name
        or producer.spec.size_bytes != size_bytes
        or producer.spec.sha256 != sha256
        or producer.idempotency_key != idempotency_key
        or artifact.job_id != producer.job_id
        or artifact.artifact_id != deterministic_input_artifact_id(producer.job_id)
        or artifact.kind != INPUT_ARTIFACT_KIND
        or artifact.size_bytes != size_bytes
        or artifact.sha256 != sha256
        or producer.metadata.get("owner") != "clio-relay"
        or producer.metadata.get("owner_session_id") != settings.owner_session_id
        or producer.metadata.get("owner_session_generation_id")
        != settings.owner_session_generation_id
    ):
        raise ValueError("owned input ingest response does not match the requested content")
    return artifact, producer


def _package_names(package: JSON) -> tuple[str, ...]:
    names: list[str] = []
    for field_name in ("name", "short_name"):
        value = package.get(field_name)
        if isinstance(value, str) and value and value not in names:
            names.append(value)
    if not names:
        raise ValueError("jarvis_describe package has no package identity")
    return tuple(names)


def _workspace_path(value: str, *, settings: RelaySettings) -> Path:
    root = settings.input_workspace_root
    if root is None:
        raise ValueError(
            "local JARVIS inputs require CLIO_RELAY_INPUT_WORKSPACE_ROOT to be configured"
        )
    supplied = Path(value)
    return supplied if supplied.is_absolute() else root / supplied


def _workspace_relative_path(value: Path, *, root: Path) -> str:
    """Return one normalized private-root-relative path without leaking the Host root."""
    try:
        relative = value.resolve(strict=True).relative_to(root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise ValueError("local JARVIS input is not within the configured workspace") from exc
    rendered = relative.as_posix()
    if not rendered or rendered in {".", ".."} or rendered.startswith("../"):
        raise ValueError("local JARVIS input has no safe workspace-relative path")
    return rendered


def _logical_name(value: str) -> str:
    name = Path(value).name
    if (
        not name
        or name in {".", ".."}
        or name != os.path.basename(name)
        or any(ord(character) < 32 or ord(character) == 127 for character in name)
    ):
        raise ValueError("local JARVIS input has no safe logical filename")
    return name


def _cluster_path_from_file_uri(uri: str) -> str:
    parsed = urllib.parse.urlsplit(uri)
    if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
        raise ValueError("owned input artifact is not backed by a cluster-local file URI")
    path = urllib.parse.unquote(parsed.path)
    if not path.startswith("/"):
        raise ValueError("owned input artifact file URI is not absolute")
    return path


def _required_string(value: JSON, key: str) -> str:
    raw = value.get(key)
    if not isinstance(raw, str) or not raw:
        raise ValueError(f"{key} must be a non-empty string")
    return raw


def _stable_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
