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
    JarvisPipelineInputRoute,
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
    requested: list[tuple[str, Path, str]] = []
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
        requested.append((setting_name, _workspace_path(raw_value, settings=settings), raw_value))
    if not requested:
        return StagedJarvisInputs(
            arguments=rewritten,
            artifact_uses=(),
            manifest_sha256=None,
        )
    if settings.input_workspace_root is None:
        raise ValueError(
            "local JARVIS inputs require CLIO_RELAY_INPUT_WORKSPACE_ROOT to be configured"
        )
    if len(requested) > settings.input_file_max_count:
        raise ValueError(
            f"JARVIS input count exceeds the configured limit of {settings.input_file_max_count}"
        )
    owner_session_id = settings.owner_session_id
    owner_session_generation_id = settings.owner_session_generation_id
    if owner_session_id is None or owner_session_generation_id is None:
        raise ValueError("local JARVIS inputs require an active relay-owned remote session")

    snapshots: list[tuple[str, str, bytes, int, str]] = []
    total_bytes = 0
    for setting_name, local_path, model_path in requested:
        snapshot = snapshot_owned_regular_file(
            local_path,
            owned_root=settings.input_workspace_root,
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
            (
                setting_name,
                _logical_name(model_path),
                snapshot.data,
                snapshot.size_bytes,
                snapshot.sha256,
            )
        )

    artifact_uses: list[ArtifactUse] = []
    manifest: list[JSON] = []
    with OwnedSessionApiClient(definition=definition, settings=settings) as client:
        for setting_name, logical_name, data, size_bytes, sha256 in snapshots:
            ingest_key = "input-ingest:" + _stable_digest(
                {
                    "cluster": definition.name,
                    "owner_session_id": owner_session_id,
                    "owner_session_generation_id": str(owner_session_generation_id),
                    "logical_name": logical_name,
                    "size_bytes": size_bytes,
                    "sha256": sha256,
                }
            )
            raw_response = client.request_json(
                method="POST",
                path="/input-artifacts/ingest",
                body={
                    "schema_version": INPUT_INGEST_SCHEMA,
                    "cluster": definition.name,
                    "logical_name": logical_name,
                    "size_bytes": size_bytes,
                    "sha256": sha256,
                    "data_base64": base64.b64encode(data).decode("ascii"),
                    "idempotency_key": ingest_key,
                },
            )
            artifact, producer = _validated_ingest_response(
                raw_response,
                definition=definition,
                settings=settings,
                logical_name=logical_name,
                size_bytes=size_bytes,
                sha256=sha256,
                idempotency_key=ingest_key,
            )
            config[setting_name] = _cluster_path_from_file_uri(artifact.uri)
            assert artifact.sha256 is not None
            use = ArtifactUse(
                artifact_id=artifact.artifact_id,
                sha256=artifact.sha256,
                provenance=ArtifactUseProvenance(
                    evidence=ArtifactUseEvidence.SCHEMA_ARG,
                    arg=setting_name,
                ),
            )
            artifact_uses.append(use)
            manifest.append(
                {
                    "setting": setting_name,
                    "artifact_id": artifact.artifact_id,
                    "producer_job_id": producer.job_id,
                    "size_bytes": artifact.size_bytes,
                    "sha256": artifact.sha256,
                }
            )
    return StagedJarvisInputs(
        arguments=rewritten,
        artifact_uses=tuple(sorted(artifact_uses, key=lambda item: item.artifact_id)),
        manifest_sha256=_stable_digest({"inputs": manifest}),
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
