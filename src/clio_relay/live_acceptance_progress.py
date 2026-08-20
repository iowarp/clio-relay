"""Runtime metadata decoding and package-progress attestation for acceptance.

Extracted from ``live_acceptance.py`` (#231 rework): the concern of turning
one raw runtime-metadata artifact or job-status payload into validated,
report-ready facts, and of proving that recorded package progress carries a
trusted provider or JARVIS-native attestation. This owns two closely
related sub-concerns that share the same validated
:class:`~clio_relay.runtime_metadata.JarvisRuntimeMetadata` document --
splitting them further would force the same decode/validate step to live
in two places.
"""

from __future__ import annotations

import json
import math
from base64 import b64decode
from typing import Any, cast

import yaml

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.live_acceptance_models import (
    CommandRunner,
    RuntimeMetadataAcceptance,
    SecureRuntimeProbeConfig,
)
from clio_relay.live_acceptance_remote_io import (
    _remote_clio_json,
    _remote_job_collection,
)
from clio_relay.progress_provenance import validate_package_progress_acceptance_metadata
from clio_relay.runtime_metadata import (
    RUNTIME_METADATA_SCHEMA,
    JarvisRuntimeMetadata,
    RuntimeMetadataSource,
    native_execution_documents,
)


def _verify_runtime_metadata_artifact(
    definition: ClusterDefinition,
    artifacts: list[dict[str, Any]],
    *,
    line_prefix: str,
    lines: list[str],
    runner: CommandRunner,
) -> RuntimeMetadataAcceptance | None:
    """Validate and report a normalized runtime metadata artifact when present."""
    runtime_artifact = next(
        (artifact for artifact in artifacts if artifact.get("kind") == "runtime_metadata"),
        None,
    )
    if runtime_artifact is None:
        return None
    artifact_id = runtime_artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError("runtime metadata artifact has no artifact id")
    payload = _remote_clio_json(
        definition,
        ["job", "read-artifact", artifact_id],
        runner=runner,
    )
    facts = _runtime_metadata_facts(
        payload,
        artifact_id=artifact_id,
        line_prefix=line_prefix,
    )
    lines.extend(facts)
    runtime = _decode_runtime_metadata_payload(payload)
    return RuntimeMetadataAcceptance(
        document=runtime,
        structured=f"{line_prefix}.structured_runtime_metadata=ok" in facts,
    )


def _runtime_metadata_facts(
    payload: dict[str, Any],
    *,
    artifact_id: str,
    line_prefix: str,
) -> list[str]:
    """Validate a runtime metadata payload and return report-ready facts."""
    runtime = _decode_runtime_metadata_payload(payload)
    return [
        f"{line_prefix}.runtime_metadata_artifact={artifact_id}",
        *_runtime_metadata_document_facts(runtime, line_prefix=line_prefix),
    ]


def _runtime_metadata_document_facts(
    runtime: dict[str, Any],
    *,
    line_prefix: str,
) -> list[str]:
    """Return report-ready facts for one already validated runtime document."""
    source = str(runtime["source"])
    facts = [f"{line_prefix}.runtime_metadata_source={source}"]
    structured_sources = {
        RuntimeMetadataSource.JARVIS_MCP.value,
        RuntimeMetadataSource.JARVIS_SIDECAR.value,
    }
    structured = source in structured_sources
    if structured:
        facts.append(f"{line_prefix}.structured_runtime_metadata=ok")
    else:
        compatibility_kind = (
            "legacy_fallback"
            if source == RuntimeMetadataSource.LEGACY_STDOUT.value
            else "untrusted_compatibility"
        )
        facts.append(f"runtime_metadata.compatibility={line_prefix}:{compatibility_kind}")
    raw_field_sources = runtime.get("field_sources")
    field_sources = (
        cast(dict[str, object], raw_field_sources) if isinstance(raw_field_sources, dict) else {}
    )
    provider = runtime.get("scheduler_provider")
    if isinstance(provider, str) and provider:
        facts.append(f"{line_prefix}.runtime_scheduler_provider={provider}")
    scheduler_job_id = runtime.get("scheduler_job_id")
    if isinstance(scheduler_job_id, str) and scheduler_job_id:
        facts.append(f"{line_prefix}.runtime_scheduler_job_id={scheduler_job_id}")
        scheduler_id_source = field_sources.get("scheduler_job_id")
        if isinstance(scheduler_id_source, str):
            facts.append(f"{line_prefix}.runtime_scheduler_job_id_source={scheduler_id_source}")
        provider_source = field_sources.get("scheduler_provider")
        if (
            structured
            and isinstance(provider, str)
            and provider
            and provider_source in structured_sources
            and scheduler_id_source in structured_sources
        ):
            facts.append(f"{line_prefix}.structured_runtime_scheduler_identity=ok")
    return facts


def _decode_runtime_metadata_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Decode and strictly validate one normalized runtime metadata artifact."""
    if payload.get("encoding") != "base64" or not isinstance(payload.get("data"), str):
        raise RelayError("runtime metadata artifact payload was not base64 encoded")
    try:
        decoded = json.loads(b64decode(cast(str, payload["data"])).decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RelayError(f"runtime metadata artifact was not valid JSON: {exc}") from exc
    if not isinstance(decoded, dict):
        raise RelayError("runtime metadata artifact was not an object")
    runtime = cast(dict[str, Any], decoded)
    if runtime.get("schema_version") != RUNTIME_METADATA_SCHEMA:
        raise RelayError("runtime metadata artifact has an unsupported schema")
    try:
        validated = JarvisRuntimeMetadata.model_validate(runtime)
    except ValueError as exc:
        raise RelayError(f"runtime metadata artifact was invalid: {exc}") from exc
    return validated.model_dump(mode="json")


def _expected_progress_adapter(pipeline_yaml: str) -> str | None:
    declaration = _expected_progress_declaration(pipeline_yaml)
    return declaration[0] if declaration is not None else None


def _secure_runtime_probe_config(pipeline_yaml: str) -> SecureRuntimeProbeConfig | None:
    """Read an acceptance-only secure runtime probe without forwarding it to JARVIS."""
    try:
        loaded = cast(object, yaml.safe_load(pipeline_yaml))
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"live-test JARVIS YAML is invalid: {exc}") from exc
    if not isinstance(loaded, dict):
        return None
    extension = cast(dict[str, object], loaded).get("x_clio_relay")
    if extension is None:
        return None
    if not isinstance(extension, dict):
        raise ConfigurationError("x_clio_relay must be an object")
    raw_probe = cast(dict[str, object], extension).get("secure_runtime_probe")
    if raw_probe is None:
        return None
    try:
        return SecureRuntimeProbeConfig.model_validate(raw_probe)
    except ValueError as exc:
        raise ConfigurationError(f"x_clio_relay.secure_runtime_probe is invalid: {exc}") from exc


def _expected_progress_package(pipeline_yaml: str) -> str | None:
    declaration = _expected_progress_declaration(pipeline_yaml)
    return declaration[1] if declaration is not None else None


def _expected_progress_declaration(pipeline_yaml: str) -> tuple[str, str | None] | None:
    """Return the one explicitly selected package progress source, if any."""
    loaded = yaml.safe_load(pipeline_yaml)
    typed_document = cast(dict[str, Any], loaded) if isinstance(loaded, dict) else {}
    packages = typed_document.get("pkgs")
    if not isinstance(packages, list):
        return None
    typed_packages = cast(list[object], packages)
    declarations: list[tuple[str, str | None]] = []
    for package in typed_packages:
        if not isinstance(package, dict):
            continue
        typed_package = cast(dict[str, Any], package)
        progress = typed_package.get("progress")
        if not isinstance(progress, dict):
            continue
        typed_progress = cast(dict[str, Any], progress)
        adapter = typed_progress.get("adapter")
        if adapter is None or adapter == "none":
            continue
        if not isinstance(adapter, str) or not adapter:
            raise ConfigurationError("package progress.adapter must be a non-empty string")
        package_name = typed_package.get("pkg_type")
        declarations.append(
            (
                adapter,
                package_name if isinstance(package_name, str) and package_name else None,
            )
        )
    if len(declarations) > 1:
        raise ConfigurationError(
            "multiple pipeline packages declare progress; select exactly one package-owned "
            "progress source"
        )
    return declarations[0] if declarations else None


def _assert_progress_adapter(
    progress: list[dict[str, Any]],
    expected_adapter: str,
    *,
    job_id: str,
    package_name: str | None = None,
) -> None:
    if _has_progress_adapter(progress, expected_adapter, job_id=job_id, package_name=package_name):
        return
    raise RelayError(f"expected package progress adapter was not recorded: {expected_adapter}")


def _has_progress_adapter(
    progress: list[dict[str, Any]],
    expected_adapter: str,
    *,
    job_id: str,
    package_name: str | None = None,
) -> bool:
    return (
        _progress_provider_attestation(
            progress,
            expected_adapter,
            job_id=job_id,
            package_name=package_name,
        )
        is not None
    )


def _progress_provider_attestation(
    progress: list[dict[str, Any]],
    expected_adapter: str,
    *,
    job_id: str,
    package_name: str | None = None,
) -> dict[str, Any] | None:
    """Return one worker-stamped, provider-approved durable progress record."""
    for item in progress:
        current = item.get("current")
        if not isinstance(current, int | float) or isinstance(current, bool):
            continue
        numeric_current = float(current)
        if not math.isfinite(numeric_current) or numeric_current < 0:
            continue
        metadata = item.get("metadata")
        if not isinstance(metadata, dict):
            continue
        typed_metadata = cast(dict[str, Any], metadata)
        if (
            typed_metadata.get("adapter") == expected_adapter
            and typed_metadata.get("source") == "jarvis_package"
            and isinstance(typed_metadata.get("package_name"), str)
            and (package_name is None or typed_metadata.get("package_name") == package_name)
            and isinstance(typed_metadata.get("package_version"), str)
            and typed_metadata.get("run_id") == job_id
            and typed_metadata.get("execution_id") == job_id
        ):
            try:
                validate_package_progress_acceptance_metadata(typed_metadata)
            except ConfigurationError:
                continue
            return dict(typed_metadata)
    return None


def _runtime_metadata_from_job_status(
    status: dict[str, Any],
    *,
    job_id: str,
) -> dict[str, Any] | None:
    """Return validated runtime metadata from one exact relay job status response."""
    raw_job = status.get("job")
    if not isinstance(raw_job, dict):
        return None
    typed_job = cast(dict[str, Any], raw_job)
    if typed_job.get("job_id") != job_id:
        return None
    raw_job_metadata = typed_job.get("metadata")
    if not isinstance(raw_job_metadata, dict):
        return None
    typed_job_metadata = cast(dict[str, Any], raw_job_metadata)
    raw_runtime = typed_job_metadata.get("runtime_metadata")
    if not isinstance(raw_runtime, dict):
        return None
    try:
        validated = JarvisRuntimeMetadata.model_validate(raw_runtime)
    except ValueError:
        return None
    return validated.model_dump(mode="json")


def _native_progress_attestation(
    runtime_metadata: dict[str, Any] | None,
    expected_adapter: str,
    *,
    package_name: str | None,
    require_nonterminal: bool,
) -> dict[str, Any] | None:
    """Return trusted JARVIS-native package progress from runtime metadata."""
    if runtime_metadata is None:
        return None
    try:
        runtime = JarvisRuntimeMetadata.model_validate(runtime_metadata)
    except ValueError:
        return None
    if runtime.source not in {
        RuntimeMetadataSource.JARVIS_MCP,
        RuntimeMetadataSource.JARVIS_SIDECAR,
    }:
        return None
    raw_contract = runtime.details.get("producer_contract")
    if not isinstance(raw_contract, dict):
        return None
    typed_contract = cast(dict[str, Any], raw_contract)
    if (
        typed_contract.get("contract_kind") != "native_execution"
        or typed_contract.get("trusted") is not True
    ):
        return None
    raw_documents = runtime.details.get("native_execution")
    if not isinstance(raw_documents, dict):
        return None
    try:
        documents = native_execution_documents(cast(dict[str, Any], raw_documents))
    except ValueError:
        return None
    if documents is None:
        return None
    snapshot = documents.progress
    if require_nonterminal and snapshot.terminal:
        return None
    terminal_progress_states = {"completed", "failed", "canceled"}
    for package in snapshot.packages:
        latest = package.latest
        if latest is None:
            continue
        if package_name is not None and package.package_name != package_name:
            continue
        if latest.metadata.get("adapter") != expected_adapter:
            continue
        if require_nonterminal and latest.state in terminal_progress_states:
            continue
        package_version = latest.metadata.get("package_version")
        return {
            "source": "jarvis_execution",
            "adapter": expected_adapter,
            "package_name": package.package_name,
            "package_id": package.package_id,
            "package_version": (
                package_version
                if isinstance(package_version, str) and package_version
                else "native"
            ),
            "execution_id": snapshot.execution_id,
            "pipeline_id": snapshot.pipeline_id,
            "execution_state": snapshot.execution_state,
            "execution_terminal": snapshot.terminal,
            "progress_state": latest.state,
            "progress_sequence": latest.sequence,
            "progress_event_count": package.event_count,
            "current": latest.current,
            "total": latest.total,
            "unit": latest.unit,
            "producer_contract": "native_execution",
            "producer_validated": True,
            "acceptance_validated": True,
        }
    return None


def _progress_attestation_identity(metadata: dict[str, Any]) -> str:
    """Return a stable evidence identity for provider or native progress."""
    if metadata.get("source") == "jarvis_execution":
        return (
            "jarvis-native:"
            f"{metadata['package_version']}:"
            f"{metadata['package_name']}:"
            f"{metadata['package_id']}:"
            f"{metadata['adapter']}"
        )
    return (
        f"{metadata['provider_distribution']}:"
        f"{metadata['provider_distribution_version']}:"
        f"{metadata['provider_entry_point']}:"
        f"{metadata['adapter']}"
    )


def _verify_progress_monitor(
    definition: ClusterDefinition,
    job_id: str,
    *,
    pattern: str,
    action_payload: dict[str, object],
    lines: list[str],
    runner: CommandRunner,
) -> None:
    _remote_clio_json(
        definition,
        [
            "monitor",
            "add-regex",
            job_id,
            "--pattern",
            pattern,
            "--action",
            "record_progress",
            "--event-type",
            "stdout.delta",
            "--action-payload-json",
            json.dumps(action_payload, sort_keys=True, separators=(",", ":")),
        ],
        runner=runner,
    )
    actions = _remote_clio_json(
        definition,
        ["monitor", "run-once", "--limit", "250"],
        runner=runner,
    )
    action_items = cast(list[dict[str, Any]], actions)
    progress_actions = [
        action for action in action_items if action.get("action") == "record_progress"
    ]
    if not progress_actions:
        raise RelayError(f"acceptance progress pattern did not record progress: {pattern}")
    progress_items = _remote_job_collection(
        definition,
        ["job", "progress", job_id],
        record_key="progress",
        label=f"monitor progress for {job_id}",
        runner=runner,
    )
    if not progress_items:
        raise RelayError("acceptance progress records missing after monitor evaluation")
    lines.append(f"acceptance.progress={len(progress_items)}")
