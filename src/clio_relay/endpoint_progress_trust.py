"""Package-progress observation trust (iowarp/clio-relay#231).

Owner module for two related layers of progress-observation trust:

- Bounded, race-tolerant sidecar-record reading:
  ``_read_bounded_sidecar_record`` (one JSONL record with a strict byte
  bound, preserving an incomplete writer's in-progress line rather than
  treating it as corrupt), ``_progress_log_checkpoint``/``_progress_log_
  checkpoint_matches`` (tail-hash a package progress log so a later reopen
  can detect the source was truncated or replaced), and ``_progress_from_
  sidecar_record`` (verify one ordered HMAC-authenticated observation).
- Cross-checked provider/notification trust: ``_trusted_provider_metadata``
  stamps a provider candidate without trusting plugin-supplied provenance;
  ``_trusted_mcp_progress_metadata``/``_trusted_native_mcp_progress_
  metadata`` validate an MCP-bridged or native-HMAC-protected JARVIS
  progress notification came from this job's pinned route and matches the
  worker's own locally-resolved provider identity/acceptance predicate;
  ``_trusted_sidecar_metadata`` stamps the regex-adapter fallback path.

Depends on ``endpoint_sidecar_types.py`` (schema constants) and
``endpoint_jarvis_recovery.py`` (``_trusted_jarvis_mcp_route`` -- the route-
trust check every MCP-bridged notification validator calls first), both
leaves relative to this module, so it stays acyclic. ``EndpointWorker``
(still resident in ``endpoint.py``) is this module's main caller.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
from pathlib import Path
from typing import Any, BinaryIO, cast

import yaml

from clio_relay.endpoint_jarvis_recovery import _trusted_jarvis_mcp_route
from clio_relay.endpoint_sidecar_types import (
    MCP_JARVIS_NATIVE_PROGRESS_BRIDGE_SCHEMA,
    MCP_PACKAGE_PROGRESS_BRIDGE_SCHEMA,
    PROGRESS_SIDECAR_RECORD_SCHEMA,
    SIDECAR_DRAIN_CHUNK_BYTES,
    _PackageProgressLogState,
)
from clio_relay.errors import ConfigurationError
from clio_relay.models import McpCallSpec, RelayJob
from clio_relay.progress_adapters import (
    PackageProgressProvider,
    package_progress_adapter_from_pipeline,
)
from clio_relay.progress_provenance import (
    PROTECTED_PROGRESS_METADATA_KEYS,
    PackageProgressSourceAuthority,
    jarvis_execution_progress_metadata,
    package_progress_metadata,
    package_progress_provider_metadata,
)


def _read_bounded_sidecar_record(
    handle: BinaryIO,
    *,
    max_bytes: int,
    allow_final_record: bool,
) -> tuple[bytes | None, str]:
    """Read one bounded JSONL record and preserve incomplete writer output."""
    record_start = handle.tell()
    line = handle.readline(max_bytes + 1)
    if line == b"":
        return None, "eof"
    if len(line) > max_bytes:
        while not line.endswith(b"\n"):
            fragment = handle.readline(SIDECAR_DRAIN_CHUNK_BYTES)
            if fragment == b"" or fragment.endswith(b"\n"):
                break
        return None, "oversized"
    if not line.endswith(b"\n") and not allow_final_record:
        handle.seek(record_start)
        return None, "incomplete"
    return line, "record"


def _progress_log_checkpoint(
    handle: BinaryIO,
    offset: int,
    *,
    path: Path,
) -> tuple[int, str | None]:
    if offset <= 0:
        return 0, None
    checkpoint_offset = max(0, offset - 4096)
    expected_length = offset - checkpoint_offset
    original_offset = handle.tell()
    try:
        handle.seek(checkpoint_offset)
        payload = handle.read(expected_length)
    except OSError as exc:
        raise ConfigurationError(
            f"could not checkpoint package progress log {path}: {exc}"
        ) from exc
    finally:
        handle.seek(original_offset)
    if len(payload) != expected_length:
        raise ConfigurationError(f"package progress log changed while it was checkpointed: {path}")
    return checkpoint_offset, hashlib.sha256(payload).hexdigest()


def _progress_log_checkpoint_matches(
    state: _PackageProgressLogState,
    handle: BinaryIO,
) -> bool:
    if state.checkpoint_sha256 is None:
        return True
    expected_length = state.offset - state.checkpoint_offset
    original_offset = handle.tell()
    try:
        handle.seek(state.checkpoint_offset)
        payload = handle.read(expected_length)
    except OSError as exc:
        raise ConfigurationError(
            f"could not verify package progress log checkpoint {state.path}: {exc}"
        ) from exc
    finally:
        handle.seek(original_offset)
    return (
        len(payload) == expected_length
        and hashlib.sha256(payload).hexdigest() == state.checkpoint_sha256
    )


def _progress_from_sidecar_record(
    record: object,
    *,
    expected_key: str,
    expected_sequence: int,
) -> dict[str, object]:
    """Verify one ordered HMAC-authenticated package-progress observation."""
    if not isinstance(record, dict):
        raise ValueError("progress sidecar record must be an object")
    typed = cast(dict[str, object], record)
    if set(typed) != {"schema_version", "sequence", "progress", "progress_hmac"}:
        raise ValueError("progress sidecar record fields did not match")
    if typed.get("schema_version") != PROGRESS_SIDECAR_RECORD_SCHEMA:
        raise ValueError("progress sidecar record schema did not match")
    sequence = typed.get("sequence")
    if isinstance(sequence, bool) or sequence != expected_sequence:
        raise ValueError("progress sidecar sequence did not match")
    progress = typed.get("progress")
    if not isinstance(progress, dict):
        raise ValueError("progress sidecar omitted its progress object")
    typed_progress = {
        str(key): value for key, value in cast(dict[object, object], progress).items()
    }
    observed_hmac = typed.get("progress_hmac")
    if not isinstance(observed_hmac, str) or len(observed_hmac) != 64:
        raise ValueError("progress sidecar HMAC was invalid")
    signed = {
        "schema_version": PROGRESS_SIDECAR_RECORD_SCHEMA,
        "sequence": expected_sequence,
        "progress": typed_progress,
    }
    canonical = json.dumps(
        signed,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    expected_hmac = hmac.new(
        expected_key.encode("utf-8"),
        canonical,
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(observed_hmac, expected_hmac):
        raise ValueError("progress sidecar HMAC did not match")
    return typed_progress


def _trusted_provider_metadata(
    metadata: dict[str, object],
    *,
    job_id: str,
    provider: PackageProgressProvider,
    source_authority: PackageProgressSourceAuthority,
    acceptance_validated: bool,
) -> dict[str, object]:
    """Stamp a provider candidate without trusting plugin-supplied provenance."""
    identity = provider.identity
    return package_progress_provider_metadata(
        metadata,
        package_name=identity.package_name,
        package_version=identity.package_version,
        run_id=job_id,
        adapter_name=identity.adapter_name,
        provider_entry_point=identity.entry_point_name,
        provider_entry_point_value=identity.entry_point_value,
        provider_distribution=identity.distribution_name,
        provider_distribution_version=identity.distribution_version,
        source_authority=source_authority,
        application_profile=identity.application_profile,
        provider_validated=True,
        acceptance_validated=acceptance_validated,
    )


def _trusted_mcp_progress_metadata(
    job: RelayJob,
    metadata: dict[str, object],
) -> dict[str, object]:
    """Cross-check one runner-bridged JARVIS provider notification."""
    route_valid, route_reason = _trusted_jarvis_mcp_route(job)
    if not route_valid:
        raise ConfigurationError(f"MCP package progress route was not trusted: {route_reason}")
    assert isinstance(job.spec, McpCallSpec)
    raw_bridge = metadata.get("mcp_progress_bridge")
    if not isinstance(raw_bridge, dict):
        raise ConfigurationError("MCP package progress bridge metadata is missing")
    bridge = {str(key): value for key, value in cast(dict[object, object], raw_bridge).items()}
    required_bridge_fields = {
        "schema_version",
        "execution_id",
        "pipeline_id",
        "notification_sequence",
        "source_authority",
        "provider",
        "provider_acceptance_validated",
        "expected_server_artifact_digest",
        "observed_server_artifact_digest",
        "execution_validated",
    }
    if set(bridge) != required_bridge_fields or bridge.get("schema_version") != (
        MCP_PACKAGE_PROGRESS_BRIDGE_SCHEMA
    ):
        raise ConfigurationError("MCP package progress bridge schema is invalid")
    execution_id = bridge.get("execution_id")
    if not isinstance(execution_id, str) or not execution_id or len(execution_id) > 4096:
        raise ConfigurationError("MCP package progress execution id is invalid")
    pipeline_id = bridge.get("pipeline_id")
    expected_pipeline_id = job.spec.arguments.get("pipeline_id")
    if not isinstance(expected_pipeline_id, str) or pipeline_id != expected_pipeline_id:
        raise ConfigurationError("MCP package progress pipeline id did not match the job")
    sequence = bridge.get("notification_sequence")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
        raise ConfigurationError("MCP package progress notification sequence is invalid")
    transport_source = bridge.get("source_authority")
    if transport_source not in {"package_log", "jarvis_stdout_fallback"}:
        raise ConfigurationError("MCP package progress source authority is invalid")
    expected_digest = job.spec.expected_server_artifact_digest
    if (
        expected_digest is None
        or bridge.get("expected_server_artifact_digest") != expected_digest
        or bridge.get("observed_server_artifact_digest") != expected_digest
    ):
        raise ConfigurationError("MCP package progress server artifact did not match discovery")
    execution_validated = bridge.get("execution_validated")
    provider_acceptance = bridge.get("provider_acceptance_validated")
    if not isinstance(execution_validated, bool) or not isinstance(provider_acceptance, bool):
        raise ConfigurationError("MCP package progress validation flags must be boolean")
    raw_provider = bridge.get("provider")
    if not isinstance(raw_provider, dict):
        raise ConfigurationError("MCP package progress provider identity is missing")
    provider_metadata = {
        str(key): value for key, value in cast(dict[object, object], raw_provider).items()
    }
    required_provider_fields = {
        "entry_point",
        "entry_point_value",
        "distribution",
        "distribution_version",
        "adapter",
        "package_name",
        "package_version",
    }
    if not required_provider_fields.issubset(provider_metadata) or not set(
        provider_metadata
    ).issubset(required_provider_fields | {"application_profile"}):
        raise ConfigurationError("MCP package progress provider identity is incomplete")
    for field_name in required_provider_fields:
        value = provider_metadata.get(field_name)
        if not isinstance(value, str) or not value:
            raise ConfigurationError(
                f"MCP package progress provider {field_name} must be a non-empty string"
            )
    package_name = cast(str, provider_metadata["package_name"])
    adapter_name = cast(str, provider_metadata["adapter"])
    provider_document = yaml.safe_dump(
        {
            "pkgs": [
                {
                    "pkg_type": package_name,
                    "progress": {"adapter": adapter_name},
                }
            ]
        },
        sort_keys=True,
    )
    local_provider = package_progress_adapter_from_pipeline(provider_document)
    if local_provider is None:
        raise ConfigurationError("MCP package progress provider is not installed locally")
    identity = local_provider.identity
    identity_matches = (
        provider_metadata.get("entry_point") == identity.entry_point_name
        and provider_metadata.get("entry_point_value") == identity.entry_point_value
        and _normalized_provider_distribution(str(provider_metadata["distribution"]))
        == _normalized_provider_distribution(identity.distribution_name)
        and provider_metadata.get("distribution_version") == identity.distribution_version
        and provider_metadata.get("adapter") == identity.adapter_name
        and provider_metadata.get("package_name") == identity.package_name
        and provider_metadata.get("package_version") == identity.package_version
        and provider_metadata.get("application_profile") == identity.application_profile
    )
    if not identity_matches:
        raise ConfigurationError("MCP package progress provider identity did not match the worker")
    candidate_metadata = dict(metadata)
    candidate_metadata.pop("mcp_progress_bridge", None)
    preliminary = _trusted_provider_metadata(
        candidate_metadata,
        job_id=job.job_id,
        provider=local_provider,
        source_authority=PackageProgressSourceAuthority.MCP_PROGRESS_NOTIFICATION,
        acceptance_validated=False,
    )
    try:
        locally_accepted = (
            local_provider.acceptance_progress_valid(cast(dict[str, Any], preliminary)) is True
        )
    except Exception as exc:
        raise ConfigurationError(
            f"MCP package progress worker acceptance predicate failed: {type(exc).__name__}: {exc}"
        ) from exc
    if locally_accepted is not provider_acceptance:
        raise ConfigurationError(
            "MCP package progress provider acceptance did not match the worker predicate"
        )
    trusted = _trusted_provider_metadata(
        candidate_metadata,
        job_id=job.job_id,
        provider=local_provider,
        source_authority=PackageProgressSourceAuthority.MCP_PROGRESS_NOTIFICATION,
        acceptance_validated=execution_validated and locally_accepted,
    )
    trusted.update(
        {
            "provider_execution_id": execution_id,
            "provider_pipeline_id": pipeline_id,
            "provider_server_artifact_digest": expected_digest,
            "provider_notification_sequence": sequence,
            "provider_transport_source_authority": transport_source,
            "provider_execution_validated": execution_validated,
        }
    )
    return trusted


def _trusted_native_mcp_progress_metadata(
    job: RelayJob,
    metadata: dict[str, object],
) -> dict[str, object]:
    """Validate an HMAC-protected native JARVIS progress observation."""
    raw_bridge = metadata.get("mcp_native_progress_bridge")
    if not isinstance(raw_bridge, dict):
        raise ConfigurationError("native MCP JARVIS progress bridge metadata is missing")
    bridge = cast(dict[str, object], raw_bridge)
    expected_fields = {
        "schema_version",
        "execution_id",
        "pipeline_id",
        "execution_state",
        "terminal",
        "transport_sequence",
        "package_name",
        "package_id",
        "event_count",
        "event_schema_version",
        "event_sequence",
        "event_state",
        "observed_at_epoch",
        "determinate",
        "skipped_event_count",
        "expected_server_artifact_digest",
        "observed_server_artifact_digest",
        "execution_validated",
    }
    if (
        set(bridge) != expected_fields
        or bridge.get("schema_version") != MCP_JARVIS_NATIVE_PROGRESS_BRIDGE_SCHEMA
        or bridge.get("event_schema_version") != "jarvis.progress.v1"
    ):
        raise ConfigurationError("native MCP JARVIS progress bridge schema did not match")
    route_valid, route_reason = _trusted_jarvis_mcp_route(job)
    if not route_valid:
        raise ConfigurationError(
            f"native MCP JARVIS progress route was not trusted: {route_reason}"
        )
    assert isinstance(job.spec, McpCallSpec)
    expected_digest = job.spec.expected_server_artifact_digest
    if (
        expected_digest is None
        or bridge.get("expected_server_artifact_digest") != expected_digest
        or bridge.get("observed_server_artifact_digest") != expected_digest
    ):
        raise ConfigurationError("native MCP JARVIS progress server artifact did not match")
    arguments = job.spec.arguments
    pipeline_id = bridge.get("pipeline_id")
    if (
        not isinstance(pipeline_id, str)
        or not pipeline_id
        or arguments.get("pipeline_id") != pipeline_id
    ):
        raise ConfigurationError("native MCP JARVIS progress pipeline identity did not match")
    string_fields = (
        "execution_id",
        "execution_state",
        "package_name",
        "package_id",
        "event_state",
    )
    for field_name in string_fields:
        value = bridge.get(field_name)
        if not isinstance(value, str) or not value:
            raise ConfigurationError(f"native MCP JARVIS progress {field_name} must be non-empty")
    if bridge["execution_state"] not in {
        "preparing",
        "scripted",
        "submitting",
        "submitted",
        "running",
        "completed",
        "failed",
        "canceled",
        "unknown",
    }:
        raise ConfigurationError("native MCP JARVIS progress execution state was invalid")
    if bridge["event_state"] not in {
        "pending",
        "starting",
        "running",
        "ready",
        "completed",
        "failed",
        "canceled",
    }:
        raise ConfigurationError("native MCP JARVIS progress event state was invalid")
    integer_fields = (
        "transport_sequence",
        "event_count",
        "event_sequence",
        "skipped_event_count",
    )
    for field_name in integer_fields:
        value = bridge.get(field_name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ConfigurationError(f"native MCP JARVIS progress {field_name} was invalid")
    if cast(int, bridge["event_count"]) < 1:
        raise ConfigurationError("native MCP JARVIS progress event count was invalid")
    observed = bridge.get("observed_at_epoch")
    if (
        isinstance(observed, bool)
        or not isinstance(observed, int | float)
        or not math.isfinite(float(observed))
        or observed < 0
    ):
        raise ConfigurationError("native MCP JARVIS progress observed time was invalid")
    for field_name in ("terminal", "determinate", "execution_validated"):
        if not isinstance(bridge.get(field_name), bool):
            raise ConfigurationError(f"native MCP JARVIS progress {field_name} was invalid")
    candidate_metadata = dict(metadata)
    candidate_metadata.pop("mcp_native_progress_bridge", None)
    return jarvis_execution_progress_metadata(
        candidate_metadata,
        relay_job_id=job.job_id,
        execution_id=cast(str, bridge["execution_id"]),
        pipeline_id=pipeline_id,
        package_name=cast(str, bridge["package_name"]),
        package_id=cast(str, bridge["package_id"]),
        progress_state=cast(str, bridge["event_state"]),
        progress_sequence=cast(int, bridge["event_sequence"]),
        observed_at_epoch=float(observed),
        determinate=cast(bool, bridge["determinate"]),
        event_count=cast(int, bridge["event_count"]),
        skipped_event_count=cast(int, bridge["skipped_event_count"]),
        execution_state=cast(str, bridge["execution_state"]),
        execution_terminal=cast(bool, bridge["terminal"]),
        transport_sequence=cast(int, bridge["transport_sequence"]),
        server_artifact_digest=expected_digest,
        execution_binding_validated=cast(bool, bridge["execution_validated"]),
    )


def _normalized_provider_distribution(value: str) -> str:
    return value.lower().replace("_", "-").replace(".", "-")


def _trusted_sidecar_metadata(metadata: dict[str, object], *, job_id: str) -> dict[str, object]:
    preserved = {
        key: value for key, value in metadata.items() if key not in PROTECTED_PROGRESS_METADATA_KEYS
    }
    preserved["adapter"] = "regex"
    return package_progress_metadata(
        preserved,
        package_name="clio_relay.bounded_command",
        package_version="builtin",
        run_id=job_id,
    )
