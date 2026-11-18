"""JARVIS service runtime models and the private authority resolver.

Relay used to bind a JARVIS service runtime from a durable ``mcp_call`` result
(``jarvis_get_execution``). That job kind is retired, so relay no longer
creates or re-verifies those bindings: ``reverify_jarvis_service_runtime``
refuses and names what a stored binding still owns, so a person can stop it.
Services are served through ``clio_relay.serve``.

The wire/data models live in ``jarvis_service_runtime_models.py`` and are
re-exported here under their original paths.
"""

from __future__ import annotations

import hmac
import json
from typing import cast

from clio_relay.adapters.worker.core_queue import ClioCoreQueue
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.jarvis_service_runtime_models import (
    JARVIS_DATASET_DESCRIPTOR_SCHEMA as JARVIS_DATASET_DESCRIPTOR_SCHEMA,
)
from clio_relay.jarvis_service_runtime_models import (
    JARVIS_SERVICE_RUNTIME_AUTHORITY_SCHEMA as JARVIS_SERVICE_RUNTIME_AUTHORITY_SCHEMA,
)
from clio_relay.jarvis_service_runtime_models import (
    JARVIS_SERVICE_RUNTIME_SCHEMA as JARVIS_SERVICE_RUNTIME_SCHEMA,
)
from clio_relay.jarvis_service_runtime_models import (
    JARVIS_SERVICE_RUNTIME_SCHEMA_V1,
    JSON,
    JarvisServiceRuntimeAuthority,
    VerifiedJarvisServiceRuntime,
)
from clio_relay.jarvis_service_runtime_models import (
    JARVIS_SERVICE_RUNTIME_SNAPSHOT_SCHEMA as JARVIS_SERVICE_RUNTIME_SNAPSHOT_SCHEMA,
)
from clio_relay.jarvis_service_runtime_models import (
    RELAY_JARVIS_RUNTIME_BINDING_SCHEMA as RELAY_JARVIS_RUNTIME_BINDING_SCHEMA,
)
from clio_relay.jarvis_service_runtime_models import (
    ClioKitJarvisExecutionQuery as ClioKitJarvisExecutionQuery,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisArtifactIdentity as JarvisArtifactIdentity,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisDatasetArray as JarvisDatasetArray,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisDatasetDescriptor as JarvisDatasetDescriptor,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisDatasetFingerprint as JarvisDatasetFingerprint,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisDatasetMember as JarvisDatasetMember,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisExecutionServiceRuntimes as JarvisExecutionServiceRuntimes,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisPrivateServiceAuthorization as JarvisPrivateServiceAuthorization,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisServiceAuthorization as JarvisServiceAuthorization,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisServiceRuntime as JarvisServiceRuntime,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisServiceRuntimeBinding as JarvisServiceRuntimeBinding,
)

OWNED_SESSION_JARVIS_RUNTIME_AUTHORITY_PATH = "/internal/jarvis-runtime-authority"
_MAX_AUTHORITY_OUTPUT_BYTES = 32 * 1024
_AUTHORITY_QUERY_TIMEOUT_SECONDS = 30
_RETIRED_BINDING = (
    "relay no longer binds JARVIS service runtimes: a binding came from an mcp_call "
    "job, which relay no longer runs"
)


def retired_jarvis_binding_message(binding_document: object) -> str:
    """Name what a stored JARVIS binding still owns, so a person can stop it."""
    document = (
        cast(dict[str, object], binding_document) if isinstance(binding_document, dict) else {}
    )

    def text(key: str) -> str | None:
        value = document.get(key)
        return value if isinstance(value, str) and value else None

    execution = text("jarvis_execution_id")
    if execution is None:
        return f"{_RETIRED_BINDING}. The stored binding names no JARVIS execution."
    job = text("scheduler_native_id")
    if job is not None:
        provider = text("scheduler_provider") or "scheduler"
        return (
            f"{_RETIRED_BINDING}. Cancel {provider} job {job} "
            f"(JARVIS execution {execution}) yourself."
        )
    service = text("service_instance_id") or "unknown"
    return f"{_RETIRED_BINDING}. Stop JARVIS execution {execution} (service {service}) yourself."


def reverify_jarvis_service_runtime(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    settings: RelaySettings | None = None,
    binding_document: object,
) -> VerifiedJarvisServiceRuntime:
    """Refuse: a stored binding's mcp_call source can no longer be re-verified."""
    del queue, definition, settings
    raise ValueError(retired_jarvis_binding_message(binding_document))


def resolve_local_verified_jarvis_service_runtime_authority(
    *,
    jarvis_bin: str,
    verified: VerifiedJarvisServiceRuntime,
) -> JarvisServiceRuntimeAuthority | None:
    """Resolve and revalidate one exact verified runtime on its cluster host."""
    runtime = verified.runtime
    binding = verified.binding
    if runtime.schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V1:
        return None
    expected_digest = binding.authorization_sha256
    if expected_digest is None:
        raise RelayError("authenticated JARVIS runtime omitted its authority digest")
    authority = resolve_local_jarvis_service_runtime_authority(
        jarvis_bin=jarvis_bin,
        execution_id=binding.jarvis_execution_id,
        pipeline_id=verified.native_execution.execution_handle.pipeline_id,
        package_id=binding.package_id,
        service_instance_id=binding.service_instance_id,
        revision=binding.service_revision,
        token_sha256=expected_digest,
    )
    _validate_resolved_authority(verified=verified, authority=authority)
    return authority


def resolve_local_jarvis_service_runtime_authority(
    *,
    jarvis_bin: str,
    execution_id: str,
    pipeline_id: str,
    package_id: str,
    service_instance_id: str,
    revision: int,
    token_sha256: str,
) -> JarvisServiceRuntimeAuthority:
    """Invoke JARVIS's bounded trusted resolver on the current cluster host."""
    if not jarvis_bin:
        raise ConfigurationError("JARVIS service runtime authority resolver is not configured")
    provider = JarvisCdProvider(jarvis_bin=jarvis_bin)
    provider.require_available()
    arguments = _authority_cli_arguments(
        execution_id=execution_id,
        pipeline_id=pipeline_id,
        package_id=package_id,
        service_instance_id=service_instance_id,
        revision=revision,
        token_sha256=token_sha256,
    )
    result = provider.run_command_streaming(
        [jarvis_bin, "execution", "resolve-service-runtime-authority", *arguments, "+json"],
        timeout_seconds=_AUTHORITY_QUERY_TIMEOUT_SECONDS,
    )
    if result.returncode != 0:
        raise RelayError(
            f"JARVIS service runtime authority resolution failed with exit code {result.returncode}"
        )
    if len(result.stdout.encode("utf-8")) > _MAX_AUTHORITY_OUTPUT_BYTES:
        raise RelayError("JARVIS service runtime authority response exceeded its byte limit")
    document = _decode_unique_json_object(
        result.stdout,
        label="JARVIS service runtime authority resolver",
    )
    return JarvisServiceRuntimeAuthority.model_validate(document)


def private_jarvis_service_runtime_authority_document(
    authority: JarvisServiceRuntimeAuthority,
) -> JSON:
    """Render the resolver's raw private wire document for relay-internal transport only."""
    document = authority.model_dump(mode="json")
    authorization = cast(dict[str, object], document["authorization"])
    authorization["token"] = authority.authorization.token.get_secret_value()
    return document


def _authority_cli_arguments(
    *,
    execution_id: str,
    pipeline_id: str,
    package_id: str,
    service_instance_id: str,
    revision: int,
    token_sha256: str,
) -> list[str]:
    """Build the exact identity-complete argument vector for JARVIS's private resolver."""
    return [
        execution_id,
        "--pipeline-id",
        pipeline_id,
        "--package-id",
        package_id,
        "--service-instance-id",
        service_instance_id,
        "--revision",
        str(revision),
        "--token-sha256",
        token_sha256,
    ]


def _validate_resolved_authority(
    *,
    verified: VerifiedJarvisServiceRuntime,
    authority: JarvisServiceRuntimeAuthority,
) -> None:
    """Require the resolver response to match every durable public identity."""
    binding = verified.binding
    pipeline_id = verified.native_execution.execution_handle.pipeline_id
    if (
        authority.execution_id != binding.jarvis_execution_id
        or authority.pipeline_id != pipeline_id
        or authority.package_id != binding.package_id
        or authority.service_instance_id != binding.service_instance_id
        or authority.revision != binding.service_revision
    ):
        raise RelayError("JARVIS service runtime authority returned a different runtime identity")
    expected_digest = binding.authorization_sha256
    if expected_digest is None or not hmac.compare_digest(
        authority.token_sha256,
        expected_digest,
    ):
        raise RelayError("JARVIS service runtime authority returned a different token digest")


def _decode_unique_json_object(value: str, *, label: str) -> JSON:
    """Decode one bounded JSON object while rejecting duplicate keys and constants."""

    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, nested in pairs:
            if key in result:
                raise ValueError(f"{label} returned duplicate JSON key: {key}")
            result[key] = nested
        return result

    def reject_constant(constant: str) -> object:
        raise ValueError(f"{label} returned non-finite JSON constant: {constant}")

    try:
        document: object = json.loads(
            value,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except (json.JSONDecodeError, UnicodeError, RecursionError):
        raise RelayError(f"{label} returned invalid JSON") from None
    if not isinstance(document, dict):
        raise RelayError(f"{label} did not return a JSON object")
    return cast(JSON, document)
