"""JARVIS service runtime models and the retired binding.

Relay used to bind a JARVIS service runtime from the durable result of an MCP
tool-call job (``jarvis_get_execution``). That job kind is retired, so relay no longer
creates or re-verifies those bindings: ``reverify_jarvis_service_runtime``
refuses and names what a stored binding still owns, so a person can stop it.
Services are served through ``clio_relay.serve``.

The wire/data models live in ``jarvis_service_runtime_models.py`` and are
re-exported here under their original paths.
"""

from __future__ import annotations

from typing import cast

from clio_relay.adapters.worker.core_queue import ClioCoreQueue
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.jarvis_service_runtime_models import (
    JARVIS_DATASET_DESCRIPTOR_SCHEMA as JARVIS_DATASET_DESCRIPTOR_SCHEMA,
)
from clio_relay.jarvis_service_runtime_models import (
    JARVIS_SERVICE_RUNTIME_SCHEMA as JARVIS_SERVICE_RUNTIME_SCHEMA,
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
    JarvisServiceAuthorization as JarvisServiceAuthorization,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisServiceRuntime as JarvisServiceRuntime,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisServiceRuntimeBinding as JarvisServiceRuntimeBinding,
)
from clio_relay.jarvis_service_runtime_models import (
    VerifiedJarvisServiceRuntime,
)

_RETIRED_BINDING = (
    "relay no longer binds JARVIS service runtimes: a binding came from an MCP "
    "tool-call job, which relay no longer runs"
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
    """Refuse: a stored binding's MCP tool-call source can no longer be re-verified."""
    del queue, definition, settings
    raise ValueError(retired_jarvis_binding_message(binding_document))
