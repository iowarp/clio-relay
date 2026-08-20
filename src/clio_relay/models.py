"""Typed relay records shared by CLI, HTTP, endpoints, and tests.

This module is a re-export facade over the owner modules the record types
were split into (iowarp/clio-relay#231): each concern below is re-imported
here under its original name so every existing
``from clio_relay.models import X`` caller and every ``clio_relay.models.X``
qualified/monkeypatch access keeps resolving unchanged -- a pure move, not a
behavior change. See each owner module's own docstring for what it owns:

- :mod:`clio_relay.models_shared` -- cross-domain constants, identity
  helpers, and canonical-JSON primitives every other owner module builds on.
- :mod:`clio_relay.models_enums` -- durable job/task/scheduler/gateway
  state-machine enums.
- :mod:`clio_relay.models_artifact_provenance` -- W3C-PROV-style artifact
  use/transform provenance records.
- :mod:`clio_relay.models_jarvis_package` -- checksum-bound JARVIS package
  input contracts.
- :mod:`clio_relay.models_jarvis_pipeline` -- checksum-bound JARVIS pipeline
  staged-input lineage, bindings, and run manifests.
- :mod:`clio_relay.models_job_specs` -- the typed ``JobSpec`` union and its
  member job-intent records.
- :mod:`clio_relay.models_job` -- the durable :class:`RelayJob` record and
  its full lifecycle, admission through garbage collection.
- :mod:`clio_relay.models_job_telemetry` -- small durable per-job telemetry
  and cursor/lease records.
- :mod:`clio_relay.models_mcp_admission` -- MCP control-query admission
  evidence and durable SEP-2663 task records.
- :mod:`clio_relay.models_scheduling` -- endpoint registration and
  cluster-scheduler durable observation records.
- :mod:`clio_relay.models_gateway` -- artifact index entries and
  scheduler-backed gateway/service runtime records.
"""

from __future__ import annotations

from clio_relay.models_artifact_provenance import (
    ArtifactMechanism,  # noqa: F401
    ArtifactUse,  # noqa: F401
    ArtifactUseEvidence,  # noqa: F401
    ArtifactUseProvenance,  # noqa: F401
    ArtifactUserOrderHead,  # noqa: F401
    TransformEnvironment,  # noqa: F401
    TransformEnvironmentTier,  # noqa: F401
    TransformRef,  # noqa: F401
    TransformReplayContract,  # noqa: F401
    TransformUseEvidence,  # noqa: F401
    UsedArtifactRef,  # noqa: F401
    artifact_use_payload,  # noqa: F401
    validate_artifact_use_collection,  # noqa: F401
)
from clio_relay.models_enums import (
    TERMINAL_STATES,  # noqa: F401
    EndpointRole,  # noqa: F401
    EventLevel,  # noqa: F401
    GatewaySessionState,  # noqa: F401
    JobGcPhase,  # noqa: F401
    JobKind,  # noqa: F401
    JobState,  # noqa: F401
    McpAdmissionClass,  # noqa: F401
    McpOperation,  # noqa: F401
    MonitorRuleAction,  # noqa: F401
    SchedulerCancelDispositionState,  # noqa: F401
    SchedulerPhase,  # noqa: F401
    TaskEventStatus,  # noqa: F401
)
from clio_relay.models_gateway import (
    ArtifactRef,  # noqa: F401
    GatewaySession,  # noqa: F401
    ServiceRuntimeSpec,  # noqa: F401
)
from clio_relay.models_jarvis_package import (
    JarvisPackageInputContractRecord,  # noqa: F401
    JarvisPackageInputRoute,  # noqa: F401
    JarvisPackageLocalFileInput,  # noqa: F401
    _jarvis_package_input_contract_sha256,  # noqa: F401
)
from clio_relay.models_jarvis_pipeline import (
    JarvisPipelineInputBinding,  # noqa: F401
    JarvisPipelineInputBindings,  # noqa: F401
    JarvisPipelineInputLineage,  # noqa: F401
    JarvisPipelineInputRoute,  # noqa: F401
    JarvisRunInputManifest,  # noqa: F401
    JarvisRunInputResolution,  # noqa: F401
    _jarvis_pipeline_input_bindings_sha256,  # noqa: F401
    _jarvis_pipeline_input_lineage_sha256,  # noqa: F401
    _jarvis_run_input_manifest_document_sha256,  # noqa: F401
    _jarvis_run_input_manifest_payload_sha256,  # noqa: F401
)
from clio_relay.models_job import (
    JobTombstone,  # noqa: F401
    JobWaitResult,  # noqa: F401
    OwnerSessionClosure,  # noqa: F401
    RelayJob,  # noqa: F401
    StorageReservationEstimate,  # noqa: F401
    TerminalJobGcPlan,  # noqa: F401
    TerminalJobGcResult,  # noqa: F401
    WaitObservation,  # noqa: F401
    _empty_artifact_uses,  # noqa: F401
    prepare_owned_jarvis_run_submission,  # noqa: F401
)
from clio_relay.models_job_specs import (
    InputArtifactIngestPolicy,  # noqa: F401
    InputArtifactSpec,  # noqa: F401
    JarvisRunSpec,  # noqa: F401
    JobSpec,  # noqa: F401
    McpCallSpec,  # noqa: F401
    RemoteAgentTaskSpec,  # noqa: F401
    _validate_jarvis_execution_id,  # noqa: F401
    deterministic_input_artifact_id,  # noqa: F401
    deterministic_jarvis_execution_id,  # noqa: F401
    is_owned_jarvis_run_spec,  # noqa: F401
)
from clio_relay.models_job_telemetry import (
    Cursor,  # noqa: F401
    Lease,  # noqa: F401
    MonitorRule,  # noqa: F401
    ProgressRecord,  # noqa: F401
    RelayEvent,  # noqa: F401
    RelayTask,  # noqa: F401
    TaskTimelineEvent,  # noqa: F401
)
from clio_relay.models_mcp_admission import (
    McpAdmissionAuthority,  # noqa: F401
    McpControlQueryEvidence,  # noqa: F401
    RelayMcpInputRound,  # noqa: F401
    RelayMcpTaskProjection,  # noqa: F401
    RelayMcpTaskRecord,  # noqa: F401
)
from clio_relay.models_scheduling import (
    EndpointRegistration,  # noqa: F401
    OwnerSessionJobMembership,  # noqa: F401
    SchedulerCancelDisposition,  # noqa: F401
    SchedulerCancelPending,  # noqa: F401
    SchedulerConnectorPlacement,  # noqa: F401
    SchedulerConnectorStepIdentity,  # noqa: F401
    SchedulerConnectorStepStatus,  # noqa: F401
    SchedulerStatus,  # noqa: F401
    _empty_scheduler_cancel_dispositions,  # noqa: F401
)
from clio_relay.models_shared import (
    CLIO_PROVENANCE_METADATA_KEY,  # noqa: F401
    INPUT_INGEST_POLICY_METADATA_KEY,  # noqa: F401
    MAX_ARTIFACT_USE_AGGREGATE_BYTES,  # noqa: F401
    MAX_ARTIFACT_USE_PROVENANCE_BYTES,  # noqa: F401
    MAX_JARVIS_PACKAGE_INPUT_CONTRACT_BYTES,  # noqa: F401
    MAX_JARVIS_PIPELINE_INPUT_BINDINGS_BYTES,  # noqa: F401
    MAX_JARVIS_RUN_INPUT_MANIFEST_BYTES,  # noqa: F401
    MAX_MCP_TASK_ARGUMENT_BYTES,  # noqa: F401
    MAX_MCP_TASK_INPUT_ROUND_BYTES,  # noqa: F401
    MAX_MCP_TASK_JSON_DEPTH,  # noqa: F401
    MAX_MCP_TASK_JSON_NODES,  # noqa: F401
    MAX_MCP_TASK_PROJECTION_BYTES,  # noqa: F401
    MAX_TRANSFORM_ENVIRONMENT_BYTES,  # noqa: F401
    MAX_TRANSFORM_REF_BYTES,  # noqa: F401
    MAX_TRANSFORM_USED_EVIDENCE,  # noqa: F401
    MCP_ADMISSION_AUTHORITY_METADATA_KEY,  # noqa: F401
    REGISTERED_JARVIS_EXECUTION_CONTRACTS,  # noqa: F401
    REGISTERED_JARVIS_USER_CONTRACT,  # noqa: F401
    RELAY_CREDENTIAL_ENV_NAMES,  # noqa: F401
    _canonical_json_bytes,  # noqa: F401
    _canonical_json_sha256,  # noqa: F401
    _require_bounded_mcp_task_json,  # noqa: F401
    _require_canonical_json_size,  # noqa: F401
    _valid_environment_name,  # noqa: F401
    new_id,  # noqa: F401
    utc_now,  # noqa: F401
    validate_mcp_env_from,  # noqa: F401
)
