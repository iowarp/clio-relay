"""Classify legacy acceptance-line text facts as proof of success (#231).

Extracted from :mod:`clio_relay.validation_report`
(``docs/design/relay-architecture-2026-08.md``, #231). Live acceptance runs
still emit a stream of ``key=value`` text facts (the pre-structured-evidence
format) alongside typed transport-probe evidence.
:func:`~clio_relay.validation_recorder.ValidationRecorder.observe_line` asks
this module whether one such line proves its check passed
(:func:`line_proves_success`) and which check scope it belongs to
(:func:`acceptance_scope`). Nothing here reads or writes a report -- it is a
pure classification function over one ``key``/``value`` pair, plus the fact
catalogs (:data:`SUCCESS_FACT_VALUES`, :data:`ACCEPTANCE_EXACT_FACTS`, ...)
that encode which keys and values are meaningful.
"""

from __future__ import annotations

SUCCESS_FACT_VALUES = frozenset(
    {
        "completed",
        "ok",
        "observed",
        "passed",
        "stopped",
        "succeeded",
        "verified",
    }
)
FAILED_OR_UNKNOWN_FACT_VALUES = frozenset(
    {
        "",
        "detached",
        "error",
        "failed",
        "false",
        "frp_stcp",
        "none",
        "not_started",
        "refused",
        "residual",
        "unknown",
        "unverified",
    }
)
TRANSPORT_SUCCESS_FACTS = {
    "transport.cleanup": {"passed"},
    "transport.healthz": {"ok"},
    "transport.http_artifacts": {"ok"},
    "transport.http_events": {"ok"},
    "transport.http_provenance": {"ok"},
    "transport.http_wait": {"succeeded"},
    "transport.remote_cleanup": {"passed"},
    "transport.remote_session_ownership": {"verified"},
}
WORKER_IDENTITY_FACTS = frozenset(
    {
        "worker.artifact-sha256",
        "worker.artifact-version",
        "worker.component-artifacts",
        "worker.component-runtime",
        "worker.components",
        "worker.scheduler-provider",
        "worker.source-identity",
    }
)
ACCEPTANCE_EXACT_FACTS = frozenset(
    {
        "acceptance.agent_child_job_id",
        "acceptance.agent_job_id",
        "acceptance.agent_prompt",
        "acceptance.agent_state",
        "acceptance.application_boundary",
        "acceptance.cluster_doctor",
        "acceptance.job_id",
        "acceptance.job_state",
        "acceptance.live_progress_adapter",
        "acceptance.monitor",
        "acceptance.package_adapter",
        "acceptance.package_owner",
        "acceptance.pipeline",
        "acceptance.progress",
    }
)
ACCEPTANCE_VERIFIED_SUFFIXES = (
    ".artifact_read",
    ".artifacts",
    ".events",
    ".progress_adapter",
    ".provenance",
    ".runtime_metadata_artifact",
    ".runtime_metadata_source",
    ".runtime_scheduler_job_id",
    ".runtime_scheduler_job_id_source",
    ".runtime_scheduler_provider",
    ".stderr_bytes",
    ".stdout_bytes",
    ".structured_runtime_metadata",
    ".structured_runtime_scheduler_identity",
    ".tasks",
)


def line_proves_success(key: str, value: str) -> bool:
    """Return whether a legacy text fact explicitly proves a successful check."""
    normalized = value.strip().lower()
    if normalized in FAILED_OR_UNKNOWN_FACT_VALUES:
        return False
    if key.startswith("transport."):
        return normalized in TRANSPORT_SUCCESS_FACTS.get(key, set())
    if key.startswith("direct_transport."):
        return False
    if key.startswith("scheduler."):
        return normalized in SUCCESS_FACT_VALUES
    if key.startswith("cluster."):
        return normalized in SUCCESS_FACT_VALUES
    if key.startswith("package-progress."):
        return normalized in SUCCESS_FACT_VALUES or (
            key == "package-progress.identity" and bool(value.strip())
        )
    if key.startswith("worker."):
        if key in WORKER_IDENTITY_FACTS:
            if key == "worker.source-identity" and "none" in normalized.split(":"):
                return False
            return bool(value.strip())
        return normalized in SUCCESS_FACT_VALUES
    if key.startswith("acceptance."):
        if key not in ACCEPTANCE_EXACT_FACTS and not key.endswith(ACCEPTANCE_VERIFIED_SUFFIXES):
            return False
        if key.endswith(("job_state", ".job_state", "_state", ".state")):
            return normalized == "succeeded"
        if key in {
            "acceptance.agent_child_job_id",
            "acceptance.agent_job_id",
            "acceptance.job_id",
        }:
            return value.startswith("job_")
        # Acceptance code emits these facts only after validating the referenced
        # record, count, artifact, or package-owned identity. Negative sentinels
        # were rejected above; arbitrary non-acceptance prefixes never enter here.
        return bool(value.strip())
    return False


def acceptance_scope(key: str) -> str:
    """Return the check scope one acceptance-line fact key belongs to."""
    if ".runtime_" in key:
        return key.split(".runtime_", 1)[0]
    for suffix in (
        "_job_id",
        ".job_id",
        "_job_state",
        ".job_state",
        "_state",
        ".stdout_bytes",
        ".stderr_bytes",
        ".artifacts",
    ):
        if key.endswith(suffix):
            return key.removesuffix(suffix).rstrip(".")
    return key.rsplit(".", 1)[0]
