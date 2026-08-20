"""Bounded remote observation waits for live acceptance.

Extracted from ``live_acceptance.py`` (#231 rework): the concern of
polling remote state until a durable condition holds -- free reserved
control-query capacity before scheduling a service, a live structured
runtime metadata record that does not require the source job to finish,
one job reaching its terminal ``succeeded`` state, and one live package
progress adapter appearing before the job goes terminal. Every wait here
raises the same typed :class:`~clio_relay.live_acceptance_models.
_AcceptanceObservationPending` (via ``_wait_for_success``) or a plain
``RelayError`` on its own bounded deadline, never a silent timeout.
"""

from __future__ import annotations

import time
from typing import Any, Literal, cast

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.live_acceptance_job_verification import _remote_job_has_event
from clio_relay.live_acceptance_models import (
    CommandRunner,
    RuntimeMetadataAcceptance,
    _AcceptanceObservationPending,
)
from clio_relay.live_acceptance_progress import (
    _has_progress_adapter,
    _native_progress_attestation,
    _runtime_metadata_document_facts,
    _runtime_metadata_from_job_status,
)
from clio_relay.live_acceptance_remote_io import _remote_clio_json, _remote_job_collection
from clio_relay.models import TERMINAL_STATES, JobState, RelayJob
from clio_relay.runtime_metadata import (
    RUNTIME_METADATA_SCHEMA,
    JarvisRuntimeMetadata,
    RuntimeMetadataSource,
)
from clio_relay.validation_report import EvidenceReference


def _require_secure_runtime_control_capacity(
    definition: ClusterDefinition,
    *,
    cluster: str,
    runner: CommandRunner,
    evidence: list[EvidenceReference] | None = None,
) -> dict[str, object]:
    """Return verified free control-query capacity before scheduling a service."""
    raw_status = _remote_clio_json(
        definition,
        ["worker", "status", "--cluster", cluster],
        runner=runner,
    )
    if not isinstance(raw_status, dict):
        raise RelayError("secure runtime worker status was not a JSON object")
    status = cast(dict[str, object], raw_status)
    configured_control = status.get("configured_control_query_concurrency")
    configured_workload = status.get("configured_workload_concurrency")
    consistent = status.get("control_query_concurrency_consistent")
    scan_truncated = status.get("scan_truncated")
    active_raw = status.get("active_leases_by_mcp_admission_class")
    active = cast(dict[str, object], active_raw) if isinstance(active_raw, dict) else None
    active_control = None if active is None else active.get("control_query")
    observed: dict[str, object] = {
        "configured_workload_concurrency": configured_workload,
        "configured_control_query_concurrency": configured_control,
        "active_control_query_leases": active_control,
        "control_query_concurrency_consistent": consistent,
        "scan_truncated": scan_truncated,
        "worker_generation_id": status.get("worker_generation_id"),
        "worker_generation_complete": status.get("worker_generation_complete"),
        "source_submitted": False,
        "scheduler_job_created": False,
    }
    if (
        type(configured_control) is int
        and type(active_control) is int
        and configured_control >= active_control
    ):
        observed["free_control_query_slots"] = configured_control - active_control
    if evidence is not None:
        evidence.append(
            EvidenceReference(
                kind="worker_capacity",
                reference=f"relay-worker://{cluster}/control-query",
                metadata=observed,
            )
        )
    if scan_truncated is not False:
        raise RelayError("secure runtime worker-capacity scan was incomplete")
    if consistent is not True:
        raise RelayError("secure runtime worker control-query policy is inconsistent")
    if type(configured_workload) is not int or configured_workload < 1:
        raise RelayError("secure runtime requires at least one workload worker slot")
    if type(configured_control) is not int or configured_control < 1:
        raise RelayError("secure runtime requires at least one reserved control-query slot")
    if type(active_control) is not int or active_control < 0:
        raise RelayError("secure runtime worker status omitted active control-query usage")
    if active_control >= configured_control:
        raise RelayError("secure runtime has no free reserved control-query slot")
    observed.update(
        {
            "free_control_query_slots": configured_control - active_control,
            "control_query_concurrency_consistent": True,
            "scan_truncated": False,
        }
    )
    return observed


def _wait_for_live_structured_runtime_metadata(
    definition: ClusterDefinition,
    job_id: str,
    *,
    line_prefix: str,
    lines: list[str],
    timeout_seconds: float,
    poll_seconds: float,
    runner: CommandRunner,
) -> RuntimeMetadataAcceptance:
    """Wait for trusted runtime metadata without waiting for its source job to finish."""
    deadline = time.monotonic() + timeout_seconds
    structured_sources = {
        RuntimeMetadataSource.JARVIS_MCP,
        RuntimeMetadataSource.JARVIS_SIDECAR,
    }
    while True:
        raw_status = _remote_clio_json(
            definition,
            ["job", "status", job_id],
            runner=runner,
        )
        if not isinstance(raw_status, dict):
            raise RelayError("secure runtime source job status was not a JSON object")
        status = cast(dict[str, Any], raw_status)
        raw_job = status.get("job")
        try:
            job = RelayJob.model_validate(raw_job)
        except ValueError as exc:
            raise RelayError(f"secure runtime source RelayJob was invalid: {exc}") from exc
        if job.job_id != job_id:
            raise RelayError(
                "secure runtime source job status changed identity: "
                f"expected={job_id} observed={job.job_id}"
            )
        reported_terminal = status.get("terminal")
        actual_terminal = job.state in TERMINAL_STATES
        if not isinstance(reported_terminal, bool) or reported_terminal is not actual_terminal:
            raise RelayError("secure runtime source job status had inconsistent terminal state")
        if actual_terminal:
            lines.append(f"{line_prefix}.job_state={job.state.value}")
            if job.state.value in {"failed", "canceled"}:
                raise RelayError(
                    "secure runtime source job "
                    f"{job.state.value} before structured runtime metadata was usable"
                )
            raise RelayError(
                "secure runtime source job succeeded before a live structured runtime was available"
            )

        raw_runtime = job.metadata.get("runtime_metadata")
        if raw_runtime is not None:
            if not isinstance(raw_runtime, dict):
                raise RelayError("secure runtime source metadata was not a JSON object")
            try:
                validated = JarvisRuntimeMetadata.model_validate(raw_runtime)
            except ValueError as exc:
                raise RelayError(f"secure runtime source metadata was invalid: {exc}") from exc
            if validated.schema_version != RUNTIME_METADATA_SCHEMA:
                raise RelayError(
                    "secure runtime source metadata used an unsupported schema version: "
                    f"{validated.schema_version}"
                )
            if validated.source in structured_sources:
                if not validated.pipeline_id or not validated.execution_id:
                    raise RelayError(
                        "secure runtime source metadata omitted pipeline_id or execution_id"
                    )
                if job.state is not JobState.RUNNING:
                    if time.monotonic() >= deadline:
                        lines.append(f"{line_prefix}.job_state={job.state.value}")
                        raise _AcceptanceObservationPending(
                            "timed out waiting for the secure runtime source job to run; "
                            "the bounded observation expired while the retained job remained "
                            f"{job.state.value}: {job_id}",
                            phase="secure_runtime_metadata",
                            identifiers={"primary_job_id": job_id},
                        )
                    time.sleep(poll_seconds)
                    continue
                document = validated.model_dump(mode="json")
                lines.append(f"{line_prefix}.job_state={job.state.value}")
                lines.extend(
                    _runtime_metadata_document_facts(
                        document,
                        line_prefix=line_prefix,
                    )
                )
                lines.append(f"{line_prefix}.source_job_retained=ok")
                return RuntimeMetadataAcceptance(document=document, structured=True)

        if time.monotonic() >= deadline:
            lines.append(f"{line_prefix}.job_state={job.state.value}")
            raise _AcceptanceObservationPending(
                "timed out waiting for structured runtime metadata from secure runtime source "
                f"job; the bounded observation expired without changing the workload: {job_id}",
                phase="secure_runtime_metadata",
                identifiers={"primary_job_id": job_id},
            )
        time.sleep(poll_seconds)


def _wait_for_success(
    definition: ClusterDefinition,
    job_id: str,
    *,
    timeout_seconds: float,
    poll_seconds: float,
    runner: CommandRunner,
    pending_phase: Literal[
        "primary_job_wait",
        "agent_job_wait",
        "agent_child_job_wait",
    ] = "primary_job_wait",
) -> dict[str, Any]:
    job = _remote_clio_json(
        definition,
        [
            "job",
            "wait",
            job_id,
            "--timeout-seconds",
            str(timeout_seconds),
            "--poll-seconds",
            str(poll_seconds),
        ],
        runner=runner,
    )
    if not isinstance(job, dict):
        raise RelayError("acceptance job wait did not return a JSON object")
    typed = cast(dict[str, Any], job)
    observed_job_id = typed.get("job_id")
    state = typed.get("state")
    if observed_job_id != job_id or not isinstance(state, str):
        raise RelayError("acceptance job wait changed or omitted its durable identity")
    if state == "succeeded":
        return typed
    if state in {"failed", "canceled"}:
        raise RelayError(f"acceptance job did not succeed: {state}")
    raise _AcceptanceObservationPending(
        f"bounded observation expired while acceptance job remained {state}: {job_id}",
        phase=pending_phase,
        identifiers={
            (
                "primary_job_id"
                if pending_phase == "primary_job_wait"
                else "agent_job_id"
                if pending_phase == "agent_job_wait"
                else "agent_child_job_id"
            ): job_id
        },
    )


def _verify_live_package_progress(
    definition: ClusterDefinition,
    job_id: str,
    expected_adapter: str,
    *,
    package_name: str | None,
    timeout_seconds: float,
    poll_seconds: float,
    runner: CommandRunner,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    saw_running = False
    while time.monotonic() < deadline:
        monitor = _remote_clio_json(
            definition,
            ["job", "monitor", job_id, "--cursor", "1", "--limit", "500"],
            runner=runner,
        )
        events = cast(list[dict[str, Any]], monitor["events"])
        event_types = {str(event.get("event_type")) for event in events}
        saw_running = saw_running or "job.running" in event_types
        progress = _remote_job_collection(
            definition,
            ["job", "progress", job_id],
            record_key="progress",
            label=f"live package progress for {job_id}",
            runner=runner,
        )
        if _has_progress_adapter(
            progress,
            expected_adapter,
            job_id=job_id,
            package_name=package_name,
        ):
            if not saw_running and not _remote_job_has_event(
                definition,
                job_id,
                "job.running",
                runner=runner,
            ):
                raise RelayError("package progress was recorded before job.running")
            return
        status = _remote_clio_json(
            definition,
            ["job", "status", job_id],
            runner=runner,
        )
        runtime_metadata = _runtime_metadata_from_job_status(status, job_id=job_id)
        native_attestation = _native_progress_attestation(
            runtime_metadata,
            expected_adapter,
            package_name=package_name,
            require_nonterminal=True,
        )
        if native_attestation is not None:
            if not saw_running and not _remote_job_has_event(
                definition,
                job_id,
                "job.running",
                runner=runner,
            ):
                raise RelayError("package progress was recorded before job.running")
            return
        if event_types & {"job.succeeded", "job.failed", "job.canceled"}:
            break
        time.sleep(poll_seconds)
    raise RelayError(
        f"expected live package progress before terminal job state: {expected_adapter}"
    )
