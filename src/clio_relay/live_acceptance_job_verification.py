"""Completed-job and agent-child-job verification for live acceptance.

Extracted from ``live_acceptance.py`` (#231 rework): the concern of
proving one relay job actually completed the way acceptance requires --
its exact event sequence, task/scheduler/artifact records, readable
stdout/stderr, a runtime-metadata artifact, and (when configured) a
provider- or JARVIS-native-attested package progress record -- plus
finding the one non-stale relay job an agent's own child run created.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, cast

from clio_relay.bounded_payload import is_delivery_refusal
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.live_acceptance_models import CommandRunner, RuntimeMetadataAcceptance
from clio_relay.live_acceptance_progress import (
    _native_progress_attestation,
    _progress_attestation_identity,
    _progress_provider_attestation,
    _verify_runtime_metadata_artifact,
)
from clio_relay.live_acceptance_remote_io import (
    _decode_artifact_text,
    _delivery_refusal_error,
    _remote_clio_json,
    _remote_job_collection,
)
from clio_relay.validation_report import EvidenceReference, ValidationRecorder, ValidationResource


def _remote_job_has_event(
    definition: ClusterDefinition,
    job_id: str,
    event_type: str,
    *,
    runner: CommandRunner,
) -> bool:
    monitor = _remote_clio_json(
        definition,
        ["job", "monitor", job_id, "--cursor", "1", "--limit", "1000"],
        runner=runner,
    )
    events = cast(list[dict[str, Any]], monitor["events"])
    return any(str(event.get("event_type")) == event_type for event in events)


def _verify_completed_job(
    definition: ClusterDefinition,
    job_id: str,
    *,
    line_prefix: str,
    lines: list[str],
    runner: CommandRunner,
    expected_progress_adapter: str | None = None,
    expected_progress_package: str | None = None,
    recorder: ValidationRecorder | None = None,
    require_structured_runtime_metadata: bool = False,
) -> RuntimeMetadataAcceptance | None:
    monitor = _remote_clio_json(
        definition,
        ["job", "monitor", job_id, "--cursor", "1", "--limit", "250"],
        runner=runner,
    )
    event_types = {event["event_type"] for event in cast(list[dict[str, Any]], monitor["events"])}
    required_events = {"job.queued", "job.running", "jarvis.started", "job.succeeded"}
    missing_events = required_events - event_types
    if missing_events:
        raise RelayError(f"acceptance job missing events: {sorted(missing_events)}")
    lines.append(f"{line_prefix}.events=ok")
    for scheduler_phase in ("pending", "allocated", "running", "completed"):
        if f"scheduler.{scheduler_phase}" in event_types:
            lines.append(f"scheduler.{scheduler_phase}=observed")

    task_items = _remote_job_collection(
        definition,
        ["job", "tasks", job_id],
        record_key="tasks",
        label=f"completed-job tasks for {job_id}",
        runner=runner,
    )
    if not task_items or not any(task["state"] == "succeeded" for task in task_items):
        raise RelayError("acceptance job missing succeeded task record")
    lines.append(f"{line_prefix}.tasks={len(task_items)}")
    if recorder is not None:
        for task in task_items:
            task_id = task.get("task_id")
            if isinstance(task_id, str):
                recorder.add_resource(
                    ValidationResource(
                        kind="relay_task",
                        resource_id=task_id,
                        role=line_prefix,
                        cluster=definition.name,
                        state=str(task.get("state")) if task.get("state") is not None else None,
                        metadata=(
                            cast(dict[str, Any], task["metadata"])
                            if isinstance(task.get("metadata"), dict)
                            else {}
                        ),
                    )
                )
        scheduler_items = monitor.get("scheduler", [])
        if isinstance(scheduler_items, list):
            for item in cast(list[object], scheduler_items):
                if not isinstance(item, dict):
                    continue
                scheduler = cast(dict[str, Any], item)
                scheduler_job_id = scheduler.get("scheduler_job_id")
                if not isinstance(scheduler_job_id, str):
                    continue
                recorder.add_resource(
                    ValidationResource(
                        kind="scheduler_job",
                        resource_id=scheduler_job_id,
                        role=line_prefix,
                        cluster=definition.name,
                        state=(
                            str(scheduler["phase"]) if scheduler.get("phase") is not None else None
                        ),
                        provider=(
                            str(scheduler["scheduler"])
                            if scheduler.get("scheduler") is not None
                            else None
                        ),
                        metadata=scheduler,
                    )
                )

    stdout = _remote_clio_json(
        definition,
        ["job", "read-log", job_id, "--stream", "stdout", "--offset", "0", "--limit", "200000"],
        runner=runner,
    )
    stderr = _remote_clio_json(
        definition,
        ["job", "read-log", job_id, "--stream", "stderr", "--offset", "0", "--limit", "200000"],
        runner=runner,
    )
    if int(stdout["next_offset"]) <= 0:
        raise RelayError("acceptance stdout log is empty")
    lines.append(f"{line_prefix}.stdout_bytes={stdout['next_offset']}")
    lines.append(f"{line_prefix}.stderr_bytes={stderr['next_offset']}")

    artifact_items = _remote_job_collection(
        definition,
        ["job", "list-artifacts", job_id],
        record_key="artifacts",
        label=f"completed-job artifacts for {job_id}",
        runner=runner,
    )
    artifact_kinds = {str(artifact["kind"]) for artifact in artifact_items}
    if not {"jarvis_pipeline", "stdout", "stderr", "provenance"}.issubset(artifact_kinds):
        raise RelayError(f"acceptance artifacts incomplete: {sorted(artifact_kinds)}")
    lines.append(f"{line_prefix}.artifacts={','.join(sorted(artifact_kinds))}")
    if recorder is not None:
        for artifact in artifact_items:
            artifact_id = artifact.get("artifact_id")
            if not isinstance(artifact_id, str):
                continue
            uri = artifact.get("uri")
            references = [str(uri)] if isinstance(uri, str) else []
            recorder.add_resource(
                ValidationResource(
                    kind="artifact",
                    resource_id=artifact_id,
                    role=str(artifact.get("kind", "unknown")),
                    cluster=definition.name,
                    references=references,
                    metadata=artifact,
                )
            )
            recorder.report.artifacts.append(
                EvidenceReference(
                    kind=str(artifact.get("kind", "artifact")),
                    reference=(
                        str(uri)
                        if isinstance(uri, str)
                        else f"relay-artifact://{definition.name}/{job_id}/{artifact_id}"
                    ),
                    sha256=(
                        str(artifact["sha256"]) if isinstance(artifact.get("sha256"), str) else None
                    ),
                )
            )

    stdout_artifact = next(artifact for artifact in artifact_items if artifact["kind"] == "stdout")
    artifact_payload = _remote_clio_json(
        definition,
        ["job", "read-artifact", str(stdout_artifact["artifact_id"])],
        runner=runner,
    )
    if is_delivery_refusal(artifact_payload):
        raise _delivery_refusal_error(artifact_payload, label="acceptance artifact payload")
    if artifact_payload.get("encoding") != "base64":
        raise RelayError("acceptance artifact payload was not base64 encoded")
    lines.append(f"{line_prefix}.artifact_read=ok")

    provenance_artifact = next(
        artifact for artifact in artifact_items if artifact["kind"] == "provenance"
    )
    provenance_payload = _remote_clio_json(
        definition,
        ["job", "read-artifact", str(provenance_artifact["artifact_id"])],
        runner=runner,
    )
    if is_delivery_refusal(provenance_payload):
        raise _delivery_refusal_error(provenance_payload, label="acceptance provenance payload")
    if provenance_payload.get("encoding") != "base64":
        raise RelayError("acceptance provenance payload was not base64 encoded")
    lines.append(f"{line_prefix}.provenance=ok")
    runtime_metadata = _verify_runtime_metadata_artifact(
        definition,
        artifact_items,
        line_prefix=line_prefix,
        lines=lines,
        runner=runner,
    )
    if require_structured_runtime_metadata and (
        runtime_metadata is None or not runtime_metadata.structured
    ):
        raise RelayError(
            "acceptance requires structured JARVIS runtime metadata, not a missing or "
            "legacy stdout-derived runtime artifact"
        )
    if expected_progress_adapter is not None:
        progress = _remote_job_collection(
            definition,
            ["job", "progress", job_id],
            record_key="progress",
            label=f"completed-job progress for {job_id}",
            runner=runner,
        )
        provider_metadata = _progress_provider_attestation(
            progress,
            expected_progress_adapter,
            job_id=job_id,
            package_name=expected_progress_package,
        )
        native_progress = (
            None
            if runtime_metadata is None
            else _native_progress_attestation(
                runtime_metadata.document,
                expected_progress_adapter,
                package_name=expected_progress_package,
                require_nonterminal=False,
            )
        )
        if provider_metadata is None:
            provider_metadata = native_progress
        if provider_metadata is None:
            raise RelayError(
                f"expected package progress adapter was not recorded: {expected_progress_adapter}"
            )
        lines.append(f"{line_prefix}.progress_adapter={expected_progress_adapter}")
        lines.append("package-progress.provider=verified")
        lines.append("package-progress.acceptance=verified")
        progress_identity = _progress_attestation_identity(provider_metadata)
        lines.append(f"package-progress.identity={progress_identity}")
        if recorder is not None:
            recorder.add_resource(
                ValidationResource(
                    kind="package_progress_provider",
                    resource_id=progress_identity,
                    role="jarvis_package_progress",
                    cluster=definition.name,
                    state="verified",
                    provider=str(
                        provider_metadata.get(
                            "provider_distribution",
                            "jarvis-native-execution",
                        )
                    ),
                    metadata=dict(provider_metadata),
                )
            )
    return runtime_metadata


def _find_agent_child_job(
    definition: ClusterDefinition,
    agent_job_id: str,
    *,
    agent_created_at: str,
    runner: CommandRunner,
) -> str:
    artifact_items = _remote_job_collection(
        definition,
        ["job", "list-artifacts", agent_job_id],
        record_key="artifacts",
        label=f"agent artifacts for {agent_job_id}",
        runner=runner,
    )
    artifact_kinds = {str(artifact["kind"]) for artifact in artifact_items}
    if "agent_result" not in artifact_kinds:
        raise RelayError("acceptance agent job missing agent_result artifact")
    candidate_texts: list[str] = []
    for artifact in artifact_items:
        if str(artifact["kind"]) not in {"agent_last_message", "stdout", "agent_result"}:
            continue
        payload = _remote_clio_json(
            definition,
            ["job", "read-artifact", str(artifact["artifact_id"])],
            runner=runner,
        )
        candidate_texts.append(_decode_artifact_text(payload))
    stdout = _remote_clio_json(
        definition,
        [
            "job",
            "read-log",
            agent_job_id,
            "--stream",
            "stdout",
            "--offset",
            "0",
            "--limit",
            "200000",
        ],
        runner=runner,
    )
    candidate_texts.append(str(stdout.get("text", "")))
    child_job_ids = sorted(
        {
            match
            for text in candidate_texts
            for match in re.findall(r"\bjob_[0-9a-f]{32}\b", text)
            if match != agent_job_id
        }
    )
    if not child_job_ids:
        raise RelayError("acceptance agent did not report a child relay job id")
    agent_created = _parse_datetime(agent_created_at)
    stale_child_ids: list[str] = []
    for child_job_id in reversed(child_job_ids):
        child_created = _child_job_created_at(
            definition,
            child_job_id,
            runner=runner,
        )
        if child_created >= agent_created:
            return child_job_id
        stale_child_ids.append(child_job_id)
    raise RelayError(
        "acceptance agent only reported stale child relay jobs created before "
        f"the agent run: {stale_child_ids}"
    )


def _child_job_created_at(
    definition: ClusterDefinition,
    child_job_id: str,
    *,
    runner: CommandRunner,
) -> datetime:
    monitor = _remote_clio_json(
        definition,
        ["job", "monitor", child_job_id, "--cursor", "1", "--limit", "1"],
        runner=runner,
    )
    return _parse_datetime(str(monitor["job"]["created_at"]))


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
