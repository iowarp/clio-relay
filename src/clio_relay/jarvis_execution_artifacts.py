"""Register terminal JARVIS execution outputs as bounded relay references."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath
from typing import Any, cast

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import RelayError
from clio_relay.models import ArtifactRef, JobKind, McpCallSpec, RelayJob
from clio_relay.spool import (
    OwnedFileSizeLimitError,
    read_owned_regular_file_bytes,
    snapshot_owned_regular_file,
)

EXECUTION_OUTPUT_OWNERSHIP_SCHEMA = "clio-relay.execution-output.v1"
EXECUTION_OUTPUT_TRUNCATION_SCHEMA = "jarvis.execution-output-truncation.v1"
MAX_RELAY_EXECUTION_OUTPUTS = 128
MAX_EXECUTION_RESULT_BYTES = 16 * 1_048_576
_OUTPUT_ROLES = frozenset({"log", "output", "frame"})


def ingest_jarvis_execution_outputs(
    queue: ClioCoreQueue,
    query_job: RelayJob,
    result_document: dict[str, Any],
) -> tuple[list[ArtifactRef], dict[str, object] | None]:
    """Index declared terminal outputs without copying their bytes.

    The query job supplies the authenticated execution result. The artifact is
    owned by the original ``jarvis_run`` job, while its backing path remains in
    the execution directory and is read later through relay's bounded route.
    """
    raw_structured = result_document.get("structured_result")
    if not isinstance(raw_structured, dict):
        return [], None
    structured = cast(dict[str, Any], raw_structured)
    record = structured.get("execution_record")
    page = structured.get("artifact_page")
    execution_id = structured.get("execution_id")
    if not isinstance(record, dict) or not isinstance(page, dict):
        return [], None
    record = cast(dict[str, Any], record)
    page = cast(dict[str, Any], page)
    if record.get("terminal") is not True or page.get("terminal") is not True:
        return [], None
    if not isinstance(execution_id, str) or not execution_id:
        raise RelayError("terminal JARVIS result omitted execution_id")
    raw_events = page.get("artifacts")
    if not isinstance(raw_events, list):
        raise RelayError("terminal JARVIS result omitted artifact declarations")
    owner = resolve_jarvis_run_owner(queue, query_job, execution_id)
    execution_root = execution_root_from_record(record)
    if execution_root is None:
        raise RelayError("terminal JARVIS result omitted an execution root")

    indexed: list[ArtifactRef] = []
    truncation: dict[str, object] | None = None
    output_count = 0
    for raw_event in cast(list[object], raw_events):
        if not isinstance(raw_event, dict):
            raise RelayError("JARVIS artifact declaration must be an object")
        raw_event = cast(dict[str, Any], raw_event)
        raw_metadata: object = raw_event.get("metadata")
        if not isinstance(raw_metadata, dict):
            raw_metadata = None
        metadata = cast(dict[str, object], raw_metadata) if raw_metadata is not None else None
        if (
            metadata is not None
            and metadata.get("schema_version") == EXECUTION_OUTPUT_TRUNCATION_SCHEMA
        ):
            truncation = _truncation_metadata(cast(dict[str, Any], metadata))
            continue
        if raw_event.get("package_id") != "jarvis.execution":
            continue
        if raw_event.get("kind") != "execution-file":
            continue
        output_count += 1
        if output_count > MAX_RELAY_EXECUTION_OUTPUTS:
            continue
        relative = _relative_output_path(raw_event.get("location"))
        role = raw_event.get("role")
        if not isinstance(role, str) or role not in _OUTPUT_ROLES:
            raise RelayError(f"unsupported JARVIS execution-output role: {role!r}")
        size = raw_event.get("size_bytes")
        checksum = raw_event.get("checksum")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise RelayError(f"invalid JARVIS execution-output size: {relative}")
        digest = _sha256_from_checksum(checksum)
        path = _safe_execution_path(execution_root, relative)
        snapshot = snapshot_owned_regular_file(path, owned_root=execution_root)
        if snapshot.size_bytes != size or snapshot.sha256 != digest:
            raise RelayError(f"JARVIS execution-output declaration changed: {relative}")
        artifact = _artifact_reference(
            owner,
            execution_id=execution_id,
            relative_path=relative,
            role=role,
            execution_root=execution_root,
            path=path,
            size=size,
            digest=digest,
        )
        indexed.append(_append_idempotent(queue, artifact))

    if output_count > MAX_RELAY_EXECUTION_OUTPUTS:
        extra = output_count - MAX_RELAY_EXECUTION_OUTPUTS
        truncation = {
            "schema_version": EXECUTION_OUTPUT_TRUNCATION_SCHEMA,
            "limit": MAX_RELAY_EXECUTION_OUTPUTS,
            "observed_count": output_count,
            "omitted_count": extra,
        }
    if truncation is not None:
        queue.append_event(
            owner.job_id,
            "jarvis.execution_outputs_truncated",
            "JARVIS execution output declaration cap exceeded",
            payload=truncation,
        )
    if indexed:
        queue.append_event(
            owner.job_id,
            "jarvis.execution_outputs_registered",
            "JARVIS execution outputs indexed as relay artifacts",
            payload={"execution_id": execution_id, "count": len(indexed)},
        )
    return indexed, truncation


def ingest_jarvis_execution_outputs_from_path(
    queue: ClioCoreQueue,
    query_job: RelayJob,
    result_path: Path,
    owned_root: Path,
) -> tuple[list[ArtifactRef], dict[str, object] | None]:
    """Read one bounded terminal result and ingest its declared outputs."""
    try:
        snapshot = read_owned_regular_file_bytes(
            result_path,
            owned_root=owned_root,
            max_bytes=MAX_EXECUTION_RESULT_BYTES,
        )
        if snapshot.data is None:
            raise RelayError("terminal JARVIS result bytes were unavailable")
        document = json.loads(snapshot.data.decode("utf-8"))
    except (
        OSError,
        OwnedFileSizeLimitError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        RuntimeError,
    ) as exc:
        raise RelayError(
            f"terminal JARVIS result could not be read for artifact ingest: {exc}"
        ) from exc
    if not isinstance(document, dict):
        raise RelayError("terminal JARVIS result must be a JSON object")
    return ingest_jarvis_execution_outputs(queue, query_job, cast(dict[str, Any], document))


def resolve_jarvis_run_owner(
    queue: ClioCoreQueue,
    query_job: RelayJob,
    execution_id: str,
) -> RelayJob:
    """Resolve the unique relay job that admitted the execution."""
    if _is_jarvis_run(query_job, execution_id):
        return query_job
    matches = [
        job
        for job in queue.list_jobs()
        if job.cluster == query_job.cluster and _is_jarvis_run(job, execution_id)
    ]
    if len(matches) != 1:
        raise RelayError(
            f"expected one owning jarvis_run for execution {execution_id}, found {len(matches)}"
        )
    return matches[0]


def _is_jarvis_run(job: RelayJob, execution_id: str) -> bool:
    raw_spec: object = cast(Any, job).spec
    if job.kind is not JobKind.MCP_CALL or not isinstance(raw_spec, McpCallSpec):
        return False
    spec = raw_spec
    return spec.tool == "jarvis_run" and spec.arguments.get("execution_id") == execution_id


def execution_root_from_record(record: dict[str, Any]) -> Path | None:
    """Derive one JARVIS execution's root directory from its terminal record.

    Shared with :mod:`clio_relay.console_stream` (#259): the terminal
    ``console`` log flush reuses this exact derivation rather than
    independently guessing at JARVIS-CD's on-disk layout.
    """
    raw_metadata: object = record.get("metadata")
    if not isinstance(raw_metadata, dict):
        return None
    metadata = cast(dict[str, object], raw_metadata)
    for key in ("pipeline_snapshot_path", "script_path"):
        raw_path: object = metadata.get(key)
        if isinstance(raw_path, str) and raw_path:
            return Path(raw_path).parent
    return None


def _relative_output_path(location: object) -> str:
    if not isinstance(location, dict):
        raise RelayError("JARVIS execution output must use an execution_path location")
    location_values = cast(dict[str, object], location)
    if location_values.get("kind") != "execution_path":
        raise RelayError("JARVIS execution output must use an execution_path location")
    value: object = location_values.get("value")
    if not isinstance(value, str) or not value:
        raise RelayError("JARVIS execution output location is invalid")
    relative = PurePosixPath(value)
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise RelayError(f"JARVIS execution output path escapes its execution root: {value}")
    return relative.as_posix()


def _safe_execution_path(root: Path, relative: str) -> Path:
    path = root / Path(*PurePosixPath(relative).parts)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise RelayError(
            f"JARVIS execution output path escapes its execution root: {relative}"
        ) from exc
    return path


def _sha256_from_checksum(value: object) -> str:
    if not isinstance(value, str) or not value.startswith("sha256:"):
        raise RelayError("JARVIS execution output checksum must be sha256:<digest>")
    digest = value.removeprefix("sha256:")
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise RelayError("JARVIS execution output checksum is not a SHA-256 digest")
    return digest


def _artifact_reference(
    owner: RelayJob,
    *,
    execution_id: str,
    relative_path: str,
    role: str,
    execution_root: Path,
    path: Path,
    size: int,
    digest: str,
) -> ArtifactRef:
    artifact_id = (
        "artifact_"
        + hashlib.sha256(f"{owner.job_id}:{execution_id}:{relative_path}".encode()).hexdigest()[:32]
    )
    return ArtifactRef(
        artifact_id=artifact_id,
        job_id=owner.job_id,
        uri=path.absolute().as_uri(),
        kind="execution_output",
        size_bytes=size,
        sha256=digest,
        metadata={
            "ownership_schema": EXECUTION_OUTPUT_OWNERSHIP_SCHEMA,
            "owned_root_uri": execution_root.absolute().as_uri(),
            "execution_id": execution_id,
            "relative_path": relative_path,
            "name": relative_path,
            "role": role,
            "produced_by": {"job_id": owner.job_id, "execution_id": execution_id},
        },
    )


def _append_idempotent(queue: ClioCoreQueue, artifact: ArtifactRef) -> ArtifactRef:
    for existing in queue.list_artifacts(artifact.job_id):
        if (
            existing.kind == artifact.kind
            and existing.metadata.get("execution_id") == artifact.metadata.get("execution_id")
            and existing.metadata.get("relative_path") == artifact.metadata.get("relative_path")
        ):
            if existing.sha256 != artifact.sha256 or existing.size_bytes != artifact.size_bytes:
                raise RelayError("JARVIS execution output changed after relay registration")
            return existing
    return queue.append_artifact(artifact)


def _truncation_metadata(metadata: dict[str, Any]) -> dict[str, object]:
    values: dict[str, object] = {"schema_version": EXECUTION_OUTPUT_TRUNCATION_SCHEMA}
    for key in ("limit", "observed_count", "omitted_count"):
        value = metadata.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise RelayError(f"invalid JARVIS execution-output truncation field: {key}")
        values[key] = value
    return values
