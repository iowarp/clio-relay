"""Register terminal JARVIS execution outputs as bounded relay references.

Also the single owner of clio-relay#265's "declared outputs" verdict. Owner
ruling (#265, superseding an earlier ``completed_outputs_missing`` side-channel
proposal): producing the declared outputs is PART of what "completed" means --
a jarvis_run whose execution finished but whose declared outputs are missing
or empty reaches terminal state FAILED with typed reason ``outputs_missing``,
never a decorated "completed". "Declared outputs" concretely means the
``execution-file``-kind entries in the terminal ``jarvis_get_execution``
poll's own ``structured_result.artifact_page.artifacts`` -- the SAME
declarations :func:`ingest_jarvis_execution_outputs` already parses to index
artifacts (#252). A ``jarvis_run`` dispatch that completes synchronously
(never deferred through clio-relay#266's watch) never carries
``artifact_page`` at all -- it is absent from ``jarvis_run``'s own
``outputSchema`` entirely (only ``jarvis_get_execution`` declares it) -- so
that path's absent declaration keeps its current semantics unchanged, per
the owner's explicit instruction never to invent a heuristic about which
files "should" exist.

Revision (#265 D1 slice, live-evidence-driven): zero declared
``execution-file`` entries on a run that DOES carry an ``artifact_page`` is
no longer silently clean. Owner ruling was previously "untouched (the
pre-existing #252 fast return)" -- that undercounted a real defect family: a
COMPLETED run whose artifact page declares NO ``execution-file`` outputs at
all (as opposed to declaring some and then finding them missing/empty on
disk) read as plain "completed", exactly the "0-step run" / "empty-output
run" shape #265's own issue text names as a false-green case. The typed
``outputs_missing`` payload now carries a top-level ``reason`` distinguishing
the two: ``no_outputs_declared`` (zero matching entries in ``artifacts``) vs
``declared_outputs_missing`` (one or more declared entries absent/empty on
disk -- the pre-existing behavior, unchanged in shape). A run whose terminal
record carries no ``artifact_page`` at all keeps its original semantics
(the early return above, unaffected by this revision).

Refinement (adversarial-review Ruling B on the D1 slice above, flagged for
owner review -- see the "honest verdict" campaign plan, clio-relay#265):
the D1 revision's *detection* stands exactly as written here (this module
still computes and returns ``no_outputs_declared`` unchanged), but its
*consumption* was corrected -- ``no_outputs_declared`` does NOT flow into
``execution_watch.resolve_execution_outcome`` and does NOT flip a job to
FAILED (unlike ``declared_outputs_missing``, which still does, unchanged).
The campaign plan mandates the typed SIGNAL, not an automatic failure: this
module's own line 17's instruction ("never invent a heuristic about which
files should exist") applies here too -- a page declaring real artifacts of
another kind (a ``pipeline-snapshot``, the relay-flushed ``console.log``,
...) or a pure-stdout application would otherwise be false-failed purely
for not declaring an ``execution-file`` entry. The signal still reaches the
durable task record on both outcomes (see ``endpoint_job_execution.py``'s
success and failure branches) so a run card can render it either way.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path, PurePosixPath
from typing import Any, cast

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ExecutionOwnerNotFoundError, RelayError
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.job_identity import job_owned_by_session
from clio_relay.models import (
    ArtifactRef,
    JobKind,
    McpCallSpec,
    RelayJob,
    validate_jarvis_execution_id,
)
from clio_relay.spool import (
    OwnedFileSizeLimitError,
    read_owned_regular_file_bytes,
    snapshot_owned_regular_file,
)

EXECUTION_OUTPUT_OWNERSHIP_SCHEMA = "clio-relay.execution-output.v1"
EXECUTION_OUTPUT_TRUNCATION_SCHEMA = "jarvis.execution-output-truncation.v1"
#: clio-relay#265: the typed terminal-failure reason/payload schema for one
#: or more declared execution outputs that turned out missing (absent on
#: disk) or empty (declared ``size_bytes == 0``) at terminal.
EXECUTION_OUTPUTS_MISSING_SCHEMA = "clio-relay.execution-outputs-missing.v1"
MAX_RELAY_EXECUTION_OUTPUTS = 128
MAX_EXECUTION_RESULT_BYTES = 16 * 1_048_576
_OUTPUT_ROLES = frozenset({"log", "output", "frame"})


def ingest_jarvis_execution_outputs(
    queue: ClioCoreQueue,
    query_job: RelayJob,
    result_document: dict[str, Any],
) -> tuple[list[ArtifactRef], dict[str, object] | None, dict[str, object] | None]:
    """Index declared terminal outputs without copying their bytes.

    The query job supplies the authenticated execution result. The artifact is
    owned by the original ``jarvis_run`` job, while its backing path remains in
    the execution directory and is read later through relay's bounded route.

    Returns ``(indexed, truncation, outputs_missing)``. ``outputs_missing`` is
    clio-relay#265's typed terminal-failure payload
    (:data:`EXECUTION_OUTPUTS_MISSING_SCHEMA`) whenever the terminal
    ``artifact_page`` was present but did NOT prove clean declared outputs --
    either at least one declared ``execution-file`` output is absent on disk
    or declared empty (``size_bytes == 0``, ``reason="declared_outputs_missing"``),
    or the page declared ZERO matching ``execution-file`` entries at all
    (``reason="no_outputs_declared"`` -- the #265 D1 revision: a completed
    run that produced no declared outputs is exactly as false-green as one
    whose declared outputs are missing). ``outputs_missing`` is ``None`` only
    when the terminal record carries no ``artifact_page`` at all (the
    pre-existing early returns below -- a synchronous dispatch's own
    semantics, unchanged) or every declared output is present and non-empty.
    A missing declared file is recorded typed here rather than left to crash
    out of ``snapshot_owned_regular_file`` uncaught: this is a real, expected
    outcome (#265's negative path), not a filesystem-identity violation.
    """
    raw_structured = result_document.get("structured_result")
    if not isinstance(raw_structured, dict):
        return [], None, None
    structured = cast(dict[str, Any], raw_structured)
    record = structured.get("execution_record")
    page = structured.get("artifact_page")
    execution_id = structured.get("execution_id")
    if not isinstance(record, dict) or not isinstance(page, dict):
        return [], None, None
    record = cast(dict[str, Any], record)
    page = cast(dict[str, Any], page)
    if record.get("terminal") is not True or page.get("terminal") is not True:
        return [], None, None
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
    missing_outputs: list[dict[str, object]] = []
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
        # #265: a declared output whose backing file is genuinely absent is
        # an expected negative-path outcome, not a filesystem-identity
        # violation -- gate on reality (a plain existence check) rather than
        # let it crash out of the owned-file open below uncaught.
        if not internal_filesystem_path(path).exists():
            missing_outputs.append(
                {
                    "relative_path": relative,
                    "role": role,
                    "reason": "absent",
                    "declared_size_bytes": size,
                }
            )
            queue.append_event(
                owner.job_id,
                "jarvis.execution_output_missing",
                f"Declared JARVIS execution output is missing on disk: {relative}",
                payload={
                    "execution_id": execution_id,
                    "relative_path": relative,
                    "role": role,
                },
            )
            continue
        if size == 0:
            missing_outputs.append(
                {
                    "relative_path": relative,
                    "role": role,
                    "reason": "empty",
                    "declared_size_bytes": 0,
                }
            )
            queue.append_event(
                owner.job_id,
                "jarvis.execution_output_empty",
                f"Declared JARVIS execution output is empty: {relative}",
                payload={
                    "execution_id": execution_id,
                    "relative_path": relative,
                    "role": role,
                },
            )
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
    outputs_missing: dict[str, object] | None = None
    if missing_outputs:
        outputs_missing = {
            "schema_version": EXECUTION_OUTPUTS_MISSING_SCHEMA,
            "reason": "declared_outputs_missing",
            "execution_id": execution_id,
            "declared_count": output_count,
            "missing": missing_outputs,
        }
        queue.append_event(
            owner.job_id,
            "jarvis.execution_outputs_missing",
            "JARVIS execution completed but declared outputs are missing or empty",
            payload=outputs_missing,
        )
    elif output_count == 0:
        # #265 D1: the terminal artifact page WAS present (the early returns
        # above already ruled out "no artifact_page at all") but declared
        # ZERO execution-file outputs -- a 0-step/empty-output run reading as
        # plain "completed" is exactly the false-green shape #265 names.
        outputs_missing = {
            "schema_version": EXECUTION_OUTPUTS_MISSING_SCHEMA,
            "reason": "no_outputs_declared",
            "execution_id": execution_id,
            "declared_count": 0,
            "missing": [],
        }
        queue.append_event(
            owner.job_id,
            "jarvis.execution_outputs_missing",
            "JARVIS execution completed with zero declared outputs",
            payload=outputs_missing,
        )
    return indexed, truncation, outputs_missing


def ingest_jarvis_execution_outputs_from_path(
    queue: ClioCoreQueue,
    query_job: RelayJob,
    result_path: Path,
    owned_root: Path,
) -> tuple[list[ArtifactRef], dict[str, object] | None, dict[str, object] | None]:
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
    """Resolve the unique relay job that admitted the execution.

    Scoped to jobs sharing ``query_job``'s cluster: this is the ingest path
    (:func:`ingest_jarvis_execution_outputs`), which always has an in-flight
    query job at hand and treats a resolution failure here as an internal
    invariant violation (an uncaught, generic :class:`RelayError` -- this
    job's own execution result failed to self-identify its admitting job,
    never legitimate caller input). clio-relay#278's bare-``execution_id``
    listing surfaces have no such incumbent job and are CALLER-facing (an
    unresolved id is ordinary, expected input) -- they use
    :func:`resolve_jarvis_run_owner_by_execution_id` instead, which shares
    the exact same match predicate and exactly-one-owner invariant via
    :func:`_jarvis_run_matches` but raises the typed, catchable
    :class:`~clio_relay.errors.ExecutionOwnerNotFoundError`.
    """
    if _is_jarvis_run(query_job, execution_id):
        return query_job
    matches = _jarvis_run_matches(queue, execution_id, cluster=query_job.cluster)
    if len(matches) != 1:
        raise RelayError(
            f"expected one owning jarvis_run for execution {execution_id}, found {len(matches)}"
        )
    return matches[0]


def resolve_jarvis_run_owner_by_execution_id(
    queue: ClioCoreQueue,
    execution_id: str,
    *,
    cluster: str | None = None,
    owns_job: Callable[[RelayJob], bool] | None = None,
) -> RelayJob:
    """Resolve the unique relay job that admitted an execution, id alone.

    clio-relay#278: the artifact-listing surfaces that accept a bare
    ``execution_id`` as an alternative to ``job_id`` -- the door's
    ``GET /executions/{execution_id}/artifacts`` route, ``relay_list_
    artifacts``'s ``execution_id`` branch, and the CLI's ``job list-
    artifacts --execution-id`` flag -- have no incumbent query job to scope
    from, unlike :func:`resolve_jarvis_run_owner`'s ingest-path caller.
    ``cluster``, when given (the caller's asserted cluster, e.g. an MCP
    ``target``'s name), scopes the scan exactly as
    :func:`resolve_jarvis_run_owner` scopes by its query job's own cluster;
    ``None`` (the default) scans every locally known job regardless of
    cluster, matching how the job_id-keyed listing routes apply no cluster
    check of their own either (cluster identity only matters for the
    caller-asserted routing decision, made separately by the caller before
    this is reached).

    ``owns_job``, when given, filters scan candidates BEFORE the exactly-
    one-owner check (adversarial-review D1 fix): ``_is_jarvis_run`` matches
    ANY admitted ``jarvis_run`` spec sharing the id, trusted or legacy,
    with no notion of which owner session submitted it. Without this
    filter, a second owner session's job that happens to match the same
    bare execution_id turns a legitimate single match into an ambiguous
    one -- silently 404ing the FIRST session's own artifacts the moment
    the raw match count stops being exactly one, even though that session
    could see its own job all along. Every caller-facing surface (the door
    route, the MCP tool, the CLI verb) passes its own ``owns_job`` (e.g.
    ``RelayApiContext.owns_job`` at the door, or
    ``jarvis_execution_artifacts.owns_local_job`` built from
    ``RelaySettings`` at the MCP/CLI surfaces); the ingest path
    (:func:`resolve_jarvis_run_owner`) has no caller-identity boundary to
    enforce and does not filter by ownership at all.

    Raises :class:`~clio_relay.errors.ExecutionOwnerNotFoundError` -- never
    an empty page pretending success, and structurally indistinguishable
    from "no job anywhere admitted this id" -- both when zero locally known
    OWNED job admitted the execution and when more than one did (an
    execution id another session's job happens to share is invisible to
    this scan, never a distinguishing 403/404 oracle) -- and for a
    malformed ``execution_id`` (clio-relay#278 D4: validated up front via
    :func:`~clio_relay.models_job_specs.validate_jarvis_execution_id`
    against JARVIS-CD's own portable-id contract, so garbage input is a
    typed local refusal rather than an O(all jobs) scan or, at the remote-
    routing surfaces, an opaque transport failure).
    """
    try:
        validate_jarvis_execution_id(execution_id)
    except ValueError as exc:
        raise ExecutionOwnerNotFoundError(f"execution_not_found: {exc}") from exc
    matches = _jarvis_run_matches(queue, execution_id, cluster=cluster, owns_job=owns_job)
    if len(matches) != 1:
        raise ExecutionOwnerNotFoundError(
            f"execution_not_found: no unique jarvis_run job admitted execution "
            f"{execution_id} (found {len(matches)})"
        )
    return matches[0]


def owns_local_job(settings: RelaySettings, job: RelayJob) -> bool:
    """Return whether ``settings``'s owner session may see one local job.

    The MCP/CLI-surface counterpart of ``RelayApiContext.owns_job`` (which
    already wraps the SAME shared
    :func:`~clio_relay.job_identity.job_owned_by_session` predicate) --
    clio-relay#278 D1: the local (non-door) callers of
    :func:`resolve_jarvis_run_owner_by_execution_id` have a
    :class:`~clio_relay.config.RelaySettings` but no
    ``RelayApiContext``, so this is the thin adapter that lets them build
    the same ``owns_job`` predicate the door passes.
    """
    return job_owned_by_session(
        job,
        owner_session_id=settings.owner_session_id,
        owner_session_generation_id=settings.owner_session_generation_id,
    )


def _jarvis_run_matches(
    queue: ClioCoreQueue,
    execution_id: str,
    *,
    cluster: str | None,
    owns_job: Callable[[RelayJob], bool] | None = None,
) -> list[RelayJob]:
    """Return every locally known job whose own jarvis_run admitted ``execution_id``.

    The one scan+predicate :func:`resolve_jarvis_run_owner` and
    :func:`resolve_jarvis_run_owner_by_execution_id` both build on
    (clio-relay#278) -- never duplicated between the two entry points.
    ``owns_job``, when given, is applied here -- BEFORE either caller's
    exactly-one-match check -- never after (adversarial-review D1).
    """
    return [
        job
        for job in queue.list_jobs()
        if (cluster is None or job.cluster == cluster)
        and (owns_job is None or owns_job(job))
        and _is_jarvis_run(job, execution_id)
    ]


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
