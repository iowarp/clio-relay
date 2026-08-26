"""clio-relay#162: refuse an empty ``jarvis_run`` pipeline before scheduler submission.

Live evidence (isolated ares deployment, relay 1.5.10): a pipeline with ZERO
packages -- a preceding ``jarvis_add_step`` was rejected 422, so nothing was
ever appended -- was still submitted through ``jarvis_run`` as a real
scheduler job (a native SLURM id, a node allocated) whose own stdout showed
the no-op plainly (``Packages: []`` ... "Stopping packages..."). The caller
got a success-shaped handle for a scheduler allocation that started and
stopped nothing.

This module owns the one pre-dispatch check that closes that hole: before
``endpoint_jarvis_dispatch.py`` ever calls ``_run_execution_streaming`` for a
``jarvis_run`` mcp_call, it queries JARVIS's own ``jarvis_describe`` with
``target="pipeline"`` (the same verified-contract dispatch shape
``endpoint_jarvis_recovery.py``'s lost-response recovery query already
proved for ``jarvis_get_execution`` -- a bounded MCP round-trip using the
packaged relay runner, trust-checked against THIS query's own pinned route)
and reads back the declared package/step count. Zero declared steps refuses
the job with typed ``jarvis_pipeline_empty`` BEFORE any scheduler submission
-- the job never enters the scheduler.

An INCONCLUSIVE precheck (the query itself failed, timed out, or returned a
document this module cannot structurally verify) is never treated as
either a refusal or a clean bill of health: it changes nothing, and
``_run_job_impl`` submits exactly as it did before #162. Inventing a
refusal from an unreadable answer would be the same kind of unfounded
heuristic the owner ruling forbids elsewhere in this codebase; inventing a
false "not empty" from an unreadable answer would silently defeat this
whole check. Either way the precheck's own outcome is recorded as a typed
event so a caller never has to guess why the check did or did not fire.
"""

from __future__ import annotations

import json
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from clio_relay.endpoint_jarvis_recovery import (
    _endpoint_mcp_runner_command,
    _minimal_mcp_runner_environment,
    _trusted_jarvis_mcp_result,
)
from clio_relay.endpoint_recovery_directory import _write_private_json_file
from clio_relay.errors import RelayError
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.models import McpCallSpec

if TYPE_CHECKING:
    from pathlib import Path

    from clio_relay.jarvis_provider import JarvisCdProvider
    from clio_relay.models import RelayJob

JARVIS_PIPELINE_EMPTY_SCHEMA = "clio-relay.jarvis-pipeline-empty.v1"
JARVIS_PIPELINE_EMPTY_REASON = "jarvis_pipeline_empty"
PIPELINE_PRECHECK_INCONCLUSIVE_SCHEMA = "clio-relay.jarvis-pipeline-precheck-inconclusive.v1"

#: MCP timeouts for the one pre-dispatch ``jarvis_describe`` query -- matches
#: execution_watch.py's own bounded poll-query bounds (the same kind of
#: bounded, single-tool-call dispatch).
PIPELINE_PRECHECK_QUERY_TIMEOUT_SECONDS = 60
PIPELINE_PRECHECK_QUERY_PROCESS_TIMEOUT_SECONDS = 75


@dataclass(frozen=True, slots=True)
class PipelinePrecheckResult:
    """The pre-dispatch ``jarvis_describe(target="pipeline")`` outcome.

    Exactly one of ``step_count``/``inconclusive_reason`` is populated.
    """

    step_count: int | None
    inconclusive_reason: str | None


def pipeline_describe_query_spec(
    base: McpCallSpec,
    *,
    pipeline_id: str,
    timeout_seconds: int,
) -> McpCallSpec:
    """Build one bounded ``jarvis_describe(target="pipeline")`` pre-dispatch request."""
    return McpCallSpec(
        server=base.server,
        server_args=base.server_args,
        env_from=base.env_from,
        expected_server_artifact_digest=base.expected_server_artifact_digest,
        expected_registered_contract=base.expected_registered_contract,
        expected_jarvis_cd_lock_binding=base.expected_jarvis_cd_lock_binding,
        tool="jarvis_describe",
        arguments={"pipeline_id": pipeline_id, "target": "pipeline"},
        timeout_seconds=timeout_seconds,
    )


def pipeline_step_count(document: dict[str, object]) -> int | None:
    """Read the declared package/step count from a trusted describe result.

    Structural only: the registered contract's own ``target="pipeline"``
    output shape is ``{"result": {"target": "pipeline", "pipeline": {...}}}``
    where ``pipeline`` is JARVIS-CD's own loosely-typed snapshot object
    (``additionalProperties: true``) carrying a ``pkgs`` list -- the same
    field name JARVIS-CD's own pipeline YAML uses
    (``progress_adapters.py``'s ``package_progress_adapter_from_pipeline``
    reads the identical key from the rendered pipeline document). Returns
    ``None`` -- inconclusive, never a false answer -- when any part of that
    shape is missing or not the expected type.
    """
    structured = document.get("structured_result")
    if not isinstance(structured, dict):
        return None
    result = cast(dict[str, object], structured).get("result")
    if not isinstance(result, dict):
        return None
    typed_result = cast(dict[str, object], result)
    if typed_result.get("target") != "pipeline":
        return None
    pipeline = typed_result.get("pipeline")
    if not isinstance(pipeline, dict):
        return None
    packages = cast(dict[str, object], pipeline).get("pkgs")
    if not isinstance(packages, list):
        return None
    return len(cast(list[object], packages))


def empty_pipeline_refusal_payload(
    *,
    pipeline_id: str,
    execution_id: str | None,
) -> dict[str, object]:
    """Typed #162 refusal payload for a pipeline with zero declared steps."""
    return {
        "schema_version": JARVIS_PIPELINE_EMPTY_SCHEMA,
        "reason": JARVIS_PIPELINE_EMPTY_REASON,
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
    }


def empty_pipeline_refusal_text(payload: dict[str, object]) -> str:
    """Render :func:`empty_pipeline_refusal_payload` as one bounded line."""
    pipeline_id = payload.get("pipeline_id")
    return (
        f"jarvis_run refused before scheduler submission: pipeline {pipeline_id} has "
        "zero declared steps"
    )


def dispatch_pipeline_describe_query(
    job: RelayJob,
    *,
    base_spec: McpCallSpec,
    provider: JarvisCdProvider,
    query_dir: Path,
    pipeline_id: str,
) -> PipelinePrecheckResult:
    """Dispatch one bounded ``jarvis_describe(target="pipeline")`` pre-dispatch query."""
    internal_filesystem_path(query_dir).mkdir(parents=True, exist_ok=True)
    query_spec = pipeline_describe_query_spec(
        base_spec,
        pipeline_id=pipeline_id,
        timeout_seconds=PIPELINE_PRECHECK_QUERY_TIMEOUT_SECONDS,
    )
    query_job = job.model_copy(update={"spec": query_spec})
    params_path = query_dir / "params.json"
    result_path = query_dir / "mcp-result.json"
    with suppress(FileNotFoundError):
        internal_filesystem_path(result_path).unlink()
    _write_private_json_file(params_path, query_spec.model_dump(mode="json", exclude_none=True))
    try:
        completed = provider.run_command_streaming(
            _endpoint_mcp_runner_command(params_path),
            process_label="jarvis pipeline precheck query",
            cwd=internal_filesystem_path(query_dir),
            env=_minimal_mcp_runner_environment(base_spec.env_from),
            timeout_seconds=PIPELINE_PRECHECK_QUERY_PROCESS_TIMEOUT_SECONDS,
        )
    except (RelayError, OSError, ValueError):
        return PipelinePrecheckResult(step_count=None, inconclusive_reason="query_dispatch_failed")
    if completed.returncode != 0:
        return PipelinePrecheckResult(step_count=None, inconclusive_reason="query_dispatch_failed")
    try:
        payload = internal_filesystem_path(result_path).read_bytes()
        document = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return PipelinePrecheckResult(
            step_count=None, inconclusive_reason="query_result_unreadable"
        )
    if not isinstance(document, dict):
        return PipelinePrecheckResult(
            step_count=None, inconclusive_reason="query_result_unreadable"
        )
    trusted, _reason = _trusted_jarvis_mcp_result(
        query_job, document, expected_tool="jarvis_describe"
    )
    if not trusted:
        return PipelinePrecheckResult(step_count=None, inconclusive_reason="query_result_untrusted")
    step_count = pipeline_step_count(cast(dict[str, object], document))
    if step_count is None:
        return PipelinePrecheckResult(
            step_count=None, inconclusive_reason="query_result_shape_unrecognized"
        )
    return PipelinePrecheckResult(step_count=step_count, inconclusive_reason=None)
