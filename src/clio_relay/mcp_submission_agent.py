"""Generic/pipeline/job/agent MCP job-submission tools: submitting a
built-in JARVIS pipeline, a JARVIS job, a remote agent task, and a plain
local relay job -- each routed to a local queue.py-backed path or a remote
(SSH or owned-session) path via the shared submission-result helpers in
mcp_submission_result.py.

Split out of mcp_server.py (iowarp/clio-relay#231) as one of three seams
the job/MCP-call submission cluster split into (a single module would
have measured well over 800 lines; mcp_submission_result.py holds the
shared result-assembly helpers, mcp_submission_mcp_call.py the MCP-call
submission path). Every function here that branches to a remote or
owned-session path calls several names tests monkeypatch at
`mcp_server_module.<name>` (`should_execute_on_cluster`,
`submit_owned_session_job`, `_owned_session_submission_result`,
`_optional_cluster_definition`, `run_remote_clio`) through the
function-scope `_mcp_server.<name>(...)` back-reference established in
slices 3-8 -- found and rewritten by the same AST-based extraction script
slice 8 introduced (exact line/column spans, not a hand-written
per-function list or a regex over the source text).
"""

from __future__ import annotations

import hashlib
from typing import Any

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.mcp_arguments import (
    _artifact_use_cli_value,
    _artifact_use_refs,
    _boolean_argument,
    _jarvis_submission_wait_timeout_seconds,
    _optional_int,
    _optional_str,
    _required_str,
    _stable_digest,
)
from clio_relay.mcp_submission_result import (
    _remote_submission_result,
    _submission_result,
)
from clio_relay.models import (
    JarvisRunSpec,
    JobKind,
    RelayJob,
    RemoteAgentTaskSpec,
    artifact_use_payload,
)

JSON = dict[str, Any]


def _submit_jarvis_pipeline(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    from clio_relay import mcp_server as _mcp_server

    cluster = _required_str(arguments, "cluster")
    pipeline_yaml = _required_str(arguments, "pipeline_yaml")
    wait_timeout_seconds = _jarvis_submission_wait_timeout_seconds(arguments)
    used_artifact_refs = _artifact_use_refs(arguments)
    digest = hashlib.sha256(pipeline_yaml.encode("utf-8")).hexdigest()
    dependency_digest = _stable_digest(
        {"used_artifact_refs": [artifact_use_payload(item) for item in used_artifact_refs]}
    )
    dependency_suffix = f":{dependency_digest}" if used_artifact_refs else ""
    idempotency_key = str(
        arguments.get("idempotency_key") or f"mcp:jarvis:{cluster}:{digest}{dependency_suffix}"
    )
    definition = _mcp_server._optional_cluster_definition(cluster)
    if (
        definition is not None
        and _mcp_server.should_execute_on_cluster(definition)
        and settings.owner_session_id is not None
    ):
        job = _mcp_server.submit_owned_session_job(
            definition=definition,
            settings=settings,
            path="/jobs/jarvis",
            payload={
                "cluster": cluster,
                "pipeline_yaml": pipeline_yaml,
                "idempotency_key": idempotency_key,
                "used_artifact_refs": [artifact_use_payload(item) for item in used_artifact_refs],
            },
        )
        return _mcp_server._owned_session_submission_result(
            job,
            definition=definition,
            settings=settings,
            wait_for_terminal_result=bool(arguments.get("wait_for_terminal", False)),
            wait_timeout_seconds=wait_timeout_seconds,
            poll_seconds=float(arguments.get("poll_seconds", 2)),
        )
    job = _submit_local_job(
        queue,
        RelayJob(
            cluster=cluster,
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml=pipeline_yaml),
            idempotency_key=idempotency_key,
            used_artifact_refs=used_artifact_refs,
        ),
        settings=settings,
    )
    return _submission_result(
        job,
        {**arguments, "wait_timeout_seconds": wait_timeout_seconds},
        queue=queue,
        definition=definition,
    )


def _submit_jarvis_job(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    from clio_relay import mcp_server as _mcp_server

    cluster = _required_str(arguments, "cluster")
    wait_timeout_seconds = _jarvis_submission_wait_timeout_seconds(arguments)
    used_artifact_refs = _artifact_use_refs(arguments)
    dependency_digest = _stable_digest(
        {"used_artifact_refs": [artifact_use_payload(item) for item in used_artifact_refs]}
    )
    dependency_suffix = f":{dependency_digest}" if used_artifact_refs else ""
    definition = _mcp_server._optional_cluster_definition(cluster)
    if definition is not None and _mcp_server.should_execute_on_cluster(definition):
        pipeline_name = _required_str(arguments, "pipeline_name")
        idempotency_key = str(
            arguments.get("idempotency_key")
            or f"mcp:jarvis-job:{cluster}:{pipeline_name}{dependency_suffix}"
        )
        if settings.owner_session_id is not None:
            job = _mcp_server.submit_owned_session_job(
                definition=definition,
                settings=settings,
                path="/jobs/jarvis-pipeline",
                payload={
                    "cluster": cluster,
                    "pipeline_name": pipeline_name,
                    "idempotency_key": idempotency_key,
                    "used_artifact_refs": [
                        artifact_use_payload(item) for item in used_artifact_refs
                    ],
                },
            )
            return _mcp_server._owned_session_submission_result(
                job,
                definition=definition,
                settings=settings,
                wait_for_terminal_result=bool(arguments.get("wait_for_terminal", False)),
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=float(arguments.get("poll_seconds", 2)),
            )
        if bool(arguments.get("wait_for_terminal", False)):
            raise ValueError(
                "wait_for_terminal is unavailable for a direct remote JARVIS pipeline "
                "submission without an owned relay session; submit asynchronously, preserve "
                "the remote receipt, and call relay_wait with its cluster, job_id, and "
                "route_revision"
            )
        remote_args = [
            "job",
            "submit-pipeline",
            "--cluster",
            cluster,
            "--pipeline-name",
            pipeline_name,
            "--idempotency-key",
            str(idempotency_key),
        ]
        for item in used_artifact_refs:
            remote_args.extend(["--used-artifact", _artifact_use_cli_value(item)])
        output = _mcp_server.run_remote_clio(definition, remote_args)
        return _remote_submission_result(output, kind=JobKind.JARVIS, definition=definition)
    pipeline_name = _required_str(arguments, "pipeline_name")
    idempotency_key = str(
        arguments.get("idempotency_key")
        or f"mcp:jarvis-job:{cluster}:{pipeline_name}{dependency_suffix}"
    )
    job = _submit_local_job(
        queue,
        RelayJob(
            cluster=cluster,
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_name=pipeline_name),
            idempotency_key=idempotency_key,
            used_artifact_refs=used_artifact_refs,
        ),
        settings=settings,
    )
    wait_arguments = {
        **arguments,
        "wait_timeout_seconds": wait_timeout_seconds,
    }
    return _submission_result(job, wait_arguments, queue=queue, definition=definition)


def _submit_remote_agent(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    from clio_relay import mcp_server as _mcp_server

    cluster = _required_str(arguments, "cluster")
    used_artifact_refs = _artifact_use_refs(arguments)
    prompt_path = _required_str(arguments, "prompt_path")
    mcp_config_path = _optional_str(arguments, "mcp_config_path")
    model = _optional_str(arguments, "model")
    workdir = _optional_str(arguments, "workdir")
    timeout_seconds = _optional_int(arguments, "timeout_seconds")
    request_followup_message = _boolean_argument(
        arguments,
        "request_followup_message",
        default=False,
    )
    identity: dict[str, object] = {
        "cluster": cluster,
        "prompt_path": prompt_path,
        "mcp_config_path": mcp_config_path,
        "model": model,
        "workdir": workdir,
        "timeout_seconds": timeout_seconds,
    }
    if request_followup_message:
        identity["request_followup_message"] = True
    if used_artifact_refs:
        identity["used_artifact_refs"] = [artifact_use_payload(item) for item in used_artifact_refs]
    idempotency_key = str(
        arguments.get("idempotency_key") or "mcp:remote-agent:" + _stable_digest(identity)
    )
    definition = _mcp_server._optional_cluster_definition(cluster)
    if (
        definition is not None
        and _mcp_server.should_execute_on_cluster(definition)
        and settings.owner_session_id is not None
    ):
        payload: dict[str, object] = {
            "cluster": cluster,
            "prompt_path": prompt_path,
            "idempotency_key": idempotency_key,
            "used_artifact_refs": [artifact_use_payload(item) for item in used_artifact_refs],
        }
        for key, value in {
            "mcp_config_path": mcp_config_path,
            "model": model,
            "workdir": workdir,
            "timeout_seconds": timeout_seconds,
        }.items():
            if value is not None:
                payload[key] = value
        job = _mcp_server.submit_owned_session_job(
            definition=definition,
            settings=settings,
            path="/jobs/remote-agent",
            payload=payload,
        )
        return _mcp_server._owned_session_submission_result(
            job,
            definition=definition,
            settings=settings,
            wait_for_terminal_result=bool(arguments.get("wait_for_terminal", False)),
            wait_timeout_seconds=float(arguments.get("wait_timeout_seconds", 600)),
            poll_seconds=float(arguments.get("poll_seconds", 2)),
        )
    job = _submit_local_job(
        queue,
        RelayJob(
            cluster=cluster,
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(
                prompt_path=prompt_path,
                mcp_config_path=mcp_config_path,
                model=model,
                workdir=workdir,
                timeout_seconds=timeout_seconds,
            ),
            idempotency_key=idempotency_key,
            used_artifact_refs=used_artifact_refs,
        ),
        settings=settings,
    )
    return _submission_result(job, arguments, queue=queue)


def _submit_local_job(
    queue: ClioCoreQueue,
    job: RelayJob,
    *,
    settings: RelaySettings,
) -> RelayJob:
    """Stamp local session ownership only after exact durable admission is open."""
    session_id = settings.owner_session_id
    generation_id = settings.owner_session_generation_id
    if session_id is None or generation_id is None:
        return queue.submit_job(job)
    admission = queue.owner_session_generation_status(
        session_id,
        session_generation_id=generation_id,
    )
    if admission.get("open") is not True:
        raise ValueError("owner session generation is not open for local MCP submission")
    metadata = dict(job.metadata)
    if {
        "owner",
        "owner_session_id",
        "owner_session_generation_id",
        "owner_session_admission_id",
    }.intersection(metadata):
        raise ValueError("local MCP job cannot supply relay-managed ownership metadata")
    metadata.update(
        {
            "owner": "clio-relay",
            "owner_session_id": session_id,
            "owner_session_generation_id": generation_id,
        }
    )
    return queue.submit_job(job.model_copy(update={"metadata": metadata}))
