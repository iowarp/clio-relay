"""JARVIS dispatch environment, refusal handling, lost-response recovery projection, and
deferred-execution watch/finalize.

Owner module for iowarp/clio-relay#231's endpoint decomposition. Composes the
registered-site Spack environment for a ``jarvis_run`` child
(``_jarvis_run_environment_values``); refuses an empty pipeline before scheduler
submission (``_refuse_empty_jarvis_pipeline``, clio-relay#162); reads a durable dispatch
refusal an earlier attempt already recorded (``_recorded_jarvis_dispatch_refusal``) and
settles recovery from an explicit JARVIS tool-error refusal
(``_refuse_jarvis_execution_recovery``); projects a recovered execution-query answer
back into the lost run response (``_write_recovered_jarvis_run_result``); and the #266
scheduler-deferred-execution watch/validate/finalize trio
(``_watch_deferred_jarvis_execution``/ ``_validated_recovered_jarvis_dispatch``/
``_finalize_recovered_jarvis_dispatch``).
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, cast

from clio_relay import execution_watch, jarvis_pipeline_precheck
from clio_relay.command_evidence import bounded_error_detail
from clio_relay.console_stream import (
    ConsoleLiveTailer,
)
from clio_relay.endpoint_jarvis_recovery import (
    _attributed_jarvis_dispatch_refusal,
    _durable_jarvis_execution_recovery,
    _trusted_jarvis_mcp_result,
    _trusted_jarvis_mcp_route,
)
from clio_relay.endpoint_recovery_directory import (
    _write_private_json_file,
)
from clio_relay.endpoint_scheduler_metadata import (
    _runtime_metadata_is_native,
)
from clio_relay.endpoint_sidecar_types import (
    MCP_JARVIS_EXECUTION_RECOVERY_RESULT_MAX_BYTES,
    MCP_JARVIS_EXECUTION_RECOVERY_SCHEMA,
    SchedulerSubmissionUnresolvedError,
)
from clio_relay.errors import RelayError
from clio_relay.filesystem_paths import (
    internal_filesystem_path,
)
from clio_relay.jarvis_dispatch_failure import (
    JARVIS_DISPATCH_REFUSAL_RESOLUTION,
    JarvisDispatchRefusal,
)
from clio_relay.jarvis_execution_artifacts import ingest_jarvis_execution_outputs_from_path
from clio_relay.jarvis_run_environment import (
    jarvis_run_environment_values,
    registered_site_spack_command,
)
from clio_relay.models import (
    JobState,
    Lease,
    McpCallSpec,
    RelayJob,
    RelayTask,
    utc_now,
)
from clio_relay.runtime_metadata import (
    JarvisRuntimeMetadata,
    runtime_metadata_from_mcp_result_document,
)
from clio_relay.spool import JobSpool, read_owned_regular_file_bytes

if TYPE_CHECKING:
    from clio_relay.core_queue import ClioCoreQueue


class JarvisDispatchMixin:
    """Mixin: JarvisDispatch methods split from EndpointWorker (clio-relay#231).

    ``queue`` is declared ``TYPE_CHECKING``-only (never assigned here) so
    strict pyright can resolve ``self.queue`` across this mixin's own
    methods: the sole composing class, ``EndpointWorker``, is what actually
    assigns it in ``__init__`` (``endpoint.py``), and a mixin has no
    ``__init__`` of its own for pyright to see that assignment through. The
    same pattern this repo's tracked sibling-mixin design gap (#271) will
    eventually need everywhere else -- scoped here to unblock this mixin's
    own already-existing ``self.queue`` call sites, not a repo-wide fix.
    """

    if TYPE_CHECKING:
        queue: ClioCoreQueue

    def _jarvis_run_environment_values(
        self,
        job: RelayJob,
        *,
        task_id: str,
    ) -> dict[str, str]:
        """Compose the site runtime identity this cluster registered for JARVIS.

        A ``jarvis_run`` resolves its ``spack_specs`` inside the JARVIS MCP child
        this worker spawns, so the child needs the same Spack executable the
        cluster registered and the Spack route already receives. A cluster that
        declares none keeps the previous environment exactly.
        """
        route_valid, _route_reason = _trusted_jarvis_mcp_route(job)
        if not route_valid:
            return {}
        spack_command = registered_site_spack_command(job.cluster)
        values = jarvis_run_environment_values(spack_command)
        if values:
            self.queue.append_event(
                job.job_id,
                "jarvis.run_environment_composed",
                "Registered cluster Spack executable composed into the JARVIS run environment",
                payload={
                    "task_id": task_id,
                    "cluster": job.cluster,
                    "spack_command": spack_command,
                },
            )
        return values

    def _refuse_empty_jarvis_pipeline(
        self,
        job: RelayJob,
        *,
        task: RelayTask,
        spool: JobSpool,
    ) -> bool:
        """clio-relay#162: refuse a ``jarvis_run`` whose pipeline has zero declared steps.

        Queries JARVIS's own ``jarvis_describe(target="pipeline")`` before
        ``_run_job_impl`` ever calls ``_run_execution_streaming`` for this
        job, so an empty pipeline never reaches scheduler submission. An
        INCONCLUSIVE precheck (see ``jarvis_pipeline_precheck``'s own
        docstring) changes nothing -- it is not itself a refusal, and
        today's pre-#162 behavior (submit, let JARVIS/the scheduler answer)
        applies unchanged; this only closes the specific hole where a real
        scheduler allocation started and stopped nothing. Returns ``True``
        when the job was refused and terminalized here.
        """
        if not isinstance(job.spec, McpCallSpec) or job.spec.tool != "jarvis_run":
            return False
        pipeline_id = job.spec.arguments.get("pipeline_id")
        if not isinstance(pipeline_id, str) or not pipeline_id:
            return False
        route_valid, _reason = _trusted_jarvis_mcp_route(job)
        if not route_valid:
            return False
        result = jarvis_pipeline_precheck.dispatch_pipeline_describe_query(
            job,
            base_spec=job.spec,
            provider=self.provider,
            query_dir=spool.path / ".pipeline-precheck",
            pipeline_id=pipeline_id,
        )
        if result.inconclusive_reason is not None:
            self.queue.append_event(
                job.job_id,
                "jarvis.pipeline_precheck_inconclusive",
                "Pre-dispatch pipeline emptiness check was inconclusive; dispatch proceeds "
                "unchanged",
                payload={
                    "schema_version": (
                        jarvis_pipeline_precheck.PIPELINE_PRECHECK_INCONCLUSIVE_SCHEMA
                    ),
                    "pipeline_id": pipeline_id,
                    "reason": result.inconclusive_reason,
                },
            )
            return False
        if result.step_count is None or result.step_count > 0:
            return False
        execution_id = job.spec.arguments.get("execution_id")
        payload = jarvis_pipeline_precheck.empty_pipeline_refusal_payload(
            pipeline_id=pipeline_id,
            execution_id=execution_id if isinstance(execution_id, str) else None,
        )
        self.queue.update_task_state(
            task.task_id,
            JobState.FAILED,
            message=f"Task failed: {task.name}",
            metadata={"jarvis_pipeline_empty_refusal": payload, "returncode": 1},
        )
        self.queue.update_job_state(
            job.job_id,
            JobState.FAILED,
            message=(
                "JARVIS run refused before scheduler submission: pipeline has zero declared steps"
            ),
            error=bounded_error_detail(
                jarvis_pipeline_precheck.empty_pipeline_refusal_text(payload)
            ),
        )
        self.queue.append_event(
            job.job_id,
            "jarvis.pipeline_empty_refused",
            f"jarvis_run refused before scheduler submission: pipeline {pipeline_id} has zero "
            "declared steps",
            payload=payload,
        )
        return True

    def _recorded_jarvis_dispatch_refusal(
        self,
        job: RelayJob,
        *,
        spool: JobSpool,
    ) -> JarvisDispatchRefusal | None:
        """Return the typed refusal an earlier dispatch already persisted.

        A worker that restarts, or one reconciling an attempt another worker
        abandoned, reads the same durable answer the original dispatch recorded
        rather than querying JARVIS for an execution the refusal proves absent.
        """
        storage_result_path = internal_filesystem_path(spool.path / "mcp-result.json")
        if not storage_result_path.is_file():
            return None
        try:
            document = json.loads(storage_result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            self.queue.append_event(
                job.job_id,
                "runtime.metadata_read_failed",
                f"MCP runtime result could not be read: {exc}",
            )
            return None
        return _attributed_jarvis_dispatch_refusal(job, document)

    def _refuse_jarvis_execution_recovery(
        self,
        job: RelayJob,
        *,
        task_id: str,
        spool: JobSpool,
        refusal: JarvisDispatchRefusal,
    ) -> None:
        """Settle ownership from one explicit JARVIS tool error on the dispatch.

        The JARVIS user contract issues a durable execution handle only when a
        run starts, so an ``isError`` answer proves no execution exists to query
        or adopt. Recording it resolves the recovery intent instead of leaving
        the durable job nonterminal behind an ownership question that cannot be
        answered.
        """
        task = self.queue.get_task(task_id)
        intent = _durable_jarvis_execution_recovery(job, task)
        if intent is None:
            return
        if intent["state"] != "pending" or intent["dispatch_state"] != "started":
            raise RelayError("JARVIS dispatch refusal did not match a released dispatch")
        storage_result_path = internal_filesystem_path(spool.path / "mcp-result.json")
        if not storage_result_path.is_file():
            raise RelayError("JARVIS dispatch refusal has no durable result artifact")
        result_sha256 = hashlib.sha256(storage_result_path.read_bytes()).hexdigest()
        self.queue.update_task_metadata(
            task_id,
            {
                "jarvis_execution_recovery": {
                    **intent,
                    "state": "resolved",
                    "last_error": None,
                    "next_retry_at": None,
                    "result_sha256": result_sha256,
                    "resolved_at": utc_now().isoformat(),
                    "resolution": JARVIS_DISPATCH_REFUSAL_RESOLUTION,
                    "scheduler_provider": None,
                    "scheduler_job_id": None,
                    "query_process": None,
                },
                "jarvis_dispatch_refusal": refusal.as_payload(),
            },
        )
        self.queue.append_event(
            job.job_id,
            "jarvis.dispatch_refused",
            f"JARVIS refused the run: {refusal.as_error_detail()}",
            payload={
                "task_id": task_id,
                "result_sha256": result_sha256,
                **refusal.as_payload(),
            },
        )

    def _write_recovered_jarvis_run_result(
        self,
        job: RelayJob,
        *,
        query_document: dict[str, object],
        spool: JobSpool,
        recovery_result_sha256: str,
    ) -> None:
        """Project a validated execution query back into the lost run response."""
        if not isinstance(job.spec, McpCallSpec) or job.spec.tool != "jarvis_run":
            raise RelayError("recovered JARVIS run result has no trusted run spec")
        raw_structured = query_document.get("structured_result")
        if not isinstance(raw_structured, dict):
            raise RelayError("recovered JARVIS execution query has no structured result")
        structured = cast(dict[str, object], raw_structured)
        handle = structured.get("execution_handle")
        record = structured.get("execution_record")
        progress = structured.get("progress")
        runtime = structured.get("runtime_metadata")
        if not all(isinstance(item, dict) for item in (handle, record, progress, runtime)):
            raise RelayError("recovered JARVIS execution query omitted native documents")
        typed_handle = cast(dict[str, object], handle)
        typed_record = cast(dict[str, object], record)
        typed_runtime = cast(dict[str, object], runtime)
        script_path = typed_runtime.get("script_path")
        if script_path is not None and not isinstance(script_path, str):
            raise RelayError("recovered JARVIS runtime script_path is invalid")
        run_result: dict[str, object] = {
            "schema_version": "clio-kit.jarvis-run.v1",
            "pipeline_id": structured.get("pipeline_id"),
            "execution_id": structured.get("execution_id"),
            "status": typed_record.get("state"),
            "mode": typed_handle.get("mode"),
            "scheduler": None,
            "script_path": script_path,
            "wait": False,
            "execution_handle": handle,
            "execution_record": record,
            "progress": progress,
            "runtime_metadata": runtime,
            # clio-relay#265: NOT part of jarvis_run's own frozen outputSchema
            # (only jarvis_get_execution declares artifact_page) -- but this
            # projected run_result becomes the durable mcp-result.json BOTH
            # recovery callers write, and jarvis_execution_artifacts.
            # ingest_jarvis_execution_outputs reads exactly this file to
            # index #252's declared execution outputs and detect #265's
            # outputs-missing verdict. Dropping it here (as an earlier
            # revision did) meant every scheduler-deferred/recovered
            # jarvis_run silently lost BOTH: no execution-output artifacts
            # ever indexed and outputs_missing structurally unreachable,
            # regardless of what JARVIS actually declared. Both callers of
            # this method dispatch their query with `artifacts` requested
            # (execution_watch.execution_watch_query_spec's
            # include_artifacts=True final poll; this module's own
            # `_recover_jarvis_execution`, "artifacts": {"page_size": 100}),
            # so `structured["artifact_page"]` is always populated here --
            # an outputSchema-additive field a schema without
            # `additionalProperties: false` (verified) tolerates on the
            # client-facing side.
            "artifact_page": structured.get("artifact_page"),
        }
        recovered_document: dict[str, object] = {
            **query_document,
            "tool": "jarvis_run",
            "arguments": job.spec.arguments,
            "protocol_result": {"structuredContent": run_result},
            "structured_result": run_result,
            "returncode": 0,
            "timed_out": False,
            "protocol_error": None,
            "stdout": "",
            "stderr": "",
            # F4 (#231 R6 review): stdout/stderr are blanked above, but a
            # spread `**query_document` (doc §6.4's T3 record-time bound,
            # runner.py's _write_mcp_result) can carry a POPULATED
            # stdout_truncation/stderr_truncation from the source execution
            # query -- a record that would otherwise claim a truncation
            # happened on content that no longer exists. Null them out
            # explicitly rather than let the spread's stale value survive.
            "stdout_truncation": None,
            "stderr_truncation": None,
            "result_validation": None,
            "package_progress_bridge": None,
            "relay_recovery": {
                "schema_version": MCP_JARVIS_EXECUTION_RECOVERY_SCHEMA,
                "source_tool": "jarvis_get_execution",
                "source_result_sha256": recovery_result_sha256,
            },
        }
        trusted, reason = _trusted_jarvis_mcp_result(job, recovered_document)
        if not trusted:
            raise RelayError(f"recovered JARVIS run result was not trusted: {reason}")
        destination = spool.path / "mcp-result.json"
        _write_private_json_file(destination, recovered_document)
        self.queue.append_event(
            job.job_id,
            "mcp.dispatch_recovered",
            "Lost JARVIS run response was reconstructed from its durable execution query",
            payload={
                "pipeline_id": structured.get("pipeline_id"),
                "execution_id": structured.get("execution_id"),
                "source_result_sha256": recovery_result_sha256,
            },
        )

    def _watch_deferred_jarvis_execution(
        self,
        job: RelayJob,
        *,
        spool: JobSpool,
        console_tailer: ConsoleLiveTailer | None,
        lease: Lease,
        last_renewed_at: list[float],
    ) -> execution_watch.ExecutionWatchResolution | None:
        """#266: keep a scheduler-deferred ``jarvis_run`` job watching to real terminal.

        Called once, before ``_ingest_mcp_runtime_metadata`` ever reads
        ``mcp-result.json``. Detection and the poll loop itself both live
        in ``execution_watch`` (the owner module; endpoint.py may not
        regrow past its ratchet, #774/#775) -- ``None`` here means today's
        fast path applies unchanged and the file is left untouched.
        """
        if not isinstance(job.spec, McpCallSpec) or job.spec.tool != "jarvis_run":
            return None
        result_path = spool.path / "mcp-result.json"
        storage_result_path = internal_filesystem_path(result_path)
        if not storage_result_path.is_file():
            return None
        try:
            document = json.loads(storage_result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        deferred = execution_watch.deferred_jarvis_execution_from_document(job, document)
        if deferred is None:
            return None

        def write_terminal_result(query_document: dict[str, object], sha256: str) -> None:
            self._write_recovered_jarvis_run_result(
                job,
                query_document=query_document,
                spool=spool,
                recovery_result_sha256=sha256,
            )

        return execution_watch.run_execution_watch(
            job,
            base_spec=job.spec,
            deferred=deferred,
            queue=self.queue,
            provider=self.provider,
            watch_dir=spool.path / ".execution-watch",
            console_tailer=console_tailer,
            poll_interval_seconds=self.settings.execution_watch_poll_interval_seconds,
            ceiling_seconds=self.settings.execution_watch_ceiling_seconds,
            renew_lease=lambda: self._renew_lease_if_needed(lease, last_renewed_at),
            is_cancellation_requested=lambda: self._job_cancellation_requested(job.job_id),
            write_terminal_result=write_terminal_result,
            now=utc_now,
        )

    def _validated_recovered_jarvis_dispatch(
        self,
        job: RelayJob,
        *,
        task: RelayTask,
        spool: JobSpool,
    ) -> JarvisRuntimeMetadata:
        """Reload one resolved, exact JARVIS response after a worker restart."""
        intent = _durable_jarvis_execution_recovery(job, task)
        if intent is None or intent["state"] != "resolved":
            raise SchedulerSubmissionUnresolvedError("JARVIS execution response has not resolved")
        result_path = spool.path / "mcp-result.json"
        try:
            snapshot = read_owned_regular_file_bytes(
                result_path,
                owned_root=spool.path,
                max_bytes=MCP_JARVIS_EXECUTION_RECOVERY_RESULT_MAX_BYTES,
            )
            if snapshot.data is None:
                raise RelayError("resolved JARVIS result snapshot omitted its bytes")
            document = json.loads(snapshot.data.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, RuntimeError) as exc:
            raise SchedulerSubmissionUnresolvedError(
                f"resolved JARVIS result could not be reloaded: {exc}"
            ) from exc
        trusted, reason = _trusted_jarvis_mcp_result(job, document)
        if not trusted:
            raise SchedulerSubmissionUnresolvedError(
                f"resolved JARVIS result was not trusted: {reason}"
            )
        typed_document = cast(dict[str, object], document)
        try:
            metadata = runtime_metadata_from_mcp_result_document(typed_document)
        except ValueError as exc:
            raise SchedulerSubmissionUnresolvedError(
                f"resolved JARVIS runtime documents were invalid: {exc}"
            ) from exc
        if (
            metadata is None
            or not _runtime_metadata_is_native(metadata)
            or metadata.pipeline_id != intent["pipeline_id"]
            or metadata.execution_id != intent["execution_id"]
            or metadata.scheduler_provider != intent["scheduler_provider"]
            or metadata.scheduler_job_id != intent["scheduler_job_id"]
        ):
            raise SchedulerSubmissionUnresolvedError(
                "resolved JARVIS runtime identity did not match its durable intent"
            )
        resolution = intent.get("resolution")
        result_sha256 = intent.get("result_sha256")
        if resolution == "dispatch_result":
            if result_sha256 != snapshot.sha256:
                raise SchedulerSubmissionUnresolvedError(
                    "resolved JARVIS dispatch result changed after recovery"
                )
        elif resolution == "execution_query":
            raw_recovery = typed_document.get("relay_recovery")
            recovery = (
                cast(dict[str, object], raw_recovery) if isinstance(raw_recovery, dict) else None
            )
            if (
                not isinstance(recovery, dict)
                or recovery.get("schema_version") != MCP_JARVIS_EXECUTION_RECOVERY_SCHEMA
                or recovery.get("source_tool") != "jarvis_get_execution"
                or recovery.get("source_result_sha256") != result_sha256
            ):
                raise SchedulerSubmissionUnresolvedError(
                    "reconstructed JARVIS result lost its exact query provenance"
                )
        else:
            raise SchedulerSubmissionUnresolvedError(
                "resolved JARVIS response has an unsupported resolution"
            )
        return metadata

    def _finalize_recovered_jarvis_dispatch(
        self,
        job: RelayJob,
        *,
        task: RelayTask,
        spool: JobSpool,
        runtime_metadata: JarvisRuntimeMetadata,
    ) -> None:
        """Index an eventually recovered handle-first response exactly once."""
        intent = _durable_jarvis_execution_recovery(job, task)
        if intent is None or intent["state"] != "resolved":
            raise RelayError("cannot finalize an unresolved JARVIS response")
        runtime_metadata_path = spool.path / "runtime-metadata.json"
        if not internal_filesystem_path(runtime_metadata_path).exists():
            spool.write_runtime_metadata(runtime_metadata.model_dump(mode="json"))
        if self._append_spool_artifact_once(
            job,
            spool,
            runtime_metadata_path,
            kind="runtime_metadata",
        ):
            self.queue.append_event(
                job.job_id,
                "runtime.metadata_available",
                "Structured runtime metadata available",
                payload={
                    "path": str(runtime_metadata_path),
                    "source": runtime_metadata.source.value,
                },
            )
        for kind, path in (
            ("stdout", spool.path / "stdout.log"),
            ("stderr", spool.path / "stderr.log"),
            ("console", spool.path / "console.log"),
            ("console_stderr", spool.path / "console_stderr.log"),
            ("log_capture", spool.log_capture_path),
            ("mcp_result", spool.path / "mcp-result.json"),
        ):
            if not internal_filesystem_path(path).is_file():
                raise RelayError(f"recovered JARVIS spool omitted {kind}: {path}")
            if kind == "mcp_result":
                # #265: this crash-recovery reconciliation path (a worker
                # restart finalizing a JARVIS dispatch abandoned mid-run) is
                # NOT wired into #265's terminal-state fold today -- it never
                # reaches `_run_job_impl`/`resolve_execution_outcome`, whose
                # target state this method's own caller already computes
                # independently (`endpoint_execution_lifecycle.py`). The
                # ingest still runs so #252's output indexing keeps working
                # and the typed `jarvis.execution_output_missing`/`_empty`/
                # `_outputs_missing` events still land on the job's event
                # log for observability -- only the FORCED-failure fold is
                # the known, documented gap here.
                ingest_jarvis_execution_outputs_from_path(self.queue, job, path, spool.path)
            created = self._append_spool_artifact_once(job, spool, path, kind=kind)
            if created and kind == "mcp_result":
                self.queue.append_event(
                    job.job_id,
                    "mcp_result.available",
                    "Result artifact available: mcp_result",
                    payload={"path": str(path)},
                )
        provenance_path = spool.path / "provenance.json"
        if internal_filesystem_path(provenance_path).is_file():
            self._append_spool_artifact_once(
                job,
                spool,
                provenance_path,
                kind="provenance",
            )
            return
        pipeline_path = spool.path / "pipeline.yaml"
        if not internal_filesystem_path(pipeline_path).is_file():
            pipeline_path = spool.path / "pipeline-reference.json"
        self._append_provenance_artifact(
            job,
            spool,
            pipeline_path=pipeline_path,
            started_at=cast(str, intent["created_at"]),
            finished_at=cast(str, intent["resolved_at"]),
            returncode=0,
            terminal_state=JobState.SUCCEEDED,
            runtime_metadata=runtime_metadata,
        )
