"""Terminal console flush and result-artifact finalization.

Owner module for iowarp/clio-relay#231's endpoint decomposition.
``_flush_terminal_console`` is the #259 best-effort final console-log flush;
``_append_optional_result_artifacts``/``_remote_agent_final_artifact``/
``_append_spool_artifact_once`` append the console/remote-agent-result artifacts a job
may or may not have produced, deduplicated against artifacts already recorded.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, cast

from clio_relay.console_stream import (
    CONSOLE_STDERR_STREAM,
    CONSOLE_STREAM,
    ConsoleLiveTailer,
    flush_terminal_console_from_path,
    flush_terminal_console_stderr_from_path,
)
from clio_relay.endpoint_sidecar_types import (
    AGENT_RESULT_MAX_BYTES,
)
from clio_relay.errors import RelayError
from clio_relay.filesystem_paths import (
    internal_filesystem_path,
)
from clio_relay.jarvis_execution_artifacts import ingest_jarvis_execution_outputs_from_path
from clio_relay.models import (
    CLIO_PROVENANCE_METADATA_KEY,
    ArtifactRef,
    JobKind,
    McpCallSpec,
    RelayJob,
)
from clio_relay.spool import JobSpool, read_owned_regular_file_bytes

if TYPE_CHECKING:
    from clio_relay.core_queue import ClioCoreQueue


class ResultFinalizationMixin:
    """Mixin: ResultFinalization methods split from EndpointWorker (clio-relay#231).

    ``queue`` is declared ``TYPE_CHECKING``-only (never assigned here) so
    strict pyright can resolve ``self.queue`` across this mixin's own
    methods -- see ``JarvisDispatchMixin``'s identical note in
    ``endpoint_jarvis_dispatch.py`` for why.
    """

    if TYPE_CHECKING:
        queue: ClioCoreQueue

    def _flush_terminal_console(
        self,
        job: RelayJob,
        spool: JobSpool,
        result_path: Path,
        console_tailer: ConsoleLiveTailer | None,
    ) -> None:
        """Guarantee the console/console_stderr streams carry the
        application's full logs.

        The #259 terminal-flush half: the live tailer wired through
        ``on_poll`` is a best-effort demo aid, but this call -- triggered
        for every ``mcp_result`` artifact index, including worker-restart
        recovery replay where no live tailer ever ran -- is the one that
        must always succeed at making ``console``/``console_stderr``
        complete, or report a typed, non-fatal reason. It never fails the
        job.
        """
        if not (
            job.kind is JobKind.MCP_CALL
            and isinstance(job.spec, McpCallSpec)
            and job.spec.tool == "jarvis_run"
        ):
            return
        outcome = flush_terminal_console_from_path(spool, result_path, tailer=console_tailer)
        if outcome.reason is not None:
            self.queue.append_event(
                job.job_id,
                f"console.{outcome.reason}",
                outcome.message or "console terminal flush reason",
                payload={"stream": CONSOLE_STREAM, "reason": outcome.reason},
            )
        stderr_outcome = flush_terminal_console_stderr_from_path(
            spool, result_path, tailer=console_tailer
        )
        if stderr_outcome.reason is not None:
            self.queue.append_event(
                job.job_id,
                f"console_stderr.{stderr_outcome.reason}",
                stderr_outcome.message or "console_stderr terminal flush reason",
                payload={"stream": CONSOLE_STDERR_STREAM, "reason": stderr_outcome.reason},
            )

    def _append_optional_result_artifacts(
        self,
        job: RelayJob,
        spool: JobSpool,
        *,
        console_tailer: ConsoleLiveTailer | None = None,
    ) -> dict[str, object] | None:
        """Index the job's optional terminal result artifacts.

        Returns clio-relay#265's typed ``outputs_missing`` payload (or
        ``None``) from the ONE ``mcp_result`` ingest this method always
        performs for a terminal jarvis_run -- the caller
        (``_run_job_impl``) folds it into the job's success/failure verdict.
        """
        outputs_missing: dict[str, object] | None = None
        candidates = {
            "agent_result": spool.path / "agent-result.json",
            "agent_last_message": spool.path / "agent-last-message.txt",
            "mcp_result": spool.path / "mcp-result.json",
        }
        for kind, path in candidates.items():
            if not internal_filesystem_path(path).exists():
                continue
            candidate = None
            if kind == "agent_last_message" and job.kind is JobKind.REMOTE_AGENT:
                candidate = self._remote_agent_final_artifact(job, spool, path)
            if kind == "mcp_result":
                _indexed, _truncation, outputs_missing = ingest_jarvis_execution_outputs_from_path(
                    self.queue, job, path, spool.path
                )
                self._flush_terminal_console(job, spool, path, console_tailer)
            if self._append_spool_artifact_once(
                job,
                spool,
                path,
                kind=kind,
                candidate=candidate,
            ):
                self.queue.append_event(
                    job.job_id,
                    f"{kind}.available",
                    f"Result artifact available: {kind}",
                    payload={"path": str(path)},
                )
        return outputs_missing

    def _remote_agent_final_artifact(
        self,
        job: RelayJob,
        spool: JobSpool,
        path: Path,
    ) -> ArtifactRef:
        """Verify the package-minted CLIO projection against final-answer bytes."""
        result_path = spool.path / "agent-result.json"
        try:
            result_snapshot = read_owned_regular_file_bytes(
                result_path,
                owned_root=spool.path,
                max_bytes=AGENT_RESULT_MAX_BYTES,
            )
            assert result_snapshot.data is not None
            result = json.loads(result_snapshot.data.decode("utf-8"))
        except (
            OSError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            RuntimeError,
            ValueError,
        ) as exc:
            raise RelayError(f"invalid remote-agent terminal record: {exc}") from exc
        if not isinstance(result, dict):
            raise RelayError("remote-agent terminal record must be an object")
        raw_projection = cast(dict[str, object], result).get("final_answer_artifact_ref")
        if not isinstance(raw_projection, dict):
            raise RelayError("remote-agent terminal record omitted final_answer_artifact_ref")
        try:
            projection = ArtifactRef.model_validate(raw_projection)
        except ValueError as exc:
            raise RelayError(f"invalid remote-agent final-answer ArtifactRef: {exc}") from exc
        owned = spool.artifact_for(path, kind="agent_last_message")
        if projection.sequence is not None:
            raise RelayError("remote-agent package must not assign the relay artifact sequence")
        if projection.job_id != job.job_id:
            raise RelayError("remote-agent final-answer artifact job lineage did not match")
        if projection.kind != "report":
            raise RelayError("remote-agent final-answer CLIO kind must be report")
        if (
            projection.uri != owned.uri
            or projection.size_bytes != owned.size_bytes
            or projection.sha256 != owned.sha256
        ):
            raise RelayError("remote-agent final-answer artifact content lineage did not match")
        required_metadata = {
            "kind": "report",
            "version": 1,
            "custody": "workspace-referenced",
            "mechanism": "model",
            "evidence_class": "hashed-at-use",
        }
        if any(projection.metadata.get(key) != value for key, value in required_metadata.items()):
            raise RelayError("remote-agent final-answer CLIO projection metadata did not match")
        return projection.model_copy(
            update={
                "metadata": {**projection.metadata, **owned.metadata},
            }
        )

    def _append_spool_artifact_once(
        self,
        job: RelayJob,
        spool: JobSpool,
        path: Path,
        *,
        kind: str,
        candidate: ArtifactRef | None = None,
    ) -> bool:
        """Index one immutable spool artifact, tolerating restart replay.

        ``mcp_result`` no longer ingests declared JARVIS execution outputs
        here (moved to each caller, #265): the ingest's typed
        ``outputs_missing`` verdict must reach the caller, and this helper's
        return type (a plain "was newly indexed" bool) is shared by five
        call sites across three modules, most of which have nothing to do
        with a terminal jarvis_run result.
        """
        candidate = candidate or spool.artifact_for(path, kind=kind)
        cursor: int | None = 1
        while cursor is not None:
            artifacts, cursor, _total = self.queue.list_artifacts_page(
                job.job_id,
                cursor=cursor,
                limit=100,
            )
            for existing in artifacts:
                if existing.kind != candidate.kind or existing.uri != candidate.uri:
                    continue
                if (
                    existing.sha256 != candidate.sha256
                    or existing.size_bytes != candidate.size_bytes
                ):
                    raise RelayError(
                        f"indexed {kind} artifact changed during restart recovery: {path}"
                    )
                if (
                    CLIO_PROVENANCE_METADATA_KEY in candidate.metadata
                    and existing.artifact_id != candidate.artifact_id
                ):
                    raise RelayError(
                        f"indexed {kind} artifact lost its package-minted identity: {path}"
                    )
                return False
        self.queue.append_artifact(candidate)
        return True
