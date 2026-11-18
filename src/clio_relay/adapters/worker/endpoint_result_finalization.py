"""Result-artifact finalization.

Owner module for iowarp/clio-relay#231's endpoint decomposition.
``_append_optional_result_artifacts``/``_remote_agent_final_artifact``/
``_append_spool_artifact_once`` append the remote-agent-result artifacts a job may or
may not have produced, deduplicated against artifacts already recorded.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, cast

from clio_relay.adapters.worker.endpoint_sidecar_types import (
    AGENT_RESULT_MAX_BYTES,
)
from clio_relay.errors import RelayError
from clio_relay.filesystem_paths import (
    internal_filesystem_path,
)
from clio_relay.models import (
    CLIO_PROVENANCE_METADATA_KEY,
    ArtifactRef,
    JobKind,
    RelayJob,
)
from clio_relay.spool import JobSpool, read_owned_regular_file_bytes

if TYPE_CHECKING:
    from clio_relay.adapters.worker.core_queue import ClioCoreQueue


class ResultFinalizationMixin:
    """Mixin: ResultFinalization methods split from EndpointWorker (clio-relay#231).

    ``queue`` is declared ``TYPE_CHECKING``-only (never assigned here) so
    strict pyright can resolve ``self.queue`` across this mixin's own
    methods; the sole composing class, ``EndpointWorker``, assigns it in
    ``__init__``.
    """

    if TYPE_CHECKING:
        queue: ClioCoreQueue

    def _append_optional_result_artifacts(
        self,
        job: RelayJob,
        spool: JobSpool,
    ) -> None:
        """Index the job's optional terminal result artifacts."""
        candidates = {
            "agent_result": spool.path / "agent-result.json",
            "agent_last_message": spool.path / "agent-last-message.txt",
        }
        for kind, path in candidates.items():
            if not internal_filesystem_path(path).exists():
                continue
            candidate = None
            if kind == "agent_last_message" and job.kind is JobKind.REMOTE_AGENT:
                candidate = self._remote_agent_final_artifact(job, spool, path)
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
        """Index one immutable spool artifact, tolerating restart replay."""
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
