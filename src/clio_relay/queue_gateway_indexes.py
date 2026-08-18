"""Durable gateway backlink/reverse-index ownership.

Owns the convergence that keeps every gateway-session index consistent with
its canonical ``GatewaySession`` record: scheduler-source manifests, the
per-job/per-session active backlinks, and the artifact/scheduler reverse
references those backlinks are rebuilt from. ``sync_gateway_session_derived``
is a module-level function (not only a bound method) because
``queue_gateways.py``'s canonical-write path must resolve it through a
lookup a test can patch on this module -- design doc CQ16 row: "Patch each
caller owner's collaborator attribute for browser CAS and backlink
synchronization" (the backlink-synchronization half). The bound method
``_sync_gateway_session_derived_unlocked`` stays a real, directly patchable
instance method (CQ19's ``queue_transitions.QueueTransitionsMixin.
_reconcile_transition_intents_unlocked`` self-calls it by that exact name on
the ``gateway_sync`` transition-intent replay path) and is a thin wrapper
over the module function, matching the ``queue_jobs.write_job``/``queue_
lease_indexes.sync_operational_indexes`` precedent.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from clio_relay import (
    queue_context,
    queue_layout,
    queue_store_lock,
    queue_store_read,
    queue_store_write,
)
from clio_relay.errors import QueueConflictError
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.models import ArtifactRef, GatewaySession, GatewaySessionState


def _metadata_scheduler_gc_state(metadata: dict[str, object]) -> tuple[set[str], bool]:
    scheduler_ids: set[str] = set()
    terminal_ids: set[str] = set()
    scheduler_marker_seen = False

    def observe(document: object) -> None:
        nonlocal scheduler_marker_seen
        if not isinstance(document, dict):
            return
        typed = cast(dict[str, object], document)
        scheduler_id = typed.get("scheduler_job_id")
        if isinstance(scheduler_id, str) and scheduler_id:
            scheduler_marker_seen = True
            scheduler_ids.add(scheduler_id)
            phase = typed.get("phase")
            if (
                isinstance(phase, str)
                and phase.lower() in queue_store_lock.GC_TERMINAL_SCHEDULER_PHASES
            ):
                terminal_ids.add(scheduler_id)
        elif typed.get("scheduler") is not None or typed.get("scheduler_provider") is not None:
            scheduler_marker_seen = True

    observe(metadata.get("runtime_metadata"))
    observe(metadata)
    observe(metadata.get("scheduler_status"))
    for field in ("scheduler_statuses", "scheduler_job_ownership"):
        documents = metadata.get(field)
        if isinstance(documents, list):
            typed_documents = cast(list[object], documents)
            if len(typed_documents) > queue_layout.MAX_SCHEDULER_METADATA_RECORDS:
                raise QueueConflictError(
                    f"{field} exceeds {queue_layout.MAX_SCHEDULER_METADATA_RECORDS} records"
                )
            for document in typed_documents:
                observe(document)
    raw_ids = metadata.get("scheduler_job_ids")
    if isinstance(raw_ids, list):
        typed_ids = cast(list[object], raw_ids)
        if len(typed_ids) > queue_layout.MAX_SCHEDULER_METADATA_RECORDS:
            raise QueueConflictError(
                f"scheduler_job_ids exceeds {queue_layout.MAX_SCHEDULER_METADATA_RECORDS} records"
            )
        for raw_id in typed_ids:
            if isinstance(raw_id, str) and raw_id:
                scheduler_marker_seen = True
                scheduler_ids.add(raw_id)
    return scheduler_ids, scheduler_marker_seen and scheduler_ids != terminal_ids


def _gateway_source_provenance(session: GatewaySession) -> tuple[dict[str, Any], ...]:
    provenance = [session.metadata]
    runtime_binding = session.gateway.get("jarvis_runtime_binding")
    if isinstance(runtime_binding, dict):
        provenance.append(cast(dict[str, Any], runtime_binding))
    return tuple(provenance)


def _gateway_direct_job_ids(session: GatewaySession) -> set[str]:
    job_ids: set[str] = set()
    for field in ("relay_job_id", "job_id"):
        value = session.metadata.get(field)
        if isinstance(value, str) and value:
            job_ids.add(value)
    for provenance in _gateway_source_provenance(session):
        value = provenance.get("source_relay_job_id")
        if isinstance(value, str) and value:
            job_ids.add(value)
    return job_ids


def _gateway_direct_artifact_ids(session: GatewaySession) -> set[str]:
    artifact_ids: set[str] = set()
    candidates = list(session.artifacts)
    for provenance in _gateway_source_provenance(session):
        value = provenance.get("source_relay_artifact_id")
        if isinstance(value, str) and value:
            candidates.append(value)
    for candidate in candidates:
        try:
            artifact_ids.add(validate_durable_record_id(candidate))
        except ValueError:
            # Gateway artifacts may be external URIs. Only relay artifact IDs
            # participate in canonical artifact and retention indexes.
            continue
    return artifact_ids


def _gateway_relation_is_preserved(
    raw_ref: dict[str, object],
    session: GatewaySession,
) -> bool:
    relation_kind = raw_ref.get("relation_kind")
    relation_key = raw_ref.get("relation_key")
    if not isinstance(relation_kind, str) or not isinstance(relation_key, str):
        raise QueueConflictError("gateway relation reference is invalid")
    if relation_kind == "direct":
        return relation_key in _gateway_direct_job_ids(session)
    if relation_kind == "artifact":
        return relation_key in _gateway_direct_artifact_ids(session)
    if relation_kind == "scheduler":
        return relation_key == session.scheduler_job_id
    raise QueueConflictError(f"unsupported gateway relation kind: {relation_kind}")


def _stable_ref_token(*values: str) -> str:
    encoded = "\x00".join(values).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:32]


def sync_gateway_session_derived(
    queue: QueueGatewayIndexesMixin,
    session_id: str,
) -> None:
    """Clear stale gateway references and rebuild them from the canonical record."""
    session = queue_store_read.read_optional(
        queue._storage_root,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        queue._storage_root / "gateway_sessions" / f"{session_id}.json",  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        GatewaySession,
    )
    queue._unindex_gateway_session_id_unlocked(session_id, preserve=None)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    if session is not None:
        queue._index_gateway_session_unlocked(session)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


class QueueGatewayIndexesMixin:
    """Own gateway backlink/reverse-index synchronization for the queue facade."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        def _job_record_path(self, family: str, job_id: str, record_id: str) -> Path: ...

    def _sync_scheduler_source_unlocked(
        self,
        job_id: str,
        *,
        source_id: str,
        metadata: dict[str, object],
    ) -> None:
        scheduler_ids, ambiguous = _metadata_scheduler_gc_state(metadata)
        source_token = _stable_ref_token(source_id)
        manifest_path = self._job_record_path(
            "scheduler_refs_by_job",
            job_id,
            source_token,
        )
        protection_path = self._job_record_path(
            "scheduler_protections_by_job",
            job_id,
            source_token,
        )
        old_ids: set[str] = set()
        try:
            raw_manifest = queue_store_read.read_json_document(manifest_path)
        except FileNotFoundError:
            raw_manifest = None
        if raw_manifest is not None:
            if not isinstance(raw_manifest, dict):
                raise QueueConflictError(f"scheduler reference is not an object: {manifest_path}")
            manifest = cast(dict[str, object], raw_manifest)
            raw_old_ids = manifest.get("scheduler_ids")
            if not isinstance(raw_old_ids, list) or not all(
                isinstance(value, str) and value for value in cast(list[object], raw_old_ids)
            ):
                raise QueueConflictError(f"scheduler reference is invalid: {manifest_path}")
            old_ids = set(cast(list[str], raw_old_ids))
        for scheduler_id in old_ids - scheduler_ids:
            queue_store_write.unlink_durable_path(
                self._scheduler_reverse_ref_path(scheduler_id, job_id, source_id),
                missing_ok=True,
            )
            gateway_paths = queue_store_read.bounded_json_record_paths(
                self._gateway_reverse_directory("scheduler", scheduler_id),
                limit=queue_layout.MAX_GATEWAY_INDEX_RECORDS,
                label=f"scheduler gateway reverse index {scheduler_id}",
            )
            for gateway_path in gateway_paths:
                gateway = queue_store_read.read_json_file(gateway_path, GatewaySession)
                self._unlink_active_gateway_job_ref_unlocked(
                    gateway.session_id,
                    job_id,
                    relation_kind="scheduler",
                    relation_key=scheduler_id,
                    source_id=source_id,
                )
        if scheduler_ids or ambiguous:
            queue_store_write.write_json(
                self._storage_root,
                manifest_path,
                {
                    "job_id": job_id,
                    "source_id": source_id,
                    "scheduler_ids": sorted(scheduler_ids),
                    "ambiguous": ambiguous,
                },
            )
        else:
            queue_store_write.unlink_durable_path(manifest_path, missing_ok=True)
        if ambiguous:
            queue_store_write.write_json(
                self._storage_root,
                protection_path,
                {"job_id": job_id, "source_id": source_id, "ambiguous": True},
            )
        else:
            queue_store_write.unlink_durable_path(protection_path, missing_ok=True)
        for scheduler_id in scheduler_ids:
            queue_store_write.write_json(
                self._storage_root,
                self._scheduler_reverse_ref_path(scheduler_id, job_id, source_id),
                {
                    "scheduler_id": scheduler_id,
                    "job_id": job_id,
                    "source_id": source_id,
                },
            )
            gateway_paths = queue_store_read.bounded_json_record_paths(
                self._gateway_reverse_directory("scheduler", scheduler_id),
                limit=queue_layout.MAX_GATEWAY_INDEX_RECORDS,
                label=f"scheduler gateway reverse index {scheduler_id}",
            )
            for gateway_path in gateway_paths:
                gateway = queue_store_read.read_json_file(gateway_path, GatewaySession)
                if gateway.state is not GatewaySessionState.CLOSED:
                    self._link_active_gateway_job_unlocked(
                        gateway,
                        job_id,
                        relation_kind="scheduler",
                        relation_key=scheduler_id,
                        source_id=source_id,
                    )

    def _index_gateway_session_unlocked(self, session: GatewaySession) -> None:
        if session.state is GatewaySessionState.CLOSED:
            return
        for job_id in _gateway_direct_job_ids(session):
            self._link_active_gateway_job_unlocked(
                session,
                job_id,
                relation_kind="direct",
                relation_key=job_id,
            )
        for artifact_id in _gateway_direct_artifact_ids(session):
            self._write_gateway_reverse_ref_unlocked("artifact", artifact_id, session)
            artifact = queue_store_read.read_optional(
                self._storage_root,
                self._storage_root / "artifacts" / f"{artifact_id}.json",
                ArtifactRef,
            )
            if artifact is not None:
                self._link_active_gateway_job_unlocked(
                    session,
                    artifact.job_id,
                    relation_kind="artifact",
                    relation_key=artifact_id,
                )
        if session.scheduler_job_id:
            scheduler_id = session.scheduler_job_id
            self._write_gateway_reverse_ref_unlocked("scheduler", scheduler_id, session)
            scheduler_paths = queue_store_read.bounded_json_record_paths(
                self._gateway_scheduler_jobs_directory(scheduler_id),
                limit=queue_layout.MAX_GATEWAY_INDEX_RECORDS,
                label=f"scheduler job reverse index {scheduler_id}",
            )
            for path in scheduler_paths:
                raw_ref = queue_store_read.read_json_document(path)
                if not isinstance(raw_ref, dict):
                    raise QueueConflictError(f"scheduler reverse reference is invalid: {path}")
                scheduler_ref = cast(dict[str, object], raw_ref)
                job_id = scheduler_ref.get("job_id")
                source_id = scheduler_ref.get("source_id")
                if not isinstance(job_id, str) or not isinstance(source_id, str):
                    raise QueueConflictError(f"scheduler reverse reference is invalid: {path}")
                self._link_active_gateway_job_unlocked(
                    session,
                    job_id,
                    relation_kind="scheduler",
                    relation_key=scheduler_id,
                    source_id=source_id,
                )

    def _sync_gateway_session_derived_unlocked(self, session_id: str) -> None:
        """Clear stale gateway references and rebuild them from the canonical record."""
        sync_gateway_session_derived(self, session_id)

    def _unindex_gateway_session_id_unlocked(
        self,
        session_id: str,
        *,
        preserve: GatewaySession | None,
    ) -> None:
        """Remove gateway backlinks by stable identity, optionally preserving live relations."""
        active_backlinks = (
            self._storage_root
            / "active_gateway_refs_by_session"
            / queue_layout.QueueLayout.durable_key(session_id)
        )
        active_paths = queue_store_read.bounded_json_record_paths(
            active_backlinks,
            limit=queue_layout.MAX_GATEWAY_INDEX_RECORDS,
            label=f"active gateway backlinks {session_id}",
        )
        for path in active_paths:
            raw_ref = queue_store_read.read_json_document(path)
            if not isinstance(raw_ref, dict):
                raise QueueConflictError(f"gateway job reference is invalid: {path}")
            job_ref = cast(dict[str, object], raw_ref)
            if preserve is not None and _gateway_relation_is_preserved(job_ref, preserve):
                continue
            job_id = job_ref.get("job_id")
            record_name = job_ref.get("record_name")
            if not isinstance(job_id, str) or not isinstance(record_name, str):
                raise QueueConflictError(f"gateway job reference is invalid: {path}")
            queue_store_write.unlink_durable_path(
                self._storage_root
                / "active_gateway_refs_by_job"
                / queue_layout.QueueLayout.durable_key(job_id)
                / record_name,
                missing_ok=True,
            )
            queue_store_write.unlink_durable_path(path, missing_ok=True)
        reverse_backlinks = (
            self._storage_root
            / "gateway_reverse_refs_by_session"
            / queue_layout.QueueLayout.durable_key(session_id)
        )
        reverse_paths = queue_store_read.bounded_json_record_paths(
            reverse_backlinks,
            limit=queue_layout.MAX_GATEWAY_INDEX_RECORDS,
            label=f"gateway reverse backlinks {session_id}",
        )
        for path in reverse_paths:
            raw_ref = queue_store_read.read_json_document(path)
            if not isinstance(raw_ref, dict):
                raise QueueConflictError(f"gateway reverse reference is invalid: {path}")
            reverse_ref = cast(dict[str, object], raw_ref)
            if preserve is not None and _gateway_relation_is_preserved(reverse_ref, preserve):
                continue
            family = reverse_ref.get("family")
            key = reverse_ref.get("relation_key")
            record_name = reverse_ref.get("record_name")
            if (
                family not in {"artifact", "scheduler"}
                or not isinstance(key, str)
                or not isinstance(record_name, str)
            ):
                raise QueueConflictError(f"gateway reverse reference is invalid: {path}")
            queue_store_write.unlink_durable_path(
                self._gateway_reverse_directory(cast(str, family), key) / record_name,
                missing_ok=True,
            )
            queue_store_write.unlink_durable_path(path, missing_ok=True)

    def _write_gateway_reverse_ref_unlocked(
        self,
        relation_kind: str,
        relation_key: str,
        session: GatewaySession,
    ) -> None:
        record_name = f"{queue_layout.QueueLayout.durable_key(session.session_id)}.json"
        queue_store_write.write_model(
            self._storage_root,
            self._gateway_reverse_directory(relation_kind, relation_key) / record_name,
            session,
        )
        queue_store_write.write_json(
            self._storage_root,
            self._storage_root
            / "gateway_reverse_refs_by_session"
            / queue_layout.QueueLayout.durable_key(session.session_id)
            / f"{_stable_ref_token(relation_kind, relation_key)}.json",
            {
                "session_id": session.session_id,
                "family": relation_kind,
                "relation_kind": relation_kind,
                "relation_key": relation_key,
                "record_name": record_name,
            },
        )

    def _link_gateways_for_artifact_unlocked(self, artifact: ArtifactRef) -> None:
        gateway_paths = queue_store_read.bounded_json_record_paths(
            self._gateway_reverse_directory("artifact", artifact.artifact_id),
            limit=queue_layout.MAX_GATEWAY_INDEX_RECORDS,
            label=f"artifact gateway reverse index {artifact.artifact_id}",
        )
        for gateway_path in gateway_paths:
            gateway = queue_store_read.read_json_file(gateway_path, GatewaySession)
            if gateway.state is not GatewaySessionState.CLOSED:
                self._link_active_gateway_job_unlocked(
                    gateway,
                    artifact.job_id,
                    relation_kind="artifact",
                    relation_key=artifact.artifact_id,
                )

    def _link_active_gateway_job_unlocked(
        self,
        session: GatewaySession,
        job_id: str,
        *,
        relation_kind: str,
        relation_key: str,
        source_id: str | None = None,
    ) -> None:
        token = _stable_ref_token(
            session.session_id,
            relation_kind,
            relation_key,
            source_id or "",
        )
        record_name = f"{token}.json"
        backlink_name = f"{_stable_ref_token(job_id, record_name)}.json"
        document: dict[str, object] = {
            "session_id": session.session_id,
            "job_id": job_id,
            "relation_kind": relation_kind,
            "relation_key": relation_key,
            "source_id": source_id,
            "record_name": record_name,
        }
        queue_store_write.write_json(
            self._storage_root,
            self._storage_root
            / "active_gateway_refs_by_job"
            / queue_layout.QueueLayout.durable_key(job_id)
            / record_name,
            document,
        )
        queue_store_write.write_json(
            self._storage_root,
            self._storage_root
            / "active_gateway_refs_by_session"
            / queue_layout.QueueLayout.durable_key(session.session_id)
            / backlink_name,
            document,
        )

    def _unlink_active_gateway_job_ref_unlocked(
        self,
        session_id: str,
        job_id: str,
        *,
        relation_kind: str,
        relation_key: str,
        source_id: str | None = None,
    ) -> None:
        record_name = (
            f"{_stable_ref_token(session_id, relation_kind, relation_key, source_id or '')}.json"
        )
        queue_store_write.unlink_durable_path(
            self._storage_root
            / "active_gateway_refs_by_job"
            / queue_layout.QueueLayout.durable_key(job_id)
            / record_name,
            missing_ok=True,
        )
        queue_store_write.unlink_durable_path(
            self._storage_root
            / "active_gateway_refs_by_session"
            / queue_layout.QueueLayout.durable_key(session_id)
            / f"{_stable_ref_token(job_id, record_name)}.json",
            missing_ok=True,
        )

    def _gateway_reverse_directory(self, relation_kind: str, relation_key: str) -> Path:
        if relation_kind not in {"artifact", "scheduler"}:
            raise QueueConflictError(f"unsupported gateway reference kind: {relation_kind}")
        return self._storage_root / f"gateways_by_{relation_kind}" / _stable_ref_token(relation_key)

    def _gateway_scheduler_jobs_directory(self, scheduler_id: str) -> Path:
        return self._storage_root / "scheduler_jobs" / _stable_ref_token(scheduler_id)

    def _scheduler_reverse_ref_path(
        self,
        scheduler_id: str,
        job_id: str,
        source_id: str,
    ) -> Path:
        return (
            self._gateway_scheduler_jobs_directory(scheduler_id)
            / f"{_stable_ref_token(job_id, source_id)}.json"
        )
