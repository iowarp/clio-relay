"""Durable owner-session generation, intake, and closure record ownership."""

from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path
from typing import cast

import clio_relay.models as models
from clio_relay import queue_context, queue_layout, queue_store_read, queue_store_write
from clio_relay.errors import QueueConflictError
from clio_relay.identifiers import validate_durable_record_id


def _validate_new_owner_session_metadata(  # pyright: ignore[reportUnusedFunction]
    metadata: dict[str, object],
) -> None:
    _validate_owner_session_identity_metadata(metadata, allow_legacy=False)


def _validate_owner_session_identity_metadata(
    metadata: dict[str, object],
    *,
    allow_legacy: bool,
) -> None:
    """Validate complete owner-session identity metadata."""
    _owner_session_identity(metadata, allow_legacy=allow_legacy)


def _owner_session_identity(
    metadata: dict[str, object],
    *,
    allow_legacy: bool,
) -> tuple[str, str | None] | None:
    owner_session_id = metadata.get("owner_session_id")
    generation_id = metadata.get("owner_session_generation_id")
    admission_session_id = metadata.get("owner_session_admission_id")
    if owner_session_id is None:
        if generation_id is not None or admission_session_id is not None:
            raise QueueConflictError(
                "owner_session_generation_id and owner_session_admission_id require "
                "owner_session_id"
            )
        return None
    if not isinstance(owner_session_id, str) or not owner_session_id:
        raise QueueConflictError("owner_session_id must be a non-empty string")
    if admission_session_id is not None and (
        not isinstance(admission_session_id, str)
        or not queue_layout.safe_global_record_id(admission_session_id)
    ):
        raise QueueConflictError("owner_session_admission_id must be a safe identifier")
    if generation_id is None and allow_legacy:
        return owner_session_id, None
    if not isinstance(generation_id, str):
        raise QueueConflictError("new owner-session records require owner_session_generation_id")
    try:
        validate_durable_record_id(generation_id)
    except ValueError as error:
        raise QueueConflictError(
            "owner_session_generation_id must be a portable durable identifier"
        ) from error
    return owner_session_id, generation_id


def _stable_ref_token(*values: str) -> str:
    return hashlib.sha256("\x00".join(values).encode("utf-8")).hexdigest()[:32]


class QueueOwnerSessionRecordsMixin:
    """Own owner-session generation, intake, membership, and closure records."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    def prepare_owner_session_start(
        self,
        owner_session_id: str,
        *,
        recorded_generation_id: str | None,
        candidate_generation_id: str,
    ) -> str:
        """Atomically select the only generation allowed to start under a transition lock."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        if recorded_generation_id is not None:
            recorded_generation_id = queue_layout.QueueLayout.require_durable_record_id(
                recorded_generation_id,
                field="recorded_generation_id",
            )
        candidate_generation_id = queue_layout.QueueLayout.require_durable_record_id(
            candidate_generation_id,
            field="candidate_generation_id",
        )
        self._store_adapter.initialize()
        session_label = queue_layout.QueueLayout.label_key(owner_session_id, domain="owner-session")
        closing_path = self._storage_root / "owner_sessions" / f"{session_label}.closing.json"
        with self._lock:
            active = self._read_owner_session_transition_record(
                self._owner_session_active_path(owner_session_id)
            )
            closing = self._read_owner_session_transition_record(closing_path)
            active_generation = self._validate_owner_session_active_record(
                owner_session_id,
                active,
            )
            closing_generation = self._validate_owner_session_closing_record(
                owner_session_id,
                closing,
            )
            if active_generation is not None:
                if closing_generation is not None:
                    previous_generation = (
                        None if active is None else active.get("previous_session_generation_id")
                    )
                    closure = self.get_owner_session_closed(
                        owner_session_id,
                        session_generation_id=closing_generation,
                    )
                    if (
                        previous_generation != closing_generation
                        or recorded_generation_id not in {None, closing_generation}
                        or closure is None
                        or closure.residual_resource_ids
                    ):
                        raise QueueConflictError(
                            f"owner session has an unfinished generation transition: "
                            f"{owner_session_id}"
                        )
                    queue_store_write.unlink_durable_path(closing_path, missing_ok=True)
                    return active_generation
                if recorded_generation_id not in {None, active_generation}:
                    previous_generation = (
                        None if active is None else active.get("previous_session_generation_id")
                    )
                    previous_closure = (
                        self.get_owner_session_closed(
                            owner_session_id,
                            session_generation_id=previous_generation,
                        )
                        if isinstance(previous_generation, str)
                        else None
                    )
                    if (
                        recorded_generation_id != previous_generation
                        or previous_closure is None
                        or previous_closure.residual_resource_ids
                    ):
                        raise QueueConflictError(
                            f"recorded owner session generation does not match active core state: "
                            f"{owner_session_id}"
                        )
                return active_generation
            if closing_generation is not None:
                if recorded_generation_id != closing_generation:
                    raise QueueConflictError(
                        f"recorded owner session generation does not match closure state: "
                        f"{owner_session_id}"
                    )
                closure = self.get_owner_session_closed(
                    owner_session_id,
                    session_generation_id=closing_generation,
                )
                if closure is None or closure.residual_resource_ids:
                    raise QueueConflictError(
                        f"owner session generation is not safely closed: {owner_session_id}"
                    )
                if candidate_generation_id == closing_generation:
                    raise QueueConflictError(
                        f"new owner session generation must differ from the closed generation: "
                        f"{owner_session_id}"
                    )
                selected_generation = candidate_generation_id
                previous_generation_id: str | None = closing_generation
            else:
                if recorded_generation_id is not None:
                    raise QueueConflictError(
                        f"recorded owner session generation has no core state: {owner_session_id}"
                    )
                selected_generation = candidate_generation_id
                previous_generation_id = None
            queue_store_write.write_json(
                self._storage_root,
                self._owner_session_active_path(owner_session_id),
                {
                    "owner_session_id": owner_session_id,
                    "session_generation_id": selected_generation,
                    "previous_session_generation_id": previous_generation_id,
                    "active": True,
                    "updated_at": models.utc_now().isoformat(),
                },
            )
            if closing_generation is not None:
                queue_store_write.unlink_durable_path(closing_path, missing_ok=True)
            return selected_generation

    def clear_owner_session_closing(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
    ) -> None:
        """Assert an exact active generation; never erase a closing transition."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = queue_layout.QueueLayout.require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        self._store_adapter.initialize()
        session_label = queue_layout.QueueLayout.label_key(owner_session_id, domain="owner-session")
        path = self._storage_root / "owner_sessions" / f"{session_label}.closing.json"
        with self._lock:
            if self._read_owner_session_transition_record(path) is not None:
                raise QueueConflictError(
                    f"owner session closing state cannot be cleared by resume: {owner_session_id}"
                )
            if self._owner_session_active_generation(owner_session_id) != session_generation_id:
                raise QueueConflictError(
                    f"owner session active generation does not match resume: {owner_session_id}"
                )

    def reopen_owner_session(
        self,
        owner_session_id: str,
        *,
        previous_session_generation_id: str,
        session_generation_id: str,
    ) -> None:
        """Activate a new generation only after exact prior-generation closure."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        previous_session_generation_id = queue_layout.QueueLayout.require_durable_record_id(
            previous_session_generation_id,
            field="previous_session_generation_id",
        )
        session_generation_id = queue_layout.QueueLayout.require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        selected = self.prepare_owner_session_start(
            owner_session_id,
            recorded_generation_id=previous_session_generation_id,
            candidate_generation_id=session_generation_id,
        )
        if selected != session_generation_id:
            raise QueueConflictError(
                f"owner session reopen selected an existing generation: {owner_session_id}"
            )

    def set_owner_session_closed(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
        residual_resource_ids: list[str] | None = None,
        legacy_unversioned_job_ids: list[str] | None = None,
    ) -> models.OwnerSessionClosure:
        """Record verified teardown completion for an owner session generation."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = queue_layout.QueueLayout.require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        raw_legacy_job_ids = legacy_unversioned_job_ids or []
        legacy_job_ids = sorted(set(raw_legacy_job_ids))
        if raw_legacy_job_ids != legacy_job_ids:
            raise ValueError("legacy_unversioned_job_ids must be unique and sorted")
        if len(legacy_job_ids) > 1_000:
            raise ValueError("legacy_unversioned_job_ids cannot exceed 1000 entries")
        if any(not queue_layout.safe_owner_legacy_job_id(job_id) for job_id in legacy_job_ids):
            raise ValueError("legacy_unversioned_job_ids contains an unsafe job id")
        if legacy_job_ids and residual_resource_ids:
            raise QueueConflictError(
                "legacy jobs cannot be covered while owner-session resources remain"
            )
        self._store_adapter.initialize()
        session_label = queue_layout.QueueLayout.label_key(owner_session_id, domain="owner-session")
        closing_path = self._storage_root / "owner_sessions" / f"{session_label}.closing.json"
        with self._lock:
            try:
                raw_closing = queue_store_read.read_json_document(closing_path)
            except (FileNotFoundError, OSError, QueueConflictError):
                raw_closing = None
            if not isinstance(raw_closing, dict):
                raise QueueConflictError(
                    f"owner session must be closing before it can be closed: {owner_session_id}"
                )
            closing = cast(dict[str, object], raw_closing)
            closing_generation = closing.get("session_generation_id")
            if (
                closing.get("owner_session_id") != owner_session_id
                or closing.get("closing") is not True
                or (closing_generation is not None and not isinstance(closing_generation, str))
            ):
                raise QueueConflictError(
                    f"owner session closing proof is invalid: {owner_session_id}"
                )
            if session_generation_id != closing_generation:
                raise QueueConflictError(
                    f"owner session generation changed before closure: {owner_session_id}"
                )
            closure = models.OwnerSessionClosure(
                owner_session_id=owner_session_id,
                session_generation_id=session_generation_id,
                residual_resource_ids=residual_resource_ids or [],
            )
            for legacy_job_id in legacy_job_ids:
                legacy_job = queue_store_read.read_required_job(self._storage_root, legacy_job_id)
                if (
                    legacy_job.metadata.get("owner_session_id") != owner_session_id
                    or legacy_job.metadata.get("owner_session_generation_id") is not None
                ):
                    raise QueueConflictError(
                        f"legacy owner-session coverage identity mismatch: {legacy_job_id}"
                    )
            closure_path = self._owner_session_closed_path(
                owner_session_id,
                session_generation_id=session_generation_id,
            )
            active_generation = self._owner_session_active_generation(owner_session_id)
            existing_closure = queue_store_read.read_optional(
                self._storage_root,
                closure_path,
                models.OwnerSessionClosure,
            )
            if active_generation not in {None, session_generation_id} or (
                active_generation is None and existing_closure is None
            ):
                raise QueueConflictError(
                    f"owner session active generation does not match closure: {owner_session_id}"
                )
            closure = self._write_immutable_owner_session_closure_unlocked(
                closure_path,
                closure,
            )
            if legacy_job_ids:
                legacy_closure = models.OwnerSessionClosure(
                    owner_session_id=owner_session_id,
                    session_generation_id=None,
                    covered_by_session_generation_id=session_generation_id,
                    covered_legacy_job_ids=legacy_job_ids,
                )
                self._write_immutable_owner_session_closure_unlocked(
                    self._owner_session_closed_path(owner_session_id),
                    legacy_closure,
                )
            queue_store_write.unlink_durable_path(
                self._owner_session_active_path(owner_session_id),
                missing_ok=True,
            )
            if not closing_path.is_file():
                raise QueueConflictError(
                    f"owner session closing proof disappeared: {owner_session_id}"
                )
            return closure

    def _write_immutable_owner_session_closure_unlocked(
        self,
        path: Path,
        closure: models.OwnerSessionClosure,
    ) -> models.OwnerSessionClosure:
        for attempt in range(queue_layout.OWNER_SESSION_CLOSURE_WRITE_ATTEMPTS):
            existing = queue_store_read.read_optional(
                self._storage_root,
                path,
                models.OwnerSessionClosure,
            )
            if existing is not None:
                if existing != closure.model_copy(update={"closed_at": existing.closed_at}):
                    raise QueueConflictError(
                        f"owner session closure history changed: {closure.owner_session_id}"
                    )
                return existing
            try:
                queue_store_write.write_model(self._storage_root, path, closure)
            except FileNotFoundError as exc:
                if attempt + 1 >= queue_layout.OWNER_SESSION_CLOSURE_WRITE_ATTEMPTS:
                    raise QueueConflictError(
                        "owner session closure directory did not remain available: "
                        f"{closure.owner_session_id}"
                    ) from exc
                continue
            persisted = queue_store_read.read_optional(
                self._storage_root,
                path,
                models.OwnerSessionClosure,
            )
            if persisted is None:
                if attempt + 1 >= queue_layout.OWNER_SESSION_CLOSURE_WRITE_ATTEMPTS:
                    raise QueueConflictError(
                        f"owner session closure did not remain durable: {closure.owner_session_id}"
                    )
                continue
            if persisted != closure.model_copy(update={"closed_at": persisted.closed_at}):
                raise QueueConflictError(
                    f"owner session closure history changed: {closure.owner_session_id}"
                )
            return persisted
        raise AssertionError("owner session closure retry loop exhausted without an outcome")

    def get_owner_session_closed(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str | None = None,
    ) -> models.OwnerSessionClosure | None:
        """Return exact verified closure history for one owner-session generation."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        if session_generation_id is not None:
            session_generation_id = queue_layout.QueueLayout.require_durable_record_id(
                session_generation_id,
                field="session_generation_id",
            )
        self._store_adapter.initialize()
        closure = queue_store_read.read_optional(
            self._storage_root,
            self._owner_session_closed_path(
                owner_session_id,
                session_generation_id=session_generation_id,
            ),
            models.OwnerSessionClosure,
        )
        if closure is None:
            return None
        if (
            closure.owner_session_id != owner_session_id
            or closure.session_generation_id != session_generation_id
        ):
            raise QueueConflictError(f"owner session closure identity mismatch: {owner_session_id}")
        return closure

    def _owner_session_closed_path(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str | None = None,
    ) -> Path:
        session_label = queue_layout.QueueLayout.label_key(owner_session_id, domain="owner-session")
        if session_generation_id is None:
            return self._storage_root / "owner_sessions" / f"{session_label}.closed.json"
        return (
            self._storage_root
            / "owner_sessions"
            / f"{session_label}.closures"
            / f"{_stable_ref_token(session_generation_id)}.json"
        )

    def _owner_session_active_path(self, owner_session_id: str) -> Path:
        session_label = queue_layout.QueueLayout.label_key(owner_session_id, domain="owner-session")
        return self._storage_root / "owner_sessions" / f"{session_label}.active.json"

    def _owner_session_membership_dir(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str | None,
    ) -> Path:
        owner_token = _stable_ref_token(owner_session_id)
        if session_generation_id is None:
            return self._storage_root / "owner_session_legacy_jobs" / owner_token
        return (
            self._storage_root
            / "owner_session_jobs"
            / owner_token
            / _stable_ref_token(session_generation_id)
        )

    def _assert_owner_session_intake_open_unlocked(
        self,
        metadata: dict[str, object],
        *,
        require_active: bool = False,
    ) -> None:
        """Enforce owner generation and closing state at the durable write boundary."""
        identity = _owner_session_identity(metadata, allow_legacy=False)
        if identity is None:
            return
        owner_session_id, session_generation_id = identity
        admission_session_id = metadata.get("owner_session_admission_id", owner_session_id)
        if not isinstance(admission_session_id, str) or not queue_layout.safe_global_record_id(
            admission_session_id
        ):
            raise QueueConflictError("owner_session_admission_id must be a safe identifier")
        closing_path = (
            self._storage_root
            / "owner_sessions"
            / f"{queue_layout.QueueLayout.durable_key(admission_session_id)}.closing.json"
        )
        closing = self._read_owner_session_transition_record(closing_path)
        if self._validate_owner_session_closing_record(admission_session_id, closing) is not None:
            raise QueueConflictError(
                f"owner session generation is closing and rejects new work: {owner_session_id}"
            )
        active_generation = self._owner_session_active_generation(admission_session_id)
        if require_active and active_generation is None:
            raise QueueConflictError(
                f"owner session generation has no active admission state: {owner_session_id}"
            )
        if active_generation is not None and active_generation != session_generation_id:
            raise QueueConflictError(
                f"owner session generation does not match active intake: {owner_session_id}"
            )
        if (
            self.get_owner_session_closed(
                admission_session_id,
                session_generation_id=session_generation_id,
            )
            is not None
        ):
            raise QueueConflictError(
                f"owner session generation is already closed: {owner_session_id}"
            )

    def _sync_owner_session_job_membership_unlocked(self, job: models.RelayJob) -> None:
        """Persist generation membership independently of active/terminal job state."""
        identity = _owner_session_identity(job.metadata, allow_legacy=True)
        if identity is None:
            return
        owner_session_id, session_generation_id = identity
        membership = models.OwnerSessionJobMembership(
            owner_session_id=owner_session_id,
            session_generation_id=session_generation_id,
            job_id=job.job_id,
            cluster=job.cluster,
            state=job.state,
            created_at=job.created_at,
            updated_at=job.updated_at,
        )
        directory = self._owner_session_membership_dir(
            owner_session_id,
            session_generation_id=session_generation_id,
        )
        target = directory / f"{_stable_ref_token(job.job_id)}.json"
        if not target.exists():
            count, over_capacity = self._store_adapter.bounded_regular_json_count(
                directory,
                limit=queue_layout.MAX_ACTIVE_JOB_RECORDS,
                label="owner-session job membership",
            )
            if over_capacity or count >= queue_layout.MAX_ACTIVE_JOB_RECORDS:
                raise QueueConflictError(
                    "owner_session_job_capacity_reached: owner-session generation job "
                    f"capacity {queue_layout.MAX_ACTIVE_JOB_RECORDS} reached"
                )
        queue_store_write.write_model(self._storage_root, target, membership)

    def _owner_session_active_generation(self, owner_session_id: str) -> str | None:
        active = self._read_owner_session_transition_record(
            self._owner_session_active_path(owner_session_id)
        )
        return self._validate_owner_session_active_record(owner_session_id, active)

    @staticmethod
    def _validate_owner_session_active_record(
        owner_session_id: str,
        active: dict[str, object] | None,
    ) -> str | None:
        if active is None:
            return None
        generation = active.get("session_generation_id")
        previous_generation = active.get("previous_session_generation_id")
        if (
            active.get("owner_session_id") != owner_session_id
            or active.get("active") is not True
            or not isinstance(generation, str)
            or not queue_layout.safe_global_record_id(generation)
            or (
                previous_generation is not None
                and not queue_layout.safe_global_record_id(previous_generation)
            )
        ):
            raise QueueConflictError(f"owner session active record is invalid: {owner_session_id}")
        return generation

    @staticmethod
    def _validate_owner_session_closing_record(
        owner_session_id: str,
        closing: dict[str, object] | None,
    ) -> str | None:
        if closing is None:
            return None
        generation = closing.get("session_generation_id")
        if (
            closing.get("owner_session_id") != owner_session_id
            or closing.get("closing") is not True
            or not isinstance(generation, str)
            or not queue_layout.safe_global_record_id(generation)
        ):
            raise QueueConflictError(f"owner session closing record is invalid: {owner_session_id}")
        return generation

    @staticmethod
    def _validate_owner_session_cleanup_intent(
        owner_session_id: str,
        session_generation_id: str,
        raw_intent: object,
        *,
        required: bool,
    ) -> dict[str, object] | None:
        """Validate the immutable policy attached to one closing generation."""
        if raw_intent is None and not required:
            return None
        if not isinstance(raw_intent, dict):
            raise QueueConflictError(f"owner session cleanup intent is invalid: {owner_session_id}")
        intent = cast(dict[str, object], raw_intent)
        operation_id = intent.get("operation_id")
        created_at = intent.get("created_at")
        stop_worker = intent.get("stop_worker")
        cancel_jobs = intent.get("cancel_jobs")
        cancel_scheduler_jobs = intent.get("cancel_scheduler_jobs")
        if (
            intent.get("schema_version") != "clio-relay.owner-session-cleanup-intent.v1"
            or intent.get("owner_session_id") != owner_session_id
            or intent.get("session_generation_id") != session_generation_id
            or not isinstance(operation_id, str)
            or not operation_id.startswith("cleanup_")
            or not queue_layout.safe_global_record_id(operation_id)
            or not isinstance(created_at, str)
            or not isinstance(stop_worker, bool)
            or not isinstance(cancel_jobs, bool)
            or not isinstance(cancel_scheduler_jobs, bool)
            or (cancel_scheduler_jobs and not cancel_jobs)
        ):
            raise QueueConflictError(f"owner session cleanup intent is invalid: {owner_session_id}")
        try:
            parsed_created_at = datetime.fromisoformat(created_at)
        except ValueError as exc:
            raise QueueConflictError(
                f"owner session cleanup intent time is invalid: {owner_session_id}"
            ) from exc
        if parsed_created_at.tzinfo is None:
            raise QueueConflictError(
                f"owner session cleanup intent time is naive: {owner_session_id}"
            )
        return intent

    def _read_owner_session_transition_record(self, path: Path) -> dict[str, object] | None:
        try:
            raw = queue_store_read.read_json_document(path)
        except FileNotFoundError:
            return None
        if not isinstance(raw, dict):
            raise QueueConflictError(f"owner session transition record is invalid: {path}")
        return cast(dict[str, object], raw)

    def owner_session_is_closing(self, owner_session_id: str) -> bool:
        """Return whether new work is quiesced for an owned relay session."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        self._store_adapter.initialize()
        session_label = queue_layout.QueueLayout.label_key(owner_session_id, domain="owner-session")
        path = self._storage_root / "owner_sessions" / f"{session_label}.closing.json"
        try:
            payload = queue_store_read.read_json_document(path)
        except (FileNotFoundError, QueueConflictError, OSError):
            return False
        if not isinstance(payload, dict):
            return False
        document = cast(dict[str, object], payload)
        return (
            document.get("owner_session_id") == owner_session_id and document.get("closing") is True
        )  # noqa: E501
