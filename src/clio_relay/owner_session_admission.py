"""Admission boundary for gateway writes owned by a desktop relay session."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Generator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from typing import cast

from filelock import FileLock

from clio_relay.cluster_config import ClusterDefinition, default_registry_path
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import RelayError
from clio_relay.remote_cli import run_remote_clio, should_execute_on_cluster
from clio_relay.session_lifecycle import status_remote_session

SessionStatusReader = Callable[..., dict[str, object]]
AdmissionStatusReader = Callable[..., dict[str, object]]
TransitionLockFactory = Callable[..., AbstractContextManager[object]]
RemoteCliRunner = Callable[[ClusterDefinition, list[str]], str]


@dataclass(frozen=True, slots=True)
class OwnerSessionGatewayAdmission:
    """Exact owner-session identity authorizing one cluster-local gateway write."""

    owner_session_id: str
    owner_session_generation_id: str
    owner_session_admission_id: str


def owner_session_transition_lock(*, cluster: str, session_id: str) -> FileLock:
    """Serialize desktop orchestration transitions for one cluster/session pair."""
    directory = default_registry_path().parent / "session-transitions"
    directory.mkdir(parents=True, exist_ok=True)
    identity = hashlib.sha256(f"{cluster}\0{session_id}".encode()).hexdigest()
    return FileLock(str(directory / f"{identity}.lock"), timeout=60)


def desktop_owner_session_admission_id(*, cluster: str, session_id: str) -> str:
    """Return the cluster-scoped desktop admission key for one remote session."""
    identity = hashlib.sha256(f"{cluster}\0{session_id}".encode()).hexdigest()
    return f"desktop_{identity}"


def owner_session_admission_status(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    remote_execution: bool,
    session_id: str,
    session_generation_id: str,
    remote_cli_runner: RemoteCliRunner | None = None,
) -> dict[str, object]:
    """Read and strictly validate exact local or remote owner-session intake state."""
    if remote_execution:
        run_cli = remote_cli_runner or run_remote_clio
        raw_status = cast(
            object,
            json.loads(
                run_cli(
                    definition,
                    [
                        "session",
                        "admission-status",
                        "--session-id",
                        session_id,
                        "--session-generation-id",
                        session_generation_id,
                    ],
                )
            ),
        )
        if not isinstance(raw_status, dict):
            raise RelayError("remote owner-session admission status was not an object")
        status = {str(key): value for key, value in cast(dict[object, object], raw_status).items()}
    else:
        status = queue.owner_session_generation_status(
            session_id,
            session_generation_id=session_generation_id,
        )
    if (
        status.get("schema_version") != "clio-relay.owner-session-admission-status.v1"
        or status.get("owner_session_id") != session_id
        or status.get("session_generation_id") != session_generation_id
        or not all(
            isinstance(status.get(key), bool) for key in ("active", "closing", "closed", "open")
        )
    ):
        raise RelayError("owner-session admission status identity or schema did not match")
    raw_intent = status.get("cleanup_intent")
    if status.get("closing") is True:
        if not isinstance(raw_intent, dict):
            raise RelayError("closing owner-session admission status omitted cleanup intent")
        intent = cast(dict[str, object], raw_intent)
        operation_id = intent.get("operation_id")
        if (
            intent.get("schema_version") != "clio-relay.owner-session-cleanup-intent.v1"
            or intent.get("owner_session_id") != session_id
            or intent.get("session_generation_id") != session_generation_id
            or not isinstance(operation_id, str)
            or re.fullmatch(r"cleanup_[A-Za-z0-9_.-]+", operation_id) is None
            or not all(
                isinstance(intent.get(key), bool)
                for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
            )
        ):
            raise RelayError("owner-session admission cleanup intent was invalid")
    elif raw_intent is not None:
        raise RelayError("open owner-session admission status contained a cleanup intent")
    return status


def require_owner_session_admission_open(
    status: dict[str, object],
    *,
    session_id: str,
    session_generation_id: str,
) -> None:
    """Fail closed unless exact generation intake is authoritatively open."""
    if not (
        status.get("owner_session_id") == session_id
        and status.get("session_generation_id") == session_generation_id
        and status.get("active") is True
        and status.get("closing") is False
        and status.get("closed") is False
        and status.get("open") is True
        and status.get("active_generation_id") == session_generation_id
        and status.get("closing_generation_id") is None
    ):
        raise RelayError(
            "owned session generation is not open for gateway admission: "
            f"{session_id}:{session_generation_id}"
        )


def require_live_owner_session_for_gateway(
    status: dict[str, object],
    *,
    session_id: str,
    session_generation_id: str,
) -> None:
    """Require a live, owned, exact-generation API before gateway side effects."""
    if not (
        status.get("session_id") == session_id
        and status.get("owner") == "clio-relay"
        and status.get("session_generation_id") == session_generation_id
        and status.get("running") is True
        and status.get("ownership_verified") is True
    ):
        raise RelayError(
            "gateway admission requires a live owned session with the exact generation: "
            f"{session_id}:{session_generation_id}"
        )


def assert_no_unscoped_desktop_admission_state(
    queue: ClioCoreQueue,
    *,
    cluster: str,
    session_id: str,
    session_generation_id: str,
) -> None:
    """Fail closed when legacy desktop state cannot be attributed to one cluster."""
    legacy = queue.owner_session_generation_status(
        session_id,
        session_generation_id=session_generation_id,
    )
    if (
        legacy.get("active_generation_id") is not None
        or legacy.get("closing_generation_id") is not None
        or legacy.get("closed") is True
    ):
        raise RelayError(
            "legacy unscoped desktop owner-session admission state cannot be safely assigned "
            f"to cluster {cluster!r} for session {session_id!r}; clean or migrate it before "
            "cluster-scoped admission"
        )


@contextmanager
def owner_session_gateway_admission(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
    session_generation_id: str,
    transition_lock_factory: TransitionLockFactory | None = None,
    session_status_reader: SessionStatusReader | None = None,
    admission_status_reader: AdmissionStatusReader | None = None,
) -> Generator[OwnerSessionGatewayAdmission, None, None]:
    """Authorize one owned gateway operation and lock through all its side effects.

    The authoritative process and intake state are checked once before creating
    the desktop mirror and again immediately before the caller receives control.
    Keeping the caller inside this context keeps the same transition lock held
    through durable gateway writes, connector setup, and any rollback they need.
    """
    if definition.name != cluster:
        raise RelayError("owner-session gateway admission cluster definition did not match")
    lock_factory = transition_lock_factory or owner_session_transition_lock
    read_session_status = session_status_reader or status_remote_session
    read_admission_status = admission_status_reader or owner_session_admission_status
    remote_execution = should_execute_on_cluster(definition)
    local_admission_id = desktop_owner_session_admission_id(
        cluster=cluster,
        session_id=session_id,
    )

    with lock_factory(cluster=cluster, session_id=session_id):
        if remote_execution:
            assert_no_unscoped_desktop_admission_state(
                queue,
                cluster=cluster,
                session_id=session_id,
                session_generation_id=session_generation_id,
            )
        _verify_authoritative_gateway_admission(
            queue=queue,
            definition=definition,
            remote_execution=remote_execution,
            session_id=session_id,
            session_generation_id=session_generation_id,
            session_status_reader=read_session_status,
            admission_status_reader=read_admission_status,
        )
        local_status_before_mirror = queue.owner_session_generation_status(
            local_admission_id,
            session_generation_id=session_generation_id,
        )
        local_mirror_preexisting = (
            local_status_before_mirror.get("active_generation_id") == session_generation_id
            and local_status_before_mirror.get("open") is True
        )
        local_admission = queue.mirror_owner_session_generation_open(
            local_admission_id,
            session_generation_id=session_generation_id,
        )
        require_owner_session_admission_open(
            local_admission,
            session_id=local_admission_id,
            session_generation_id=session_generation_id,
        )

        # A teardown may have won before the mirror write. Re-read the remote
        # authority before exposing the admission to any connector side effect.
        authoritative_status: dict[str, object] | None = None
        try:
            authoritative_status = _read_authoritative_gateway_admission(
                queue=queue,
                definition=definition,
                remote_execution=remote_execution,
                session_id=session_id,
                session_generation_id=session_generation_id,
                session_status_reader=read_session_status,
                admission_status_reader=read_admission_status,
            )
            require_owner_session_admission_open(
                authoritative_status,
                session_id=session_id,
                session_generation_id=session_generation_id,
            )
        except RelayError as admission_error:
            if not local_mirror_preexisting:
                if authoritative_status is None:
                    try:
                        authoritative_status = read_admission_status(
                            queue=queue,
                            definition=definition,
                            remote_execution=remote_execution,
                            session_id=session_id,
                            session_generation_id=session_generation_id,
                        )
                    except (RelayError, ValueError):
                        authoritative_status = None
                if authoritative_status is not None:
                    try:
                        _reconcile_new_local_mirror_after_authoritative_close(
                            queue=queue,
                            local_admission_id=local_admission_id,
                            session_id=session_id,
                            session_generation_id=session_generation_id,
                            authoritative_status=authoritative_status,
                        )
                    except (RelayError, ValueError) as reconciliation_error:
                        raise RelayError(
                            f"{admission_error}; newly created local admission mirror "
                            f"could not be reconciled: {reconciliation_error}"
                        ) from reconciliation_error
            raise
        yield OwnerSessionGatewayAdmission(
            owner_session_id=session_id,
            owner_session_generation_id=session_generation_id,
            owner_session_admission_id=local_admission_id,
        )


def _verify_authoritative_gateway_admission(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    remote_execution: bool,
    session_id: str,
    session_generation_id: str,
    session_status_reader: SessionStatusReader,
    admission_status_reader: AdmissionStatusReader,
) -> None:
    """Verify exact live process identity and authoritative intake state together."""
    authoritative_admission = _read_authoritative_gateway_admission(
        queue=queue,
        definition=definition,
        remote_execution=remote_execution,
        session_id=session_id,
        session_generation_id=session_generation_id,
        session_status_reader=session_status_reader,
        admission_status_reader=admission_status_reader,
    )
    require_owner_session_admission_open(
        authoritative_admission,
        session_id=session_id,
        session_generation_id=session_generation_id,
    )


def _read_authoritative_gateway_admission(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    remote_execution: bool,
    session_id: str,
    session_generation_id: str,
    session_status_reader: SessionStatusReader,
    admission_status_reader: AdmissionStatusReader,
) -> dict[str, object]:
    """Read exact process and intake evidence without discarding denied status."""
    process_status = session_status_reader(
        definition=definition,
        session_id=session_id,
    )
    require_live_owner_session_for_gateway(
        process_status,
        session_id=session_id,
        session_generation_id=session_generation_id,
    )
    return admission_status_reader(
        queue=queue,
        definition=definition,
        remote_execution=remote_execution,
        session_id=session_id,
        session_generation_id=session_generation_id,
    )


def _reconcile_new_local_mirror_after_authoritative_close(
    *,
    queue: ClioCoreQueue,
    local_admission_id: str,
    session_id: str,
    session_generation_id: str,
    authoritative_status: dict[str, object],
) -> None:
    """Close a mirror created by this attempt when remote closure is proven."""
    if (
        authoritative_status.get("owner_session_id") != session_id
        or authoritative_status.get("session_generation_id") != session_generation_id
        or authoritative_status.get("open") is not False
    ):
        return
    raw_intent = authoritative_status.get("cleanup_intent")
    if authoritative_status.get("closing") is True:
        if not isinstance(raw_intent, dict):
            return
        intent = cast(dict[str, object], raw_intent)
        operation_id = intent.get("operation_id")
        if (
            intent.get("schema_version") != "clio-relay.owner-session-cleanup-intent.v1"
            or intent.get("owner_session_id") != session_id
            or intent.get("session_generation_id") != session_generation_id
            or not isinstance(operation_id, str)
            or re.fullmatch(r"cleanup_[A-Za-z0-9_.-]+", operation_id) is None
            or not all(
                isinstance(intent.get(key), bool)
                for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
            )
        ):
            return
        stop_worker = cast(bool, intent["stop_worker"])
        cancel_jobs = cast(bool, intent["cancel_jobs"])
        cancel_scheduler_jobs = cast(bool, intent["cancel_scheduler_jobs"])
    elif authoritative_status.get("closed") is True:
        identity = hashlib.sha256(
            f"{local_admission_id}\0{session_generation_id}".encode()
        ).hexdigest()
        operation_id = f"cleanup_admission_reconcile_{identity}"
        stop_worker = False
        cancel_jobs = False
        cancel_scheduler_jobs = False
    else:
        return
    queue.set_owner_session_closing(
        local_admission_id,
        session_generation_id=session_generation_id,
        operation_id=operation_id,
        stop_worker=stop_worker,
        cancel_jobs=cancel_jobs,
        cancel_scheduler_jobs=cancel_scheduler_jobs,
    )
    queue.set_owner_session_closed(
        local_admission_id,
        session_generation_id=session_generation_id,
        residual_resource_ids=[],
    )
