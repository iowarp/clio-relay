from __future__ import annotations

import importlib
import json
import os
import stat
import subprocess
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Literal, cast

import pytest
from pytest import MonkeyPatch

import clio_relay.session_lifecycle as session_lifecycle
from clio_relay import __version__
from clio_relay.cluster_config import (
    MAX_CLUSTER_REGISTRY_BYTES,
    ClusterDefinition,
    ClusterRegistry,
    RemoteMcpServerConfig,
)
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import RelayError
from clio_relay.session_lifecycle import (
    SESSION_CONNECTORS_CHECK_ID,
    SESSION_GATEWAY_CHECK_ID,
    SESSION_SCHEDULER_CANCELED_CHECK_ID,
    SESSION_WORKER_CHECK_ID,
    CleanupResource,
    OwnedSessionCleanupFinalizeRequest,
    OwnedSessionCleanupReportReadRequest,
    OwnedSessionCleanupTarget,
    OwnedSessionRecoveryStatus,
    OwnedSessionStartPlan,
    OwnedSessionStartRequest,
    RemoteSessionStateEvidence,
    SessionApiReleaseIdentity,
    SessionLifecycleReport,
    challenge_remote_session_identity,
    detach_remote_session,
    execute_owned_session_cleanup_finalize,
    execute_owned_session_cleanup_report_read,
    execute_owned_session_start,
    inspect_owned_session_recovery_status,
    session_lifecycle_report_sha256,
    start_remote_session,
    status_remote_session,
    teardown_remote_session,
)


def _api_release_identity() -> SessionApiReleaseIdentity:
    return SessionApiReleaseIdentity.model_validate(
        {
            "distribution_version": __version__,
            "artifact_sha256": "a" * 64,
            "software": {
                "version": __version__,
                "commit": "1" * 40,
                "tag": f"v{__version__}",
                "dirty": False,
            },
        }
    )


class _FakeSessionTransaction:
    """Small filesystem-backed transaction seam for platform-neutral lifecycle tests."""

    def __init__(self, path: Path, *, session_id: str = "session-start") -> None:
        self.path = path
        self.session_id = session_id
        self.path.mkdir(parents=True, exist_ok=True)

    def __enter__(self) -> _FakeSessionTransaction:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read_json(
        self,
        name: str,
        *,
        required: bool = True,
    ) -> dict[str, object] | None:
        path = self.path / name
        if not path.exists():
            if required:
                raise AssertionError(f"missing required test document: {name}")
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
        assert isinstance(value, dict)
        return cast(dict[str, object], value)

    def atomic_write(
        self,
        name: str,
        payload: bytes,
        *,
        maximum_bytes: int = 1024 * 1024,
    ) -> None:
        assert len(payload) <= maximum_bytes
        (self.path / name).write_bytes(payload)

    def atomic_write_immutable(
        self,
        name: str,
        payload: bytes,
        *,
        maximum_bytes: int,
    ) -> None:
        assert len(payload) <= maximum_bytes
        path = self.path / name
        if path.exists():
            if path.read_bytes() != payload:
                raise RelayError(f"owned session immutable file already differs: {name}")
            return
        path.write_bytes(payload)

    def cleanup_report_candidate_names(self) -> list[str]:
        return sorted(
            path.name
            for path in self.path.iterdir()
            if path.name.startswith("coordinator-cleanup-report-")
            or path.name.startswith(".coordinator-cleanup-report-")
        )

    def stat_regular(self, name: str, *, required: bool = True) -> os.stat_result | None:
        path = self.path / name
        if not path.exists():
            if required:
                raise AssertionError(f"missing required test file: {name}")
            return None
        return path.stat()

    def read_bytes(
        self,
        name: str,
        *,
        maximum_bytes: int,
        required: bool = True,
    ) -> bytes | None:
        path = self.path / name
        try:
            linked = path.lstat()
        except FileNotFoundError:
            if required:
                raise AssertionError(f"missing required test file: {name}") from None
            return None
        if not stat.S_ISREG(linked.st_mode):
            raise RelayError(f"owned session file is not one owner-private regular file: {name}")
        payload = path.read_bytes()
        if len(payload) > maximum_bytes:
            raise AssertionError("test transaction observed an unexpected bounded read")
        return payload

    def open_output(self, name: str) -> int:
        """Open one test-owned output file like the pinned production transaction."""
        return os.open(self.path / name, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)

    def unlink_verified(
        self,
        name: str,
        *,
        expected_device: int,
        expected_inode: int,
        expected_size: int,
        expected_sha256: str | None,
        maximum_bytes: int | None,
    ) -> bool:
        path = self.path / name
        observed = path.stat()
        assert (observed.st_dev, observed.st_ino, observed.st_size) == (
            expected_device,
            expected_inode,
            expected_size,
        )
        assert expected_sha256 is None
        assert maximum_bytes is None
        path.unlink()
        return True


def _fake_transaction_opener(
    transaction: _FakeSessionTransaction,
) -> Callable[..., _FakeSessionTransaction]:
    """Return a fully typed replacement for the owned-session transaction factory."""

    def open_transaction(
        *,
        session_id: str,
        create: bool,
        timeout_seconds: float,
        home: Path | None = None,
    ) -> _FakeSessionTransaction:
        del session_id, create, timeout_seconds, home
        return transaction

    return open_transaction


def _ignore_remote_port(_port: int) -> None:
    """Represent an available port without introducing an unknown lambda parameter."""


def _fixed_process_start_identity(_pid: int) -> str:
    """Return the process identity used by replacement-session tests."""

    return "linux-proc:7000"


def _successful_process_wait(**_kwargs: object) -> int:
    """Represent a test process that exits successfully when waited on."""

    return 0


def _fixed_api_readiness(elapsed_seconds: float) -> Callable[..., float]:
    """Return a typed API-readiness replacement with a fixed elapsed time."""

    def ready(
        *,
        process: subprocess.Popen[bytes],
        port: int,
        require_token: bool,
    ) -> float:
        del process, port, require_token
        return elapsed_seconds

    return ready


@pytest.fixture(autouse=True)
def use_fake_recorded_scope(monkeypatch: MonkeyPatch) -> None:
    """Represent exact cgroup membership without consulting the host's systemd."""

    def recorded_scope_processes(
        *,
        proc_root: Path,
        systemd_unit: str,
        systemd_cgroup_path: str,
        systemd_invocation_id: str,
        systemd_description: str,
    ) -> list[object]:
        assert systemd_unit.startswith("clio-relay-session-")
        assert systemd_unit.endswith(".scope")
        assert systemd_cgroup_path
        assert len(systemd_invocation_id) == 32
        assert systemd_description.startswith("clio-relay-owned-session:")
        membership = proc_root / "scope.procs"
        process_ids = (
            [int(line) for line in membership.read_text(encoding="ascii").splitlines()]
            if membership.exists()
            else []
        )
        processes: list[object] = []
        for process_id in process_ids:
            try:
                processes.append(
                    session_lifecycle._read_proc_identity(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                        proc_root=proc_root,
                        pid=process_id,
                    )
                )
            except (FileNotFoundError, ProcessLookupError):
                continue
        return processes

    monkeypatch.setattr(
        session_lifecycle,
        "_recorded_scope_processes",
        recorded_scope_processes,
    )


def _owned_session_recovery_fixture(
    root: Path,
    *,
    session_id: str = "session-1",
    generation_id: str = "generation-1",
    pid: int = 4321,
) -> tuple[Path, Path, Path, ClioCoreQueue]:
    home = root / "home"
    session_dir = home / ".local" / "share" / "clio-relay" / "sessions" / session_id
    session_dir.mkdir(parents=True)
    (session_dir / "transition.lock").write_text("", encoding="utf-8")
    definition = ClusterDefinition(name="ares", ssh_host="ares")
    registry_bytes = ClusterRegistry(clusters={"ares": definition}).model_dump_json().encode()
    registry_path = session_dir / f"cluster-registry-{generation_id}.json"
    registry_path.write_bytes(registry_bytes)
    release = _api_release_identity()
    owner_token = "b" * 64
    systemd_unit = f"clio-relay-session-{generation_id}.scope"
    systemd_cgroup_path = f"/sys/fs/cgroup/user.slice/{systemd_unit}"
    systemd_invocation_id = "1" * 32
    systemd_description = f"clio-relay-owned-session:{session_id}:{generation_id}:{'2' * 32}"
    receipt_path = session_dir / f"api-startup-{generation_id}.json"
    receipt: dict[str, object] = {
        "schema_version": "clio-relay.owner-session-api-startup.v1",
        "cluster": "ares",
        "session_id": session_id,
        "session_generation_id": generation_id,
        "api_pid": pid,
        "api_pgid": pid,
        "process_start_ticks": "123456",
        "api_release_identity_sha256": release.sha256(),
        "cluster_registry_path": str(registry_path),
        "cluster_registry_sha256": session_lifecycle.hashlib.sha256(registry_bytes).hexdigest(),
        "cluster_route_revision": session_lifecycle.cluster_route_revision(definition),
        "systemd_unit": systemd_unit,
        "systemd_cgroup_path": systemd_cgroup_path,
        "systemd_invocation_id": systemd_invocation_id,
        "systemd_description": systemd_description,
        "observed_at": datetime.now(UTC).isoformat(),
    }
    receipt["hmac_sha256"] = session_lifecycle._startup_receipt_signature(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        receipt,
        owner_token=owner_token,
    )
    receipt_payload = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    receipt_path.write_bytes(receipt_payload)
    metadata = {
        "cluster": "ares",
        "session_id": session_id,
        "remote_api_port": 8765,
        "api_pid": pid,
        "api_pgid": pid,
        "owner_token": owner_token,
        "session_generation_id": generation_id,
        "api_release_identity": release.model_dump(mode="json"),
        "api_release_identity_sha256": release.sha256(),
        "cluster_registry_path": str(registry_path),
        "cluster_registry_sha256": session_lifecycle.hashlib.sha256(registry_bytes).hexdigest(),
        "cluster_route_revision": session_lifecycle.cluster_route_revision(definition),
        "cluster_authority_verified": True,
        "process_start_ticks": "123456",
        "containment_mode": "linux_systemd_scope",
        "systemd_unit": systemd_unit,
        "systemd_cgroup_path": systemd_cgroup_path,
        "systemd_invocation_id": systemd_invocation_id,
        "systemd_description": systemd_description,
        "containment_broker_pid": pid + 1,
        "containment_broker_start_identity": "linux-proc:654321",
        "api_startup_receipt_path": str(receipt_path),
        "api_startup_receipt_sha256": session_lifecycle.hashlib.sha256(receipt_payload).hexdigest(),
        "started_at": datetime.now(UTC).isoformat(),
        "owner": "clio-relay",
    }
    (session_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    proc_root = root / "proc"
    proc_root.mkdir()
    (proc_root / "scope.procs").write_text("", encoding="ascii")
    queue = ClioCoreQueue(root / "core")
    assert (
        queue.prepare_owner_session_start(
            session_id,
            recorded_generation_id=None,
            candidate_generation_id=generation_id,
        )
        == generation_id
    )
    return home, session_dir, proc_root, queue


def _owned_session_start_request() -> OwnedSessionStartRequest:
    definition = ClusterDefinition(name="ares", ssh_host="ares")
    registry = ClusterRegistry(clusters={"ares": definition})
    payload = json.dumps(
        registry.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return OwnedSessionStartRequest(
        cluster="ares",
        session_id="session-start",
        start_operation_id="start_test",
        remote_api_port=18765,
        require_token=False,
        cluster_registry=registry.model_dump(mode="json"),
        cluster_registry_sha256=session_lifecycle.hashlib.sha256(payload).hexdigest(),
        cluster_route_revision=session_lifecycle.cluster_route_revision(definition),
    )


def _legacy_existing_start_fixture(
    root: Path,
) -> tuple[
    OwnedSessionStartRequest,
    _FakeSessionTransaction,
    dict[str, object],
    SessionApiReleaseIdentity,
    ClioCoreQueue,
]:
    """Create one internally exact v1 journal and old-release metadata pair."""
    current_release = _api_release_identity()
    old_release = current_release.model_copy(update={"artifact_sha256": "f" * 64})
    request = _owned_session_start_request().model_copy(
        update={
            "start_operation_id": "start_replace_old_release",
            "replace": True,
            "expected_api_release_identity": current_release,
        }
    )
    transaction = _FakeSessionTransaction(
        root / "home" / ".local" / "share" / "clio-relay" / "sessions" / request.session_id,
        session_id=request.session_id,
    )
    generation = "generation-legacy"
    owner_token = "b" * 64
    registry_path = transaction.path / f"cluster-registry-{generation}.json"
    registry_payload = json.dumps(
        request.cluster_registry,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    registry_path.write_bytes(registry_payload)
    systemd_unit = f"clio-relay-session-{generation}.scope"
    systemd_description = f"clio-relay-owned-session:{request.session_id}:{generation}:{'2' * 32}"
    metadata: dict[str, object] = {
        "cluster": request.cluster,
        "session_id": request.session_id,
        "remote_api_port": request.remote_api_port,
        "api_pid": 4321,
        "api_pgid": 4321,
        "owner_token": owner_token,
        "session_generation_id": generation,
        "api_release_identity": old_release.model_dump(mode="json"),
        "api_release_identity_sha256": old_release.sha256(),
        "cluster_registry_path": str(registry_path),
        "cluster_registry_sha256": request.cluster_registry_sha256,
        "cluster_route_revision": request.cluster_route_revision,
        "cluster_authority_verified": True,
        "process_start_ticks": "123456",
        "containment_mode": "linux_systemd_scope",
        "systemd_unit": systemd_unit,
        "systemd_cgroup_path": f"/sys/fs/cgroup/user.slice/{systemd_unit}",
        "systemd_invocation_id": "1" * 32,
        "systemd_description": systemd_description,
        "containment_broker_pid": 4322,
        "containment_broker_start_identity": "linux-proc:654321",
        "api_startup_receipt_path": str(transaction.path / f"api-startup-{generation}.json"),
        "api_startup_receipt_sha256": "3" * 64,
        "started_at": datetime.now(UTC).isoformat(),
        "owner": "clio-relay",
    }
    transaction.atomic_write("metadata.json", json.dumps(metadata).encode())
    legacy = {
        "schema_version": "clio-relay.owner-session-attempt.v1",
        "operation": "start",
        "cluster": request.cluster,
        "session_id": request.session_id,
        "session_generation_id": generation,
        "owner_token": owner_token,
        "owner_token_sha256": session_lifecycle.hashlib.sha256(owner_token.encode()).hexdigest(),
        "api_release_identity_sha256": old_release.sha256(),
        "cluster_registry_path": str(registry_path),
        "cluster_registry_sha256": request.cluster_registry_sha256,
        "cluster_route_revision": request.cluster_route_revision,
        "remote_api_port": request.remote_api_port,
        "start_phase": "contained",
        "systemd_unit": systemd_unit,
        "systemd_description": systemd_description,
        "systemd_cgroup_path": metadata["systemd_cgroup_path"],
        "systemd_invocation_id": metadata["systemd_invocation_id"],
        "containment_broker_pid": metadata["containment_broker_pid"],
        "containment_broker_start_identity": metadata["containment_broker_start_identity"],
        "observed_at": datetime.now(UTC).isoformat(),
        "error": None,
    }
    transaction.atomic_write(
        "start-attempt.json",
        json.dumps(legacy, sort_keys=True, separators=(",", ":")).encode(),
    )
    queue = ClioCoreQueue(root / "core")
    assert (
        queue.prepare_owner_session_start(
            request.session_id,
            recorded_generation_id=None,
            candidate_generation_id=generation,
        )
        == generation
    )
    return request, transaction, metadata, old_release, queue


def _legacy_existing_status(
    *,
    request: OwnedSessionStartRequest,
    metadata: dict[str, object],
    old_release: SessionApiReleaseIdentity,
    journal_bound: bool,
) -> OwnedSessionRecoveryStatus:
    """Return verified old-generation evidence for executor boundary tests."""
    return OwnedSessionRecoveryStatus(
        cluster=request.cluster,
        session_id=request.session_id,
        session_generation_id=cast(str, metadata["session_generation_id"]),
        start_operation_id=request.start_operation_id if journal_bound else None,
        cluster_route_revision=request.cluster_route_revision,
        api_pid=cast(int, metadata["api_pid"]),
        remote_api_port=request.remote_api_port,
        leader_process_state="absent",
        process_state="absent",
        running=False,
        process_absence_verified=True,
        generation_process_absence_verified=True,
        metadata_verified=True,
        cluster_registry_verified=True,
        durable_generation_verified=True,
        ownership_verified=True,
        recovery_verified=True,
        api_release_identity=old_release,
        api_release_identity_verified=True,
        start_state="ready" if journal_bound else "unknown",
        start_phase="contained" if journal_bound else None,
        start_attempt_verified=journal_bound,
        start_replace=request.replace if journal_bound else None,
        start_require_token=request.require_token if journal_bound else None,
        start_expected_api_release_identity_sha256=(
            request.expected_api_release_identity.sha256()
            if journal_bound and request.expected_api_release_identity is not None
            else None
        ),
    )


def _durable_start_plan() -> tuple[
    ClusterDefinition,
    SessionApiReleaseIdentity,
    OwnedSessionStartPlan,
]:
    definition = ClusterDefinition(name="ares", ssh_host="ares")
    release = _api_release_identity()
    plan = session_lifecycle.plan_remote_session_start(
        cluster="ares",
        definition=definition,
        session_id="session-start",
        remote_api_port=18765,
        replace=False,
        require_token=False,
        start_operation_id="start_test",
        expected_api_release_identity_sha256=release.sha256(),
    )
    return definition, release, plan


def _durable_start_status(
    plan: OwnedSessionStartPlan,
    *,
    state: Literal["starting", "ready", "failed", "not_current"] = "starting",
) -> OwnedSessionRecoveryStatus:
    generation = None if state == "not_current" else "generation-start"
    verified = state != "not_current"
    ready = state == "ready"
    return OwnedSessionRecoveryStatus(
        cluster=plan.cluster,
        session_id=plan.session_id,
        session_generation_id=generation,
        start_operation_id=plan.start_operation_id,
        cluster_route_revision=plan.cluster_route_revision,
        remote_api_port=plan.remote_api_port if verified else None,
        leader_process_state="owned_running" if ready else "absent",
        running=ready,
        ownership_verified=ready,
        recovery_verified=ready,
        start_state=state,
        start_phase="contained" if ready else ("admitted" if verified else None),
        start_attempt_verified=verified,
        start_retryable=state == "starting",
        start_replace=plan.retry_selector.replace if verified else None,
        start_require_token=plan.retry_selector.require_token if verified else None,
        start_expected_api_release_identity_sha256=(
            plan.expected_api_release_identity_sha256 if verified else None
        ),
        start_error="remote start failed" if state == "failed" else None,
        errors=(
            ["another operation owns the current transition"] if state == "not_current" else []
        ),
    )


def _write_owned_generation_process(
    *,
    proc_root: Path,
    metadata: dict[str, object],
    pid: int,
    command: bytes,
    start_ticks: str = "654321",
) -> None:
    pid_dir = proc_root / str(pid)
    pid_dir.mkdir()
    fields = ["S", "0", str(pid), *("0" for _ in range(16)), start_ticks]
    (pid_dir / "stat").write_text(
        f"{pid} (owned-child) {' '.join(fields)}",
        encoding="utf-8",
    )
    markers = [
        f"CLIO_RELAY_SESSION_OWNER_TOKEN={metadata['owner_token']}",
        f"CLIO_RELAY_SESSION_GENERATION_ID={metadata['session_generation_id']}",
        f"CLIO_RELAY_OWNER_SESSION_ID={metadata['session_id']}",
        f"CLIO_RELAY_OWNER_SESSION_CLUSTER={metadata['cluster']}",
        f"CLIO_RELAY_REMOTE_CLUSTER={metadata['cluster']}",
        (f"CLIO_RELAY_API_RELEASE_IDENTITY_SHA256={metadata['api_release_identity_sha256']}"),
        f"CLIO_RELAY_CLUSTER_REGISTRY={metadata['cluster_registry_path']}",
        f"CLIO_RELAY_SESSION_REGISTRY_SHA256={metadata['cluster_registry_sha256']}",
        f"CLIO_RELAY_SESSION_ROUTE_REVISION={metadata['cluster_route_revision']}",
    ]
    (pid_dir / "environ").write_bytes("\0".join(markers).encode() + b"\0")
    (pid_dir / "cmdline").write_bytes(command)
    with (proc_root / "scope.procs").open("a", encoding="ascii") as membership:
        membership.write(f"{pid}\n")


def test_dead_owned_session_recovery_requires_metadata_registry_and_core(
    tmp_path: Path,
) -> None:
    home, _session_dir, proc_root, queue = _owned_session_recovery_fixture(tmp_path)

    status = inspect_owned_session_recovery_status(
        cluster="ares",
        session_id="session-1",
        core_dir=queue.root,
        home=home,
        proc_root=proc_root,
    )

    assert status.recovery_verified is True
    assert status.metadata_verified is True
    assert status.cluster_registry_verified is True
    assert status.durable_generation_verified is True
    assert status.process_state == "absent"
    assert status.process_absence_verified is True
    assert status.running is False
    assert status.ownership_verified is True
    assert status.errors == []


def test_owned_session_recovery_accepts_canonical_home_transaction(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Accept a pinned transaction when HOME is an alias of its canonical path."""
    home, session_dir, proc_root, queue = _owned_session_recovery_fixture(tmp_path)
    home_alias = tmp_path / "home-alias"
    original_resolve = Path.resolve

    def resolve_home_alias(path: Path, strict: bool = False) -> Path:
        if path == home_alias:
            return home
        return original_resolve(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", resolve_home_alias)
    transaction = _FakeSessionTransaction(session_dir, session_id="session-1")

    status = inspect_owned_session_recovery_status(
        cluster="ares",
        session_id="session-1",
        core_dir=queue.root,
        home=home_alias,
        proc_root=proc_root,
        transaction=cast(session_lifecycle._OwnedSessionTransaction, transaction),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    )

    assert status.metadata_verified is True
    assert status.api_pid == 4321
    assert status.recovery_verified is True
    assert status.errors == []


def test_start_persists_candidate_before_core_admission(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    request = _owned_session_start_request()
    monkeypatch.setattr(
        session_lifecycle,
        "_current_session_api_release_identity",
        _api_release_identity,
    )
    monkeypatch.setattr(session_lifecycle, "_assert_remote_port_available", _ignore_remote_port)

    def effective_user_id() -> int:
        return tmp_path.stat().st_uid

    monkeypatch.setattr(os, "geteuid", effective_user_id, raising=False)
    transaction = _FakeSessionTransaction(
        tmp_path / "home" / ".local" / "share" / "clio-relay" / "sessions" / request.session_id,
        session_id=request.session_id,
    )
    monkeypatch.setattr(
        session_lifecycle,
        "open_owned_session_transaction",
        _fake_transaction_opener(transaction),
    )

    def crash_before_admission(*_args: object, **_kwargs: object) -> str:
        raise RuntimeError("simulated admission crash")

    monkeypatch.setattr(
        ClioCoreQueue,
        "prepare_owner_session_start",
        crash_before_admission,
    )

    with pytest.raises(RuntimeError, match="simulated admission crash"):
        execute_owned_session_start(
            request,
            home=tmp_path / "home",
            core_dir=tmp_path / "core",
            proc_root=tmp_path / "proc",
        )

    attempt_path = (
        tmp_path
        / "home"
        / ".local"
        / "share"
        / "clio-relay"
        / "sessions"
        / request.session_id
        / "start-attempt.json"
    )
    attempt = json.loads(attempt_path.read_text(encoding="utf-8"))
    assert attempt["start_phase"] == "pending"
    assert attempt["session_generation_id"]
    assert attempt["systemd_unit"] == (
        f"clio-relay-session-{attempt['session_generation_id']}.scope"
    )
    assert (
        attempt["owner_token_sha256"]
        == session_lifecycle.hashlib.sha256(attempt["owner_token"].encode()).hexdigest()
    )


@pytest.mark.parametrize(
    ("phase", "cgroup_path", "invocation_id", "broker_pid", "broker_start"),
    [
        ("pending", None, None, None, None),
        ("admitted", None, None, None, None),
        ("scope_bound", "/sys/fs/cgroup/test.scope", "1" * 32, None, None),
        ("contained", "/sys/fs/cgroup/test.scope", "1" * 32, 4322, "linux-proc:1"),
    ],
)
def test_start_attempt_accepts_every_durable_crash_boundary(
    tmp_path: Path,
    phase: str,
    cgroup_path: str | None,
    invocation_id: str | None,
    broker_pid: int | None,
    broker_start: str | None,
) -> None:
    request = _owned_session_start_request()
    transaction = _FakeSessionTransaction(
        tmp_path / "session",
        session_id=request.session_id,
    )
    with transaction:
        generation = "generation-crash"
        token = "a" * 64
        identity: dict[str, object] = {
            "cluster": request.cluster,
            "session_id": request.session_id,
            "start_operation_id": request.start_operation_id,
            "session_generation_id": generation,
            "owner_token": token,
            "owner_token_sha256": session_lifecycle.hashlib.sha256(token.encode()).hexdigest(),
            "api_release_identity_sha256": "b" * 64,
            "expected_api_release_identity_sha256": None,
            "cluster_registry_path": str(transaction.path / f"cluster-registry-{generation}.json"),
            "cluster_registry_sha256": request.cluster_registry_sha256,
            "cluster_route_revision": request.cluster_route_revision,
            "remote_api_port": request.remote_api_port,
            "replace": request.replace,
            "require_token": request.require_token,
            "start_phase": phase,
            "systemd_unit": f"clio-relay-session-{generation}.scope",
            "systemd_description": (
                f"clio-relay-owned-session:{request.session_id}:{generation}:{'2' * 32}"
            ),
            "systemd_cgroup_path": cgroup_path,
            "systemd_invocation_id": invocation_id,
            "containment_broker_pid": broker_pid,
            "containment_broker_start_identity": broker_start,
        }
        session_lifecycle._write_session_attempt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cast(session_lifecycle._OwnedSessionTransaction, transaction),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            operation="start",
            identity=identity,
        )

        recovered = session_lifecycle._validated_resumable_start_attempt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cast(session_lifecycle._OwnedSessionTransaction, transaction),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            request=request,
            release_identity_sha256="b" * 64,
        )

    assert recovered is not None
    assert recovered["start_phase"] == phase


def test_distinct_operation_cannot_overwrite_nonterminal_start_transition(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    original_request = _owned_session_start_request().model_copy(
        update={"start_operation_id": "start_original"}
    )
    retry_request = original_request.model_copy(update={"start_operation_id": "start_distinct"})
    transaction = _FakeSessionTransaction(
        tmp_path / "home" / ".local" / "share" / "clio-relay" / "sessions" / "session-start",
        session_id=original_request.session_id,
    )
    generation = "generation-pending"
    token = "a" * 64
    identity: dict[str, object] = {
        "cluster": original_request.cluster,
        "session_id": original_request.session_id,
        "start_operation_id": original_request.start_operation_id,
        "session_generation_id": generation,
        "owner_token": token,
        "owner_token_sha256": session_lifecycle.hashlib.sha256(token.encode()).hexdigest(),
        "api_release_identity_sha256": _api_release_identity().sha256(),
        "expected_api_release_identity_sha256": None,
        "cluster_registry_path": str(transaction.path / f"cluster-registry-{generation}.json"),
        "cluster_registry_sha256": original_request.cluster_registry_sha256,
        "cluster_route_revision": original_request.cluster_route_revision,
        "remote_api_port": original_request.remote_api_port,
        "replace": original_request.replace,
        "require_token": original_request.require_token,
        "start_phase": "pending",
        "systemd_unit": f"clio-relay-session-{generation}.scope",
        "systemd_description": (
            f"clio-relay-owned-session:{original_request.session_id}:{generation}:{'2' * 32}"
        ),
        "systemd_cgroup_path": None,
        "systemd_invocation_id": None,
        "containment_broker_pid": None,
        "containment_broker_start_identity": None,
    }
    session_lifecycle._write_session_attempt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cast(session_lifecycle._OwnedSessionTransaction, transaction),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        operation="start",
        identity=identity,
    )
    before = (transaction.path / "start-attempt.json").read_bytes()
    monkeypatch.setattr(
        session_lifecycle,
        "open_owned_session_transaction",
        _fake_transaction_opener(transaction),
    )
    monkeypatch.setattr(
        session_lifecycle,
        "_current_session_api_release_identity",
        _api_release_identity,
    )
    monkeypatch.setattr(os, "geteuid", lambda: 0, raising=False)
    mutation_attempted = False

    def refuse_mutation(*_args: object, **_kwargs: object) -> None:
        nonlocal mutation_attempted
        mutation_attempted = True
        raise AssertionError("distinct operation reached start mutation")

    monkeypatch.setattr(session_lifecycle, "_assert_remote_port_available", refuse_mutation)
    monkeypatch.setattr(session_lifecycle, "_terminate_recorded_session_scope", refuse_mutation)

    with pytest.raises(RelayError, match="prior owned-session start attempt identity is invalid"):
        execute_owned_session_start(
            retry_request,
            home=tmp_path / "home",
            core_dir=tmp_path / "core",
            proc_root=tmp_path / "proc",
        )

    assert mutation_attempted is False
    assert (transaction.path / "start-attempt.json").read_bytes() == before


def test_same_completed_operation_cannot_create_a_second_generation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    request = _owned_session_start_request()
    transaction = _FakeSessionTransaction(
        tmp_path / "home" / ".local" / "share" / "clio-relay" / "sessions" / request.session_id,
        session_id=request.session_id,
    )
    generation = "generation-completed"
    token = "a" * 64
    identity: dict[str, object] = {
        "cluster": request.cluster,
        "session_id": request.session_id,
        "start_operation_id": request.start_operation_id,
        "session_generation_id": generation,
        "owner_token": token,
        "owner_token_sha256": session_lifecycle.hashlib.sha256(token.encode()).hexdigest(),
        "api_release_identity_sha256": _api_release_identity().sha256(),
        "expected_api_release_identity_sha256": None,
        "cluster_registry_path": str(transaction.path / f"cluster-registry-{generation}.json"),
        "cluster_registry_sha256": request.cluster_registry_sha256,
        "cluster_route_revision": request.cluster_route_revision,
        "remote_api_port": request.remote_api_port,
        "replace": request.replace,
        "require_token": request.require_token,
        "start_phase": "contained",
        "systemd_unit": f"clio-relay-session-{generation}.scope",
        "systemd_description": (
            f"clio-relay-owned-session:{request.session_id}:{generation}:{'2' * 32}"
        ),
        "systemd_cgroup_path": "/sys/fs/cgroup/test.scope",
        "systemd_invocation_id": "1" * 32,
        "containment_broker_pid": 4322,
        "containment_broker_start_identity": "linux-proc:1",
    }
    session_lifecycle._write_session_attempt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cast(session_lifecycle._OwnedSessionTransaction, transaction),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        operation="start",
        identity=identity,
    )
    transaction.atomic_write("metadata.json", b"{}")
    before = (transaction.path / "start-attempt.json").read_bytes()
    monkeypatch.setattr(
        session_lifecycle,
        "open_owned_session_transaction",
        _fake_transaction_opener(transaction),
    )
    monkeypatch.setattr(
        session_lifecycle,
        "_current_session_api_release_identity",
        _api_release_identity,
    )
    monkeypatch.setattr(os, "geteuid", lambda: 0, raising=False)

    def completed_status(**_kwargs: object) -> OwnedSessionRecoveryStatus:
        return OwnedSessionRecoveryStatus(
            cluster=request.cluster,
            session_id=request.session_id,
            session_generation_id=generation,
            start_operation_id=request.start_operation_id,
            cluster_route_revision=request.cluster_route_revision,
            remote_api_port=request.remote_api_port,
            recovery_verified=True,
            ownership_verified=True,
            running=False,
            start_state="ready",
            start_phase="contained",
            start_attempt_verified=True,
            api_release_identity=_api_release_identity(),
        )

    monkeypatch.setattr(
        session_lifecycle,
        "inspect_owned_session_recovery_status",
        completed_status,
    )
    mutation_attempted = False

    def refuse_mutation(*_args: object, **_kwargs: object) -> None:
        nonlocal mutation_attempted
        mutation_attempted = True
        raise AssertionError("completed operation reached generation mutation")

    monkeypatch.setattr(session_lifecycle, "_assert_remote_port_available", refuse_mutation)
    monkeypatch.setattr(ClioCoreQueue, "prepare_owner_session_start", refuse_mutation)

    with pytest.raises(RelayError, match="already completed; use a fresh operation id"):
        execute_owned_session_start(
            request,
            home=tmp_path / "home",
            core_dir=tmp_path / "core",
            proc_root=tmp_path / "proc",
        )

    assert mutation_attempted is False
    assert (transaction.path / "start-attempt.json").read_bytes() == before


def test_legacy_start_attempt_migrates_only_to_caller_planned_v2_operation(
    tmp_path: Path,
) -> None:
    request = _owned_session_start_request().model_copy(
        update={"start_operation_id": "start_planned_after_upgrade"}
    )
    transaction = _FakeSessionTransaction(tmp_path / "session", session_id=request.session_id)
    generation = "generation-legacy"
    token = "a" * 64
    legacy = {
        "schema_version": "clio-relay.owner-session-attempt.v1",
        "operation": "start",
        "cluster": request.cluster,
        "session_id": request.session_id,
        "session_generation_id": generation,
        "owner_token": token,
        "owner_token_sha256": session_lifecycle.hashlib.sha256(token.encode()).hexdigest(),
        "api_release_identity_sha256": _api_release_identity().sha256(),
        "cluster_registry_path": str(transaction.path / f"cluster-registry-{generation}.json"),
        "cluster_registry_sha256": request.cluster_registry_sha256,
        "cluster_route_revision": request.cluster_route_revision,
        "remote_api_port": request.remote_api_port,
        "start_phase": "pending",
        "systemd_unit": f"clio-relay-session-{generation}.scope",
        "systemd_description": (
            f"clio-relay-owned-session:{request.session_id}:{generation}:{'2' * 32}"
        ),
        "systemd_cgroup_path": None,
        "systemd_invocation_id": None,
        "containment_broker_pid": None,
        "containment_broker_start_identity": None,
        "observed_at": datetime.now(UTC).isoformat(),
        "error": None,
    }
    transaction.atomic_write(
        "start-attempt.json",
        json.dumps(legacy, sort_keys=True, separators=(",", ":")).encode(),
    )

    with pytest.raises(RelayError, match="identity is invalid"):
        session_lifecycle._validated_start_attempt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cast(session_lifecycle._OwnedSessionTransaction, transaction),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cluster=request.cluster,
            session_id=request.session_id,
            start_operation_id=request.start_operation_id,
        )

    migrated = session_lifecycle._migrate_legacy_start_attempt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cast(session_lifecycle._OwnedSessionTransaction, transaction),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        request=request,
        release_identity_sha256=_api_release_identity().sha256(),
    )

    assert migrated is not None
    assert migrated["schema_version"] == "clio-relay.owner-session-attempt.v2"
    assert migrated["start_operation_id"] == request.start_operation_id
    assert migrated["replace"] is False
    assert migrated["require_token"] is False


def test_legacy_old_release_replacement_requires_exact_identity_proof(
    tmp_path: Path,
) -> None:
    request = _owned_session_start_request().model_copy(
        update={
            "start_operation_id": "start_planned_replacement",
            "replace": True,
        }
    )
    transaction = _FakeSessionTransaction(tmp_path / "session", session_id=request.session_id)
    generation = "generation-legacy"
    token = "a" * 64
    legacy = {
        "schema_version": "clio-relay.owner-session-attempt.v1",
        "operation": "start",
        "cluster": request.cluster,
        "session_id": request.session_id,
        "session_generation_id": generation,
        "owner_token": token,
        "owner_token_sha256": session_lifecycle.hashlib.sha256(token.encode()).hexdigest(),
        "api_release_identity_sha256": "f" * 64,
        "cluster_registry_path": str(transaction.path / f"cluster-registry-{generation}.json"),
        "cluster_registry_sha256": request.cluster_registry_sha256,
        "cluster_route_revision": request.cluster_route_revision,
        "remote_api_port": request.remote_api_port,
        "start_phase": "pending",
        "systemd_unit": f"clio-relay-session-{generation}.scope",
        "systemd_description": (
            f"clio-relay-owned-session:{request.session_id}:{generation}:{'2' * 32}"
        ),
        "systemd_cgroup_path": None,
        "systemd_invocation_id": None,
        "containment_broker_pid": None,
        "containment_broker_start_identity": None,
        "observed_at": datetime.now(UTC).isoformat(),
        "error": None,
    }
    transaction.atomic_write(
        "start-attempt.json",
        json.dumps(legacy, sort_keys=True, separators=(",", ":")).encode(),
    )
    current_release_sha256 = _api_release_identity().sha256()

    with pytest.raises(RelayError, match="release identity changed"):
        session_lifecycle._migrate_legacy_start_attempt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cast(session_lifecycle._OwnedSessionTransaction, transaction),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            request=request,
            release_identity_sha256=current_release_sha256,
        )

    migrated = session_lifecycle._migrate_legacy_start_attempt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cast(session_lifecycle._OwnedSessionTransaction, transaction),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        request=request,
        release_identity_sha256=current_release_sha256,
        replacement_identity_verified=True,
    )

    assert migrated is not None
    assert migrated["schema_version"] == "clio-relay.owner-session-attempt.v2"
    assert migrated["start_operation_id"] == request.start_operation_id
    assert migrated["api_release_identity_sha256"] == current_release_sha256
    assert migrated["replace"] is True


def test_executor_replaces_exact_legacy_old_release_session(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    request, transaction, metadata, old_release, queue = _legacy_existing_start_fixture(tmp_path)
    current_release = _api_release_identity()
    home = tmp_path / "home"
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    monkeypatch.setattr(
        session_lifecycle,
        "open_owned_session_transaction",
        _fake_transaction_opener(transaction),
    )
    monkeypatch.setattr(
        session_lifecycle,
        "_current_session_api_release_identity",
        lambda: current_release,
    )
    effective_uid = getattr(os, "geteuid", lambda: 0)()
    monkeypatch.setattr(os, "geteuid", lambda: effective_uid, raising=False)
    inspection_count = 0

    def inspect(**_kwargs: object) -> OwnedSessionRecoveryStatus:
        nonlocal inspection_count
        inspection_count += 1
        if inspection_count <= 2:
            return _legacy_existing_status(
                request=request,
                metadata=metadata,
                old_release=old_release,
                journal_bound=inspection_count == 2,
            )
        committed = transaction.read_json("metadata.json")
        assert committed is not None
        return OwnedSessionRecoveryStatus(
            cluster=request.cluster,
            session_id=request.session_id,
            session_generation_id=cast(str, committed["session_generation_id"]),
            start_operation_id=request.start_operation_id,
            cluster_route_revision=request.cluster_route_revision,
            api_pid=7001,
            remote_api_port=request.remote_api_port,
            leader_process_state="owned_running",
            process_state="owned_running",
            running=True,
            metadata_verified=True,
            cluster_registry_verified=True,
            durable_generation_verified=True,
            ownership_verified=True,
            recovery_verified=True,
            api_release_identity=current_release,
            api_release_identity_verified=True,
            start_state="ready",
            start_phase="contained",
            start_attempt_verified=True,
            start_replace=True,
            start_require_token=request.require_token,
            start_expected_api_release_identity_sha256=current_release.sha256(),
        )

    monkeypatch.setattr(session_lifecycle, "inspect_owned_session_recovery_status", inspect)
    monkeypatch.setattr(session_lifecycle, "_assert_remote_port_available", _ignore_remote_port)
    base_interpreter = tmp_path / "uv-python" / "python3.12"
    base_interpreter.parent.mkdir(parents=True)
    base_interpreter.write_bytes(b"test interpreter")
    provider_interpreter = tmp_path / "uv-tools" / "clio-relay" / "bin" / "python"
    provider_interpreter.parent.mkdir(parents=True)
    if os.name == "nt":
        provider_interpreter.write_bytes(base_interpreter.read_bytes())
    else:
        provider_interpreter.symlink_to(base_interpreter)
        assert provider_interpreter.absolute() != provider_interpreter.resolve(strict=True)
    monkeypatch.setattr(session_lifecycle.sys, "executable", str(provider_interpreter))
    containment_module = importlib.import_module("clio_relay.process_containment")
    monkeypatch.setattr(
        containment_module,
        "process_start_identity",
        _fixed_process_start_identity,
    )

    launch_commands: list[list[str]] = []

    def spawn(command: list[str], **kwargs: object) -> object:
        launch_commands.append(command)
        containment = {
            "mode": "linux_systemd_scope",
            "enforceable": True,
            "systemd_unit": f"{kwargs['linux_systemd_unit_base']}.scope",
            "systemd_description": kwargs["linux_systemd_description"],
            "cgroup_path": "/sys/fs/cgroup/user.slice/replacement.scope",
            "systemd_invocation_id": "4" * 32,
        }
        cast(Callable[[int, dict[str, object]], None], kwargs["on_ready"])(
            7000,
            containment,
        )
        cast(Callable[[int, dict[str, object]], str], kwargs["credential_payload_factory"])(
            7000,
            containment,
        )
        return SimpleNamespace(
            pid=7000,
            poll=lambda: None,
            terminate=lambda: None,
            wait=_successful_process_wait,
        )

    monkeypatch.setattr(containment_module, "spawn_owned_process", spawn)

    def receipt(**kwargs: object) -> object:
        transaction.atomic_write(cast(str, kwargs["receipt_name"]), b"{}")
        return session_lifecycle._OwnedGenerationProcess(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            pid=7001,
            process_group_id=7001,
            start_ticks="999999",
        )

    monkeypatch.setattr(session_lifecycle, "_wait_for_api_startup_receipt", receipt)
    monkeypatch.setattr(session_lifecycle, "_wait_for_api_ready", _fixed_api_readiness(0.125))

    lines = execute_owned_session_start(
        request,
        home=home,
        core_dir=queue.root,
        proc_root=proc_root,
    )

    attempt = transaction.read_json("start-attempt.json")
    assert attempt is not None
    assert inspection_count == 2
    assert attempt["schema_version"] == "clio-relay.owner-session-attempt.v2"
    assert attempt["start_operation_id"] == request.start_operation_id
    assert attempt["api_release_identity_sha256"] == current_release.sha256()
    committed = transaction.read_json("metadata.json")
    assert committed is not None
    assert committed["api_release_identity_sha256"] == current_release.sha256()
    assert committed["api_pid"] == 7001
    assert launch_commands[0][0] == str(provider_interpreter.absolute())
    assert f"start_operation_id={request.start_operation_id}" in lines
    assert f"remote_api_port={request.remote_api_port}" in lines


def test_executor_refuses_mismatched_legacy_journal_without_mutation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    request, transaction, metadata, old_release, queue = _legacy_existing_start_fixture(tmp_path)
    legacy = transaction.read_json("start-attempt.json")
    assert legacy is not None
    stale_token = "c" * 64
    legacy["owner_token"] = stale_token
    legacy["owner_token_sha256"] = session_lifecycle.hashlib.sha256(
        stale_token.encode()
    ).hexdigest()
    transaction.atomic_write(
        "start-attempt.json",
        json.dumps(legacy, sort_keys=True, separators=(",", ":")).encode(),
    )
    before = (transaction.path / "start-attempt.json").read_bytes()
    monkeypatch.setattr(
        session_lifecycle,
        "open_owned_session_transaction",
        _fake_transaction_opener(transaction),
    )
    monkeypatch.setattr(
        session_lifecycle,
        "_current_session_api_release_identity",
        _api_release_identity,
    )
    monkeypatch.setattr(os, "geteuid", lambda: 0, raising=False)

    def legacy_status(**_kwargs: object) -> OwnedSessionRecoveryStatus:
        return _legacy_existing_status(
            request=request,
            metadata=metadata,
            old_release=old_release,
            journal_bound=False,
        )

    monkeypatch.setattr(
        session_lifecycle,
        "inspect_owned_session_recovery_status",
        legacy_status,
    )
    mutation_attempted = False

    def refuse_mutation(_port: int) -> None:
        nonlocal mutation_attempted
        mutation_attempted = True

    monkeypatch.setattr(session_lifecycle, "_assert_remote_port_available", refuse_mutation)

    with pytest.raises(
        RelayError,
        match="legacy start journal does not match exact verified session metadata",
    ):
        execute_owned_session_start(
            request,
            home=tmp_path / "home",
            core_dir=queue.root,
            proc_root=tmp_path / "proc",
        )

    assert mutation_attempted is False
    assert (transaction.path / "start-attempt.json").read_bytes() == before


def test_old_release_migration_crash_retries_same_replacement_with_real_inspection(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    generation = "generation-legacy"
    home, session_dir, proc_root, queue = _owned_session_recovery_fixture(
        tmp_path,
        session_id="session-start",
        generation_id=generation,
    )
    current_release = _api_release_identity()
    old_release = current_release.model_copy(update={"artifact_sha256": "f" * 64})
    raw_metadata = cast(
        object,
        json.loads((session_dir / "metadata.json").read_text(encoding="utf-8")),
    )
    assert isinstance(raw_metadata, dict)
    metadata = cast(dict[str, object], raw_metadata)
    owner_token = cast(str, metadata["owner_token"])
    registry_path = Path(cast(str, metadata["cluster_registry_path"]))
    registry_document = json.loads(registry_path.read_bytes())
    registry = ClusterRegistry.model_validate(registry_document)
    registry_payload = json.dumps(
        registry.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    registry_path.write_bytes(registry_payload)
    registry_sha256 = session_lifecycle.hashlib.sha256(registry_payload).hexdigest()
    receipt_path = Path(cast(str, metadata["api_startup_receipt_path"]))
    raw_receipt = cast(object, json.loads(receipt_path.read_text(encoding="utf-8")))
    assert isinstance(raw_receipt, dict)
    receipt = cast(dict[str, object], raw_receipt)
    receipt["api_release_identity_sha256"] = old_release.sha256()
    receipt["cluster_registry_sha256"] = registry_sha256
    receipt["hmac_sha256"] = session_lifecycle._startup_receipt_signature(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        receipt,
        owner_token=owner_token,
    )
    receipt_payload = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    receipt_path.write_bytes(receipt_payload)
    metadata["api_release_identity"] = old_release.model_dump(mode="json")
    metadata["api_release_identity_sha256"] = old_release.sha256()
    metadata["cluster_registry_sha256"] = registry_sha256
    metadata["api_startup_receipt_sha256"] = session_lifecycle.hashlib.sha256(
        receipt_payload
    ).hexdigest()
    (session_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    request = OwnedSessionStartRequest(
        cluster="ares",
        session_id="session-start",
        start_operation_id="start_replace_after_migration_crash",
        remote_api_port=cast(int, metadata["remote_api_port"]),
        replace=True,
        require_token=False,
        expected_api_release_identity=current_release,
        cluster_registry=registry.model_dump(mode="json"),
        cluster_registry_sha256=registry_sha256,
        cluster_route_revision=cast(str, metadata["cluster_route_revision"]),
    )
    legacy: dict[str, object] = {
        "schema_version": "clio-relay.owner-session-attempt.v1",
        "operation": "start",
        "cluster": request.cluster,
        "session_id": request.session_id,
        "session_generation_id": generation,
        "owner_token": owner_token,
        "owner_token_sha256": session_lifecycle.hashlib.sha256(owner_token.encode()).hexdigest(),
        "api_release_identity_sha256": old_release.sha256(),
        "cluster_registry_path": str(registry_path),
        "cluster_registry_sha256": request.cluster_registry_sha256,
        "cluster_route_revision": request.cluster_route_revision,
        "remote_api_port": request.remote_api_port,
        "start_phase": "contained",
        "systemd_unit": metadata["systemd_unit"],
        "systemd_description": metadata["systemd_description"],
        "systemd_cgroup_path": metadata["systemd_cgroup_path"],
        "systemd_invocation_id": metadata["systemd_invocation_id"],
        "containment_broker_pid": metadata["containment_broker_pid"],
        "containment_broker_start_identity": metadata["containment_broker_start_identity"],
        "observed_at": datetime.now(UTC).isoformat(),
        "error": None,
    }
    transaction = _FakeSessionTransaction(session_dir, session_id=request.session_id)
    transaction.atomic_write(
        "start-attempt.json",
        json.dumps(legacy, sort_keys=True, separators=(",", ":")).encode(),
    )
    monkeypatch.setattr(
        session_lifecycle,
        "open_owned_session_transaction",
        _fake_transaction_opener(transaction),
    )
    monkeypatch.setattr(
        session_lifecycle,
        "_current_session_api_release_identity",
        lambda: current_release,
    )
    effective_uid = getattr(os, "geteuid", lambda: 0)()
    monkeypatch.setattr(os, "geteuid", lambda: effective_uid, raising=False)
    migrate = session_lifecycle._migrate_legacy_start_attempt  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    class MigrationCrash(RuntimeError):
        """Simulated process loss immediately after the durable v2 write."""

    def migrate_then_crash(
        selected_transaction: session_lifecycle._OwnedSessionTransaction,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        *,
        request: OwnedSessionStartRequest,
        release_identity_sha256: str,
        replacement_identity_verified: bool = False,
    ) -> dict[str, object] | None:
        migrated = migrate(
            selected_transaction,
            request=request,
            release_identity_sha256=release_identity_sha256,
            replacement_identity_verified=replacement_identity_verified,
        )
        assert migrated is not None
        raise MigrationCrash

    monkeypatch.setattr(
        session_lifecycle,
        "_migrate_legacy_start_attempt",
        migrate_then_crash,
    )

    with pytest.raises(MigrationCrash):
        execute_owned_session_start(
            request,
            home=home,
            core_dir=queue.root,
            proc_root=proc_root,
        )

    migrated = transaction.read_json("start-attempt.json")
    assert migrated is not None
    assert migrated["schema_version"] == "clio-relay.owner-session-attempt.v2"
    assert migrated["api_release_identity_sha256"] == current_release.sha256()
    status = inspect_owned_session_recovery_status(
        cluster=request.cluster,
        session_id=request.session_id,
        core_dir=queue.root,
        home=home,
        proc_root=proc_root,
        effective_uid=effective_uid,
        transaction=cast(session_lifecycle._OwnedSessionTransaction, transaction),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        expected_start_operation_id=request.start_operation_id,
        expected_cluster_route_revision=request.cluster_route_revision,
    )
    assert status.recovery_verified is True
    assert status.start_attempt_verified is True
    assert status.start_state == "starting"
    assert status.start_retryable is True

    monkeypatch.setattr(session_lifecycle, "_migrate_legacy_start_attempt", migrate)
    monkeypatch.setattr(session_lifecycle, "_assert_remote_port_available", _ignore_remote_port)

    class ReplacementResumed(RuntimeError):
        """The retry reached replacement admission instead of terminal refusal."""

    def replacement_admission(
        _queue: ClioCoreQueue,
        owner_session_id: str,
        *,
        recorded_generation_id: str | None,
        candidate_generation_id: str,
    ) -> str:
        assert owner_session_id == request.session_id
        assert recorded_generation_id == generation
        assert candidate_generation_id
        raise ReplacementResumed

    monkeypatch.setattr(ClioCoreQueue, "prepare_owner_session_start", replacement_admission)

    with pytest.raises(ReplacementResumed):
        execute_owned_session_start(
            request,
            home=home,
            core_dir=queue.root,
            proc_root=proc_root,
        )


def test_durable_start_deadline_observes_late_ready_transition(
    monkeypatch: MonkeyPatch,
) -> None:
    definition, release, plan = _durable_start_plan()

    def deadline(**_kwargs: object) -> list[str]:
        raise session_lifecycle._RemoteSessionCommandDeadline(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            "start transport deadline"
        )

    def ready_status(**_kwargs: object) -> OwnedSessionRecoveryStatus:
        return _durable_start_status(plan, state="ready")

    monkeypatch.setattr(
        session_lifecycle,
        "status_remote_session_start",
        ready_status,
    )

    result = session_lifecycle.start_remote_session_durable(
        definition=definition,
        plan=plan,
        api_token=None,
        expected_api_release_identity=release,
        starter=deadline,
    )

    assert result.state == "ready"
    assert result.terminal is True
    assert result.transport_deadline_exceeded is True
    assert result.session_generation_id == "generation-start"


def test_synchronous_start_output_must_bind_exact_remote_port(
    monkeypatch: MonkeyPatch,
) -> None:
    definition, release, plan = _durable_start_plan()

    def incomplete(**_kwargs: object) -> list[str]:
        return [
            f"session_started={plan.session_id}",
            f"start_operation_id={plan.start_operation_id}",
            "session_generation_id=generation-start",
        ]

    def unavailable(**_kwargs: object) -> OwnedSessionRecoveryStatus:
        raise RelayError("status unavailable")

    monkeypatch.setattr(
        session_lifecycle,
        "status_remote_session_start",
        unavailable,
    )

    result = session_lifecycle.start_remote_session_durable(
        definition=definition,
        plan=plan,
        api_token=None,
        expected_api_release_identity=release,
        starter=incomplete,
    )

    assert result.state == "ambiguous"
    assert result.recovery_verified is False


def test_durable_start_keeps_verified_transition_pending_without_aggregate_timeout(
    monkeypatch: MonkeyPatch,
) -> None:
    definition, release, plan = _durable_start_plan()
    observations = 0

    def deadline(**_kwargs: object) -> list[str]:
        raise session_lifecycle._RemoteSessionCommandDeadline(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            "start transport deadline"
        )

    def observe(**_kwargs: object) -> OwnedSessionRecoveryStatus:
        nonlocal observations
        observations += 1
        return _durable_start_status(plan, state="starting")

    monkeypatch.setattr(session_lifecycle, "status_remote_session_start", observe)

    result = session_lifecycle.start_remote_session_durable(
        definition=definition,
        plan=plan,
        api_token=None,
        expected_api_release_identity=release,
        starter=deadline,
    )

    assert observations == 1
    assert result.state == "starting"
    assert result.terminal is False
    assert result.retryable is True
    assert result.transition_accepted is True
    assert result.session_generation_id == "generation-start"


def test_durable_start_status_transport_failure_is_ambiguous(
    monkeypatch: MonkeyPatch,
) -> None:
    definition, release, plan = _durable_start_plan()

    def deadline(**_kwargs: object) -> list[str]:
        raise session_lifecycle._RemoteSessionCommandDeadline(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            "start transport deadline"
        )

    def unavailable(**_kwargs: object) -> OwnedSessionRecoveryStatus:
        raise RelayError("status transport unavailable")

    monkeypatch.setattr(session_lifecycle, "status_remote_session_start", unavailable)

    result = session_lifecycle.start_remote_session_durable(
        definition=definition,
        plan=plan,
        api_token=None,
        expected_api_release_identity=release,
        starter=deadline,
    )

    assert result.state == "ambiguous"
    assert result.terminal is False
    assert result.retryable is True
    assert result.transition_accepted is None
    assert result.session_generation_id is None


def test_exact_start_rejection_during_lock_contention_is_not_terminal(
    monkeypatch: MonkeyPatch,
) -> None:
    definition, release, plan = _durable_start_plan()
    rejection = session_lifecycle.OwnedSessionStartRejection(
        cluster=plan.cluster,
        session_id=plan.session_id,
        start_operation_id=plan.start_operation_id,
        cluster_route_revision=plan.cluster_route_revision,
        error="owned session transition lock timed out",
    )

    def rejected(**_kwargs: object) -> list[str]:
        raise session_lifecycle._RemoteSessionCommandRejected(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            rejection
        )

    def locked(**_kwargs: object) -> OwnedSessionRecoveryStatus:
        raise RelayError("owned session transition lock is held")

    monkeypatch.setattr(session_lifecycle, "status_remote_session_start", locked)

    result = session_lifecycle.start_remote_session_durable(
        definition=definition,
        plan=plan,
        api_token=None,
        expected_api_release_identity=release,
        starter=rejected,
    )

    assert result.state == "ambiguous"
    assert result.terminal is False
    assert result.retryable is True


def test_unstructured_ssh_nonzero_is_ambiguous_not_terminal(
    monkeypatch: MonkeyPatch,
) -> None:
    def connection_reset(
        _command: list[str],
        *,
        input_bytes: bytes = b"",
        timeout_seconds: float,
        stdout_limit: int,
        stderr_limit: int,
        environment: dict[str, str] | None = None,
    ) -> session_lifecycle._BoundedCommandResult:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        del input_bytes, timeout_seconds, stdout_limit, stderr_limit, environment
        return session_lifecycle._BoundedCommandResult(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            returncode=255,
            stdout=b"",
            stderr=b"connection reset after remote acceptance",
        )

    monkeypatch.setattr(
        session_lifecycle,
        "_run_bounded_command",
        connection_reset,
    )

    with pytest.raises(
        session_lifecycle._RemoteSessionCommandAmbiguous,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        match="without an exact structured response",
    ):
        session_lifecycle._ssh_script(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            ClusterDefinition(name="ares", ssh_host="ares"),
            "true\n",
        )


def test_durable_start_projects_terminal_failure_and_stops_retrying(
    monkeypatch: MonkeyPatch,
) -> None:
    definition, release, plan = _durable_start_plan()

    def deadline(**_kwargs: object) -> list[str]:
        raise session_lifecycle._RemoteSessionCommandDeadline(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            "start transport deadline"
        )

    def failed_status(**_kwargs: object) -> OwnedSessionRecoveryStatus:
        return _durable_start_status(plan, state="failed")

    monkeypatch.setattr(
        session_lifecycle,
        "status_remote_session_start",
        failed_status,
    )

    result = session_lifecycle.start_remote_session_durable(
        definition=definition,
        plan=plan,
        api_token=None,
        expected_api_release_identity=release,
        starter=deadline,
    )

    assert result.state == "failed"
    assert result.terminal is True
    assert result.retryable is False
    assert result.error == "remote start failed"


def test_completed_ready_operation_stays_terminal_after_api_exit() -> None:
    _definition, _release, plan = _durable_start_plan()
    status = _durable_start_status(plan, state="ready").model_copy(
        update={"leader_process_state": "absent", "running": False}
    )

    result = session_lifecycle._session_start_result_from_status(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        plan=plan,
        status=status,
        transport_deadline_exceeded=False,
    )

    assert result.state == "ready"
    assert result.terminal is True
    assert result.retryable is False
    assert result.running is False
    assert result.session_generation_id == "generation-start"


def test_completed_ready_operation_reports_api_down_when_only_child_remains() -> None:
    _definition, _release, plan = _durable_start_plan()
    status = _durable_start_status(plan, state="ready").model_copy(
        update={
            "leader_process_state": "absent",
            "process_state": "owned_running",
            "running": True,
            "generation_process_pids": [5432],
        }
    )

    result = session_lifecycle._session_start_result_from_status(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        plan=plan,
        status=status,
        transport_deadline_exceeded=False,
    )

    assert result.state == "ready"
    assert result.terminal is True
    assert result.running is False
    assert status.running is True


def test_superseded_start_selector_is_terminal_not_current() -> None:
    _definition, _release, plan = _durable_start_plan()

    result = session_lifecycle._session_start_result_from_status(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        plan=plan,
        status=_durable_start_status(plan, state="not_current"),
        transport_deadline_exceeded=False,
    )

    assert result.state == "not_current"
    assert result.terminal is True
    assert result.retryable is False
    assert result.transition_accepted is None


def test_start_selector_intent_drift_is_terminally_refused() -> None:
    _definition, _release, plan = _durable_start_plan()
    status = _durable_start_status(plan, state="starting").model_copy(
        update={"remote_api_port": plan.remote_api_port + 1}
    )

    result = session_lifecycle._session_start_result_from_status(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        plan=plan,
        status=status,
        transport_deadline_exceeded=False,
    )

    assert result.state == "failed"
    assert result.terminal is True
    assert result.retryable is False
    assert "does not match" in cast(str, result.error)


def test_api_readiness_rejects_wrong_auth_policy(monkeypatch: MonkeyPatch) -> None:
    class Response:
        status = 200

        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self, _size: int) -> bytes:
            return b'{"ok":true,"auth":false}'

    moments = iter((0.0, 0.0, 61.0))

    def ignore_sleep(_seconds: float) -> None:
        return None

    def open_response(
        _url: str,
        data: bytes | None = None,
        timeout: float | None = None,
        *,
        cafile: str | None = None,
        capath: str | None = None,
        cadefault: bool = False,
        context: object | None = None,
    ) -> Response:
        del data, timeout, cafile, capath, cadefault, context
        return Response()

    monkeypatch.setattr(session_lifecycle.time, "monotonic", lambda: next(moments))
    monkeypatch.setattr(session_lifecycle.time, "sleep", ignore_sleep)
    monkeypatch.setattr(
        session_lifecycle.urllib.request,
        "urlopen",
        open_response,
    )
    process = cast(subprocess.Popen[bytes], SimpleNamespace(poll=lambda: None))

    with pytest.raises(RelayError, match="did not become ready"):
        session_lifecycle._wait_for_api_ready(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            process=process,
            port=18765,
            require_token=True,
        )


def test_no_require_token_suppresses_ambient_api_token(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "ambient-token")

    assert (
        session_lifecycle._owned_session_api_token(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            require_token=False
        )
        is None
    )
    assert (
        session_lifecycle._owned_session_api_token(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            require_token=True
        )
        == "ambient-token"
    )


def test_contained_start_crash_is_promoted_only_after_full_identity_recheck(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    generation = "generation-crash"
    custom_home, session_dir, proc_root, queue = _owned_session_recovery_fixture(
        tmp_path,
        session_id="session-start",
        generation_id=generation,
    )
    recovered_metadata = json.loads((session_dir / "metadata.json").read_text(encoding="utf-8"))
    registry_path = session_dir / f"cluster-registry-{generation}.json"
    registry_bytes = registry_path.read_bytes()
    request = _owned_session_start_request().model_copy(
        update={
            "remote_api_port": 8765,
            "cluster_registry": json.loads(registry_bytes),
            "cluster_registry_sha256": recovered_metadata["cluster_registry_sha256"],
            "cluster_route_revision": recovered_metadata["cluster_route_revision"],
        }
    )
    release = _api_release_identity()
    _write_owned_generation_process(
        proc_root=proc_root,
        metadata=recovered_metadata,
        pid=4321,
        command=b"python\0-I\0-c\0clio-relay\0api\0start\0",
        start_ticks="123456",
    )
    (session_dir / "metadata.json").unlink()
    transaction = _FakeSessionTransaction(session_dir, session_id=request.session_id)
    attempt_identity: dict[str, object] = {
        "cluster": request.cluster,
        "session_id": request.session_id,
        "start_operation_id": request.start_operation_id,
        "session_generation_id": generation,
        "owner_token": recovered_metadata["owner_token"],
        "owner_token_sha256": session_lifecycle.hashlib.sha256(
            cast(str, recovered_metadata["owner_token"]).encode()
        ).hexdigest(),
        "api_release_identity_sha256": release.sha256(),
        "expected_api_release_identity_sha256": None,
        "cluster_registry_path": str(registry_path),
        "cluster_registry_sha256": request.cluster_registry_sha256,
        "cluster_route_revision": request.cluster_route_revision,
        "remote_api_port": request.remote_api_port,
        "replace": request.replace,
        "require_token": request.require_token,
        "start_phase": "contained",
        "systemd_unit": recovered_metadata["systemd_unit"],
        "systemd_description": recovered_metadata["systemd_description"],
        "systemd_cgroup_path": recovered_metadata["systemd_cgroup_path"],
        "systemd_invocation_id": recovered_metadata["systemd_invocation_id"],
        "containment_broker_pid": recovered_metadata["containment_broker_pid"],
        "containment_broker_start_identity": recovered_metadata[
            "containment_broker_start_identity"
        ],
    }
    session_lifecycle._write_session_attempt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cast(session_lifecycle._OwnedSessionTransaction, transaction),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        operation="start",
        identity=attempt_identity,
    )
    attempt = transaction.read_json("start-attempt.json")
    assert attempt is not None
    process_identity = session_lifecycle._OwnedGenerationProcess(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        pid=4321,
        process_group_id=4321,
        start_ticks="123456",
    )
    receipt_checks = 0

    def verify_receipt(**_kwargs: object) -> object:
        nonlocal receipt_checks
        receipt_checks += 1
        return process_identity

    monkeypatch.setattr(session_lifecycle, "_wait_for_api_startup_receipt", verify_receipt)
    monkeypatch.setattr(session_lifecycle, "_wait_for_api_ready", _fixed_api_readiness(0.25))

    lines = session_lifecycle._promote_resumable_contained_start(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        transaction=cast(session_lifecycle._OwnedSessionTransaction, transaction),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        attempt=attempt,
        request=request,
        release_identity=release,
        queue=queue,
        proc_root=proc_root,
        home=custom_home,
    )

    assert receipt_checks == 2
    assert lines is not None
    assert f"session_generation_id={generation}" in lines
    metadata = transaction.read_json("metadata.json")
    assert metadata is not None
    assert metadata["api_pid"] == 4321


def test_recovery_counts_non_clio_generation_child_when_leader_is_absent(
    tmp_path: Path,
) -> None:
    home, session_dir, proc_root, queue = _owned_session_recovery_fixture(tmp_path)
    metadata = json.loads((session_dir / "metadata.json").read_text(encoding="utf-8"))
    _write_owned_generation_process(
        proc_root=proc_root,
        metadata=metadata,
        pid=5432,
        command=b"frpc\0-c\0owned.toml\0",
    )

    status = inspect_owned_session_recovery_status(
        cluster="ares",
        session_id="session-1",
        core_dir=queue.root,
        home=home,
        proc_root=proc_root,
    )

    assert status.recovery_verified is True
    assert status.leader_process_state == "absent"
    assert status.process_state == "owned_running"
    assert status.running is True
    assert status.generation_process_pids == [5432]
    assert status.process_absence_verified is False
    assert status.generation_process_absence_verified is False


def test_recovery_does_not_read_unrelated_process_environment(tmp_path: Path) -> None:
    home, _session_dir, proc_root, queue = _owned_session_recovery_fixture(tmp_path)
    unrelated = proc_root / "9999"
    unrelated.mkdir()
    (unrelated / "environ").write_text("protected", encoding="utf-8")
    (unrelated / "environ").chmod(0)

    status = inspect_owned_session_recovery_status(
        cluster="ares",
        session_id="session-1",
        core_dir=queue.root,
        home=home,
        proc_root=proc_root,
    )

    assert status.recovery_verified is True
    assert status.process_absence_verified is True


def test_dead_owned_session_recovery_rejects_reused_recorded_pid(tmp_path: Path) -> None:
    home, _session_dir, proc_root, queue = _owned_session_recovery_fixture(tmp_path)
    pid_dir = proc_root / "4321"
    pid_dir.mkdir()
    fields = ["S", "0", "4321", *(["0"] * 16), "999999"]
    (pid_dir / "stat").write_text(f"4321 (foreign) {' '.join(fields)}", encoding="utf-8")

    status = inspect_owned_session_recovery_status(
        cluster="ares",
        session_id="session-1",
        core_dir=queue.root,
        home=home,
        proc_root=proc_root,
    )

    assert status.recovery_verified is False
    assert status.process_state == "reused"
    assert status.process_absence_verified is False
    assert status.ownership_verified is False
    assert any("was reused" in error for error in status.errors)


def test_dead_owned_session_recovery_rejects_generation_mismatch(tmp_path: Path) -> None:
    home, session_dir, proc_root, queue = _owned_session_recovery_fixture(tmp_path)
    metadata_path = session_dir / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["session_generation_id"] = "generation-2"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    status = inspect_owned_session_recovery_status(
        cluster="ares",
        session_id="session-1",
        core_dir=queue.root,
        home=home,
        proc_root=proc_root,
    )

    assert status.recovery_verified is False
    assert status.durable_generation_verified is False
    assert status.ownership_verified is False


def test_recovery_rejects_conflicting_active_generation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    home, _session_dir, proc_root, queue = _owned_session_recovery_fixture(tmp_path)

    def conflicting_status(
        _self: ClioCoreQueue,
        owner_session_id: str,
        *,
        session_generation_id: str,
    ) -> dict[str, object]:
        return {
            "owner_session_id": owner_session_id,
            "session_generation_id": session_generation_id,
            "active_generation_id": "generation-b",
            "closing_generation_id": session_generation_id,
            "active": False,
            "closing": True,
            "closed": False,
            "open": False,
            "cleanup_intent": None,
        }

    monkeypatch.setattr(ClioCoreQueue, "owner_session_generation_status", conflicting_status)

    status = inspect_owned_session_recovery_status(
        cluster="ares",
        session_id="session-1",
        core_dir=queue.root,
        home=home,
        proc_root=proc_root,
    )

    assert status.durable_generation_verified is False
    assert status.recovery_verified is False
    assert status.ownership_verified is False


def test_owned_session_recovery_rejects_mismatched_release_identity(tmp_path: Path) -> None:
    home, session_dir, proc_root, queue = _owned_session_recovery_fixture(tmp_path)
    metadata_path = session_dir / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["api_release_identity_sha256"] = "f" * 64
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    status = inspect_owned_session_recovery_status(
        cluster="ares",
        session_id="session-1",
        core_dir=queue.root,
        home=home,
        proc_root=proc_root,
    )

    assert status.recovery_verified is False
    assert status.metadata_verified is False
    assert status.ownership_verified is False


def test_owned_session_recovery_rejects_symlinked_metadata(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    home, session_dir, proc_root, queue = _owned_session_recovery_fixture(tmp_path)
    metadata_path = session_dir / "metadata.json"
    if os.name == "posix":
        # The POSIX reader opens metadata through the pinned session descriptor
        # with O_NOFOLLOW, so exercise the real filesystem boundary.
        symlink_target = metadata_path.with_name("metadata-symlink-target.json")
        metadata_path.rename(symlink_target)
        metadata_path.symlink_to(symlink_target)
    else:
        original_lstat = Path.lstat

        def symlinked_metadata_lstat(path: Path) -> os.stat_result | SimpleNamespace:
            if path == metadata_path:
                return SimpleNamespace(st_mode=stat.S_IFLNK, st_uid=0)
            return original_lstat(path)

        monkeypatch.setattr(Path, "lstat", symlinked_metadata_lstat)

    status = inspect_owned_session_recovery_status(
        cluster="ares",
        session_id="session-1",
        core_dir=queue.root,
        home=home,
        proc_root=proc_root,
    )

    assert status.recovery_verified is False
    assert status.metadata_verified is False
    assert any("safely" in error or "regular file" in error for error in status.errors)


def test_owned_session_recovery_rejects_symlinked_session_parent(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    home, session_dir, proc_root, queue = _owned_session_recovery_fixture(tmp_path)
    sessions_parent = session_dir.parent
    if os.name == "posix":
        # The POSIX reader intentionally pins directories with openat/fstat and
        # never consults Path.lstat. Exercise that real boundary instead of
        # mocking an API the implementation correctly does not use.
        symlink_target = sessions_parent.with_name("sessions-symlink-target")
        sessions_parent.rename(symlink_target)
        sessions_parent.symlink_to(symlink_target, target_is_directory=True)
    else:
        original_lstat = Path.lstat

        def symlinked_parent_lstat(path: Path) -> os.stat_result | SimpleNamespace:
            if path == sessions_parent:
                return SimpleNamespace(st_mode=stat.S_IFLNK, st_uid=0)
            return original_lstat(path)

        monkeypatch.setattr(Path, "lstat", symlinked_parent_lstat)

    status = inspect_owned_session_recovery_status(
        cluster="ares",
        session_id="session-1",
        core_dir=queue.root,
        home=home,
        proc_root=proc_root,
    )

    assert status.recovery_verified is False
    assert status.metadata_verified is False
    assert any("safely" in error or "directory" in error for error in status.errors)


def test_cleanup_receipt_supports_idempotent_pending_retry(tmp_path: Path) -> None:
    home, session_dir, proc_root, queue = _owned_session_recovery_fixture(tmp_path)
    (session_dir / "api.log").write_text("closed\n", encoding="utf-8")
    (session_dir / "api.pid").write_text("4321\n", encoding="ascii")
    target_names = sorted(
        [
            "api.log",
            "api.pid",
            "api-startup-generation-1.json",
            "cluster-registry-generation-1.json",
        ]
    )
    targets: list[OwnedSessionCleanupTarget] = []
    for name in target_names:
        path = session_dir / name
        path_stat = path.stat()
        targets.append(
            OwnedSessionCleanupTarget(
                name=name,
                present=True,
                device=path_stat.st_dev,
                inode=path_stat.st_ino,
                size=path_stat.st_size,
                sha256=(
                    None
                    if name == "api.log"
                    else session_lifecycle.hashlib.sha256(path.read_bytes()).hexdigest()
                ),
                identity_mode="inode" if name == "api.log" else "content_sha256",
            )
        )
    intent = queue.set_owner_session_closing(
        "session-1",
        session_generation_id="generation-1",
    )
    observed_at = datetime.now(UTC)
    report = SessionLifecycleReport(
        cluster="ares",
        session_id="session-1",
        session_generation_id="generation-1",
        mode="teardown",
        cleanup_operation_id=str(intent["operation_id"]),
        cleanup_policy={
            "stop_worker": False,
            "cancel_jobs": False,
            "cancel_scheduler_jobs": False,
        },
        prior_session_status=RemoteSessionStateEvidence(
            api_pid=4321,
            session_generation_id="generation-1",
            process_start_marker="123456",
            running=False,
            ownership_verified=True,
            observed_at=observed_at,
        ),
        post_session_status=RemoteSessionStateEvidence(
            api_pid=4321,
            session_generation_id="generation-1",
            process_start_marker="123456",
            running=False,
            ownership_verified=True,
            observed_at=observed_at,
        ),
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id="4321",
                location="ares",
                action="stop",
                ownership_verified=True,
                outcome="missing",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="remote_session_files",
                resource_id="session-1:generation-1",
                location="ares",
                action="close",
                ownership_verified=True,
                outcome="closed",
                verified_after_operation=True,
                metadata={
                    "metadata_sanitized": True,
                    "target_identities": [target.model_dump(mode="json") for target in targets],
                },
            ),
        ],
    )
    receipt = {
        "schema_version": "clio-relay.owner-session-cleanup-receipt.v1",
        "owner": "clio-relay",
        "cluster": "ares",
        "session_id": "session-1",
        "session_generation_id": "generation-1",
        "api_pid": 4321,
        "api_pgid": 4321,
        "remote_api_port": 8765,
        "process_start_ticks": "123456",
        "owner_token_sha256": session_lifecycle.hashlib.sha256(("b" * 64).encode()).hexdigest(),
        "api_release_identity_sha256": _api_release_identity().sha256(),
        "cluster_registry_path": str(session_dir / "cluster-registry-generation-1.json"),
        "cluster_registry_sha256": next(
            target.sha256
            for target in targets
            if target.name == "cluster-registry-generation-1.json"
        ),
        "cluster_route_revision": session_lifecycle.cluster_route_revision(
            ClusterDefinition(name="ares", ssh_host="ares")
        ),
        "containment_mode": "linux_systemd_scope",
        "systemd_unit": "clio-relay-session-generation-1.scope",
        "systemd_cgroup_path": ("/sys/fs/cgroup/user.slice/clio-relay-session-generation-1.scope"),
        "systemd_invocation_id": "1" * 32,
        "systemd_description": ("clio-relay-owned-session:session-1:generation-1:" + "2" * 32),
        "containment_broker_pid": 4322,
        "containment_broker_start_identity": "linux-proc:654321",
        "metadata_sha256": "a" * 64,
        "cleanup_operation_id": intent["operation_id"],
        "cleanup_policy": {
            "stop_worker": False,
            "cancel_jobs": False,
            "cancel_scheduler_jobs": False,
        },
        "cleanup_paths": target_names,
        "cleanup_targets": [target.model_dump(mode="json") for target in targets],
        "cleanup_paths_pending": True,
        "cluster_registry_verified": True,
        "cluster_registry_removed": False,
        "completed_at": observed_at.isoformat(),
        "report": report.model_dump(mode="json"),
        "coordinator_report": None,
        "coordinator_report_sha256": None,
    }
    (session_dir / "metadata.json").write_text(json.dumps(receipt), encoding="utf-8")

    status = inspect_owned_session_recovery_status(
        cluster="ares",
        session_id="session-1",
        core_dir=queue.root,
        home=home,
        proc_root=proc_root,
    )

    assert status.recovery_verified is True
    assert status.cleanup_receipt is True
    assert status.process_state == "cleanup_pending"
    assert status.durable_generation_verified is True
    assert status.errors == []

    queue.set_owner_session_closed(
        "session-1",
        session_generation_id="generation-1",
    )
    closed_status = inspect_owned_session_recovery_status(
        cluster="ares",
        session_id="session-1",
        core_dir=queue.root,
        home=home,
        proc_root=proc_root,
    )
    assert closed_status.recovery_verified is True
    assert closed_status.process_state == "already_closed"


def test_cleanup_deletes_oversized_api_log_by_pinned_inode(tmp_path: Path) -> None:
    _home, session_dir, _proc_root, _queue = _owned_session_recovery_fixture(tmp_path)
    log_path = session_dir / "api.log"
    with log_path.open("wb") as log:
        log.truncate(20 * 1024 * 1024)

    with _FakeSessionTransaction(session_dir, session_id="session-1") as transaction:
        target = session_lifecycle._capture_cleanup_target(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cast(session_lifecycle._OwnedSessionTransaction, transaction),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            name="api.log",
            maximum_bytes=None,
        )
        assert target.identity_mode == "inode"
        assert target.sha256 is None
        assert target.size == 20 * 1024 * 1024
        session_lifecycle._delete_cleanup_targets(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cast(session_lifecycle._OwnedSessionTransaction, transaction),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            [target],
        )

    assert not log_path.exists()


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    [
        ("link_count", "owner-private regular"),
        ("same_size_content", "changed while it was read"),
    ],
)
def test_owned_session_read_revalidates_descriptor_and_path_after_read(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    mutation: str,
    expected_error: str,
) -> None:
    payload = b"same-size-payload"

    def status(*, links: int = 1, timestamp: int = 10) -> SimpleNamespace:
        return SimpleNamespace(
            st_mode=stat.S_IFREG | 0o600,
            st_nlink=links,
            st_uid=1000,
            st_dev=7,
            st_ino=11,
            st_size=len(payload),
            st_mtime_ns=timestamp,
            st_ctime_ns=timestamp,
        )

    initial_opened = status()
    initial_linked = status()
    final_opened = status(
        links=2 if mutation == "link_count" else 1,
        timestamp=20 if mutation == "same_size_content" else 10,
    )
    final_linked = status(
        links=2 if mutation == "link_count" else 1,
        timestamp=20 if mutation == "same_size_content" else 10,
    )
    fstats = iter([initial_opened, final_opened])
    stats = iter([initial_linked, final_linked])
    reads = iter([payload, b""])

    def open_file(
        _path: str,
        _flags: int,
        _mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        del dir_fd
        return 41

    def fstat_file(_descriptor: int) -> SimpleNamespace:
        return next(fstats)

    def stat_file(
        _path: str,
        *,
        dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> SimpleNamespace:
        del dir_fd, follow_symlinks
        return next(stats)

    def read_file(_descriptor: int, _size: int) -> bytes:
        return next(reads)

    def close_file(_descriptor: int) -> None:
        return None

    monkeypatch.setattr(os, "open", open_file)
    monkeypatch.setattr(os, "fstat", fstat_file)
    monkeypatch.setattr(os, "stat", stat_file)
    monkeypatch.setattr(os, "read", read_file)
    monkeypatch.setattr(os, "close", close_file)
    transaction = session_lifecycle._OwnedSessionTransaction(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        session_id="session-1",
        path=tmp_path,
        sessions_fd=-1,
        directory_fd=9,
        lock_fd=-1,
        uid=1000,
        _fcntl=cast(
            session_lifecycle._FcntlModule,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            SimpleNamespace(),
        ),
    )

    with pytest.raises(RelayError, match=expected_error):
        transaction.read_bytes("metadata.json", maximum_bytes=1024)


def test_cleanup_report_finalization_is_immutable_and_idempotent(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    observed_at = datetime.now(UTC)
    policy = {
        "stop_worker": False,
        "cancel_jobs": False,
        "cancel_scheduler_jobs": False,
    }
    remote_report = SessionLifecycleReport(
        cluster="ares",
        session_id="session-1",
        session_generation_id="generation-1",
        mode="teardown",
        cleanup_operation_id="cleanup-finalize",
        cleanup_policy=policy,
        prior_session_status=RemoteSessionStateEvidence(
            api_pid=4321,
            session_generation_id="generation-1",
            process_start_marker="123456",
            running=True,
            ownership_verified=True,
            observed_at=observed_at,
        ),
        post_session_status=RemoteSessionStateEvidence(
            api_pid=4321,
            session_generation_id="generation-1",
            process_start_marker="123456",
            running=False,
            ownership_verified=True,
            observed_at=observed_at,
        ),
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id="4321",
                location="ares",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
            )
        ],
    )
    coordinator_report = remote_report.model_copy(deep=True)
    coordinator_report.resources.extend(
        CleanupResource(
            kind="relay_job",
            resource_id=f"job-{index:05d}",
            location="ares",
            action="retain",
            ownership_verified=True,
            outcome="retained",
            verified_after_operation=True,
            observed_state="running",
        )
        for index in range(9_999)
    )
    coordinator_payload = session_lifecycle.session_lifecycle_report_bytes(coordinator_report)
    assert len(coordinator_payload) > 1024 * 1024
    assert len(coordinator_report.resources) == 10_000

    class FakeTransaction:
        def __init__(self) -> None:
            self.document: dict[str, object] = {
                "cleanup_operation_id": "cleanup-finalize",
                "cleanup_policy": policy,
                "report": remote_report.model_dump(mode="json"),
                "coordinator_report_ref": None,
            }
            self.writes: list[bytes] = []
            self.sidecars: dict[str, bytes] = {}

        def __enter__(self) -> FakeTransaction:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read_json(self, _name: str) -> dict[str, object]:
            return dict(self.document)

        def atomic_write(
            self,
            _name: str,
            payload: bytes,
            *,
            maximum_bytes: int = 1024 * 1024,
        ) -> None:
            assert len(payload) <= maximum_bytes
            self.writes.append(payload)
            loaded = json.loads(payload)
            assert isinstance(loaded, dict)
            self.document = cast(dict[str, object], loaded)

        def atomic_write_immutable(
            self,
            name: str,
            payload: bytes,
            *,
            maximum_bytes: int,
        ) -> None:
            assert len(payload) <= maximum_bytes
            existing = self.sidecars.get(name)
            if existing is not None and existing != payload:
                raise RelayError("owned session immutable file already differs")
            self.sidecars[name] = payload

        def cleanup_report_candidate_names(self) -> list[str]:
            return sorted(self.sidecars)

    transaction = FakeTransaction()

    def inspect(**_kwargs: object) -> OwnedSessionRecoveryStatus:
        raw_reference = transaction.document.get("coordinator_report_ref")
        reference = (
            session_lifecycle.OwnedSessionCleanupReportReference.model_validate(raw_reference)
            if isinstance(raw_reference, dict)
            else None
        )
        bound = reference is not None and reference.name in transaction.sidecars
        return OwnedSessionRecoveryStatus(
            cluster="ares",
            session_id="session-1",
            session_generation_id="generation-1",
            cleanup_receipt=True,
            cleanup_paths_pending=False,
            coordinator_report_ref=reference if bound else None,
            coordinator_report_sha256=(
                reference.sha256 if bound and reference is not None else None
            ),
            coordinator_report_bound=bound,
            ownership_verified=True,
            recovery_verified=True,
        )

    def open_transaction(
        *,
        session_id: str,
        create: bool,
        timeout_seconds: float,
        home: Path | None = None,
    ) -> FakeTransaction:
        del session_id, create, timeout_seconds, home
        return transaction

    monkeypatch.setattr(
        session_lifecycle,
        "open_owned_session_transaction",
        open_transaction,
    )
    monkeypatch.setattr(session_lifecycle, "inspect_owned_session_recovery_status", inspect)
    monkeypatch.setattr(os, "geteuid", lambda: 1000, raising=False)
    request = OwnedSessionCleanupFinalizeRequest(
        cluster="ares",
        session_id="session-1",
        expected_session_generation_id="generation-1",
        expected_cleanup_operation_id="cleanup-finalize",
        expected_cleanup_policy=policy,
        coordinator_report=coordinator_report,
        coordinator_report_sha256=session_lifecycle_report_sha256(coordinator_report),
    )

    finalized = execute_owned_session_cleanup_finalize(
        request,
        home=tmp_path,
        core_dir=tmp_path / "core",
    )
    repeated = execute_owned_session_cleanup_finalize(
        request,
        home=tmp_path,
        core_dir=tmp_path / "core",
    )

    assert finalized.coordinator_report_bound is True
    assert repeated.coordinator_report_sha256 == request.coordinator_report_sha256
    assert len(transaction.writes) == 1
    assert len(transaction.sidecars) == 1
    assert len(transaction.writes[0]) < 1024 * 1024
    assert next(iter(transaction.sidecars.values())) == coordinator_payload
    assert len(finalized.model_dump_json().encode("utf-8")) < 1024 * 1024

    replacement = coordinator_report.model_copy(deep=True)
    replacement.resources.append(
        CleanupResource(
            kind="owner_session",
            resource_id="session-1:generation-1",
            location="ares",
            action="close",
            ownership_verified=True,
            outcome="closed",
            verified_after_operation=True,
        )
    )
    with pytest.raises(RelayError, match="immutable"):
        execute_owned_session_cleanup_finalize(
            request.model_copy(
                update={
                    "coordinator_report": replacement,
                    "coordinator_report_sha256": session_lifecycle_report_sha256(replacement),
                }
            ),
            home=tmp_path,
            core_dir=tmp_path / "core",
        )
    assert len(transaction.writes) == 1


def _cleanup_sidecar_report() -> SessionLifecycleReport:
    """Build one exact coordinator report for cleanup-sidecar boundary tests."""
    return SessionLifecycleReport(
        cluster="ares",
        session_id="session-1",
        session_generation_id="generation-1",
        mode="teardown",
        cleanup_operation_id="cleanup-sidecar",
        cleanup_policy={
            "stop_worker": False,
            "cancel_jobs": False,
            "cancel_scheduler_jobs": False,
        },
        relay_cancel_requested=False,
        scheduler_cancel_requested=False,
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "cluster",
        "session",
        "generation",
        "operation",
        "policy",
        "relay_disposition",
        "scheduler_disposition",
    ],
)
def test_cleanup_finalize_rejects_request_report_identity_drift(mutation: str) -> None:
    report = _cleanup_sidecar_report()
    request_values: dict[str, object] = {
        "cluster": "ares",
        "session_id": "session-1",
        "expected_session_generation_id": "generation-1",
        "expected_cleanup_operation_id": "cleanup-sidecar",
        "expected_cleanup_policy": dict(report.cleanup_policy),
        "coordinator_report": report,
        "coordinator_report_sha256": session_lifecycle_report_sha256(report),
    }
    if mutation == "cluster":
        request_values["cluster"] = "other-cluster"
    elif mutation == "session":
        request_values["session_id"] = "other-session"
    elif mutation == "generation":
        request_values["expected_session_generation_id"] = "generation-other"
    elif mutation == "operation":
        request_values["expected_cleanup_operation_id"] = "cleanup-other"
    elif mutation == "policy":
        request_values["expected_cleanup_policy"] = {
            "stop_worker": True,
            "cancel_jobs": False,
            "cancel_scheduler_jobs": False,
        }
    elif mutation == "relay_disposition":
        report.relay_cancel_requested = True
        request_values["coordinator_report_sha256"] = session_lifecycle_report_sha256(report)
    else:
        report.scheduler_cancel_requested = True
        request_values["coordinator_report_sha256"] = session_lifecycle_report_sha256(report)

    request = OwnedSessionCleanupFinalizeRequest.model_validate(request_values)
    with pytest.raises(RelayError, match="identity or policy"):
        execute_owned_session_cleanup_finalize(request)


@pytest.mark.parametrize("mutation", ["operation", "policy"])
def test_cleanup_finalize_rejects_receipt_identity_drift(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    mutation: str,
) -> None:
    report = _cleanup_sidecar_report()
    transaction = _FakeSessionTransaction(tmp_path, session_id="session-1")
    receipt_operation = "cleanup-other" if mutation == "operation" else "cleanup-sidecar"
    receipt_policy = dict(report.cleanup_policy)
    if mutation == "policy":
        receipt_policy["stop_worker"] = True
    transaction.atomic_write(
        "metadata.json",
        json.dumps(
            {
                "cleanup_operation_id": receipt_operation,
                "cleanup_policy": receipt_policy,
                "report": report.model_dump(mode="json"),
                "coordinator_report_ref": None,
            }
        ).encode("utf-8"),
    )
    status = OwnedSessionRecoveryStatus(
        cluster="ares",
        session_id="session-1",
        session_generation_id="generation-1",
        cleanup_receipt=True,
        cleanup_paths_pending=False,
        ownership_verified=True,
        recovery_verified=True,
    )

    def open_transaction(**_kwargs: object) -> _FakeSessionTransaction:
        return transaction

    def inspect_status(**_kwargs: object) -> OwnedSessionRecoveryStatus:
        return status

    monkeypatch.setattr(
        session_lifecycle,
        "open_owned_session_transaction",
        open_transaction,
    )
    monkeypatch.setattr(
        session_lifecycle,
        "inspect_owned_session_recovery_status",
        inspect_status,
    )
    monkeypatch.setattr(os, "geteuid", lambda: 1000, raising=False)
    request = OwnedSessionCleanupFinalizeRequest(
        cluster="ares",
        session_id="session-1",
        expected_session_generation_id="generation-1",
        expected_cleanup_operation_id="cleanup-sidecar",
        expected_cleanup_policy=report.cleanup_policy,
        coordinator_report=report,
        coordinator_report_sha256=session_lifecycle_report_sha256(report),
    )

    with pytest.raises(RelayError, match=f"receipt {mutation}"):
        execute_owned_session_cleanup_finalize(
            request,
            home=tmp_path,
            core_dir=tmp_path / "core",
        )


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    [
        ("truncate", "size"),
        ("tamper", "digest"),
        ("metadata_operation", "name"),
        ("request_reference", "refused"),
        ("request_generation", "refused"),
    ],
)
def test_cleanup_report_read_server_boundary_rejects_drift(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    mutation: str,
    expected_error: str,
) -> None:
    report = _cleanup_sidecar_report()
    reference, payload = session_lifecycle._coordinator_report_reference(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        report
    )
    transaction = _FakeSessionTransaction(tmp_path, session_id="session-1")
    (tmp_path / reference.name).write_bytes(payload)
    operation_id = "cleanup-other" if mutation == "metadata_operation" else "cleanup-sidecar"
    transaction.atomic_write(
        "metadata.json",
        json.dumps({"cleanup_operation_id": operation_id}).encode("utf-8"),
    )
    if mutation == "truncate":
        (tmp_path / reference.name).write_bytes(payload[:-1])
    elif mutation == "tamper":
        (tmp_path / reference.name).write_bytes(payload[:-1] + bytes([payload[-1] ^ 1]))
    request_reference = (
        reference.model_copy(update={"name": f"coordinator-cleanup-report-{'f' * 64}.json"})
        if mutation == "request_reference"
        else reference
    )
    request_generation = "generation-other" if mutation == "request_generation" else "generation-1"
    status = OwnedSessionRecoveryStatus(
        cluster="ares",
        session_id="session-1",
        session_generation_id="generation-1",
        cleanup_receipt=True,
        cleanup_paths_pending=False,
        coordinator_report_ref=reference,
        coordinator_report_sha256=reference.sha256,
        coordinator_report_bound=True,
        ownership_verified=True,
        recovery_verified=True,
    )

    def open_transaction(**_kwargs: object) -> _FakeSessionTransaction:
        return transaction

    def inspect_status(**_kwargs: object) -> OwnedSessionRecoveryStatus:
        return status

    monkeypatch.setattr(
        session_lifecycle,
        "open_owned_session_transaction",
        open_transaction,
    )
    monkeypatch.setattr(
        session_lifecycle,
        "inspect_owned_session_recovery_status",
        inspect_status,
    )
    monkeypatch.setattr(os, "geteuid", lambda: 1000, raising=False)
    request = OwnedSessionCleanupReportReadRequest(
        cluster="ares",
        session_id="session-1",
        expected_session_generation_id=request_generation,
        coordinator_report_ref=request_reference,
    )

    with pytest.raises(RelayError, match=expected_error):
        execute_owned_session_cleanup_report_read(
            request,
            home=tmp_path,
            core_dir=tmp_path / "core",
        )


def test_legacy_inline_cleanup_report_migration_recovers_after_metadata_write_failure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    report = _cleanup_sidecar_report()
    report_sha256 = session_lifecycle_report_sha256(report)

    class CrashOnceTransaction(_FakeSessionTransaction):
        """Fail once after sidecar publication but before metadata compaction."""

        def __init__(self, path: Path) -> None:
            super().__init__(path, session_id="session-1")
            self.fail_metadata_write = True
            self.successful_metadata_writes = 0

        def atomic_write(
            self,
            name: str,
            payload: bytes,
            *,
            maximum_bytes: int = 1024 * 1024,
        ) -> None:
            if name == "metadata.json" and self.fail_metadata_write:
                self.fail_metadata_write = False
                raise RelayError("simulated metadata compaction failure")
            super().atomic_write(name, payload, maximum_bytes=maximum_bytes)
            if name == "metadata.json":
                self.successful_metadata_writes += 1

    transaction = CrashOnceTransaction(tmp_path)
    initial_document = {
        "cleanup_operation_id": "cleanup-sidecar",
        "cleanup_policy": report.cleanup_policy,
        "report": report.model_dump(mode="json"),
        "coordinator_report": report.model_dump(mode="json"),
        "coordinator_report_sha256": report_sha256,
    }
    (tmp_path / "metadata.json").write_text(json.dumps(initial_document), encoding="utf-8")

    def inspect(**_kwargs: object) -> OwnedSessionRecoveryStatus:
        document = transaction.read_json("metadata.json")
        assert document is not None
        raw_reference = document.get("coordinator_report_ref")
        reference = (
            session_lifecycle.OwnedSessionCleanupReportReference.model_validate(raw_reference)
            if isinstance(raw_reference, dict)
            else None
        )
        return OwnedSessionRecoveryStatus(
            cluster="ares",
            session_id="session-1",
            session_generation_id="generation-1",
            cleanup_receipt=True,
            cleanup_paths_pending=False,
            coordinator_report_ref=reference,
            coordinator_report_sha256=reference.sha256 if reference is not None else report_sha256,
            coordinator_report_bound=True,
            ownership_verified=True,
            recovery_verified=True,
        )

    def open_transaction(**_kwargs: object) -> CrashOnceTransaction:
        return transaction

    monkeypatch.setattr(
        session_lifecycle,
        "open_owned_session_transaction",
        open_transaction,
    )
    monkeypatch.setattr(session_lifecycle, "inspect_owned_session_recovery_status", inspect)
    monkeypatch.setattr(os, "geteuid", lambda: 1000, raising=False)
    request = OwnedSessionCleanupFinalizeRequest(
        cluster="ares",
        session_id="session-1",
        expected_session_generation_id="generation-1",
        expected_cleanup_operation_id="cleanup-sidecar",
        expected_cleanup_policy=report.cleanup_policy,
        coordinator_report=report,
        coordinator_report_sha256=report_sha256,
    )

    with pytest.raises(RelayError, match="simulated metadata compaction failure"):
        execute_owned_session_cleanup_finalize(
            request,
            home=tmp_path,
            core_dir=tmp_path / "core",
        )
    reference, payload = session_lifecycle._coordinator_report_reference(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        report
    )
    assert (tmp_path / reference.name).read_bytes() == payload
    assert transaction.read_json("metadata.json") == initial_document

    recovered = execute_owned_session_cleanup_finalize(
        request,
        home=tmp_path,
        core_dir=tmp_path / "core",
    )
    compacted = transaction.read_json("metadata.json")

    assert recovered.coordinator_report_ref == reference
    assert compacted is not None
    assert compacted["coordinator_report_ref"] == reference.model_dump(mode="json")
    assert "coordinator_report" not in compacted
    assert "coordinator_report_sha256" not in compacted
    assert transaction.successful_metadata_writes == 1


def test_immutable_sidecar_publication_recovers_and_rejects_hostile_links(
    tmp_path: Path,
) -> None:
    report = _cleanup_sidecar_report()
    reference, payload = session_lifecycle._coordinator_report_reference(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        report
    )
    if os.name != "posix":
        transaction = _FakeSessionTransaction(tmp_path, session_id="session-1")
        transaction.atomic_write_immutable(
            reference.name,
            payload,
            maximum_bytes=session_lifecycle.MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
        )
        transaction.atomic_write_immutable(
            reference.name,
            payload,
            maximum_bytes=session_lifecycle.MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
        )
        with pytest.raises(RelayError, match="differs"):
            transaction.atomic_write_immutable(
                reference.name,
                payload + b"x",
                maximum_bytes=session_lifecycle.MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
            )
        return

    fcntl = cast(
        session_lifecycle._FcntlModule,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        importlib.import_module("fcntl"),
    )

    def transaction_for(path: Path) -> session_lifecycle._OwnedSessionTransaction:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        path.mkdir(mode=0o700)
        path.chmod(0o700)
        directory_fd = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
        )
        lock_path = path / "transition.lock"
        lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        lock_path.chmod(0o600)
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        return session_lifecycle._OwnedSessionTransaction(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            session_id="session-1",
            path=path,
            sessions_fd=os.dup(directory_fd),
            directory_fd=directory_fd,
            lock_fd=lock_fd,
            uid=os.geteuid(),
            _fcntl=fcntl,
        )

    pending_only = tmp_path / "pending-only"
    pending_path = pending_only / f".{reference.name}.pending"
    with transaction_for(pending_only) as transaction:
        pending_path.write_bytes(payload)
        pending_path.chmod(0o600)
        transaction.atomic_write_immutable(
            reference.name,
            payload,
            maximum_bytes=session_lifecycle.MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
        )
    assert (pending_only / reference.name).read_bytes() == payload
    assert not pending_path.exists()

    partial_pending = tmp_path / "partial-pending"
    pending_path = partial_pending / f".{reference.name}.pending"
    with transaction_for(partial_pending) as transaction:
        pending_path.write_bytes(payload[:37])
        pending_path.chmod(0o600)
        transaction.atomic_write_immutable(
            reference.name,
            payload,
            maximum_bytes=session_lifecycle.MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
        )
    assert (partial_pending / reference.name).read_bytes() == payload
    assert not pending_path.exists()

    linked_window = tmp_path / "linked-window"
    pending_path = linked_window / f".{reference.name}.pending"
    final_path = linked_window / reference.name
    with transaction_for(linked_window) as transaction:
        pending_path.write_bytes(payload)
        pending_path.chmod(0o600)
        os.link(pending_path, final_path)
        assert final_path.stat().st_nlink == 2
        transaction.atomic_write_immutable(
            reference.name,
            payload,
            maximum_bytes=session_lifecycle.MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
        )
    assert final_path.read_bytes() == payload
    assert final_path.stat().st_nlink == 1
    assert not pending_path.exists()

    corrupt_linked_window = tmp_path / "corrupt-linked-window"
    pending_path = corrupt_linked_window / f".{reference.name}.pending"
    final_path = corrupt_linked_window / reference.name
    with transaction_for(corrupt_linked_window) as transaction:
        pending_path.write_bytes(payload[:-1] + bytes([payload[-1] ^ 1]))
        pending_path.chmod(0o600)
        os.link(pending_path, final_path)
        with pytest.raises(RelayError, match="linked file differs"):
            transaction.atomic_write_immutable(
                reference.name,
                payload,
                maximum_bytes=session_lifecycle.MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
            )
        assert final_path.stat().st_nlink == 2
        assert pending_path.exists()

    ambiguous = tmp_path / "ambiguous"
    pending_path = ambiguous / f".{reference.name}.pending"
    final_path = ambiguous / reference.name
    with transaction_for(ambiguous) as transaction:
        pending_path.write_bytes(payload)
        pending_path.chmod(0o600)
        final_path.write_bytes(payload)
        final_path.chmod(0o600)
        with pytest.raises(RelayError, match="ambiguous"):
            transaction.atomic_write_immutable(
                reference.name,
                payload,
                maximum_bytes=session_lifecycle.MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
            )

    hardlinked = tmp_path / "hardlinked"
    final_path = hardlinked / reference.name
    outside_link = tmp_path / "outside-hardlink.json"
    with transaction_for(hardlinked) as transaction:
        final_path.write_bytes(payload)
        final_path.chmod(0o600)
        os.link(final_path, outside_link)
        with pytest.raises(RelayError, match="owner-private regular"):
            transaction.atomic_write_immutable(
                reference.name,
                payload,
                maximum_bytes=session_lifecycle.MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
            )

    symlinked = tmp_path / "symlinked"
    final_path = symlinked / reference.name
    symlink_target = tmp_path / "symlink-target.json"
    symlink_target.write_bytes(payload)
    symlink_target.chmod(0o600)
    with transaction_for(symlinked) as transaction:
        final_path.symlink_to(symlink_target)
        with pytest.raises(RelayError, match="opened safely|owner-private regular"):
            transaction.atomic_write_immutable(
                reference.name,
                payload,
                maximum_bytes=session_lifecycle.MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
            )


def test_cleanup_report_retention_prunes_one_old_generation_and_preserves_current(
    tmp_path: Path,
) -> None:
    old_report = _cleanup_sidecar_report()
    current_report = old_report.model_copy(
        update={
            "session_generation_id": "generation-2",
            "cleanup_operation_id": "cleanup-sidecar-2",
        }
    )
    old_reference, old_payload = session_lifecycle._coordinator_report_reference(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        old_report
    )
    current_reference, current_payload = session_lifecycle._coordinator_report_reference(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        current_report
    )
    transaction = _FakeSessionTransaction(tmp_path, session_id="session-1")
    (tmp_path / old_reference.name).write_bytes(old_payload)
    (tmp_path / current_reference.name).write_bytes(current_payload)

    session_lifecycle._prune_unreferenced_cleanup_report_sidecars(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        transaction,  # pyright: ignore[reportArgumentType]
        preserve_names={
            current_reference.name,
            f".{current_reference.name}.pending",
        },
    )

    assert not (tmp_path / old_reference.name).exists()
    assert (tmp_path / current_reference.name).read_bytes() == current_payload


@pytest.mark.parametrize("mutation", ["multiple", "pending"])
def test_cleanup_report_retention_refuses_ambiguous_old_candidates(
    tmp_path: Path,
    mutation: str,
) -> None:
    current = _cleanup_sidecar_report().model_copy(
        update={
            "session_generation_id": "generation-current",
            "cleanup_operation_id": "cleanup-current",
        }
    )
    current_reference, _ = session_lifecycle._coordinator_report_reference(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        current
    )
    transaction = _FakeSessionTransaction(tmp_path, session_id="session-1")
    if mutation == "pending":
        old = _cleanup_sidecar_report()
        old_reference, payload = session_lifecycle._coordinator_report_reference(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            old
        )
        (tmp_path / f".{old_reference.name}.pending").write_bytes(payload)
        expected = "unreferenced cleanup report pending"
    else:
        for index in range(2):
            old = _cleanup_sidecar_report().model_copy(
                update={
                    "session_generation_id": f"generation-old-{index}",
                    "cleanup_operation_id": f"cleanup-old-{index}",
                }
            )
            old_reference, payload = session_lifecycle._coordinator_report_reference(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                old
            )
            (tmp_path / old_reference.name).write_bytes(payload)
        expected = "multiple unreferenced"

    with pytest.raises(RelayError, match=expected):
        session_lifecycle._prune_unreferenced_cleanup_report_sidecars(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            transaction,  # pyright: ignore[reportArgumentType]
            preserve_names={
                current_reference.name,
                f".{current_reference.name}.pending",
            },
        )


@pytest.mark.parametrize(
    ("names", "expected_error"),
    [
        (["coordinator-cleanup-report-invalid.json"], "invalid name"),
        (
            [f"coordinator-cleanup-report-{index:064x}.json" for index in range(5)],
            "too many cleanup report candidates",
        ),
        ([f"ordinary-{index}" for index in range(257)], "directory exceeds its entry limit"),
    ],
)
def test_cleanup_report_candidate_scan_is_bounded_and_strict(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    names: list[str],
    expected_error: str,
) -> None:
    class FakeScan:
        def __enter__(self) -> object:
            return iter(SimpleNamespace(name=name) for name in names)

        def __exit__(self, *_args: object) -> None:
            return None

    def scan_directory(_path: int) -> FakeScan:
        return FakeScan()

    monkeypatch.setattr(os, "scandir", scan_directory)
    transaction = session_lifecycle._OwnedSessionTransaction(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        session_id="session-1",
        path=tmp_path,
        sessions_fd=-1,
        directory_fd=-1,
        lock_fd=-1,
        uid=0,
        _fcntl=cast(
            session_lifecycle._FcntlModule,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            SimpleNamespace(),
        ),
    )

    with pytest.raises(RelayError, match=expected_error):
        transaction.cleanup_report_candidate_names()


@pytest.mark.parametrize("mutation", ["symlink", "hardlink"])
def test_cleanup_report_retention_refuses_hostile_old_links(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    mutation: str,
) -> None:
    report = _cleanup_sidecar_report()
    reference, payload = session_lifecycle._coordinator_report_reference(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        report
    )
    preserved_report = report.model_copy(
        update={
            "session_generation_id": "generation-preserved",
            "cleanup_operation_id": "cleanup-preserved",
        }
    )
    preserved_reference, _ = session_lifecycle._coordinator_report_reference(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        preserved_report
    )
    if os.name != "posix":
        transaction = _FakeSessionTransaction(tmp_path, session_id="session-1")
        (tmp_path / reference.name).write_bytes(payload)

        def refuse_hostile(
            _name: str,
            *,
            required: bool = True,
        ) -> os.stat_result | None:
            assert required is True
            raise RelayError("owned session file is not one owner-private regular file")

        monkeypatch.setattr(transaction, "stat_regular", refuse_hostile)
        with pytest.raises(RelayError, match="owner-private regular"):
            session_lifecycle._prune_unreferenced_cleanup_report_sidecars(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                transaction,  # pyright: ignore[reportArgumentType]
                preserve_names={preserved_reference.name},
            )
        return

    directory = tmp_path / mutation
    directory.mkdir(mode=0o700)
    directory_fd = os.open(
        directory,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
    )
    lock_path = directory / "transition.lock"
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
    fcntl = cast(
        session_lifecycle._FcntlModule,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        importlib.import_module("fcntl"),
    )
    fcntl.flock(lock_fd, fcntl.LOCK_EX)
    with session_lifecycle._OwnedSessionTransaction(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        session_id="session-1",
        path=directory,
        sessions_fd=os.dup(directory_fd),
        directory_fd=directory_fd,
        lock_fd=lock_fd,
        uid=os.geteuid(),
        _fcntl=fcntl,
    ) as transaction:
        candidate = directory / reference.name
        if mutation == "symlink":
            target = tmp_path / "retention-symlink-target"
            target.write_bytes(payload)
            target.chmod(0o600)
            candidate.symlink_to(target)
        else:
            candidate.write_bytes(payload)
            candidate.chmod(0o600)
            os.link(candidate, tmp_path / "retention-outside-hardlink")
        with pytest.raises(RelayError, match="owner-private regular"):
            session_lifecycle._prune_unreferenced_cleanup_report_sidecars(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                transaction,
                preserve_names={preserved_reference.name},
            )


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    [
        ("truncate", "size"),
        ("tamper", "digest"),
        ("symlink", "owner-private regular"),
        ("reference", "name"),
    ],
)
def test_coordinator_report_sidecar_rejects_identity_drift(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    mutation: str,
    expected_error: str,
) -> None:
    report = SessionLifecycleReport(
        cluster="ares",
        session_id="session-1",
        session_generation_id="generation-1",
        mode="teardown",
        cleanup_operation_id="cleanup-sidecar",
        cleanup_policy={
            "stop_worker": False,
            "cancel_jobs": False,
            "cancel_scheduler_jobs": False,
        },
    )
    reference, payload = session_lifecycle._coordinator_report_reference(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        report
    )
    transaction = _FakeSessionTransaction(tmp_path, session_id="session-1")
    transaction.atomic_write_immutable(
        reference.name,
        payload,
        maximum_bytes=session_lifecycle.MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
    )
    selected_reference = reference
    if mutation == "truncate":
        (tmp_path / reference.name).write_bytes(payload[:-1])
    elif mutation == "tamper":
        (tmp_path / reference.name).write_bytes(payload[:-1] + bytes([payload[-1] ^ 1]))
    elif mutation == "symlink":
        original_read = transaction.read_bytes

        def reject_symlink(
            name: str,
            *,
            maximum_bytes: int,
            required: bool = True,
        ) -> bytes | None:
            if name == reference.name:
                raise RelayError(
                    f"owned session file is not one owner-private regular file: {name}"
                )
            return original_read(name, maximum_bytes=maximum_bytes, required=required)

        monkeypatch.setattr(transaction, "read_bytes", reject_symlink)
    else:
        selected_reference = reference.model_copy(
            update={"name": f"coordinator-cleanup-report-{'f' * 64}.json"}
        )

    with pytest.raises(RelayError, match=expected_error):
        session_lifecycle._read_coordinator_report_sidecar(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            transaction,  # pyright: ignore[reportArgumentType]
            selected_reference,
            expected_session_generation_id="generation-1",
            expected_cleanup_operation_id="cleanup-sidecar",
        )


def test_large_cleanup_finalize_uses_separate_bounded_ssh_stdin(
    monkeypatch: MonkeyPatch,
) -> None:
    marker = "report-body-must-not-enter-the-shell"
    report = SessionLifecycleReport(
        cluster="ares",
        session_id="session-1",
        session_generation_id="generation-1",
        mode="teardown",
        cleanup_operation_id="cleanup-transport",
        cleanup_policy={
            "stop_worker": False,
            "cancel_jobs": False,
            "cancel_scheduler_jobs": False,
        },
        resources=[
            CleanupResource(
                kind="relay_job",
                resource_id="job-1",
                location="ares",
                action="retain",
                ownership_verified=True,
                outcome="retained",
                verified_after_operation=True,
                detail=marker + ("x" * (1100 * 1024)),
            )
        ],
    )
    reference, report_payload = session_lifecycle._coordinator_report_reference(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        report
    )
    assert len(report_payload) > 1024 * 1024
    status = OwnedSessionRecoveryStatus(
        cluster="ares",
        session_id="session-1",
        session_generation_id="generation-1",
        cleanup_receipt=True,
        cleanup_paths_pending=False,
        coordinator_report_ref=reference,
        coordinator_report_sha256=reference.sha256,
        coordinator_report_bound=True,
        ownership_verified=True,
        recovery_verified=True,
    )
    status_payload = status.model_dump_json().encode("utf-8")
    assert len(status_payload) < 1024 * 1024
    assert status.coordinator_report is None
    observed: dict[str, object] = {}

    def run_bounded(
        command: list[str],
        *,
        input_bytes: bytes,
        timeout_seconds: float,
        stdout_limit: int,
        stderr_limit: int,
        environment: dict[str, str] | None = None,
    ) -> session_lifecycle._BoundedCommandResult:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        observed.update(
            command=command,
            input_bytes=input_bytes,
            timeout_seconds=timeout_seconds,
            stdout_limit=stdout_limit,
            stderr_limit=stderr_limit,
            environment=environment,
        )
        return session_lifecycle._BoundedCommandResult(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            returncode=0,
            stdout=status_payload,
            stderr=b"",
        )

    monkeypatch.setattr(session_lifecycle, "_run_bounded_command", run_bounded)
    finalized = session_lifecycle.finalize_remote_session_cleanup_report(
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        cluster="ares",
        session_id="session-1",
        session_generation_id="generation-1",
        cleanup_operation_id="cleanup-transport",
        cleanup_policy=report.cleanup_policy,
        report=report,
    )

    command = cast(list[str], observed["command"])
    input_bytes = cast(bytes, observed["input_bytes"])
    assert finalized.coordinator_report_ref == reference
    assert len(input_bytes) > 1024 * 1024
    assert marker.encode("utf-8") in input_bytes
    assert marker not in " ".join(command)
    assert len(" ".join(command).encode("utf-8")) < 64 * 1024
    assert observed["stdout_limit"] == 1024 * 1024
    with pytest.raises(RelayError, match="stdin exceeds"):
        session_lifecycle._ssh_stdin_command(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            ClusterDefinition(name="ares", ssh_host="ares"),
            "true",
            input_bytes=b"oversized",
            input_limit=1,
            stdout_limit=1,
        )


def test_scheduler_cancellation_evidence_rejects_an_extra_relay_link() -> None:
    report = SessionLifecycleReport(
        cluster="ares",
        session_id="session-1",
        session_generation_id="generation-1",
        mode="teardown",
        scheduler_cancel_requested=True,
        resources=[
            CleanupResource(
                kind="relay_job",
                resource_id="relay-1",
                location="ares",
                action="cancel",
                ownership_verified=True,
                outcome="canceled",
                verified_after_operation=True,
                metadata={"scheduler_job_ids": ["scheduler-1"]},
            ),
            *[
                CleanupResource(
                    kind="scheduler_job",
                    resource_id=scheduler_id,
                    location="ares",
                    action="cancel",
                    ownership_verified=True,
                    outcome="canceled",
                    verified_after_operation=True,
                    metadata={"relay_job_id": "relay-1"},
                )
                for scheduler_id in ("scheduler-1", "scheduler-unexpected")
            ],
        ],
    )

    checks = {
        check.check_id: check for check in report.to_live_validation_report(cancel_jobs=True).checks
    }

    assert checks[SESSION_SCHEDULER_CANCELED_CHECK_ID].status.value == "failed"


def test_scheduler_cancellation_evidence_rejects_a_missing_gateway_record() -> None:
    observed_at = datetime.now(UTC)
    report = SessionLifecycleReport(
        cluster="ares",
        session_id="session-1",
        session_generation_id="generation-1",
        mode="teardown",
        scheduler_cancel_requested=True,
        prior_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id="generation-1",
            running=True,
            ownership_verified=True,
            observed_at=observed_at,
        ),
        post_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id="generation-1",
            running=False,
            ownership_verified=True,
            observed_at=observed_at,
        ),
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id="123",
                location="ares",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="scheduler_job",
                resource_id="scheduler-1",
                location="ares",
                provider="slurm",
                action="cancel",
                ownership_verified=True,
                outcome="canceled",
                verified_after_operation=True,
                metadata={"gateway_session_id": "missing-gateway"},
            ),
        ],
    )

    canonical = report.to_live_validation_report()
    checks = {check.check_id: check for check in canonical.checks}

    assert checks[SESSION_SCHEDULER_CANCELED_CHECK_ID].status.value == "failed"
    assert canonical.status.value == "failed"


def test_scheduler_cancellation_evidence_accepts_a_linked_gateway_cleanup() -> None:
    observed_at = datetime.now(UTC)
    report = SessionLifecycleReport(
        cluster="ares",
        session_id="session-1",
        session_generation_id="generation-1",
        mode="teardown",
        scheduler_cancel_requested=True,
        prior_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id="generation-1",
            running=True,
            ownership_verified=True,
            observed_at=observed_at,
        ),
        post_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id="generation-1",
            running=False,
            ownership_verified=True,
            observed_at=observed_at,
        ),
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id="123",
                location="ares",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="desktop_connector",
                resource_id="desktop-connector-1",
                location="desktop",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
                metadata={"gateway_session_id": "gateway-1"},
            ),
            CleanupResource(
                kind="remote_connector",
                resource_id="remote-connector-1",
                location="ares",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
                metadata={"gateway_session_id": "gateway-1"},
            ),
            CleanupResource(
                kind="gateway_record",
                resource_id="gateway-1",
                location="desktop",
                action="close",
                ownership_verified=True,
                outcome="closed",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="scheduler_job",
                resource_id="scheduler-1",
                location="ares",
                provider="slurm",
                action="cancel",
                ownership_verified=True,
                outcome="canceled",
                verified_after_operation=True,
                metadata={"gateway_session_id": "gateway-1"},
            ),
        ],
    )

    canonical = report.to_live_validation_report()
    checks = {check.check_id: check for check in canonical.checks}

    assert checks[SESSION_SCHEDULER_CANCELED_CHECK_ID].status.value == "passed"
    assert canonical.status.value == "passed"


def test_start_remote_session_writes_owned_pid_and_metadata(monkeypatch: MonkeyPatch) -> None:
    scripts: list[str] = []

    def fake_ssh(_definition: ClusterDefinition, script: str) -> str:
        scripts.append(script)
        return "session_started=session-1\napi_pid=123\nremote_api_port=9001\n"

    monkeypatch.setattr(session_lifecycle, "_ssh_script", fake_ssh)

    lines = start_remote_session(
        cluster="ares",
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        session_id="session-1",
        remote_api_port=9001,
        api_token="token",
        expected_api_release_identity=_api_release_identity(),
    )

    assert "session_started=session-1" in lines
    script = scripts[0]
    assert "CLIO_RELAY_API_TOKEN='token'" in script
    assert "clio-relay session start-owned" in script
    assert "umask 077" in script
    assert '"cluster":"ares"' in script
    assert '"session_id":"session-1"' in script
    assert '"remote_api_port":9001' in script
    assert '"require_token":true' in script
    assert '"replace":false' in script
    assert "cluster_registry_sha256" in script
    assert "cluster_route_revision" in script
    assert "clio-relay.session-api-release.v1" in script
    assert "exec 9>" not in script
    assert "kill --" not in script
    assert "last_cleanup" not in script
    assert "\x00" not in script
    assert "pkill" not in script


def test_start_remote_session_checks_existing_api_release_before_reuse(
    monkeypatch: MonkeyPatch,
) -> None:
    scripts: list[str] = []

    def fake_ssh(_definition: ClusterDefinition, script: str) -> str:
        scripts.append(script)
        return "session_already_running=session-1\n"

    monkeypatch.setattr(session_lifecycle, "_ssh_script", fake_ssh)

    start_remote_session(
        cluster="ares",
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        session_id="session-1",
        remote_api_port=9001,
        api_token="token",
        expected_api_release_identity=_api_release_identity(),
        replace=False,
    )

    script = scripts[0]
    assert "clio-relay session start-owned" in script
    assert '"replace":false' in script
    assert '"expected_api_release_identity"' in script
    assert "exec 9>" not in script


def test_start_remote_session_stages_large_registry_without_python_argv(
    monkeypatch: MonkeyPatch,
) -> None:
    scripts: list[str] = []

    def fake_ssh(_definition: ClusterDefinition, script: str) -> str:
        scripts.append(script)
        return "session_started=session-1\n"

    monkeypatch.setattr(session_lifecycle, "_ssh_script", fake_ssh)
    registration = RemoteMcpServerConfig(
        command="science-mcp",
        args=[f"{index:03d}-" + ("x" * 4_000) for index in range(40)],
        allow_tools=["inspect"],
    )

    start_remote_session(
        cluster="alpha",
        definition=ClusterDefinition(
            name="alpha",
            ssh_host="alpha",
            remote_mcp_servers={"science": registration},
        ),
        session_id="session-1",
        remote_api_port=9001,
        api_token="token",
        expected_api_release_identity=_api_release_identity(),
    )

    script = scripts[0]
    assert len(script.encode("utf-8")) > 128 * 1024
    assert "printf '%s'" in script
    assert "clio-relay session start-owned" in script
    assert "python3 -" not in script


def test_start_remote_session_rejects_registry_over_configuration_limit(
    monkeypatch: MonkeyPatch,
) -> None:
    def unexpected_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        del args, kwargs
        pytest.fail("oversized session authority must fail before SSH")

    monkeypatch.setattr(session_lifecycle, "_ssh_script", unexpected_run)
    registration = RemoteMcpServerConfig(
        command="science-mcp",
        args=["x" * 4_000 for _ in range(256)],
        allow_tools=["inspect"],
    )
    definition = ClusterDefinition(
        name="alpha",
        ssh_host="alpha",
        remote_mcp_servers={f"science-{index}": registration for index in range(5)},
    )

    with pytest.raises(
        RelayError,
        match=rf"{MAX_CLUSTER_REGISTRY_BYTES}-byte configuration limit",
    ):
        start_remote_session(
            cluster="alpha",
            definition=definition,
            session_id="session-1",
            remote_api_port=9001,
            api_token="token",
            expected_api_release_identity=_api_release_identity(),
        )


def test_status_remote_session_returns_json(monkeypatch: MonkeyPatch) -> None:
    scripts: list[str] = []

    def fake_ssh(_definition: ClusterDefinition, script: str) -> str:
        scripts.append(script)
        return json.dumps({"session_id": "session-1", "running": True})

    monkeypatch.setattr(session_lifecycle, "_ssh_script", fake_ssh)

    status = status_remote_session(
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        session_id="session-1",
    )

    assert status == {"session_id": "session-1", "running": True}
    script = scripts[0]
    command_index = script.index("clio-relay session recovery-status")
    for export in (
        'export PATH="$HOME/.local/bin:$PATH";',
        "export CLIO_RELAY_CLI_MODE=local;",
        "export CLIO_RELAY_REMOTE_CLUSTER=ares;",
        "export CLIO_RELAY_CORE_DIR=",
        "export CLIO_RELAY_SPOOL_DIR=",
    ):
        assert script.index(export) < command_index
    assert "metadata.json" not in script
    assert "--pre-start-cleanup-probe" not in script


def test_status_remote_session_marks_pre_start_cleanup_probe_explicitly(
    monkeypatch: MonkeyPatch,
) -> None:
    scripts: list[str] = []

    def fake_ssh(_definition: ClusterDefinition, script: str) -> str:
        scripts.append(script)
        return OwnedSessionRecoveryStatus(
            cluster="ares",
            session_id="fresh-session",
            cleanup_receipt=False,
            recovery_verified=False,
            errors=["owned session transition is not currently observable"],
        ).model_dump_json()

    monkeypatch.setattr(session_lifecycle, "_ssh_script", fake_ssh)

    status = status_remote_session(
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        session_id="fresh-session",
        pre_start_cleanup_probe=True,
    )

    assert status["cleanup_receipt"] is False
    assert status["recovery_verified"] is False
    assert (
        "clio-relay session recovery-status --cluster ares "
        "--session-id fresh-session --pre-start-cleanup-probe"
    ) in scripts[0]


def test_remote_session_start_status_uses_cluster_environment(
    monkeypatch: MonkeyPatch,
) -> None:
    scripts: list[str] = []
    definition, _release, plan = _durable_start_plan()
    expected = _durable_start_status(plan)

    def fake_ssh(
        _definition: ClusterDefinition,
        script: str,
        *,
        timeout_seconds: float,
    ) -> str:
        assert timeout_seconds > 0
        scripts.append(script)
        return expected.model_dump_json()

    monkeypatch.setattr(session_lifecycle, "_ssh_script", fake_ssh)

    observed = session_lifecycle.status_remote_session_start(
        definition=definition,
        selector=plan.status_selector,
    )

    assert observed == expected
    script = scripts[0]
    command_index = script.index("clio-relay session start-status-owned")
    for export in (
        'export PATH="$HOME/.local/bin:$PATH";',
        "export CLIO_RELAY_CLI_MODE=local;",
        "export CLIO_RELAY_REMOTE_CLUSTER=ares;",
        "export CLIO_RELAY_CORE_DIR=",
        "export CLIO_RELAY_SPOOL_DIR=",
    ):
        assert script.index(export) < command_index


def test_remote_session_identity_challenge_binds_process_cluster_and_nonce(
    monkeypatch: MonkeyPatch,
) -> None:
    scripts: list[str] = []
    nonce = "1" * 64
    expected = {
        "schema_version": "clio-relay.session-identity.v1",
        "cluster": "ares",
        "session_id": "session-1",
        "session_generation_id": "generation-1",
        "nonce": nonce,
        "hmac_sha256": "a" * 64,
    }

    def fake_ssh(_definition: ClusterDefinition, script: str) -> str:
        scripts.append(script)
        return json.dumps(expected)

    monkeypatch.setattr(session_lifecycle, "_ssh_script", fake_ssh)

    observed = challenge_remote_session_identity(
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        session_id="session-1",
        session_generation_id="generation-1",
        nonce=nonce,
    )

    assert observed == expected
    script = scripts[0]
    assert "clio-relay session challenge-owned" in script
    assert '"session_generation_id":"generation-1"' in script
    assert f'"nonce":"{nonce}"' in script
    assert "metadata.json" not in script
    with pytest.raises(ValueError, match="256-bit"):
        challenge_remote_session_identity(
            definition=ClusterDefinition(name="ares", ssh_host="ares"),
            session_id="session-1",
            session_generation_id="generation-1",
            nonce="weak",
        )


def test_remote_session_command_timeout_is_reported(monkeypatch: MonkeyPatch) -> None:
    def timed_out(*_args: object, **_kwargs: object) -> session_lifecycle._BoundedCommandResult:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        raise RelayError("bounded command timed out after 120 seconds")

    monkeypatch.setattr(session_lifecycle, "_run_bounded_command", timed_out)

    with pytest.raises(RelayError, match="timed out after 120 seconds"):
        status_remote_session(
            definition=ClusterDefinition(name="ares", ssh_host="ares"),
            session_id="session-1",
        )


def test_detach_remote_session_retains_verified_remote_api(monkeypatch: MonkeyPatch) -> None:
    def fake_status(**_kwargs: object) -> dict[str, object]:
        return {
            "session_id": "session-1",
            "api_pid": 123,
            "running": True,
            "ownership_verified": True,
            "session_generation_id": "generation-123",
        }

    monkeypatch.setattr(
        "clio_relay.session_lifecycle.status_remote_session",
        fake_status,
    )

    report = detach_remote_session(
        definition=ClusterDefinition(name="cluster", ssh_host="cluster"),
        session_id="session-1",
        cluster="cluster",
    )

    assert report.mode == "detach"
    assert report.session_generation_id == "generation-123"
    assert report.resources[0].action == "retain"
    assert report.resources[0].outcome == "retained"
    assert report.resources[0].ownership_verified is True
    assert report.residual_resources == []
    cleanup = report.to_cleanup_evidence()
    assert cleanup.mode == "detach"
    assert cleanup.remaining_resources == []
    assert report.validation_resources()[0].kind == "relay_session"
    assert report.json_payload()["cleanup_evidence"] == cleanup.model_dump(mode="json")


@pytest.mark.parametrize(
    ("running", "ownership_verified", "expected_outcome"),
    [(False, True, "missing"), (True, False, "refused")],
)
def test_detach_remote_session_rejects_unverified_remote_api_retention(
    monkeypatch: MonkeyPatch,
    running: bool,
    ownership_verified: bool,
    expected_outcome: str,
) -> None:
    def fake_status(**_kwargs: object) -> dict[str, object]:
        return {
            "session_id": "session-1",
            "api_pid": 123,
            "running": running,
            "ownership_verified": ownership_verified,
            "session_generation_id": "generation-123",
        }

    monkeypatch.setattr("clio_relay.session_lifecycle.status_remote_session", fake_status)

    report = detach_remote_session(
        definition=ClusterDefinition(name="cluster", ssh_host="cluster"),
        session_id="session-1",
        cluster="cluster",
    )

    assert report.resources[0].outcome == expected_outcome
    assert report.resources[0].verified_after_operation is False
    assert report.resources[0].residual is True
    assert report.errors
    assert report.json_payload()["ok"] is False
    assert report.to_live_validation_report().status.value == "failed"


def test_detach_report_requires_verified_connector_and_gateway_dispositions() -> None:
    report = SessionLifecycleReport(
        cluster="cluster",
        session_id="session-1",
        session_generation_id="generation-1",
        mode="detach",
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id="123",
                location="cluster",
                action="retain",
                ownership_verified=True,
                outcome="retained",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="desktop_connector",
                resource_id="456",
                location="desktop",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
                metadata={"gateway_session_id": "gateway-1"},
            ),
            CleanupResource(
                kind="remote_connector",
                resource_id="789",
                location="cluster",
                action="retain",
                ownership_verified=True,
                outcome="retained",
                verified_after_operation=True,
                metadata={"gateway_session_id": "gateway-1"},
            ),
            CleanupResource(
                kind="gateway_record",
                resource_id="gateway-1",
                location="desktop",
                action="retain",
                ownership_verified=True,
                outcome="retained",
                verified_after_operation=True,
                observed_state="degraded",
            ),
        ],
    )

    canonical = report.to_live_validation_report()
    checks = {check.check_id: check.status.value for check in canonical.checks}

    assert checks[SESSION_CONNECTORS_CHECK_ID] == "passed"
    assert checks[SESSION_GATEWAY_CHECK_ID] == "passed"
    assert canonical.status.value == "passed"

    missing_gateway = report.model_copy(
        update={
            "resources": [
                resource for resource in report.resources if resource.kind != "gateway_record"
            ]
        }
    )
    incomplete_checks = {
        check.check_id: check.status.value
        for check in missing_gateway.to_live_validation_report().checks
    }
    assert incomplete_checks[SESSION_CONNECTORS_CHECK_ID] == "failed"

    duplicate_first_gateway = report.model_copy(
        update={
            "resources": [
                *report.resources,
                *[
                    resource.model_copy(update={"resource_id": f"duplicate-{resource.resource_id}"})
                    for resource in report.resources
                    if resource.kind in {"desktop_connector", "remote_connector"}
                ],
                CleanupResource(
                    kind="gateway_record",
                    resource_id="gateway-2",
                    location="desktop",
                    action="retain",
                    ownership_verified=True,
                    outcome="retained",
                    verified_after_operation=True,
                ),
            ]
        }
    )
    duplicate_checks = {
        check.check_id: check.status.value
        for check in duplicate_first_gateway.to_live_validation_report().checks
    }
    assert duplicate_checks[SESSION_CONNECTORS_CHECK_ID] == "failed"


@pytest.mark.parametrize(
    ("outcome", "observed_state"),
    [("stopped", "inactive"), ("missing", "not-found")],
)
def test_worker_cleanup_requires_exact_terminal_post_stop_evidence(
    outcome: Literal["stopped", "missing"],
    observed_state: str,
) -> None:
    observed_at = datetime.now(UTC)
    report = SessionLifecycleReport(
        cluster="cluster",
        session_id="session-1",
        session_generation_id="generation-1",
        mode="teardown",
        prior_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id="generation-1",
            running=True,
            ownership_verified=True,
            observed_at=observed_at,
        ),
        post_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id="generation-1",
            running=False,
            ownership_verified=True,
            observed_at=observed_at,
        ),
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id="123",
                location="cluster",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="worker_service",
                resource_id="clio-relay-worker-cluster.service",
                location="cluster",
                action="stop",
                ownership_verified=True,
                outcome=outcome,
                verified_after_operation=True,
                observed_state=observed_state,
            ),
        ],
    )

    canonical = report.to_live_validation_report(stop_worker=True)
    worker_check = next(
        check for check in canonical.checks if check.check_id == SESSION_WORKER_CHECK_ID
    )

    assert worker_check.status.value == "passed"
    assert canonical.status.value == "passed"
    cleanup_payload = report.json_payload()["cleanup_evidence"]
    assert isinstance(cleanup_payload, dict)
    assert cleanup_payload["stop_worker"] is True


def test_teardown_remote_session_kills_owned_pid_and_optional_worker(
    monkeypatch: MonkeyPatch,
) -> None:
    scripts: list[str] = []

    def fake_ssh(_definition: ClusterDefinition, script: str) -> str:
        scripts.append(script)
        return json.dumps(
            {
                "cluster": "ares",
                "session_id": "session-1",
                "mode": "teardown",
                "cleanup_operation_id": "cleanup-test",
                "cleanup_policy": {
                    "stop_worker": True,
                    "cancel_jobs": False,
                    "cancel_scheduler_jobs": False,
                },
                "relay_cancel_requested": False,
                "scheduler_cancel_requested": False,
                "resources": [
                    {
                        "kind": "remote_relay_api",
                        "resource_id": "123",
                        "location": "ares",
                        "action": "stop",
                        "ownership_verified": True,
                        "outcome": "stopped",
                        "residual": False,
                    },
                    {
                        "kind": "worker_service",
                        "resource_id": "clio-relay-worker-ares.service",
                        "location": "ares",
                        "action": "stop",
                        "ownership_verified": True,
                        "outcome": "stopped",
                        "residual": False,
                    },
                ],
                "errors": [],
            }
        )

    monkeypatch.setattr(session_lifecycle, "_ssh_script", fake_ssh)

    report = teardown_remote_session(
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        session_id="session-1",
        expected_session_generation_id="generation-1",
        expected_cleanup_operation_id="cleanup-test",
        stop_worker=True,
        cluster="ares",
    )

    assert report.resources[0].outcome == "stopped"
    assert report.resources[1].resource_id == "clio-relay-worker-ares.service"
    assert report.to_cleanup_evidence(stop_worker=True).stop_worker is True
    assert "clio-relay session teardown-owned" in scripts[0]
    assert '"expected_cleanup_operation_id":"cleanup-test"' in scripts[0]
    assert '"expected_session_generation_id":"generation-1"' in scripts[0]
    assert '"stop_worker":true' in scripts[0]
    assert "os.killpg" not in scripts[0]
    assert "last_cleanup" not in scripts[0]

    with pytest.raises(RelayError, match="cleanup operation does not match"):
        teardown_remote_session(
            definition=ClusterDefinition(name="ares", ssh_host="ares"),
            session_id="session-1",
            expected_session_generation_id="generation-1",
            expected_cleanup_operation_id="cleanup-other",
            stop_worker=True,
            cluster="ares",
        )


def test_owned_teardown_delegates_to_pinned_cluster_local_executor() -> None:
    script = session_lifecycle._owned_teardown_script(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        definition=ClusterDefinition(name="ares", ssh_host="ares"),
        session_id="session-1",
        expected_session_generation_id="generation-1",
        expected_cleanup_operation_id="cleanup-test",
        stop_worker=False,
        cancel_jobs=False,
        cancel_scheduler_jobs=False,
        cluster="ares",
    )

    assert "clio-relay session teardown-owned" in script
    assert '"expected_session_generation_id":"generation-1"' in script
    assert '"expected_cleanup_operation_id":"cleanup-test"' in script
    assert "os.killpg" not in script
    assert "exec 9>" not in script
    assert "metadata.json" not in script
    assert "last_cleanup" not in script
