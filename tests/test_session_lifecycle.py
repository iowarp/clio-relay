from __future__ import annotations

import json
import os
import stat
import subprocess
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
    OwnedSessionCleanupTarget,
    OwnedSessionRecoveryStatus,
    OwnedSessionStartRequest,
    RemoteSessionStateEvidence,
    SessionApiReleaseIdentity,
    SessionLifecycleReport,
    challenge_remote_session_identity,
    detach_remote_session,
    execute_owned_session_cleanup_finalize,
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

    def atomic_write(self, name: str, payload: bytes) -> None:
        (self.path / name).write_bytes(payload)

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
        if not path.exists():
            if required:
                raise AssertionError(f"missing required test file: {name}")
            return None
        payload = path.read_bytes()
        if len(payload) > maximum_bytes:
            raise AssertionError("test transaction observed an unexpected bounded read")
        return payload

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


@pytest.fixture(autouse=True)
def _use_fake_recorded_scope(monkeypatch: MonkeyPatch) -> None:
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
        processes = []
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
        remote_api_port=18765,
        require_token=False,
        cluster_registry=registry.model_dump(mode="json"),
        cluster_registry_sha256=session_lifecycle.hashlib.sha256(payload).hexdigest(),
        cluster_route_revision=session_lifecycle.cluster_route_revision(definition),
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
    monkeypatch.setattr(session_lifecycle, "_assert_remote_port_available", lambda _port: None)
    monkeypatch.setattr(
        os, "geteuid", lambda: os.getuid() if hasattr(os, "getuid") else 0, raising=False
    )
    transaction = _FakeSessionTransaction(
        tmp_path / "home" / ".local" / "share" / "clio-relay" / "sessions" / request.session_id,
        session_id=request.session_id,
    )
    monkeypatch.setattr(
        session_lifecycle,
        "open_owned_session_transaction",
        lambda **_kwargs: transaction,
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
            "session_generation_id": generation,
            "owner_token": token,
            "owner_token_sha256": session_lifecycle.hashlib.sha256(token.encode()).hexdigest(),
            "api_release_identity_sha256": "b" * 64,
            "cluster_registry_path": str(transaction.path / f"cluster-registry-{generation}.json"),
            "cluster_registry_sha256": request.cluster_registry_sha256,
            "cluster_route_revision": request.cluster_route_revision,
            "remote_api_port": request.remote_api_port,
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
            transaction,
            operation="start",
            identity=identity,
        )

        recovered = session_lifecycle._validated_resumable_start_attempt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            transaction,
            request=request,
        )

    assert recovered is not None
    assert recovered["start_phase"] == phase


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
            transaction,
            name="api.log",
            maximum_bytes=None,
        )
        assert target.identity_mode == "inode"
        assert target.sha256 is None
        assert target.size == 20 * 1024 * 1024
        session_lifecycle._delete_cleanup_targets(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            transaction,
            [target],
        )

    assert not log_path.exists()


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

    class FakeTransaction:
        def __init__(self) -> None:
            self.document: dict[str, object] = {
                "cleanup_operation_id": "cleanup-finalize",
                "cleanup_policy": policy,
                "report": remote_report.model_dump(mode="json"),
                "coordinator_report": None,
                "coordinator_report_sha256": None,
            }
            self.writes: list[bytes] = []

        def __enter__(self) -> FakeTransaction:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read_json(self, _name: str) -> dict[str, object]:
            return dict(self.document)

        def atomic_write(self, _name: str, payload: bytes) -> None:
            self.writes.append(payload)
            loaded = json.loads(payload)
            assert isinstance(loaded, dict)
            self.document = cast(dict[str, object], loaded)

    transaction = FakeTransaction()

    def inspect(**_kwargs: object) -> OwnedSessionRecoveryStatus:
        coordinator = transaction.document.get("coordinator_report")
        digest = transaction.document.get("coordinator_report_sha256")
        bound = isinstance(coordinator, dict) and isinstance(digest, str)
        return OwnedSessionRecoveryStatus(
            cluster="ares",
            session_id="session-1",
            session_generation_id="generation-1",
            cleanup_receipt=True,
            cleanup_paths_pending=False,
            coordinator_report=(cast(dict[str, object], coordinator) if bound else None),
            coordinator_report_sha256=cast(str, digest) if bound else None,
            coordinator_report_bound=bound,
            ownership_verified=True,
            recovery_verified=True,
        )

    monkeypatch.setattr(
        session_lifecycle,
        "open_owned_session_transaction",
        lambda **_kwargs: transaction,
    )
    monkeypatch.setattr(session_lifecycle, "inspect_owned_session_recovery_status", inspect)
    monkeypatch.setattr(os, "geteuid", lambda: 1000, raising=False)
    request = OwnedSessionCleanupFinalizeRequest(
        cluster="ares",
        session_id="session-1",
        expected_session_generation_id="generation-1",
        expected_cleanup_operation_id="cleanup-finalize",
        expected_cleanup_policy=policy,
        coordinator_report=remote_report,
        coordinator_report_sha256=session_lifecycle_report_sha256(remote_report),
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

    replacement = remote_report.model_copy(deep=True)
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
    assert "clio-relay session recovery-status" in scripts[0]
    assert "metadata.json" not in scripts[0]


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
