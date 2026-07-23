"""Cross-process worker lifetime and migration exclusion coverage."""

from __future__ import annotations

import os
import stat
import subprocess
import sys
import time
from collections.abc import Generator
from contextlib import contextmanager, suppress
from io import StringIO
from pathlib import Path
from typing import Any, Literal

import pytest

import clio_relay.core_queue as core_queue_module
import clio_relay.endpoint as endpoint_module
import clio_relay.storage_runtime as storage_runtime_module
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue, LegacyQueueStateError
from clio_relay.errors import ConfigurationError
from clio_relay.models import EndpointRole, JarvisRunSpec, JobKind, RelayEvent, RelayJob
from clio_relay.storage_runtime import storage_managed_queue
from clio_relay.worker_lifetime_lock import (
    WORKER_LIFETIME_GUARD_FD_ENV,
    LockedCoreIdentity,
    WorkerLifetimeLock,
    WorkerLifetimeLockUnavailable,
    exclusive_migration_lifetime,
)


def _start_shared_lock_holder(core_dir: Path) -> subprocess.Popen[str]:
    """Start a real child that owns a shared lifetime lock until stdin closes."""
    source = """
from pathlib import Path
import sys
from clio_relay.worker_lifetime_lock import WorkerLifetimeLock

lock = WorkerLifetimeLock(Path(sys.argv[1]), mode="shared").acquire()
print("ready", flush=True)
sys.stdin.readline()
lock.release()
"""
    process = subprocess.Popen(
        [sys.executable, "-c", source, str(core_dir)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    assert process.stdout.readline().strip() == "ready"
    return process


def _release_lock_holder(process: subprocess.Popen[str]) -> None:
    """Release and reap a child lock holder with bounded failure reporting."""
    assert process.stdin is not None
    process.stdin.write("release\n")
    process.stdin.flush()
    stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 0, f"stdout={stdout!r} stderr={stderr!r}"


def test_shared_workers_exclude_migration_across_processes(tmp_path: Path) -> None:
    """Multiple workers coexist, while migration waits for every worker to exit."""
    core_dir = tmp_path / "core"
    process = _start_shared_lock_holder(core_dir)
    local_shared = WorkerLifetimeLock(
        core_dir,
        mode="shared",
        timeout_seconds=0,
    ).acquire()
    try:
        with pytest.raises(WorkerLifetimeLockUnavailable):
            WorkerLifetimeLock(
                core_dir,
                mode="exclusive",
                timeout_seconds=0,
            ).acquire()
    finally:
        local_shared.release()
        _release_lock_holder(process)

    exclusive = WorkerLifetimeLock(
        core_dir,
        mode="exclusive",
        timeout_seconds=0,
    ).acquire()
    try:
        with pytest.raises(WorkerLifetimeLockUnavailable):
            WorkerLifetimeLock(
                core_dir,
                mode="shared",
                timeout_seconds=0,
            ).acquire()
    finally:
        exclusive.release()


def test_old_shared_writer_prevents_missing_seal_mutation_until_exclusive(
    tmp_path: Path,
) -> None:
    """A pre-seal shared writer fences every queue record and marker creation."""
    core_dir = tmp_path / "core"
    started = tmp_path / "seal-started"
    shared = WorkerLifetimeLock(core_dir, mode="shared").acquire()
    source = """
from pathlib import Path
import sys
from clio_relay.core_queue import ClioCoreQueue

core, started = map(Path, sys.argv[1:])
started.write_text("started", encoding="utf-8")
ClioCoreQueue(core).initialize()
"""
    process = subprocess.Popen(
        [sys.executable, "-c", source, str(core_dir), str(started)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not started.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert started.exists()
        time.sleep(0.1)
        assert process.poll() is None
        assert not (core_dir / "migrations").exists()
        assert not (core_dir / "jobs").exists()
        assert not (core_dir / ".lock").exists()
    finally:
        shared.release()
    stdout, stderr = process.communicate(timeout=30)
    assert process.returncode == 0, f"stdout={stdout!r} stderr={stderr!r}"
    assert (core_dir / "migrations" / "legacy-record-audit-v1.json").is_file()


def test_lifetime_locks_are_scoped_to_physical_core(tmp_path: Path) -> None:
    """An exclusive migration on one core never blocks a different core."""
    first = WorkerLifetimeLock(tmp_path / "first", mode="exclusive").acquire()
    second = WorkerLifetimeLock(
        tmp_path / "second",
        mode="shared",
        timeout_seconds=0,
    ).acquire()
    second.release()
    first.release()


def test_stable_lexical_core_alias_is_accepted(tmp_path: Path) -> None:
    """Equivalent lexical paths resolve to one physical lifetime-lock identity."""
    parent = tmp_path / "physical"
    core_dir = parent / "core"
    core_dir.mkdir(parents=True)
    (parent / "unused").mkdir()
    alias = parent / "unused" / ".." / "core"

    shared = WorkerLifetimeLock(alias, mode="shared", timeout_seconds=0).acquire()
    second = WorkerLifetimeLock(core_dir, mode="shared", timeout_seconds=0).acquire()
    assert shared.core_dir == core_dir.resolve()
    second.release()
    shared.release()

    ClioCoreQueue(alias).initialize(migrate_legacy_output=True)


def test_retargeted_core_alias_is_rejected_before_worker_write(tmp_path: Path) -> None:
    """A worker cannot continue after its queue alias changes physical identity."""
    if os.name == "nt":
        # Stable lexical aliases are covered portably above. Windows junction
        # creation is privilege/policy dependent; CI exercises retargeting on
        # POSIX where managed bootstrap runs.
        return
    first_parent = tmp_path / "first"
    second_parent = tmp_path / "second"
    first_core = first_parent / "core"
    second_core = second_parent / "core"
    first_core.mkdir(parents=True)
    second_core.mkdir(parents=True)
    alias_parent = tmp_path / "current"
    alias_parent.symlink_to(first_parent, target_is_directory=True)
    aliased_core = alias_parent / "core"
    queue = ClioCoreQueue(aliased_core)
    queue.initialize()
    worker = endpoint_module.EndpointWorker(
        role=EndpointRole.WORKER,
        cluster="test",
        settings=RelaySettings(core_dir=aliased_core, spool_dir=tmp_path / "spool"),
        queue=queue,
    )
    try:
        alias_parent.unlink()
        alias_parent.symlink_to(second_parent, target_is_directory=True)
        with pytest.raises(ConfigurationError, match="identity changed"):
            worker.register()
    finally:
        worker.close()


def test_migration_rejects_alias_retarget_after_exclusive_acquisition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A post-lock alias retarget fails before migration writes the new target."""
    if os.name == "nt":
        return
    first_parent = tmp_path / "first-migration"
    second_parent = tmp_path / "second-migration"
    first_core = first_parent / "core"
    second_core = second_parent / "core"
    first_core.mkdir(parents=True)
    second_core.mkdir(parents=True)
    alias_parent = tmp_path / "migration-current"
    alias_parent.symlink_to(first_parent, target_is_directory=True)
    queue = ClioCoreQueue(alias_parent / "core")
    actual_guard = exclusive_migration_lifetime

    @contextmanager
    def retargeting_guard(
        root: Path,
        **_kwargs: object,
    ) -> Generator[LockedCoreIdentity, None, None]:
        with actual_guard(root) as locked_core:
            alias_parent.unlink()
            alias_parent.symlink_to(second_parent, target_is_directory=True)
            yield locked_core

    monkeypatch.setattr(
        core_queue_module,
        "exclusive_migration_lifetime",
        retargeting_guard,
    )

    with pytest.raises(ConfigurationError, match="does not match its core lifetime lock"):
        queue.initialize(migrate_legacy_output=True)

    assert list(second_core.iterdir()) == []


def test_worker_lock_is_acquired_before_default_queue_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default worker queue initialization cannot race ahead of shared ownership."""
    events: list[str] = []

    class RecordingLock:
        """Minimal acquisition recorder for constructor ordering."""

        def __init__(
            self,
            core_dir: Path,
            *,
            mode: Literal["shared", "exclusive"],
            timeout_seconds: float | None = None,
        ) -> None:
            del timeout_seconds
            assert mode == "shared"
            core_dir.mkdir(parents=True, exist_ok=True)
            self.core_dir = core_dir.resolve()

        def acquire(self) -> RecordingLock:
            """Record shared ownership."""
            events.append("lock")
            return self

        def release(self) -> None:
            """Record rollback after queue construction fails."""
            events.append("release")

    def fail_queue(
        _settings: RelaySettings,
        *,
        writer_lifetime_lock: WorkerLifetimeLock | None = None,
    ) -> ClioCoreQueue:
        assert events == ["lock"]
        assert writer_lifetime_lock is not None
        events.append("queue")
        raise RuntimeError("queue construction failed")

    monkeypatch.setattr(endpoint_module, "WorkerLifetimeLock", RecordingLock)
    monkeypatch.setattr(endpoint_module, "storage_managed_queue", fail_queue)

    with pytest.raises(RuntimeError, match="queue construction failed"):
        endpoint_module.EndpointWorker(
            role=EndpointRole.WORKER,
            cluster="test",
            settings=RelaySettings(
                core_dir=tmp_path / "core",
                spool_dir=tmp_path / "spool",
            ),
        )

    assert events == ["lock", "queue", "release"]


def test_every_production_queue_holds_shared_writer_lifetime_ownership(
    tmp_path: Path,
) -> None:
    """API, MCP, CLI, and future managed queues all exclude migration until close."""
    core_dir = tmp_path / "core"
    queue = storage_managed_queue(RelaySettings(core_dir=core_dir, spool_dir=tmp_path / "spool"))
    retained_submit = queue.submit_job
    try:
        with pytest.raises(WorkerLifetimeLockUnavailable):
            WorkerLifetimeLock(
                core_dir,
                mode="exclusive",
                timeout_seconds=0,
            ).acquire()
    finally:
        queue.close()

    assert queue.closed is True
    with pytest.raises(ConfigurationError, match="managed queue is closed"):
        queue.submit_job(
            RelayJob(
                cluster="test",
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(command=["true"]),
                idempotency_key="closed-queue-submit",
            )
        )
    with pytest.raises(ConfigurationError, match="managed queue is closed"):
        retained_submit(
            RelayJob(
                cluster="test",
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(command=["true"]),
                idempotency_key="retained-closed-queue-submit",
            )
        )

    exclusive = WorkerLifetimeLock(
        core_dir,
        mode="exclusive",
        timeout_seconds=0,
    ).acquire()
    exclusive.release()


def test_migration_factory_returns_closed_non_writer_queue(tmp_path: Path) -> None:
    """Explicit migration cannot return an unfenced queue capable of later writes."""
    core_dir = tmp_path / "core"
    queue = storage_managed_queue(
        RelaySettings(core_dir=core_dir, spool_dir=tmp_path / "spool"),
        migrate_legacy_output=True,
    )

    assert queue.closed is True
    with pytest.raises(ConfigurationError, match="managed queue is closed"):
        queue.get_job("missing")
    exclusive = WorkerLifetimeLock(
        core_dir,
        mode="exclusive",
        timeout_seconds=0,
    ).acquire()
    exclusive.release()


def test_managed_queue_reuses_callers_shared_lifetime_without_owning_it(
    tmp_path: Path,
) -> None:
    """Endpoint workers can retain one shared lock across queue construction and close."""
    core_dir = tmp_path / "core"
    shared = WorkerLifetimeLock(core_dir, mode="shared").acquire()
    queue = storage_managed_queue(
        RelaySettings(core_dir=core_dir, spool_dir=tmp_path / "spool"),
        writer_lifetime_lock=shared,
    )

    queue.close()
    with pytest.raises(WorkerLifetimeLockUnavailable):
        WorkerLifetimeLock(
            core_dir,
            mode="exclusive",
            timeout_seconds=0,
        ).acquire()
    shared.release()


def test_desktop_endpoint_close_releases_its_managed_writer_lifetime(
    tmp_path: Path,
) -> None:
    """A non-worker endpoint releases the managed queue lock on explicit close."""
    core_dir = tmp_path / "core"
    endpoint = endpoint_module.EndpointWorker(
        role=EndpointRole.DESKTOP,
        cluster="desktop",
        settings=RelaySettings(core_dir=core_dir, spool_dir=tmp_path / "spool"),
    )
    with pytest.raises(WorkerLifetimeLockUnavailable):
        WorkerLifetimeLock(
            core_dir,
            mode="exclusive",
            timeout_seconds=0,
        ).acquire()

    endpoint.close()
    exclusive = WorkerLifetimeLock(
        core_dir,
        mode="exclusive",
        timeout_seconds=0,
    ).acquire()
    exclusive.release()


def test_http_api_shutdown_releases_managed_writer_lifetime(tmp_path: Path) -> None:
    """The HTTP server retains shared ownership only through its app lifespan."""
    from fastapi.testclient import TestClient

    from clio_relay.http_api import create_app

    core_dir = tmp_path / "core"
    settings = RelaySettings(core_dir=core_dir, spool_dir=tmp_path / "spool")
    app = create_app(settings)
    with TestClient(app), pytest.raises(WorkerLifetimeLockUnavailable):
        WorkerLifetimeLock(
            core_dir,
            mode="exclusive",
            timeout_seconds=0,
        ).acquire()

    exclusive = WorkerLifetimeLock(
        core_dir,
        mode="exclusive",
        timeout_seconds=0,
    ).acquire()
    exclusive.release()
    with (
        pytest.raises(RuntimeError, match="cannot restart after shutdown"),
        TestClient(app),
    ):
        pass


def test_mcp_stdio_exit_releases_managed_writer_lifetime(tmp_path: Path) -> None:
    """The stdio MCP server releases shared ownership when its input closes."""
    from clio_relay.mcp_server import serve_stdio

    core_dir = tmp_path / "core"
    serve_stdio(
        stdin=StringIO(""),
        stdout=StringIO(),
        settings=RelaySettings(core_dir=core_dir, spool_dir=tmp_path / "spool"),
    )

    exclusive = WorkerLifetimeLock(
        core_dir,
        mode="exclusive",
        timeout_seconds=0,
    ).acquire()
    exclusive.release()


def test_authoritative_migration_api_enters_exclusive_lifetime_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct Python migration cannot reach its audit without exclusive ownership."""
    root = tmp_path / "core"
    root.mkdir()
    guarded = False
    original_audit = ClioCoreQueue._audit_legacy_state_before_initialization  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def guarded_audit(self: ClioCoreQueue) -> Any:
        nonlocal guarded
        with pytest.raises(WorkerLifetimeLockUnavailable):
            WorkerLifetimeLock(
                root,
                mode="shared",
                timeout_seconds=0,
            ).acquire()
        guarded = True
        assert guarded
        return original_audit(self)

    monkeypatch.setattr(
        ClioCoreQueue,
        "_audit_legacy_state_before_initialization",
        guarded_audit,
    )

    ClioCoreQueue(root).initialize(migrate_legacy_output=True)
    assert guarded


def test_external_guard_evidence_validates_real_exclusive_handoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A child descriptor retains EX after the shell-side descriptor closes."""
    if os.name != "posix":
        return
    core_dir = tmp_path / "core"
    guard = WorkerLifetimeLock(core_dir, mode="exclusive").acquire()
    owner_descriptor = guard._fd  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert owner_descriptor is not None
    inherited_descriptor = os.dup(owner_descriptor)
    guard._fd = None  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    os.close(owner_descriptor)
    monkeypatch.setenv(WORKER_LIFETIME_GUARD_FD_ENV, str(inherited_descriptor))
    try:
        with (
            exclusive_migration_lifetime(core_dir),
            pytest.raises(WorkerLifetimeLockUnavailable),
        ):
            WorkerLifetimeLock(
                core_dir,
                mode="shared",
                timeout_seconds=0,
            ).acquire()
    finally:
        os.close(inherited_descriptor)


def _write_oversized_legacy_output(root: Path) -> None:
    """Write one exact v0.9-style duplicated output event above the normal cap."""
    path = root / "events" / "legacy_job" / "00000000000000000001.json"
    path.parent.mkdir(parents=True)
    text = "legacy-output\n" * 30_000
    event = RelayEvent(
        job_id="legacy_job",
        seq=1,
        event_type="stdout.delta",
        message=text.rstrip("\n"),
        payload={"stream": "stdout", "text": text},
    )
    path.write_text(event.model_dump_json(indent=2), encoding="utf-8")


def test_normal_storage_startup_refuses_legacy_before_storage_mutation(
    tmp_path: Path,
) -> None:
    """Ordinary init audits legacy output before runtime creates `.storage` or spool."""
    core_dir = tmp_path / "core"
    spool_dir = tmp_path / "spool"
    _write_oversized_legacy_output(core_dir)

    with pytest.raises(LegacyQueueStateError):
        storage_managed_queue(RelaySettings(core_dir=core_dir, spool_dir=spool_dir))

    assert not (core_dir / ".storage").exists()
    assert not spool_dir.exists()


def test_storage_migration_waits_for_exclusive_before_runtime_mutation(
    tmp_path: Path,
) -> None:
    """A live shared worker prevents `.storage` creation before migration EX."""
    core_dir = tmp_path / "core"
    spool_dir = tmp_path / "spool"
    started = tmp_path / "started"
    shared = WorkerLifetimeLock(core_dir, mode="shared").acquire()
    source = """
from pathlib import Path
import sys
from clio_relay.config import RelaySettings
from clio_relay.storage_runtime import storage_managed_queue

core, spool, started = map(Path, sys.argv[1:])
started.write_text("started", encoding="utf-8")
storage_managed_queue(
    RelaySettings(core_dir=core, spool_dir=spool),
    migrate_legacy_output=True,
)
"""
    process = subprocess.Popen(
        [sys.executable, "-c", source, str(core_dir), str(spool_dir), str(started)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 10
    while not started.exists() and process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.01)
    assert started.exists()
    time.sleep(0.1)
    assert process.poll() is None
    assert not (core_dir / ".storage").exists()
    assert not spool_dir.exists()
    shared.release()
    stdout, stderr = process.communicate(timeout=30)
    assert process.returncode == 0, f"stdout={stdout!r} stderr={stderr!r}"
    assert (core_dir / ".storage").is_dir()


def test_storage_migration_pins_runtime_when_alias_retargets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runtime and policy remain on locked A when an ancestor alias retargets to B."""
    if os.name == "nt":
        return
    first_parent = tmp_path / "storage-first"
    second_parent = tmp_path / "storage-second"
    first_core = first_parent / "core"
    second_core = second_parent / "core"
    first_core.mkdir(parents=True)
    second_core.mkdir(parents=True)
    alias_parent = tmp_path / "storage-current"
    alias_parent.symlink_to(first_parent, target_is_directory=True)
    actual_guard = exclusive_migration_lifetime

    @contextmanager
    def retargeting_guard(
        root: Path,
        **_kwargs: object,
    ) -> Generator[LockedCoreIdentity, None, None]:
        with actual_guard(root) as locked_core:
            alias_parent.unlink()
            alias_parent.symlink_to(second_parent, target_is_directory=True)
            yield locked_core

    monkeypatch.setattr(
        storage_runtime_module,
        "exclusive_migration_lifetime",
        retargeting_guard,
    )
    queue = storage_managed_queue(
        RelaySettings(
            core_dir=alias_parent / "core",
            spool_dir=tmp_path / "spool",
        ),
        migrate_legacy_output=True,
    )

    assert queue.root == first_core.resolve()
    assert queue.storage_runtime.config.core_root == first_core.resolve()
    assert (first_core / ".storage").is_dir()
    assert list(second_core.iterdir()) == []


def test_storage_startup_refuses_core_replacement_before_seal_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A shared-to-exclusive seal handoff never initializes a replacement root."""
    if os.name == "nt":
        return
    core_dir = tmp_path / "core"
    displaced_core = tmp_path / "displaced-core"
    core_dir.mkdir()
    actual_guard = exclusive_migration_lifetime

    @contextmanager
    def replacing_guard(
        root: Path,
        **_kwargs: object,
    ) -> Generator[LockedCoreIdentity, None, None]:
        core_dir.rename(displaced_core)
        core_dir.mkdir()
        with actual_guard(root) as locked_core:
            yield locked_core

    monkeypatch.setattr(
        storage_runtime_module,
        "exclusive_migration_lifetime",
        replacing_guard,
    )

    with pytest.raises(
        ConfigurationError,
        match="queue root changed before establishing its indexed-era seal",
    ):
        storage_managed_queue(RelaySettings(core_dir=core_dir, spool_dir=tmp_path / "spool"))

    assert not (core_dir / "migrations").exists()
    assert not (displaced_core / "migrations").exists()


def test_storage_startup_bounds_initial_shared_lifetime_wait(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A surviving exclusive owner cannot hang initial shared startup forever."""
    core_dir = tmp_path / "core"
    exclusive = WorkerLifetimeLock(core_dir, mode="exclusive", timeout_seconds=0).acquire()
    monkeypatch.setattr(
        storage_runtime_module,
        "QUEUE_SEAL_LIFETIME_TIMEOUT_SECONDS",
        0.01,
    )
    try:
        started = time.monotonic()
        with pytest.raises(WorkerLifetimeLockUnavailable, match="timed out acquiring"):
            storage_managed_queue(RelaySettings(core_dir=core_dir, spool_dir=tmp_path / "spool"))
        assert time.monotonic() - started < 1
    finally:
        exclusive.release()


def test_queue_seal_handoff_bounds_exclusive_wait_and_restores_shared(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A surviving shared writer causes a bounded seal refusal, not a startup hang."""
    core_dir = tmp_path / "core"
    survivor = WorkerLifetimeLock(core_dir, mode="shared", timeout_seconds=0).acquire()
    candidate = WorkerLifetimeLock(core_dir, mode="shared", timeout_seconds=0).acquire()
    monkeypatch.setattr(
        storage_runtime_module,
        "QUEUE_SEAL_LIFETIME_TIMEOUT_SECONDS",
        0.01,
    )
    try:
        started = time.monotonic()
        with pytest.raises(WorkerLifetimeLockUnavailable, match="timed out acquiring"):
            storage_runtime_module.initialize_queue_with_shared_writer_fencing(candidate)
        assert time.monotonic() - started < 1
        assert candidate.acquired is True
        assert not (core_dir / "migrations").exists()
    finally:
        candidate.release()
        survivor.release()


def test_queue_seal_handoff_bounds_shared_reacquire(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A new exclusive owner cannot hang the post-seal shared reacquire."""
    core_dir = tmp_path / "core"
    candidate = WorkerLifetimeLock(core_dir, mode="shared", timeout_seconds=0).acquire()
    actual_guard = exclusive_migration_lifetime
    blockers: list[WorkerLifetimeLock] = []

    @contextmanager
    def install_exclusive_survivor(
        root: Path,
        **kwargs: object,
    ) -> Generator[LockedCoreIdentity, None, None]:
        timeout = kwargs.get("timeout_seconds")
        assert isinstance(timeout, float)
        with actual_guard(root, timeout_seconds=timeout) as locked_core:
            yield locked_core
        blockers.append(WorkerLifetimeLock(root, mode="exclusive", timeout_seconds=0).acquire())

    monkeypatch.setattr(
        storage_runtime_module,
        "QUEUE_SEAL_LIFETIME_TIMEOUT_SECONDS",
        0.01,
    )
    monkeypatch.setattr(
        storage_runtime_module,
        "exclusive_migration_lifetime",
        install_exclusive_survivor,
    )
    try:
        started = time.monotonic()
        with pytest.raises(
            WorkerLifetimeLockUnavailable,
            match="restoring shared writer ownership",
        ):
            storage_runtime_module.initialize_queue_with_shared_writer_fencing(candidate)
        assert time.monotonic() - started < 1
        assert candidate.acquired is False
        assert blockers and blockers[0].acquired
    finally:
        candidate.release()
        for blocker in blockers:
            blocker.release()


@pytest.mark.parametrize("fail_during_audit", [False, True])
def test_locked_initialization_never_writes_replacement_root_after_path_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fail_during_audit: bool,
) -> None:
    """Descriptor-pinned initialization keeps every write on the locked inode."""
    if os.name != "posix":
        return
    core_dir = tmp_path / "core"
    displaced_core = tmp_path / "displaced-core"
    core_dir.mkdir()
    original_audit = ClioCoreQueue._audit_legacy_state_before_initialization  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    swapped = False

    def swap_during_audit(queue: ClioCoreQueue) -> object:
        nonlocal swapped
        result = original_audit(queue)
        if not swapped:
            core_dir.rename(displaced_core)
            core_dir.mkdir()
            swapped = True
        if fail_during_audit:
            raise RuntimeError("injected audit failure after root replacement")
        return result

    monkeypatch.setattr(
        ClioCoreQueue,
        "_audit_legacy_state_before_initialization",
        swap_during_audit,
    )

    with (
        exclusive_migration_lifetime(core_dir) as locked_core,
        pytest.raises(ConfigurationError, match="queue root identity changed"),
    ):
        ClioCoreQueue(core_dir).initialize(locked_core=locked_core)

    assert swapped is True
    assert list(core_dir.iterdir()) == []
    if fail_during_audit:
        assert not (displaced_core / "migrations").exists()
    else:
        assert (displaced_core / "migrations").is_dir()


def test_locked_initialization_accepts_its_kernel_descriptor_alias(tmp_path: Path) -> None:
    """A kernel fd alias is authenticated by fstat, never rejected as a user symlink."""
    if os.name != "posix":
        return
    core_dir = tmp_path / "core"
    core_dir.mkdir(mode=0o755)

    with exclusive_migration_lifetime(core_dir) as locked_core:
        descriptor = locked_core.filesystem_root_descriptor
        assert descriptor is not None
        assert locked_core.filesystem_root in {
            Path("/proc/self/fd") / str(descriptor),
            Path("/dev/fd") / str(descriptor),
        }
        ClioCoreQueue(core_dir).initialize(locked_core=locked_core)

    assert stat.S_IMODE(core_dir.stat().st_mode) == 0o700
    assert (core_dir / "migrations" / "legacy-record-audit-v1.json").is_file()
    for family in ("endpoints", "jobs", "gateway_sessions", "monitor_rules"):
        assert stat.S_IMODE((core_dir / "global_order" / family).stat().st_mode) == 0o700


@pytest.mark.parametrize(
    "relative_link",
    (Path("jobs"), Path("global_order") / "endpoints"),
)
def test_locked_permission_repair_rejects_links_below_pinned_root(
    tmp_path: Path,
    relative_link: Path,
) -> None:
    """Every repaired child is opened relative to the root fd with no link following."""
    if os.name != "posix":
        return
    core_dir = tmp_path / "core"
    core_dir.mkdir(mode=0o700)
    outside = tmp_path / "outside"
    outside.mkdir(mode=0o755)
    link = core_dir / relative_link
    link.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    link.symlink_to(outside, target_is_directory=True)

    with (
        exclusive_migration_lifetime(core_dir) as locked_core,
        pytest.raises(
            ConfigurationError,
            match="queue directory protections cannot be repaired through the pinned root",
        ),
    ):
        ClioCoreQueue(core_dir).initialize(locked_core=locked_core)

    assert stat.S_IMODE(outside.stat().st_mode) == 0o755
    assert list(outside.iterdir()) == []


@pytest.mark.parametrize("failure_mode", ["closed", "reused"])
def test_locked_core_descriptor_failure_is_fail_closed(
    tmp_path: Path,
    failure_mode: str,
) -> None:
    """A closed or reused borrowed root descriptor cannot authorize queue I/O."""
    if os.name != "posix":
        return
    core_dir = tmp_path / "core"
    core_dir.mkdir(mode=0o700)
    other_dir = tmp_path / "other"
    other_dir.mkdir(mode=0o700)

    with exclusive_migration_lifetime(core_dir) as locked_core:
        descriptor = locked_core.filesystem_root_descriptor
        assert descriptor is not None
        os.close(descriptor)
        replacement: int | None = None
        try:
            if failure_mode == "reused":
                flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                replacement = os.open(other_dir, flags)
                if replacement != descriptor:
                    os.dup2(replacement, descriptor)
            message = "identity changed" if failure_mode == "reused" else "unavailable"
            with pytest.raises(ConfigurationError, match=message):
                _ = locked_core.filesystem_root_descriptor
        finally:
            if replacement is not None and replacement != descriptor:
                os.close(replacement)
            with suppress(OSError):
                os.close(descriptor)
