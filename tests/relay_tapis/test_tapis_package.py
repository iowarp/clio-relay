"""The launcher's package mode, through the stub, with a ``jarvis`` that records its calls.

The real jarvis-cd run is ``python -m tests.relay_tapis.live_package`` on Ares
(ledger, W10 evidence).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from clio_relay.adapters.tapis import TapisAdapter, launcher, mapping
from clio_relay.jobs.handles import JobHandle, JobState
from clio_relay.jobs.spec import JobSpec, PackageRef
from tests.fixtures.tapis_stub import TapisStub
from tests.fixtures.tapis_stub.support import (
    demo_checkout,
    package_archive,
    sign_in,
    use_memory_keyring,
)

from .live_package import guarded_adapter, run_package, slurm_system

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]


async def test_a_package_runs_through_a_jarvis_of_its_own(
    stub: TapisStub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, jarvis_log: Path
) -> None:
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    adapter = guarded_adapter(stub, tmp_path)
    assert adapter.runs_packages
    digest = adapter.context.content.put(package_archive(demo_checkout(tmp_path), ["w10demo"]))
    spec = JobSpec(
        package=PackageRef(
            name="w10demo.hello",
            version="0.1.0",
            sha256=digest,
            parameters={"greeting": "hi there", "count": 3},
        )
    )
    handle, status, job_dir = await run_package(stub, adapter, spec, "op-package")
    calls = [line.split("\x1f") for line in jarvis_log.read_text().splitlines()]
    print(f"package calls {calls}")
    assert status.state == JobState.COMPLETED, (status, (job_dir / "stderr").read_text())
    assert status.exit_code == 0
    root = str(job_dir / ".relay-jarvis")
    assert {call[0] for call in calls} == {root}
    pipeline = "relay_" + str(handle.native_id).replace("-", "_")
    assert [call[1:] for call in calls if call[1:3] != ["repo", "add"]] == [
        ["init", f"{root}/config", f"{root}/private", f"{root}/shared"],
        ["ppl", "create", pipeline],
        ["ppl", "append", "w10demo.hello", "pkg", "greeting=hi there", "count=3"],
        ["ppl", "run"],
    ]
    (added,) = [call[3] for call in calls if call[1:3] == ["repo", "add"]]
    assert added.endswith(f"/packages/{digest}/w10demo")
    assert (job_dir / ".relay-jarvis" / "shared" / "ran").is_file()
    assert not (tmp_path / "home" / ".ppi-jarvis").exists()


@pytest.mark.parametrize(
    ("name", "flat", "code", "said"),
    [
        ("hello", False, 64, "package name hello is not <repo>.<package>"),
        ("w10demo.hello", True, 66, "package archive has no w10demo/w10demo directory"),
    ],
)
async def test_a_package_the_launcher_cannot_place_fails_before_jarvis(
    stub: TapisStub,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    jarvis_log: Path,
    name: str,
    flat: bool,
    code: int,
    said: str,
) -> None:
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    adapter = guarded_adapter(stub, tmp_path, site=f"pkg-refused-{code}")
    checkout = demo_checkout(tmp_path)
    root = checkout / "w10demo" if flat else checkout
    digest = adapter.context.content.put(package_archive(root, ["w10demo"]))
    spec = JobSpec(package=PackageRef(name=name, version="0.1.0", sha256=digest))
    _, status, job_dir = await run_package(stub, adapter, spec, "op-bad-package")
    print(f"refused package {name} flat={flat}: {status.exit_code} {status.message}")
    assert status.state == JobState.FAILED
    assert status.exit_code == code
    assert said in (job_dir / "relay.launcher.log").read_text()
    assert not jarvis_log.exists()


async def _start_service(
    stub: TapisStub, adapter: TapisAdapter, digest: str, operation_id: str
) -> tuple[JobHandle, Path, dict[str, Any]]:
    """Submit ``w10demo.hello`` as a service and wait until jarvis has started it."""
    spec = JobSpec(
        package=PackageRef(name="w10demo.hello", version="0.1.0", sha256=digest, service=True)
    )
    handle = await adapter.run(spec, operation_id=operation_id)
    job_dir = stub.state.host_path(str(handle.data["system_id"]), str(handle.data["job_dir"]))
    while not (job_dir / ".relay-jarvis" / "shared" / "started").exists():
        status = await adapter.status(handle)
        assert not status.state.final, (status, (job_dir / "stderr").read_text())
        await asyncio.sleep(0.1)
    request = stub.state.jobs[str(handle.native_id)].request
    return handle, job_dir, request


async def _cancel_and_wait(adapter: TapisAdapter, handle: JobHandle, job_dir: Path) -> JobState:
    """Cancel through Tapis, then wait for the launcher to end."""
    await adapter.cancel(handle)
    while not (job_dir / "tapisjob.exitcode").exists():
        await asyncio.sleep(0.1)
    return (await adapter.status(handle)).state


@pytest.mark.parametrize("batch", [False, True], ids=["fork", "batch"])
async def test_a_service_package_holds_until_cancelled_then_stops(
    stub: TapisStub,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    jarvis_log: Path,
    batch: bool,
) -> None:
    """RL45: ppl start, held until a Tapis cancel, then ppl stop.

    A FORK cancel is ``pkill -9 -P`` on the launcher, which kills only its sleep.
    A BATCH cancel is Slurm's TERM, which the launcher gets because the batch
    script execs it.
    """
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    system = slurm_system("debug", 30) if batch else {}
    adapter = guarded_adapter(stub, tmp_path, site=f"svc-{batch}", **system)
    digest = adapter.context.content.put(package_archive(demo_checkout(tmp_path), ["w10demo"]))
    handle, job_dir, request = await _start_service(stub, adapter, digest, "op-service")
    fields = launcher.decode(request["parameterSet"]["appArgs"][0]["arg"])
    assert fields[1] == "package-service"
    assert request["jobType"] == ("BATCH" if batch else "FORK")
    assert request.get("cmdPrefix") == ("exec" if batch else None)
    assert not (job_dir / ".relay-jarvis" / "shared" / "stopped").exists()
    state = await _cancel_and_wait(adapter, handle, job_dir)
    calls = [line.split("\x1f")[1:] for line in jarvis_log.read_text().splitlines()]
    print(f"service calls {calls}")
    assert state == JobState.CANCELLED
    assert calls[-2:] == [["ppl", "start"], ["ppl", "stop"]]
    assert (job_dir / ".relay-jarvis" / "shared" / "stopped").is_file()
    assert (job_dir / "relay.exitcode").read_text().strip() == "0"


async def test_a_service_that_fails_to_start_is_stopped_and_fails(
    stub: TapisStub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, jarvis_log: Path
) -> None:
    use_memory_keyring(monkeypatch)
    monkeypatch.setenv("FAKE_JARVIS_START_RC", "3")
    await sign_in(stub)
    adapter = guarded_adapter(stub, tmp_path, site="svc-fails")
    digest = adapter.context.content.put(package_archive(demo_checkout(tmp_path), ["w10demo"]))
    spec = JobSpec(
        package=PackageRef(name="w10demo.hello", version="0.1.0", sha256=digest, service=True)
    )
    _, status, _ = await run_package(stub, adapter, spec, "op-service-fails")
    calls = [line.split("\x1f")[1:] for line in jarvis_log.read_text().splitlines()]
    assert status.state == JobState.FAILED
    assert status.exit_code == 3
    assert calls[-2:] == [["ppl", "start"], ["ppl", "stop"]]


async def test_a_batch_job_execs_the_launcher_only_where_the_system_allows_it() -> None:
    spec = JobSpec(executable="true")
    common: dict[str, Any] = {
        "operation_id": "op-prefix",
        "app_id": "relay",
        "app_version": "1",
        "system_id": "sys",
        "job_dir": "/w/jobs/op-prefix",
        "content_dir_host": "/w/content",
        "launcher_arg": "x",
        "max_minutes": 30,
    }
    batch = mapping.job_request(spec, system_type="BATCH", exec_prefix=True, **common)
    fork = mapping.job_request(spec, system_type="FORK", exec_prefix=True, **common)
    plain = mapping.job_request(spec, system_type="BATCH", **common)
    assert batch["cmdPrefix"] == "exec"
    assert "cmdPrefix" not in fork
    assert "cmdPrefix" not in plain
