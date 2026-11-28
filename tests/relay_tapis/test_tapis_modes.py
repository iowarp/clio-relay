"""Both Tapis modes on the stub.

- ``tms``: a TACC-managed host. The system is TACC's, named by ``system_id``,
  and Tapis's trust manager makes and holds the key. Relay makes none.
- ``pki``: the person's own host, registered by relay with its own key pair.

Each mode registers, moves files (with a chunked write), runs a job and then
removes what relay registered.
"""

from __future__ import annotations

import asyncio
import tarfile
from pathlib import Path

import pytest

from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.tapis import TapisAdapter, TapisConfig, register
from clio_relay.adapters.tapis.launcher import archive_name
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec
from clio_relay.sites.models import SiteEntry
from tests.fixtures.tapis_stub import TapisStub
from tests.fixtures.tapis_stub.support import (
    USERNAME,
    seed_host,
    sign_in,
    system_id,
    tapis_config,
    use_memory_keyring,
)

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]

TACC_HOST = "vista.tacc.utexas.edu"
PICK_HOST = "stampede3.tacc.utexas.edu"


def mode_adapter(stub: TapisStub, tmp_path: Path, authn: str, name: str = "") -> TapisAdapter:
    site = name or authn
    work = tmp_path / "site"
    work.mkdir(parents=True, exist_ok=True)
    if authn == "tms":
        found = f"tacc-{site}"
        stub.seed_system(
            found, host=TACC_HOST, defaultAuthnMethod="TMS_KEYS", jobWorkingDir=str(work)
        )
        config = tapis_config(stub, authn="tms", system_id=found)
    else:
        seed_host(stub, site, work)
        config = tapis_config(stub)
    context = AdapterContext(
        site_id=site, state_dir=tmp_path / "relay", content=ContentStore(tmp_path / "content")
    )
    return TapisAdapter(TapisConfig.model_validate(config), context)


async def run_job(
    stub: TapisStub, adapter: TapisAdapter, spec: JobSpec, operation_id: str
) -> tuple[JobHandle, JobStatus, Path]:
    """Submit ``spec`` and follow it to the end, with no deadline."""
    handle = await adapter.run(spec, operation_id=operation_id)
    while not (status := await adapter.status(handle)).state.final:
        await asyncio.sleep(0.2)
    system = str(handle.data["system_id"])
    return handle, status, stub.state.host_path(system, str(handle.data["job_dir"]))


@pytest.mark.parametrize("authn", ["tms", "pki"])
async def test_a_mode_registers_moves_files_runs_and_cleans_up(
    stub: TapisStub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, authn: str
) -> None:
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    adapter = mode_adapter(stub, tmp_path, authn)
    found = await adapter.system_id()
    made: list[str] = []
    monkeypatch.setattr(register, "make_key_pair", made.append)
    if authn == "tms":
        registered = await register.register_host(adapter, None)
        assert not registered.created_system and not registered.key_installed
        assert made == []
    assert stub.state.credentials[(found, USERNAME)]["tms"] is (authn == "tms")

    files = await adapter.files()
    await files.write("in/data.txt", b"chunk one ", final=False)
    await files.write("in/data.txt", b"chunk two", offset=10)
    assert await files.read("in/data.txt") == b"chunk one chunk two"
    assert [entry.size for entry in await files.list("in")] == [19]
    await files.delete("in", recursive=True)
    with pytest.raises(Refusal) as gone:
        await files.stat("in")
    assert gone.value.reason == RefusalReason.PATH_NOT_FOUND

    spec = JobSpec(executable="echo", arguments=[f"{authn} mode ok"])
    handle, status, job_dir = await run_job(stub, adapter, spec, f"op-{authn}")
    print(f"{authn}: {handle.native_id} {status.state} {status.exit_code}")
    assert status.state == JobState.COMPLETED, (status, (job_dir / "stderr").read_text())
    assert f"{authn} mode ok" in (job_dir / "stdout").read_text()

    removed = await register.unregister_host(adapter, None)
    assert removed["credential"] is True
    assert (found, USERNAME) not in stub.state.credentials
    if authn == "tms":
        assert found in stub.state.systems, "relay never deletes TACC's system"


async def test_a_tacc_site_with_no_system_id_names_the_systems_to_pick(
    stub: TapisStub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A catalog TACC site has no ``system_id``: relay names the systems on its host."""
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    for found in ("s3-a", "s3-b"):
        stub.seed_system(found, host=PICK_HOST, defaultAuthnMethod="TMS_KEYS")
    stub.seed_system("elsewhere", host="frontera.tacc.utexas.edu")
    entry = SiteEntry.model_validate(
        {
            "id": "stampede3",
            "address": PICK_HOST,
            "adapter": "tapis",
            "config": tapis_config(stub, authn="tms"),
        }
    )
    context = AdapterContext(
        site_id="stampede3", state_dir=tmp_path / "relay", content=ContentStore(tmp_path / "c")
    )
    adapter = TapisAdapter(TapisConfig.model_validate(entry.config), context)
    with pytest.raises(Refusal) as refused:
        await adapter.on_site_added(entry)
    print(f"tms with no system id {refused.value.to_dict()}")
    assert refused.value.reason == RefusalReason.INVALID_CONFIG
    assert refused.value.data["candidates"] == ["s3-a", "s3-b"]
    assert "set system_id" in refused.value.message
    assert system_id("stampede3") not in stub.state.systems

    entry.config["system_id"] = "s3-b"
    adapter = TapisAdapter(TapisConfig.model_validate(entry.config), context)
    added = await adapter.on_site_added(entry)
    assert added is not None and added["registered"] is True and added["authn"] == "tms"
    await register.unregister_host(adapter, None)


async def test_the_launcher_app_is_made_once_and_reused(
    stub: TapisStub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The launcher tar goes up once, and its app is made once, for every job after."""
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    apps_before = len(stub.state.requests_to("/v3/apps", "POST"))
    adapters = [mode_adapter(stub, tmp_path, "pki", "reuse") for _ in range(2)]
    for index, adapter in enumerate(adapters):
        spec = JobSpec(executable="echo", arguments=[f"run {index}"])
        _, status, _ = await run_job(stub, adapter, spec, f"op-reuse-{index}")
        assert status.state == JobState.COMPLETED, status
    assert len(stub.state.requests_to("/v3/apps", "POST")) == apps_before + 1
    name = archive_name()
    found = await adapters[0].system_id()
    uploads = [
        item
        for item in stub.state.requests_to(f"/v3/files/ops/{found}/", "POST")
        if item["path"].endswith(f"/launcher/{name}")
    ]
    assert len(uploads) == 1
    (tar_path,) = (tmp_path / "site").rglob(name)
    with tarfile.open(tar_path) as tar:
        (member,) = tar.getmembers()
    assert member.name == "tapisjob_app.sh"
    assert member.mode == 0o755


async def test_cancel_after_the_end_reports_the_end(
    stub: TapisStub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tapis answers 409 for a job that already ended; relay reports how it ended."""
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    adapter = mode_adapter(stub, tmp_path, "pki", "late")
    spec = JobSpec(executable="true")
    handle, status, _ = await run_job(stub, adapter, spec, "op-late-cancel")
    assert status.state == JobState.COMPLETED
    after = await adapter.cancel(handle)
    assert after.state == JobState.COMPLETED
    (refused,) = [
        item
        for item in stub.state.requests_to("/v3/jobs/", "POST")
        if item["path"] == f"/v3/jobs/{handle.native_id}/cancel"
    ]
    assert refused["status"] == 409
