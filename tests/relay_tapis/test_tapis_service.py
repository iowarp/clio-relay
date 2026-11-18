"""LQ36 to LQ38: ``tapis`` sites through relay's operations service and the stub."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import pytest

from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.tapis import TapisAdapter, TapisConfig, register
from clio_relay.adapters.tapis.mapping import NO_WALLTIME_MINUTES, TAPISLOCAL, max_minutes
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import InputRef, JobSpec
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from tests.fixtures.tapis_stub import TapisStub
from tests.fixtures.tapis_stub.support import (
    USERNAME,
    seed_host,
    sign_in,
    system_id,
    tapis_config,
    use_memory_keyring,
)

from .conftest import add_site

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]

SITE = "tp"


def _site(stub: TapisStub, **fields: Any) -> dict[str, Any]:
    return {
        "id": SITE,
        "adapter": "tapis",
        "identity": "tacc",
        "config": tapis_config(stub),
        "policy": "open",
        "spend": {"ask": "never"},
    } | fields


async def _until(check: Any, what: str) -> None:
    # Waits on the stub's own thread, with no deadline: a stall shows as a hung test.
    while not check():
        await asyncio.sleep(0.05)
    assert check(), what


async def test_run_survives_a_restart(
    stub: TapisStub, relay_paths: RelayPaths, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """LQ36: relay restarts while the Tapis job waits; the new relay finishes it."""
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    seed_host(stub, SITE, tmp_path / "site")
    add_site(relay_paths, _site(stub))
    payload = f"relay input {time.time_ns()}\n".encode()
    first = RelayService(relay_paths)
    digest = first.content.put(payload)
    spec = JobSpec(
        executable="cat",
        arguments=["input.txt"],
        inputs=[InputRef(name="input.txt", sha256=digest)],
    )
    stub.state.hold = True
    try:
        record = await first.start_run(SITE, spec)
        assert record.state == OperationState.QUEUED, record
        uuid = first.view(record)["native_id"]
        assert uuid in stub.state.jobs
        job = stub.state.jobs[uuid]
        await _until(lambda: job.status == "QUEUED", "the stub job reached QUEUED")
        polled = await first.refresh(record.operation_id)
        assert polled.state == OperationState.QUEUED
        await first.close()
        del first

        restarted = RelayService(relay_paths)
        observed = await restarted.refresh(record.operation_id)
        assert observed.state == OperationState.QUEUED
        job.released.set()
        done = await restarted.wait(record.operation_id, 120, poll_s=0.1)
    finally:
        stub.state.hold = False
    view = restarted.view(done)
    assert done.state == OperationState.COMPLETED, view
    assert view["native_id"] == uuid
    assert view["exit_code"] == 0
    assert payload.decode() in view["result"]["stdout_tail"]
    (submit,) = [
        item
        for item in stub.state.requests_to("/v3/jobs/submit", "POST")
        if json.loads(item["body"])["description"].endswith(record.operation_id)
    ]
    body = json.loads(submit["body"])
    assert body["appId"] == system_id(SITE)
    assert body["jobType"] == "FORK"
    (file_input,) = body["fileInputs"]
    assert file_input["sourceUrl"].startswith(f"{TAPISLOCAL}/")
    assert file_input["sourceUrl"].endswith(digest)
    assert file_input["targetPath"] == "input.txt"
    # No walltime in the spec: relay asks for a year, never Tapis's 10 minute default.
    assert body["maxMinutes"] == NO_WALLTIME_MINUTES
    print(f"LQ36 operation {record.operation_id} Tapis job {uuid} app {body['appId']}")
    print(f"LQ36 history {[change.state.value for change in done.history]}")
    print(f"LQ36 stub job history {job.history}")
    print(f"LQ36 fileInputs {body['fileInputs']}")
    await restarted.close()


async def test_find_reconciles_through_tapis_search(
    stub: TapisStub, relay_paths: RelayPaths, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """LQ36: a submit whose handle relay never recorded is found by its description."""
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    seed_host(stub, SITE, tmp_path / "site")
    add_site(relay_paths, _site(stub))
    service = RelayService(relay_paths)
    record = await service.start_run(SITE, JobSpec(executable="true"))
    uuid = service.view(record)["native_id"]
    jobs_dir = relay_paths.adapter_dir(SITE, "tapis") / "jobs"
    for item in jobs_dir.iterdir():
        item.unlink()
    context = service.adapter_named(service.site(SITE), "tapis").context
    fresh = TapisAdapter(TapisConfig.model_validate(tapis_config(stub)), context)
    found = await fresh.find(record.operation_id)
    assert found is not None and found.native_id == uuid
    searched = stub.state.requests_to("/v3/jobs/search", "GET")
    assert searched and searched[-1]["query"]["description.eq"].endswith(record.operation_id)
    assert await fresh.find("op-never-submitted") is None
    await service.cancel(record.operation_id)
    await service.close()


async def test_describe_routes_exec_and_serve_to_ssh(
    stub: TapisStub, relay_paths: RelayPaths, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """LQ37: ``exec`` and ``serve`` route to the ``ssh`` alternative."""
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    seed_host(stub, SITE, tmp_path / "site")
    add_site(
        relay_paths,
        _site(stub, alternatives={"ssh": {"ssh_profile": "fx-ssh"}}),
    )
    service = RelayService(relay_paths)
    described = await service.describe(SITE)
    routes = {item["verb"]: item for item in described["routes"]}
    print(f"LQ37 routes {routes}")
    for verb in ("describe", "run", "files"):
        assert routes[verb]["adapter"] == "tapis", routes[verb]
    for verb in ("exec", "serve"):
        assert routes[verb]["adapter"] == "ssh", routes[verb]
        assert routes[verb]["via"] == "alternative"
    info = described["info"]
    assert info["adapter"] == "tapis"
    assert info["facts"]["credential"] == "ok"
    assert info["facts"]["job_type"] == "FORK"
    await service.close()


async def test_guarded_site_refuses_a_free_form_run(
    stub: TapisStub, relay_paths: RelayPaths, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """LQ38: a free-form run on a guarded site never reaches Tapis's jobs service."""
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    seed_host(stub, SITE, tmp_path / "site")
    add_site(
        relay_paths,
        _site(stub, config=tapis_config(stub, launcher="guarded"), policy="guarded"),
    )
    before = len(stub.state.requests_to("/v3/jobs"))
    service = RelayService(relay_paths)
    spec = JobSpec(executable="echo", arguments=["free-form"])
    with pytest.raises(Refusal) as refused:
        await service.start_run(SITE, spec)
    print(f"LQ38 service refusal {refused.value.to_dict()}")
    assert refused.value.reason in (
        RefusalReason.GUARD_UNAVAILABLE,
        RefusalReason.POLICY_REFUSED,
    )
    assert service.operations.ids() == []
    # The adapter refuses on its own as well, for a guard that let it through.
    adapter = service.adapter_named(service.site(SITE), "tapis")
    with pytest.raises(Refusal) as direct:
        await adapter.run(spec, operation_id="op-guarded")
    print(f"LQ38 adapter refusal {direct.value.to_dict()}")
    assert direct.value.reason == RefusalReason.POLICY_REFUSED
    assert len(stub.state.requests_to("/v3/jobs")) == before
    await service.close()


async def test_a_tacc_host_gets_a_tms_credential_and_no_key(
    stub: TapisStub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The LQ-L17 path against the stub: TACC's trust manager holds the key, relay makes none."""
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    tacc_system = "stampede3-tapis-stub"
    stub.seed_system(tacc_system, host="stampede3.tacc.utexas.edu", jobWorkingDir=str(tmp_path))
    context = AdapterContext(
        site_id="stampede3", state_dir=tmp_path / "relay", content=ContentStore(tmp_path / "c")
    )
    config = TapisConfig.model_validate(
        tapis_config(stub, authn="tms", system_id=tacc_system, launcher="guarded")
    )
    adapter = TapisAdapter(config, context)
    made: list[str] = []

    def no_key(comment: str) -> None:
        made.append(comment)

    monkeypatch.setattr(register, "make_key_pair", no_key)
    registered = await register.register_host(adapter, None)
    print(f"tms registration {registered}")
    assert not registered.created_system and not registered.key_installed
    assert made == []
    credential = stub.state.credentials[(tacc_system, USERNAME)]
    assert credential["tms"] is True
    removed = await register.unregister_host(adapter, None)
    assert removed == {"system_id": tacc_system, "credential": True}
    assert tacc_system in stub.state.systems, "relay never deletes a system it did not make"


async def test_the_site_hook_on_a_tacc_host_registers_once(
    stub: TapisStub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``on_site_added`` with no ssh way in: a TMS credential, then nothing on a second run."""
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    tacc_system = "vista-tapis-stub"
    stub.seed_system(tacc_system, host="vista.tacc.utexas.edu", jobWorkingDir=str(tmp_path))
    entry = SiteEntry.model_validate(
        {
            "id": "vista",
            "adapter": "tapis",
            "config": tapis_config(stub, authn="tms", system_id=tacc_system),
        }
    )
    context = AdapterContext(
        site_id="vista", state_dir=tmp_path / "relay", content=ContentStore(tmp_path / "c")
    )
    adapter = TapisAdapter(TapisConfig.model_validate(entry.config), context)
    first = await adapter.on_site_added(entry)
    print(f"tms site hook {first}")
    assert first is not None and first["registered"] is True and first["authn"] == "tms"
    assert not first["key_installed"]
    assert stub.state.credentials[(tacc_system, USERNAME)]["tms"] is True
    again = await adapter.on_site_added(entry)
    assert again == {"system_id": tacc_system, "registered": False, "credential": "ok"}
    await register.unregister_host(adapter, None)


async def test_the_site_hook_before_sign_in_asks_for_it(
    stub: TapisStub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Signed out, the hook refuses ``needs_sign_in`` before any call to Tapis Systems."""
    use_memory_keyring(monkeypatch)
    entry = SiteEntry.model_validate(
        {"id": "vista", "adapter": "tapis", "config": tapis_config(stub, authn="tms")}
    )
    context = AdapterContext(
        site_id="vista", state_dir=tmp_path / "relay", content=ContentStore(tmp_path / "c")
    )
    adapter = TapisAdapter(TapisConfig.model_validate(entry.config), context)
    before = len(stub.state.requests_to("/v3/systems"))
    with pytest.raises(Refusal) as refused:
        await adapter.on_site_added(entry)
    print(f"site hook signed out {refused.value.to_dict()}")
    assert refused.value.reason == RefusalReason.NEEDS_SIGN_IN
    assert len(stub.state.requests_to("/v3/systems")) == before


async def test_max_minutes_never_falls_to_the_tapis_default() -> None:
    """Tapis fails a job past ``maxMinutes`` and defaults it to 10, so relay always sends one."""
    fork = {"id": "s"}
    batch = {
        "id": "b",
        "batchDefaultLogicalQueue": "normal",
        "batchLogicalQueues": [
            {"name": "normal", "maxMinutes": 2880},
            {"name": "open", "maxJobs": -1},
        ],
    }
    plain = JobSpec(executable="true")
    assert max_minutes(plain, fork, "FORK") == NO_WALLTIME_MINUTES
    assert max_minutes(plain, batch, "BATCH") == 2880
    open_queue = JobSpec.model_validate(
        {"executable": "true", "attributes": {"queue_name": "open"}}
    )
    assert max_minutes(open_queue, batch, "BATCH") == NO_WALLTIME_MINUTES
    timed = JobSpec.model_validate(
        {"executable": "true", "attributes": {"duration": 90.5, "queue_name": "open"}}
    )
    assert max_minutes(timed, batch, "BATCH") == 2
    assert max_minutes(timed, fork, "FORK") == 2


@pytest.mark.parametrize(
    ("limit", "minutes"),
    [
        ("2-00:00:00", 2880),
        ("1:00:00", 60),
        ("30:00", 30),
        ("30:01", 31),
        ("45", 45),
        ("infinite", None),
        ("UNLIMITED", None),
        ("", None),
        ("0:00", None),
    ],
)
async def test_slurm_time_limits_become_minutes(limit: str, minutes: int | None) -> None:
    assert register.slurm_minutes(limit) == minutes
