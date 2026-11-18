"""RelayService with the guard hook: what a guarded site refuses (LQ16, LQ18)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from clio_relay.adapters.hooks import set_hook
from clio_relay.guard import Guard, create_guard
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec
from clio_relay.sites.models import SiteEntry
from clio_relay.sites.store import SiteStore
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths


@pytest.fixture
def service(tmp_path: Path) -> Iterator[RelayService]:
    set_hook("guard", create_guard)
    paths = RelayPaths(config_dir=tmp_path / "config", state_dir=tmp_path / "state")
    store = SiteStore(paths.sites_file)
    for site_id, policy in (("hpc", "guarded"), ("lab", "open")):
        store.add(
            SiteEntry.model_validate(
                {
                    "id": site_id,
                    "adapter": "fixture",
                    "config": {"queue_s": 0.1, "run_s": 0.1},
                    "policy": policy,
                    "spend": {"ask": "never"},
                }
            )
        )
    try:
        yield RelayService(paths)
    finally:
        set_hook("guard", None)


@pytest.mark.asyncio
async def test_the_service_uses_the_guard(service: RelayService) -> None:
    assert isinstance(service.guard, Guard)


@pytest.mark.asyncio
async def test_a_guarded_site_records_nothing_for_a_free_form_run(service: RelayService) -> None:
    with pytest.raises(Refusal) as raised:
        await service.start_run("hpc", JobSpec(executable="bash", arguments=["-c", "rm -rf ~"]))
    assert raised.value.reason == RefusalReason.POLICY_REFUSED
    assert raised.value.data["next_step"] == "package_review"
    assert service.operations.all() == []
    record = await service.start_run("lab", JobSpec(executable="hostname"))
    assert record.site == "lab"


@pytest.mark.asyncio
async def test_guarded_exec_runs_read_only_commands_only(service: RelayService) -> None:
    with pytest.raises(Refusal) as raised:
        await service.exec("hpc", ["rm", "-rf", "/scratch"], max_output_bytes=4096)
    assert raised.value.reason == RefusalReason.POLICY_REFUSED
    assert "squeue" in raised.value.data["read_only"]
    done = await service.exec("hpc", ["hostname"], max_output_bytes=4096)
    assert done["stdout"] == "fixture-login-1\n"
    lab = await service.exec("lab", ["echo", "anything"], max_output_bytes=4096)
    assert lab["stdout"] == "anything\n"
