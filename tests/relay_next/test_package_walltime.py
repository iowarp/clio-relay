"""A package's contract sets its walltime and layout, so the spend ask prices what runs (RL15)."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from clio_relay.adapters.hooks import ask_text
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.spec import JobSpec
from clio_relay.packages import PackageCatalog, PackageEntry, contract_value
from clio_relay.sites.models import SiteEntry
from clio_relay.sites.store import SiteStore
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from ..guard.conftest import listed, make_repo

pytestmark = [pytest.mark.release_platform("posix")]

CONTRACT = """\
version: 0.2.0
parameters:
  properties:
    nprocs: {type: integer, default: 8}
    ppn: {type: integer, default: 4}
    walltime: {type: integer, description: Seconds; absent means no limit}
    queue: {type: string, default: ""}
resources:
  process_count: {param: nprocs}
  processes_per_node: {param: ppn}
  duration: {param: walltime}
  queue_name: {param: queue}
"""


def _stage(paths: RelayPaths, tmp_path: Path) -> PackageEntry:
    repo = make_repo(tmp_path / "src")
    (repo / "demo" / "hello" / "contract.yaml").write_text(CONTRACT)
    [entry] = PackageCatalog(paths.packages_dir).store("fx", [listed(repo, "demo.hello")])
    return entry


def _site(paths: RelayPaths, *, duration: str | None = None) -> None:
    entry: dict[str, Any] = {
        "id": "fx",
        "adapter": "fixture",
        "config": {"queue_s": 0.1, "run_s": 0.1},
        "policy": "open",
        "spend": {"ask": "always"},
    }
    if duration is not None:
        entry["defaults"] = {"duration": duration, "queue": "batch"}
    SiteStore(paths.sites_file).add(SiteEntry.model_validate(entry))


def _run(parameters: dict[str, Any], **extra: Any) -> JobSpec:
    return JobSpec.model_validate(
        {"package": {"name": "demo.hello", "parameters": parameters}, **extra}
    )


def test_contract_values_read_the_parameters() -> None:
    values = {"nprocs": 10, "ppn": "4", "walltime": 1800}
    assert contract_value(3, values) == 3
    assert contract_value({"param": "walltime"}, values) == 1800
    assert contract_value({"param": "missing"}, values) is None
    assert contract_value({"ceil_div": ["nprocs", "ppn"]}, values) == 3
    assert contract_value({"div": ["nprocs", "ppn"]}, values) == 2
    assert contract_value({"mul": ["walltime", 2]}, values) == 3600
    assert contract_value({"add": ["missing", 2]}, values) is None
    assert contract_value({"div": ["nprocs", 0]}, values) is None
    assert contract_value({"pow": ["nprocs", 2]}, values) is None


@pytest.mark.asyncio
async def test_the_spend_ask_prices_the_package_walltime(
    relay_paths: RelayPaths, tmp_path: Path
) -> None:
    _site(relay_paths)
    _stage(relay_paths, tmp_path)
    service = RelayService(relay_paths)
    record = await service.start_run("fx", _run({"walltime": 1800}))
    assert record.state == OperationState.AWAITING_INPUT
    charge = (record.result or {})["charge"]
    assert (charge["nodes"], charge["walltime_s"]) == (2, 1800)
    assert charge["node_hours"] == pytest.approx(1.0)
    (ask,) = service.outstanding(record)
    assert "Walltime: 30 min, up to 1 node-hour in all" in ask_text(ask)
    assert "sets no walltime" not in ask_text(ask)
    stored = JobSpec.model_validate(service.operation(record.operation_id).spec)
    assert stored.attributes.duration == timedelta(seconds=1800)
    assert stored.attributes.queue_name is None
    assert (stored.resources.process_count, stored.resources.processes_per_node) == (8, 4)
    await service.close()


@pytest.mark.asyncio
async def test_the_site_default_fills_what_the_package_leaves_open(
    relay_paths: RelayPaths, tmp_path: Path
) -> None:
    _site(relay_paths, duration="PT2H")
    _stage(relay_paths, tmp_path)
    service = RelayService(relay_paths)
    record = await service.start_run("fx", _run({"nprocs": 4, "queue": "long"}))
    charge = (record.result or {})["charge"]
    assert (charge["nodes"], charge["walltime_s"], charge["queue"]) == (1, 7200, "long")

    given = _run(
        {"walltime": 1800},
        attributes={"duration": 600},
        resources={"node_count": 1},
    )
    record = await service.start_run("fx", given)
    charge = (record.result or {})["charge"]
    assert (charge["nodes"], charge["walltime_s"], charge["queue"]) == (1, 600, "batch")
    stored = JobSpec.model_validate(service.operation(record.operation_id).spec)
    assert stored.resources.process_count is None
    await service.close()
