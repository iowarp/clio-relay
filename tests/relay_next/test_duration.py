"""Relay never sets a walltime of its own: no duration means the site's or the scheduler's."""

from __future__ import annotations

from datetime import timedelta

import pytest

from clio_relay.jobs.records import OperationState
from clio_relay.jobs.spec import JobSpec
from clio_relay.sites.models import SiteEntry, SpendPolicy
from clio_relay.sites.store import SiteStore
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

pytestmark = [pytest.mark.release_platform("posix")]

ECHO = JobSpec(executable="echo", arguments=["hi"])


def _site(paths: RelayPaths, *, spend: str | float, duration: str | None = None) -> None:
    entry: dict[str, object] = {
        "id": "fx",
        "adapter": "fixture",
        "config": {"queue_s": 0.1, "run_s": 0.1},
        "policy": "open",
        "spend": {"ask": spend},
    }
    if duration is not None:
        entry["defaults"] = {"duration": duration}
    SiteStore(paths.sites_file).add(SiteEntry.model_validate(entry))


def test_a_spec_without_a_duration_sends_no_walltime() -> None:
    assert ECHO.attributes.duration is None
    assert ECHO.node_hours() is None
    assert ECHO.attributes.to_psij().duration is None
    again = JobSpec.model_validate(ECHO.model_dump(mode="json"))
    assert again.attributes.duration is None
    given = JobSpec.model_validate({"executable": "x", "attributes": {"duration": "PT2H"}})
    assert given.attributes.to_psij().duration == timedelta(hours=2)
    assert given.node_hours() == pytest.approx(2.0)


@pytest.mark.parametrize(
    ("ask", "node_hours", "expected"),
    [
        ("always", None, True),
        ("never", None, False),
        (5.0, None, True),
        (5.0, 1.0, False),
        (5.0, 6.0, True),
    ],
)
def test_an_unknown_cost_asks_unless_the_site_never_does(
    ask: str | float, node_hours: float | None, expected: bool
) -> None:
    assert SpendPolicy.model_validate({"ask": ask}).needs_approval(node_hours) is expected


@pytest.mark.asyncio
async def test_unknown_cost_asks_and_says_so(relay_paths: RelayPaths) -> None:
    _site(relay_paths, spend=100.0)
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    assert record.state == OperationState.AWAITING_INPUT
    charge = (record.result or {})["charge"]
    assert charge["walltime_s"] is None and charge["node_hours"] is None
    (ask,) = service.outstanding(record)
    assert "unknown" in ask.message
    assert "scheduler's default limit applies" in ask.message
    await service.close()


@pytest.mark.asyncio
async def test_the_site_default_walltime_applies(relay_paths: RelayPaths) -> None:
    _site(relay_paths, spend=100.0, duration="PT2H")
    service = RelayService(relay_paths)
    record = await service.start_run("fx", ECHO)
    assert record.state == OperationState.QUEUED
    charge = (record.result or {})["charge"]
    assert charge["walltime_s"] == 7200
    assert charge["node_hours"] == pytest.approx(2.0)
    stored = service.operation(record.operation_id)
    assert stored.spec["attributes"]["duration"] in ("PT2H", 7200, 7200.0)
    await service.close()
