"""RL5: an alternative whose settings do not build is skipped, and the skip is shown."""

from __future__ import annotations

import pytest

from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import SiteEntry
from clio_relay.sites.routing import route, routes
from clio_relay.sites.store import SiteStore
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

pytestmark = pytest.mark.release_platform("posix")


def _site(paths: RelayPaths, alternatives: dict[str, dict[str, object]]) -> SiteEntry:
    entry = SiteEntry.model_validate(
        {
            "id": "fx",
            "adapter": "fixture",
            "config": {"queue_s": -1},
            "alternatives": alternatives,
        }
    )
    return SiteStore(paths.sites_file).add(entry)


def test_route_skips_unusable_settings_and_moves_on(relay_paths: RelayPaths) -> None:
    entry = _site(relay_paths, {"local": {}})
    routed = route(entry, "exec")
    assert routed.adapter == "local"
    (skipped,) = routed.skipped
    assert (skipped.site, skipped.adapter, skipped.reason) == ("fx", "fixture", "invalid_config")
    assert skipped.message
    exec_route = next(item for item in routes(entry) if item.verb == "exec")
    assert exec_route.adapter == "local"
    assert exec_route.via == "alternative"
    assert [item.adapter for item in exec_route.skipped] == ["fixture"]


def test_every_alternative_unusable_returns_each_reason(relay_paths: RelayPaths) -> None:
    entry = _site(relay_paths, {"local": {"no_such_setting": 1}})
    with pytest.raises(Refusal) as refused:
        route(entry, "exec")
    assert refused.value.reason == RefusalReason.INVALID_CONFIG
    assert "fixture:" in refused.value.message
    assert "local:" in refused.value.message
    data = refused.value.to_dict()["data"]
    assert [item["adapter"] for item in data["skipped"]] == ["fixture", "local"]
    assert all(item["site"] == "fx" for item in data["skipped"])


@pytest.mark.asyncio
async def test_service_result_and_describe_show_the_skip(relay_paths: RelayPaths) -> None:
    _site(relay_paths, {"local": {}})
    service = RelayService(relay_paths)
    result = await service.exec("fx", ["echo", "routed"], max_output_bytes=1024)
    assert result["exit_code"] == 0
    assert "routed" in result["stdout"]
    assert [item["adapter"] for item in result["skipped"]] == ["fixture"]
    described = await service.describe("fx")
    exec_route = next(item for item in described["routes"] if item["verb"] == "exec")
    assert exec_route["adapter"] == "local"
    assert exec_route["skipped"][0]["reason"] == "invalid_config"
    await service.close()