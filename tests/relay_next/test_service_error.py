"""A service relay cannot bring to the desktop is an error; one review covers relay's jarvis-cd."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from clio_relay import jarvis_submit
from clio_relay.adapters.base import BaseAdapter
from clio_relay.surface.guarding import review_asks
from clio_relay.surface.results import operation_result


def _view(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "operation_id": "op_1",
        "site": "ares",
        "state": "running",
        "kind": "serve",
        "result": result,
    }


def test_a_service_that_cannot_reach_the_desktop_is_an_error_result() -> None:
    error = {"reason": "host_key_unverified", "message": "ssh could not verify n1's host key"}
    found = operation_result(_view({"service_error": error}))
    assert found.is_error
    text = getattr(found.content[0], "text", "")
    assert "host_key_unverified" in text
    assert not operation_result(_view({})).is_error
    # A service that was ready and later lost its forward reports it without failing the task.
    assert not operation_result(_view({"service_error": error, "ready_at": "t"})).is_error


class _Bootstraps(BaseAdapter[Any]):
    async def install_jarvis(self, *_a: Any, **_k: Any) -> Any:  # pyright: ignore[reportIncompatibleMethodOverride]
        return None


class _Provider:
    def __init__(self) -> None:
        self.asked: list[dict[str, Any]] = []

    async def approve(self, **kwargs: Any) -> Any:
        self.asked.append(kwargs)
        return kwargs


def _service(adapter: Any) -> Any:
    def named(_site: Any, _adapter: str) -> Any:
        return adapter

    return SimpleNamespace(ask_provider=_Provider(), adapter_named=named)


def _reviews(service: Any) -> list[Any]:
    site: Any = SimpleNamespace(id="ares", title="Ares")
    record: Any = SimpleNamespace(operation_id="op_1", spec_digest="d" * 64, adapter="ssh")
    decision: Any = SimpleNamespace(
        data={"asks": [{"message": "Review pvserver.", "binding": {"kind": "package_review"}}]}
    )
    return asyncio.run(review_asks(service, site, record, decision))


def test_a_package_review_also_covers_relays_jarvis_cd() -> None:
    adapter = object.__new__(_Bootstraps)
    adapter.config = SimpleNamespace(jarvis=None)
    service = _service(adapter)
    (asked,) = _reviews(service)
    assert asked["binding"]["jarvis"] == jarvis_submit.JARVIS_PIN
    assert "a yes also lets relay install jarvis-cd" in asked["message"]


def test_a_site_with_its_own_jarvis_cd_gets_the_review_alone() -> None:
    adapter = object.__new__(_Bootstraps)
    adapter.config = SimpleNamespace(jarvis="/opt/jarvis/bin/jarvis")
    (asked,) = _reviews(_service(adapter))
    assert "jarvis" not in asked["binding"]
    assert asked["message"] == "Review pvserver."
