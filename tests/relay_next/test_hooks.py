"""Hook entry points: a hook whose module is not installed falls back to the stub."""

from __future__ import annotations

from collections.abc import Callable
from importlib.metadata import EntryPoint

import pytest

from clio_relay.adapters import hooks


def _stub(_context: hooks.HookContext) -> object:
    return object()


def _points(*points: EntryPoint) -> Callable[..., list[EntryPoint]]:
    def entry_points(*, group: str) -> list[EntryPoint]:
        assert group == hooks.HOOK_GROUP
        return list(points)

    return entry_points


def test_missing_hook_module_uses_the_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    point = EntryPoint("asks", "clio_relay_not_installed_xyz.asks:factory", hooks.HOOK_GROUP)
    monkeypatch.setattr(hooks, "entry_points", _points(point))

    assert hooks.hook_factory("asks", _stub) is _stub


def test_a_broken_import_inside_an_installed_hook_is_raised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    point = EntryPoint("asks", "clio_relay.adapters.hooks:factory", hooks.HOOK_GROUP)
    monkeypatch.setattr(hooks, "entry_points", _points(point))

    def load(self: EntryPoint) -> object:
        raise ModuleNotFoundError("No module named 'some_dependency'", name="some_dependency")

    monkeypatch.setattr(EntryPoint, "load", load)

    with pytest.raises(ModuleNotFoundError):
        hooks.hook_factory("asks", _stub)


def test_an_installed_hook_replaces_the_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    point = EntryPoint("guard", "clio_relay.adapters.hooks:HookContext", hooks.HOOK_GROUP)
    monkeypatch.setattr(hooks, "entry_points", _points(point))

    assert hooks.hook_factory("guard", _stub) is hooks.HookContext
