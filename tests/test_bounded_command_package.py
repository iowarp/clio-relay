from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, cast

from pytest import MonkeyPatch, raises


def test_bounded_tail_discards_oldest_output(monkeypatch: MonkeyPatch) -> None:
    package = _load_bounded_package(monkeypatch)
    tail = cast(Any, package)._BoundedTextTail(limit=8)

    tail.append("abcdef")
    tail.append("ghij")

    assert tail.size == 8
    assert tail.render() == "cdefghij"


def test_bounded_command_streams_but_retains_only_bounded_tails(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    package = _load_bounded_package(monkeypatch)

    def discard_output(*args: object, **kwargs: object) -> None:
        del args, kwargs

    monkeypatch.setattr(package, "print", discard_output, raising=False)
    command = [
        sys.executable,
        "-c",
        (
            "import sys; "
            "sys.stdout.write('x' * 1100000 + 'stdout-end'); "
            "sys.stderr.write('y' * 1100000 + 'stderr-end')"
        ),
    ]

    result = cast(Any, package)._run_streaming(
        command,
        cwd=tmp_path,
        env=os.environ.copy(),
        timeout=20,
        progress_config=None,
    )

    assert result.returncode == 0
    assert len(result.stdout) == 1_048_576
    assert len(result.stderr) == 1_048_576
    assert result.stdout.endswith("stdout-end")
    assert result.stderr.endswith("stderr-end")


def test_bounded_command_scrubs_relay_capabilities_but_keeps_provider_credentials(
    monkeypatch: MonkeyPatch,
) -> None:
    package = _load_bounded_package(monkeypatch)
    environment = {
        "CLIO_RELAY_API_TOKEN": "api",
        "CLIO_RELAY_FRP_TOKEN": "frp",
        "CLIO_RELAY_STCP_SECRET": "stcp",
        "CLIO_RELAY_PROGRESS_FILE": "progress",
        "CLIO_RELAY_PROGRESS_TOKEN": "progress-token",
        "CLIO_RELAY_RUNTIME_METADATA_FILE": "runtime",
        "CLIO_RELAY_RUNTIME_METADATA_TOKEN": "runtime-token",
        "CLIO_RELAY_CONNECTOR_OWNER_TOKEN": "owner",
        "SITE_PROVIDER_TOKEN": "provider-owned",
        "PATH": "kept",
    }

    scrubbed = cast(Any, package)._scrub_relay_environment(environment)

    assert scrubbed == {"SITE_PROVIDER_TOKEN": "provider-owned", "PATH": "kept"}


def test_configure_menu_declares_every_configured_setting(monkeypatch: MonkeyPatch) -> None:
    package = _load_bounded_package(monkeypatch)
    menu = _configure_menu(package)

    assert set(menu) == {"command", "workdir", "env", "timeout_seconds", "progress"}
    assert menu["command"]["type"] is list
    assert menu["workdir"]["type"] is str
    assert menu["env"]["type"] is dict
    assert menu["timeout_seconds"]["type"] is int
    assert menu["progress"]["type"] is dict
    for name, option in menu.items():
        message = option.get("msg")
        assert isinstance(message, str) and message, f"{name} declares no description"


def test_configure_menu_marks_only_the_command_as_required(monkeypatch: MonkeyPatch) -> None:
    # JARVIS derives "required" from a missing or None menu default, so an optional
    # setting without a concrete default is rejected before the package ever runs.
    package = _load_bounded_package(monkeypatch)
    menu = _configure_menu(package)

    assert "default" not in menu["command"]
    for name in ("workdir", "env", "timeout_seconds", "progress"):
        assert menu[name].get("default") is not None, f"{name} would become required"


def test_menu_defaults_execute_a_command_without_limit_or_workdir(
    monkeypatch: MonkeyPatch,
) -> None:
    # JARVIS applies every menu default into the package config before start(),
    # so the declared defaults must mean "no working directory" and "no timeout".
    package = _load_bounded_package(monkeypatch)

    def discard_output(*args: object, **kwargs: object) -> None:
        del args, kwargs

    monkeypatch.setattr(package, "print", discard_output, raising=False)
    command = cast(Any, package).BoundedCommand()
    command.config = {
        name: option["default"]
        for name, option in _configure_menu(package).items()
        if "default" in option
    }
    assert set(command.config) == {"workdir", "env", "timeout_seconds", "progress"}
    command.config["command"] = [sys.executable, "-c", "print('menu-default-run')"]

    command.start()


def test_configure_rejects_a_command_that_is_not_a_string_vector(
    monkeypatch: MonkeyPatch,
) -> None:
    package = _load_bounded_package(monkeypatch)
    command = cast(Any, package).BoundedCommand()
    config: dict[str, Any] = {}
    command.config = config

    with raises(ValueError):
        command._configure(command="hostname")
    with raises(ValueError):
        command._configure(command=[])
    with raises(ValueError):
        command._configure(command=["hostname", 3])

    command._configure(workdir="/tmp")
    assert config == {"workdir": "/tmp"}


def test_default_progress_setting_configures_no_adapter(monkeypatch: MonkeyPatch) -> None:
    package = _load_bounded_package(monkeypatch)
    progress = _load_progress_module(monkeypatch)
    default = _configure_menu(package)["progress"]["default"]
    assert cast(Any, progress).adapter_from_config(default) is None


def _configure_menu(package: ModuleType) -> dict[str, dict[str, Any]]:
    command = cast(Any, package).BoundedCommand()
    options = cast(list[dict[str, Any]], command._configure_menu())
    menu: dict[str, dict[str, Any]] = {}
    for option in options:
        assert isinstance(option, dict)
        name = option.get("name")
        assert isinstance(name, str) and name
        assert name not in menu
        menu[name] = option
    return menu


def _load_progress_module(monkeypatch: MonkeyPatch) -> ModuleType:
    path = (
        Path(__file__).parents[1]
        / "jarvis-packages"
        / "clio_relay"
        / "clio_relay"
        / "bounded_command"
        / "progress.py"
    )
    spec = importlib.util.spec_from_file_location("clio_relay_bounded_progress_test", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load bounded command progress module")
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def _load_bounded_package(monkeypatch: MonkeyPatch) -> ModuleType:
    package_root = Path(__file__).parents[1] / "jarvis-packages" / "clio_relay" / "clio_relay"

    class Application:
        config: dict[str, Any]

    jarvis_api = ModuleType("clio_relay._jarvis_api")
    cast(Any, jarvis_api).Application = Application
    progress = ModuleType("clio_relay.bounded_command.progress")

    def no_progress_adapter(config: object) -> None:
        del config

    def discard_progress_record(record: dict[str, object]) -> None:
        del record

    cast(Any, progress).adapter_from_config = no_progress_adapter
    cast(Any, progress).append_progress_record = discard_progress_record
    bounded_package = ModuleType("clio_relay.bounded_command")
    cast(Any, bounded_package).__path__ = []
    monkeypatch.setitem(sys.modules, "clio_relay._jarvis_api", jarvis_api)
    monkeypatch.setitem(sys.modules, "clio_relay.bounded_command", bounded_package)
    monkeypatch.setitem(sys.modules, "clio_relay.bounded_command.progress", progress)

    path = package_root / "bounded_command" / "pkg.py"
    spec = importlib.util.spec_from_file_location("clio_relay_bounded_command_pkg_test", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load bounded command package")
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module
