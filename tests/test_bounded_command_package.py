from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, cast

from pytest import MonkeyPatch


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
