from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path
from typing import Protocol, cast


class ProgressModule(Protocol):
    def adapter_from_config(self, config: object) -> object:
        """Build a progress adapter."""
        ...

    def append_progress_record(self, record: dict[str, object]) -> None:
        """Append a progress record."""
        ...


class Adapter(Protocol):
    def observe_stdout(self, line: str) -> list[dict[str, object]]:
        """Observe stdout."""
        ...


def test_bounded_command_rejects_lammps_progress_adapter() -> None:
    module = _load_progress_module()
    try:
        module.adapter_from_config({"adapter": "lammps", "total_steps": 150})
    except ValueError as exc:
        assert "unsupported progress adapter: lammps" in str(exc)
    else:
        raise AssertionError("bounded_command must not own LAMMPS semantics")


def test_regex_progress_adapter_writes_side_channel(tmp_path: Path) -> None:
    module = _load_progress_module()
    adapter = cast(
        Adapter,
        module.adapter_from_config(
            {
                "adapter": "regex",
                "pattern": r"iter=(?P<current>\d+) of (?P<total>\d+)",
                "label": "iteration",
                "current_group": "current",
                "total_group": "total",
            }
        ),
    )

    record = adapter.observe_stdout("iter=4 of 10\n")[0]
    sidecar = tmp_path / "progress.jsonl"
    previous = os.environ.get("CLIO_RELAY_PROGRESS_FILE")
    previous_token = os.environ.get("CLIO_RELAY_PROGRESS_TOKEN")
    os.environ["CLIO_RELAY_PROGRESS_FILE"] = str(sidecar)
    os.environ["CLIO_RELAY_PROGRESS_TOKEN"] = "test-token"
    try:
        module.append_progress_record(record)
    finally:
        if previous is None:
            os.environ.pop("CLIO_RELAY_PROGRESS_FILE", None)
        else:
            os.environ["CLIO_RELAY_PROGRESS_FILE"] = previous
        if previous_token is None:
            os.environ.pop("CLIO_RELAY_PROGRESS_TOKEN", None)
        else:
            os.environ["CLIO_RELAY_PROGRESS_TOKEN"] = previous_token

    decoded = json.loads(sidecar.read_text(encoding="utf-8"))
    assert decoded["label"] == "iteration"
    assert decoded["current"] == 4
    assert decoded["total"] == 10
    assert decoded["metadata"]["source"] == "jarvis_package"
    assert decoded["metadata"]["package_name"] == "clio_relay.bounded_command"
    assert decoded["metadata"]["relay_progress_token"] == "test-token"


def test_regex_progress_adapter_cannot_spoof_package_identity() -> None:
    module = _load_progress_module()
    adapter = cast(
        Adapter,
        module.adapter_from_config(
            {
                "adapter": "regex",
                "pattern": r"step=(?P<current>\d+)",
                "metadata": {
                    "source": "jarvis_package",
                    "adapter": "lammps",
                    "package_name": "builtin.lammps",
                    "package_version": "builtin",
                    "run_id": "job_spoofed",
                    "user_field": "kept",
                },
            }
        ),
    )

    record = adapter.observe_stdout("step=4\n")[0]
    metadata = cast(dict[str, object], record["metadata"])

    assert metadata["source"] == "jarvis_package"
    assert metadata["adapter"] == "regex"
    assert metadata["package_name"] == "clio_relay.bounded_command"
    assert metadata["package_version"] == "builtin"
    assert "run_id" not in metadata
    assert metadata["user_field"] == "kept"


def _load_progress_module() -> ProgressModule:
    path = (
        Path(__file__).parents[1]
        / "jarvis-packages"
        / "clio_relay"
        / "clio_relay"
        / "bounded_command"
        / "progress.py"
    )
    spec = importlib.util.spec_from_file_location("bounded_command_progress_test", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load progress module")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return cast(ProgressModule, module)
