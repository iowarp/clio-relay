from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Protocol, cast


class ProgressModule(Protocol):
    def adapter_from_config(self, config: object) -> object:
        """Build a progress adapter."""
        ...

    def render_progress_marker(self, record: dict[str, object]) -> str:
        """Render a progress marker."""
        ...


class Adapter(Protocol):
    def observe_stdout(self, line: str) -> list[dict[str, object]]:
        """Observe stdout."""
        ...


def test_lammps_progress_adapter_emits_eta_metadata() -> None:
    module = _load_progress_module()
    adapter = cast(
        Adapter,
        module.adapter_from_config(
            {
                "adapter": "lammps",
                "total_steps": 150,
                "warmup_samples": 1,
                "metadata": {"application": "materio"},
            }
        ),
    )

    assert adapter.observe_stdout("Step Temp E_pair\n") == []
    first = adapter.observe_stdout("0 1.44 -6.0\n")
    adapter.observe_stdout("25 1.40 -5.9\n")
    third = adapter.observe_stdout("50 1.41 -5.8\n")

    assert first[0]["label"] == "timestep"
    assert first[0]["current"] == 0
    assert first[0]["total"] == 150
    metadata = cast(dict[str, object], third[0]["metadata"])
    assert metadata["adapter"] == "lammps"
    assert metadata["application"] == "materio"
    assert "eta_seconds" in metadata
    assert metadata["remaining_steps"] == 100


def test_regex_progress_adapter_emits_marker() -> None:
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
    marker = module.render_progress_marker(record)

    assert marker.startswith("CLIO_PROGRESS ")
    decoded = json.loads(marker.removeprefix("CLIO_PROGRESS "))
    assert decoded["label"] == "iteration"
    assert decoded["current"] == 4
    assert decoded["total"] == 10


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
