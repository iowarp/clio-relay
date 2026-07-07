"""Application progress adapters bound to declared JARVIS package identities."""

from __future__ import annotations

import statistics
import time
from dataclasses import dataclass, field
from typing import Any, cast

import yaml


@dataclass
class LammpsThermoProgressAdapter:
    """Parse LAMMPS thermo output after a declared JARVIS LAMMPS package starts."""

    package_name: str = "builtin.lammps"
    total_steps: float | None = None
    warmup_samples: int = 2
    sample_window: int = 8
    active_columns: list[str] = field(default_factory=lambda: [])
    active_step_column: int | None = None
    samples: list[tuple[float, float]] = field(default_factory=lambda: [])
    last_step: float | None = None

    def observe_stdout(self, text: str) -> list[dict[str, object]]:
        """Extract progress observations from stdout text."""
        records: list[dict[str, object]] = []
        for line in text.splitlines():
            record = self.observe_line(line)
            if record is not None:
                records.append(record)
        return records

    def observe_line(self, line: str) -> dict[str, object] | None:
        """Extract one progress observation from a LAMMPS output line."""
        stripped = line.strip()
        if stripped == "":
            return None
        if _looks_like_thermo_header(stripped):
            self.active_columns = stripped.split()
            try:
                self.active_step_column = self.active_columns.index("Step")
            except ValueError:
                self.active_step_column = None
            return None
        if stripped.startswith("Loop time of "):
            self.active_columns = []
            self.active_step_column = None
            return None
        if self.active_step_column is None:
            return None
        parts = stripped.split()
        if len(parts) != len(self.active_columns):
            return None
        try:
            step = float(parts[self.active_step_column])
        except ValueError:
            return None
        if step < 0:
            return None
        self.last_step = step
        now = time.monotonic()
        self.samples.append((step, now))
        self.samples = self.samples[-self.sample_window :]
        prediction = self._prediction(step)
        return _drop_none(
            {
                "label": "timestep",
                "current": step,
                "total": self.total_steps,
                "unit": "step",
                "message": _lammps_message(step, self.total_steps),
                "metadata": {
                    "source": "jarvis_package",
                    "package_name": self.package_name,
                    "adapter": "lammps",
                    "columns": self.active_columns,
                    "step_column": "Step",
                    **prediction,
                },
            }
        )

    def _prediction(self, current_step: float) -> dict[str, object]:
        if self.total_steps is None or len(self.samples) <= self.warmup_samples:
            return {"confidence": "warming_up", "samples": len(self.samples)}
        rates: list[float] = []
        for (previous_step, previous_time), (step, timestamp) in zip(
            self.samples,
            self.samples[1:],
            strict=False,
        ):
            step_delta = step - previous_step
            time_delta = timestamp - previous_time
            if step_delta <= 0 or time_delta < 0:
                continue
            rates.append(time_delta / step_delta)
        if not rates:
            return {"confidence": "warming_up", "samples": len(self.samples)}
        ordered = sorted(rates)
        trimmed = ordered[1:-1] if len(ordered) > 2 else ordered
        seconds_per_step = statistics.fmean(trimmed)
        remaining_steps = max(0.0, self.total_steps - current_step)
        return {
            "prediction_method": "trimmed_step_time_after_warmup",
            "seconds_per_step": seconds_per_step,
            "eta_seconds": remaining_steps * seconds_per_step,
            "remaining_steps": remaining_steps,
            "samples": len(self.samples),
            "confidence": "observed" if len(rates) >= 2 else "low_sample",
        }


def package_progress_adapter_from_pipeline(
    pipeline_yaml: str,
) -> LammpsThermoProgressAdapter | None:
    """Return a package-owned progress adapter for a declared JARVIS pipeline."""
    loaded = cast(object, yaml.safe_load(pipeline_yaml))
    if not isinstance(loaded, dict):
        return None
    typed_document = cast(dict[str, object], loaded)
    packages = typed_document.get("pkgs")
    if not isinstance(packages, list):
        return None
    typed_packages = cast(list[object], packages)
    for package in typed_packages:
        if not isinstance(package, dict):
            continue
        typed_package = cast(dict[str, Any], package)
        package_type = typed_package.get("pkg_type")
        if package_type not in {"builtin.lammps", "lammps", "jarvis_cd.builtin.lammps"}:
            continue
        return LammpsThermoProgressAdapter(
            package_name=str(package_type),
            total_steps=_optional_float(
                typed_package.get("total_steps")
                or typed_package.get("steps")
                or _nested_progress_total(typed_package.get("progress"))
            ),
        )
    return None


def _nested_progress_total(value: object) -> object:
    if not isinstance(value, dict):
        return None
    typed = cast(dict[str, object], value)
    return typed.get("total_steps") or typed.get("total")


def _optional_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str) and value != "":
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _looks_like_thermo_header(line: str) -> bool:
    columns = line.split()
    return "Step" in columns and len(columns) >= 2


def _lammps_message(step: float, total_steps: float | None) -> str:
    if total_steps is None:
        return f"LAMMPS step {int(step)}"
    return f"LAMMPS step {int(step)} of {int(total_steps)}"


def _drop_none(value: dict[str, object | None]) -> dict[str, object]:
    return {key: item for key, item in value.items() if item is not None}
