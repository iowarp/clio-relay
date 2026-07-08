"""LAMMPS package progress adapter for the builtin JARVIS LAMMPS package."""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from typing import Any, cast

from clio_relay.progress_provenance import package_progress_metadata


@dataclass
class LammpsThermoProgressAdapter:
    """Parse LAMMPS thermo output after a declared JARVIS LAMMPS package starts."""

    package_name: str = "builtin.lammps"
    package_version: str = "unknown"
    run_id: str = ""
    total_steps: float | None = None
    warmup_samples: int = 2
    sample_window: int = 8
    active_columns: list[str] = field(default_factory=lambda: [])
    active_step_column: int | None = None
    active_time_column: int | None = None
    active_time_column_name: str | None = None
    samples: list[tuple[float, float]] = field(default_factory=lambda: [])
    last_step: float | None = None
    completed_steps: float = 0.0
    active_run_steps: float | None = None
    active_run_start_step: float | None = None
    active_package_stdout: bool = False
    emitted_keys: set[tuple[float, float, float, float | None]] = field(
        default_factory=lambda: set()
    )

    def observe_stdout(self, text: str) -> list[dict[str, object]]:
        """Extract progress observations from stdout text."""
        records: list[dict[str, object]] = []
        for line in text.splitlines():
            record = self.observe_line(line)
            if record is not None:
                records.append(record)
        return records

    def observe_jarvis_stdout(self, text: str) -> list[dict[str, object]]:
        """Extract progress only from this package's JARVIS stdout scope."""
        records: list[dict[str, object]] = []
        for line in text.splitlines():
            stripped = line.strip()
            if stripped == f"[{self.package_name}] [START] BEGIN":
                self.active_package_stdout = True
                continue
            if stripped == f"[{self.package_name}] [START] END":
                self.active_package_stdout = False
                continue
            if not self.active_package_stdout:
                continue
            record = self.observe_line(line)
            if record is not None:
                records.append(record)
        return records

    def observe_line(self, line: str) -> dict[str, object] | None:
        """Extract one progress observation from a LAMMPS output line."""
        stripped = line.strip()
        if stripped == "":
            return None
        reset_step = _parse_reset_timestep(stripped)
        if reset_step is not None:
            self.active_run_start_step = reset_step
            self.last_step = reset_step
            return None
        run_steps = _parse_run_steps(stripped)
        if run_steps is not None:
            self.active_run_steps = run_steps
            self.active_run_start_step = None
            return None
        if _looks_like_thermo_header(stripped):
            self.active_columns = stripped.split()
            try:
                self.active_step_column = self.active_columns.index("Step")
            except ValueError:
                self.active_step_column = None
            self.active_time_column = None
            self.active_time_column_name = None
            for candidate in ("CPU", "Cpu", "cpu"):
                if candidate in self.active_columns:
                    self.active_time_column = self.active_columns.index(candidate)
                    self.active_time_column_name = candidate
                    break
            return None
        if stripped.startswith("Loop time of "):
            if self.active_run_steps is not None:
                self.completed_steps += self.active_run_steps
            elif self.active_run_start_step is not None and self.last_step is not None:
                self.completed_steps += max(0.0, self.last_step - self.active_run_start_step)
            self.active_run_steps = None
            self.active_run_start_step = None
            self.active_columns = []
            self.active_step_column = None
            self.active_time_column = None
            self.active_time_column_name = None
            return None
        if self.active_step_column is None:
            return None
        if (
            self.total_steps is not None
            and self.completed_steps >= self.total_steps
            and self.active_run_steps is None
        ):
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
        elapsed_seconds = None
        if self.active_time_column is not None:
            elapsed_seconds = _optional_float(parts[self.active_time_column])
            if elapsed_seconds is not None and elapsed_seconds < 0:
                elapsed_seconds = None
        if self.active_run_start_step is None:
            self.active_run_start_step = step
        self.last_step = step
        current = self.completed_steps + max(0.0, step - self.active_run_start_step)
        if self.active_run_steps is not None:
            current = min(self.completed_steps + self.active_run_steps, current)
        progress_key = (self.completed_steps, current, step, elapsed_seconds)
        if progress_key in self.emitted_keys:
            return None
        self.emitted_keys.add(progress_key)
        if elapsed_seconds is not None:
            self.samples.append((current, elapsed_seconds))
            self.samples = self.samples[-self.sample_window :]
        prediction = self._prediction(current, elapsed_seconds=elapsed_seconds)
        return _drop_none(
            {
                "label": "timestep",
                "current": current,
                "total": self.total_steps,
                "unit": "step",
                "message": _lammps_message(current, self.total_steps),
                "metadata": {
                    "adapter": "lammps",
                    "columns": self.active_columns,
                    "step_column": "Step",
                    "absolute_step": step,
                    "run_start_step": self.active_run_start_step,
                    "run_steps": self.active_run_steps,
                    "completed_prior_runs": self.completed_steps,
                    "timing_column": self.active_time_column_name,
                    **prediction,
                }
                | package_progress_metadata(
                    {},
                    package_name=self.package_name,
                    package_version=self.package_version,
                    run_id=self.run_id,
                ),
            }
        )

    def _prediction(
        self,
        current_step: float,
        *,
        elapsed_seconds: float | None,
    ) -> dict[str, object]:
        if elapsed_seconds is None:
            return {
                "confidence": "timing_unavailable",
                "samples": len(self.samples),
                "prediction_status": "no_lammps_timing_column",
            }
        if self.total_steps is None or len(self.samples) <= self.warmup_samples:
            return {
                "confidence": "warming_up",
                "samples": len(self.samples),
                "prediction_status": "warming_up",
                "elapsed_seconds": elapsed_seconds,
            }
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
            return {
                "confidence": "warming_up",
                "samples": len(self.samples),
                "prediction_status": "warming_up",
                "elapsed_seconds": elapsed_seconds,
            }
        ordered = sorted(rates)
        trimmed = ordered[1:-1] if len(ordered) > 2 else ordered
        seconds_per_step = statistics.fmean(trimmed)
        remaining_steps = max(0.0, self.total_steps - current_step)
        return {
            "prediction_method": "trimmed_mean_step_time_after_warmup",
            "rate_samples": len(rates),
            "trimmed_rate_samples": len(trimmed),
            "min_seconds_per_step": min(trimmed),
            "max_seconds_per_step": max(trimmed),
            "seconds_per_step": seconds_per_step,
            "eta_seconds": remaining_steps * seconds_per_step,
            "elapsed_seconds": elapsed_seconds,
            "remaining_steps": remaining_steps,
            "samples": len(self.samples),
            "prediction_status": "observed_lammps_timing",
            "timing_source": "lammps_thermo_cpu",
            "confidence": "observed" if len(rates) >= 2 else "low_sample",
        }


def adapter_from_package(package: dict[str, Any]) -> LammpsThermoProgressAdapter | None:
    """Return a LAMMPS adapter for a declared builtin.lammps package."""
    package_type = package.get("pkg_type")
    if package_type != "builtin.lammps":
        return None
    return LammpsThermoProgressAdapter(
        package_name=str(package_type),
        package_version=str(
            package.get("pkg_version")
            or package.get("version")
            or package.get("package_version")
            or "builtin"
        ),
        total_steps=_optional_float(
            package.get("total_steps")
            or package.get("steps")
            or _nested_progress_total(package.get("progress"))
        ),
    )


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


def _parse_run_steps(line: str) -> float | None:
    parts = line.split()
    if len(parts) < 2 or parts[0] != "run":
        return None
    return _optional_float(parts[1])


def _parse_reset_timestep(line: str) -> float | None:
    parts = line.split()
    if len(parts) < 2 or parts[0] != "reset_timestep":
        return None
    return _optional_float(parts[1])


def _lammps_message(step: float, total_steps: float | None) -> str:
    if total_steps is None:
        return f"LAMMPS step {int(step)}"
    return f"LAMMPS step {int(step)} of {int(total_steps)}"


def _drop_none(value: dict[str, object | None]) -> dict[str, object]:
    return {key: item for key, item in value.items() if item is not None}
