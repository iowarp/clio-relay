"""Progress adapters for bounded command JARVIS packages."""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Protocol

PROGRESS_PREFIX = "CLIO_PROGRESS "


class ProgressAdapter(Protocol):
    """Observe application output and emit structured progress records."""

    def observe_stdout(self, line: str) -> list[dict[str, object]]:
        """Return zero or more progress records derived from one stdout line."""
        ...


@dataclass
class GenericRegexProgressAdapter:
    """Extract progress from stdout using caller-supplied regular expressions."""

    pattern: re.Pattern[str]
    label: str = "progress"
    unit: str | None = None
    current_group: str = "current"
    total_group: str | None = None
    message_group: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)

    def observe_stdout(self, line: str) -> list[dict[str, object]]:
        """Extract all matching progress observations from a stdout line."""
        records: list[dict[str, object]] = []
        for match in self.pattern.finditer(line):
            current = _group_float(match, self.current_group)
            total = _group_float(match, self.total_group) if self.total_group else None
            message = _group_text(match, self.message_group) if self.message_group else None
            records.append(
                _drop_none(
                    {
                        "label": self.label,
                        "current": current,
                        "total": total,
                        "unit": self.unit,
                        "message": message,
                        "metadata": {"adapter": "regex", **self.metadata},
                    }
                )
            )
        return records


@dataclass
class LammpsProgressAdapter:
    """Extract LAMMPS timestep progress and estimate remaining runtime."""

    total_steps: float
    label: str = "timestep"
    unit: str = "step"
    step_column: int = 0
    warmup_samples: int = 2
    sample_window: int = 8
    metadata: dict[str, object] = field(default_factory=dict)
    samples: list[tuple[float, float]] = field(default_factory=list)

    def observe_stdout(self, line: str) -> list[dict[str, object]]:
        """Extract a timestep from a LAMMPS thermo row."""
        step = self._parse_step(line)
        if step is None:
            return []
        now = time.monotonic()
        self.samples.append((step, now))
        self.samples = self.samples[-self.sample_window :]
        prediction = self._prediction(step)
        return [
            _drop_none(
                {
                    "label": self.label,
                    "current": step,
                    "total": self.total_steps,
                    "unit": self.unit,
                    "message": f"LAMMPS step {int(step)} of {int(self.total_steps)}",
                    "metadata": {
                        "adapter": "lammps",
                        "prediction_method": "trimmed_step_time_after_warmup",
                        **prediction,
                        **self.metadata,
                    },
                }
            )
        ]

    def _parse_step(self, line: str) -> float | None:
        stripped = line.strip()
        if stripped == "" or stripped.startswith("Step "):
            return None
        parts = stripped.split()
        if len(parts) <= self.step_column:
            return None
        try:
            step = float(parts[self.step_column])
        except ValueError:
            return None
        if step < 0 or step > self.total_steps:
            return None
        return step

    def _prediction(self, current_step: float) -> dict[str, object]:
        if len(self.samples) <= self.warmup_samples:
            return {"confidence": "warming_up", "samples": len(self.samples)}
        rates: list[float] = []
        usable = self.samples[-self.sample_window :]
        for (previous_step, previous_time), (step, timestamp) in zip(
            usable,
            usable[1:],
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
        if len(ordered) > 2:
            ordered = ordered[1:-1]
        seconds_per_step = sum(ordered) / len(ordered)
        remaining_steps = max(0.0, self.total_steps - current_step)
        return {
            "seconds_per_step": seconds_per_step,
            "eta_seconds": remaining_steps * seconds_per_step,
            "remaining_steps": remaining_steps,
            "samples": len(self.samples),
            "confidence": "observed" if len(rates) >= 2 else "low_sample",
        }


def adapter_from_config(config: object) -> ProgressAdapter | None:
    """Build a progress adapter from bounded command package configuration."""
    if config is None:
        return None
    if not isinstance(config, dict):
        raise ValueError("progress must be an object")
    adapter = str(config.get("adapter", "regex"))
    if adapter == "none":
        return None
    if adapter == "regex":
        pattern = config.get("pattern")
        if not isinstance(pattern, str) or pattern == "":
            raise ValueError("regex progress adapter requires pattern")
        return GenericRegexProgressAdapter(
            pattern=re.compile(pattern),
            label=str(config.get("label", "progress")),
            unit=_optional_str(config.get("unit")),
            current_group=str(config.get("current_group", "current")),
            total_group=_optional_str(config.get("total_group")),
            message_group=_optional_str(config.get("message_group")),
            metadata=_metadata(config),
        )
    if adapter == "lammps":
        total_steps = config.get("total_steps")
        if not isinstance(total_steps, int | float) or isinstance(total_steps, bool):
            raise ValueError("lammps progress adapter requires numeric total_steps")
        return LammpsProgressAdapter(
            total_steps=float(total_steps),
            label=str(config.get("label", "timestep")),
            unit=str(config.get("unit", "step")),
            step_column=int(config.get("step_column", 0)),
            warmup_samples=int(config.get("warmup_samples", 2)),
            sample_window=int(config.get("sample_window", 8)),
            metadata=_metadata(config),
        )
    raise ValueError(f"unsupported progress adapter: {adapter}")


def render_progress_marker(record: dict[str, object]) -> str:
    """Render a relay-readable progress marker line."""
    return PROGRESS_PREFIX + json.dumps(record, sort_keys=True)


def _group_text(match: re.Match[str], group: str | None) -> str | None:
    if group is None:
        return None
    return match.group(int(group)) if group.isdigit() else match.group(group)


def _group_float(match: re.Match[str], group: str) -> float:
    return float(_group_text(match, group))


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value != "" else None


def _metadata(config: dict[str, object]) -> dict[str, object]:
    value = config.get("metadata", {})
    return value if isinstance(value, dict) else {}


def _drop_none(value: dict[str, object | None]) -> dict[str, object]:
    return {key: item for key, item in value.items() if item is not None}
