"""Generic progress adapters for bounded command JARVIS packages."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

PROGRESS_FILE_ENV = "CLIO_RELAY_PROGRESS_FILE"
PACKAGE_NAME = "clio_relay.bounded_command"


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
                        "metadata": {
                            "source": "jarvis_package",
                            "package_name": PACKAGE_NAME,
                            "adapter": "regex",
                            **self.metadata,
                        },
                    }
                )
            )
        return records


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
    raise ValueError(f"unsupported progress adapter: {adapter}")


def append_progress_record(record: dict[str, object]) -> None:
    """Append a trusted package progress record to the relay side-channel file."""
    path_value = os.getenv(PROGRESS_FILE_ENV)
    if path_value is None or path_value == "":
        return
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True))
        handle.write("\n")


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
