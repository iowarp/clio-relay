"""Generic progress adapters for bounded command JARVIS packages."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, cast

PROGRESS_FILE_ENV = "CLIO_RELAY_PROGRESS_FILE"
PROGRESS_TOKEN_ENV = "CLIO_RELAY_PROGRESS_TOKEN"
PACKAGE_NAME = "clio_relay.bounded_command"
PROGRESS_RECORD_MAX_BYTES = 65_536
PROGRESS_SIDECAR_MAX_BYTES = 16 * 1_048_576
PROGRESS_SIDECAR_RECORD_SCHEMA = "clio-relay.progress-sidecar-record.v1"
_PROGRESS_SEQUENCES: dict[str, int] = {}
_PROGRESS_SEQUENCE_LOCK = threading.Lock()


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
    metadata: dict[str, object] = field(default_factory=lambda: dict[str, object]())

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
                            **_metadata(config=None, metadata=self.metadata),
                            "source": "jarvis_package",
                            "package_name": PACKAGE_NAME,
                            "package_version": "builtin",
                            "adapter": "regex",
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
    typed = cast(dict[str, object], config)
    adapter = str(typed.get("adapter", "regex"))
    if adapter == "none":
        return None
    if adapter == "regex":
        pattern = typed.get("pattern")
        if not isinstance(pattern, str) or pattern == "":
            raise ValueError("regex progress adapter requires pattern")
        return GenericRegexProgressAdapter(
            pattern=re.compile(pattern),
            label=str(typed.get("label", "progress")),
            unit=_optional_str(typed.get("unit")),
            current_group=str(typed.get("current_group", "current")),
            total_group=_optional_str(typed.get("total_group")),
            message_group=_optional_str(typed.get("message_group")),
            metadata=_metadata(typed),
        )
    raise ValueError(f"unsupported progress adapter: {adapter}")


def append_progress_record(record: dict[str, object]) -> None:
    """Append a trusted package progress record to the relay side-channel file."""
    path_value = os.getenv(PROGRESS_FILE_ENV)
    token = os.getenv(PROGRESS_TOKEN_ENV)
    if path_value is None or path_value == "" or token is None or token == "":
        return
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise ValueError("progress sidecar cannot be a symbolic link")
    with _PROGRESS_SEQUENCE_LOCK:
        sequence = _PROGRESS_SEQUENCES.get(str(path), 0) + 1
        signed = {
            "schema_version": PROGRESS_SIDECAR_RECORD_SCHEMA,
            "sequence": sequence,
            "progress": record,
        }
        canonical = json.dumps(
            signed,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        envelope = {
            **signed,
            "progress_hmac": hmac.new(
                token.encode("utf-8"),
                canonical,
                hashlib.sha256,
            ).hexdigest(),
        }
        encoded = (
            json.dumps(
                envelope,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
        if len(encoded) > PROGRESS_RECORD_MAX_BYTES:
            raise ValueError("progress sidecar record exceeded its byte limit")
        flags = os.O_APPEND | os.O_WRONLY
        flags |= getattr(os, "O_BINARY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        try:
            os.set_inheritable(descriptor, False)
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode):
                raise ValueError("progress sidecar is not a regular file")
            if opened.st_nlink != 1:
                raise ValueError("progress sidecar hardlink count changed")
            if os.name != "nt" and (
                opened.st_uid != os.getuid() or stat.S_IMODE(opened.st_mode) != 0o600
            ):
                raise ValueError("progress sidecar ownership or mode changed")
            if opened.st_size + len(encoded) > PROGRESS_SIDECAR_MAX_BYTES:
                raise ValueError("progress sidecar exceeded its byte limit")
            view = memoryview(encoded)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("progress sidecar append made no progress")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        _PROGRESS_SEQUENCES[str(path)] = sequence


def _group_text(match: re.Match[str], group: str | None) -> str | None:
    if group is None:
        return None
    return match.group(int(group)) if group.isdigit() else match.group(group)


def _group_float(match: re.Match[str], group: str) -> float:
    value = _group_text(match, group)
    if value is None:
        raise ValueError(f"progress regex group did not match: {group}")
    return float(value)


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value != "" else None


def _metadata(
    config: dict[str, object] | None = None,
    *,
    metadata: object | None = None,
) -> dict[str, object]:
    value = config.get("metadata", {}) if config is not None else metadata
    if not isinstance(value, dict):
        return {}
    protected = {"source", "package_name", "package_version", "run_id", "execution_id", "adapter"}
    typed = cast(dict[object, object], value)
    return {str(key): item for key, item in typed.items() if str(key) not in protected}


def _drop_none(value: dict[str, object | None]) -> dict[str, object]:
    return {key: item for key, item in value.items() if item is not None}
