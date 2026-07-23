"""Pytest plugin for explicit, machine-readable release-platform partitions."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Final

import pytest
from _pytest.config import Config
from _pytest.main import Session
from _pytest.nodes import Item
from _pytest.terminal import TerminalReporter

from clio_relay.command_evidence import (
    PYTEST_PLATFORM_NODE_ID_MAX_BYTES,
    PYTEST_PLATFORM_NODE_IDS_MAX_BYTES,
    PYTEST_PLATFORM_NODE_IDS_MAX_COUNT,
    PYTEST_PLATFORM_PARTITION_MARKER,
    PYTEST_PLATFORM_PARTITION_PAYLOAD_MAX_BYTES,
)

_RELEASE_PLATFORM_MARKER: Final = "release_platform"
_SUPPORTED_RELEASE_PLATFORMS: Final = frozenset({"posix", "windows"})


@dataclass
class _PlatformNodeIds:
    node_ids: list[str] = field(default_factory=list[str])
    seen: set[str] = field(default_factory=set[str])
    aggregate_bytes: int = 0
    truncated: bool = False

    def reset(self) -> None:
        """Clear platform-partition identifiers before a new pytest session."""
        self.node_ids.clear()
        self.seen.clear()
        self.aggregate_bytes = 0
        self.truncated = False

    def add(self, node_id: str) -> None:
        """Record one unique platform-partition node ID within durable bounds."""
        if node_id in self.seen:
            return
        if len(self.node_ids) >= PYTEST_PLATFORM_NODE_IDS_MAX_COUNT:
            self.truncated = True
            return
        try:
            node_id_bytes = len(node_id.encode("utf-8"))
        except UnicodeEncodeError:
            self.truncated = True
            return
        if node_id_bytes > PYTEST_PLATFORM_NODE_ID_MAX_BYTES:
            self.truncated = True
            return
        if self.aggregate_bytes + node_id_bytes > PYTEST_PLATFORM_NODE_IDS_MAX_BYTES:
            self.truncated = True
            return
        self.node_ids.append(node_id)
        self.seen.add(node_id)
        self.aggregate_bytes += node_id_bytes


@dataclass
class _PlatformPartition:
    platform: str = ""
    selected: _PlatformNodeIds = field(default_factory=_PlatformNodeIds)
    excluded: _PlatformNodeIds = field(default_factory=_PlatformNodeIds)

    def reset(self, platform: str) -> None:
        """Start one exact platform partition for a new pytest session."""
        self.platform = platform
        self.selected.reset()
        self.excluded.reset()


_PLATFORM_PARTITION: Final[_PlatformPartition] = _PlatformPartition()


def pytest_configure(config: Config) -> None:
    """Register the explicit platform marker used by every pytest invocation."""
    config.addinivalue_line(
        "markers",
        "release_platform(name): execute only on the named release platform ('posix' or 'windows')",
    )


def pytest_sessionstart(session: Session) -> None:
    """Reset partition evidence before collection starts."""
    del session
    _PLATFORM_PARTITION.reset(_current_release_platform())


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[Item]) -> None:
    """Select marked tests for the actual platform and evidence every partition."""
    selected_items: list[Item] = []
    for item in items:
        try:
            required_platform = _marked_release_platform(item)
        except ValueError as exc:
            raise pytest.UsageError(f"{item.nodeid}: {exc}") from exc
        if required_platform is None:
            selected_items.append(item)
        elif required_platform == _PLATFORM_PARTITION.platform:
            _PLATFORM_PARTITION.selected.add(item.nodeid)
            selected_items.append(item)
        else:
            _PLATFORM_PARTITION.excluded.add(item.nodeid)
    items[:] = selected_items


def pytest_terminal_summary(
    terminalreporter: TerminalReporter,
    exitstatus: int | pytest.ExitCode,
) -> None:
    """Emit the exact marked-test partition for durable command evidence."""
    del exitstatus
    terminalreporter.write_line(
        f"{PYTEST_PLATFORM_PARTITION_MARKER}{_platform_partition_payload(_PLATFORM_PARTITION)}",
    )


def _current_release_platform() -> str:
    """Return the canonical release-platform label for this process."""
    if os.name == "nt":
        return "windows"
    if os.name == "posix":
        return "posix"
    raise pytest.UsageError(f"unsupported release platform os.name={os.name!r}")


def _marked_release_platform(item: Item) -> str | None:
    """Return one validated release-platform requirement for a collected item."""
    markers = list(item.iter_markers(name=_RELEASE_PLATFORM_MARKER))
    if not markers:
        return None
    if len(markers) != 1:
        raise ValueError("release_platform must appear exactly once")
    marker = markers[0]
    if marker.kwargs or len(marker.args) != 1 or not isinstance(marker.args[0], str):
        raise ValueError("release_platform requires exactly one positional string argument")
    platform = marker.args[0]
    if platform not in _SUPPORTED_RELEASE_PLATFORMS:
        supported = ", ".join(sorted(_SUPPORTED_RELEASE_PLATFORMS))
        raise ValueError(
            f"release_platform value {platform!r} is unsupported; expected one of: {supported}"
        )
    return platform


def _platform_partition_payload(partition: _PlatformPartition) -> str:
    """Serialize the bounded marked-test partition for command-report ingestion."""
    selected = list(partition.selected.node_ids)
    excluded = list(partition.excluded.node_ids)
    truncated = partition.selected.truncated or partition.excluded.truncated
    while True:
        payload = json.dumps(
            {
                "excluded_node_ids": excluded,
                "platform": partition.platform,
                "selected_node_ids": selected,
                "truncated": truncated,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        try:
            payload_size = len(payload.encode("utf-8"))
        except UnicodeEncodeError:
            payload_size = PYTEST_PLATFORM_PARTITION_PAYLOAD_MAX_BYTES + 1
        if payload_size <= PYTEST_PLATFORM_PARTITION_PAYLOAD_MAX_BYTES:
            return payload
        truncated = True
        if excluded:
            excluded.pop()
        elif selected:
            selected.pop()
        else:
            return (
                '{"excluded_node_ids":[],"platform":"'
                f'{partition.platform}","selected_node_ids":[],"truncated":true}}'
            )
