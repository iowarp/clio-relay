"""Pytest plugin that rejects non-executed release-validation outcomes."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Final

import pytest
from _pytest.main import Session
from _pytest.nodes import Item
from _pytest.reports import CollectReport, TestReport
from _pytest.terminal import TerminalReporter

from clio_relay.command_evidence import (
    PYTEST_FAILED_NODE_ID_MAX_BYTES,
    PYTEST_FAILED_NODE_IDS_MARKER,
    PYTEST_FAILED_NODE_IDS_MAX_BYTES,
    PYTEST_FAILED_NODE_IDS_MAX_COUNT,
    PYTEST_FAILED_NODE_IDS_PAYLOAD_MAX_BYTES,
)


@dataclass
class _OutcomeCounts:
    skipped: int = 0
    xfailed: int = 0
    xpassed: int = 0
    deselected: int = 0

    @property
    def rejected(self) -> bool:
        return any((self.skipped, self.xfailed, self.xpassed, self.deselected))

    def summary(self) -> str:
        return ", ".join(
            (
                f"skipped={self.skipped}",
                f"xfailed={self.xfailed}",
                f"xpassed={self.xpassed}",
                f"deselected={self.deselected}",
            )
        )


@dataclass
class _FailedNodeIds:
    node_ids: list[str] = field(default_factory=list[str])
    seen: set[str] = field(default_factory=set[str])
    aggregate_bytes: int = 0
    truncated: bool = False

    def reset(self) -> None:
        """Clear all failure identifiers before a new pytest session."""
        self.node_ids.clear()
        self.seen.clear()
        self.aggregate_bytes = 0
        self.truncated = False

    def add(self, node_id: str) -> None:
        """Record one unique node ID without exceeding durable-report bounds."""
        if node_id in self.seen:
            return
        if len(self.node_ids) >= PYTEST_FAILED_NODE_IDS_MAX_COUNT:
            self.truncated = True
            return
        try:
            node_id_bytes = len(node_id.encode("utf-8"))
        except UnicodeEncodeError:
            self.truncated = True
            return
        if node_id_bytes > PYTEST_FAILED_NODE_ID_MAX_BYTES:
            self.truncated = True
            return
        if self.aggregate_bytes + node_id_bytes > PYTEST_FAILED_NODE_IDS_MAX_BYTES:
            self.truncated = True
            return
        self.node_ids.append(node_id)
        self.seen.add(node_id)
        self.aggregate_bytes += node_id_bytes


_COUNTS: Final[_OutcomeCounts] = _OutcomeCounts()
_FAILED_NODE_IDS: Final[_FailedNodeIds] = _FailedNodeIds()


def pytest_sessionstart(session: Session) -> None:
    """Reset outcome counts before collection starts."""
    del session
    _COUNTS.skipped = 0
    _COUNTS.xfailed = 0
    _COUNTS.xpassed = 0
    _COUNTS.deselected = 0
    _FAILED_NODE_IDS.reset()


def pytest_deselected(items: list[Item]) -> None:
    """Record tests removed from execution by collection filters."""
    _COUNTS.deselected += len(items)


def pytest_collectreport(report: CollectReport) -> None:
    """Record collection-time skips such as module-level skip directives."""
    if report.failed:
        _FAILED_NODE_IDS.add(report.nodeid)
    if report.skipped:
        _COUNTS.skipped += 1


def pytest_runtest_logreport(report: TestReport) -> None:
    """Classify each non-executed or expected-failure test outcome."""
    if report.failed:
        _FAILED_NODE_IDS.add(report.nodeid)
    was_xfail = bool(getattr(report, "wasxfail", False))
    if report.skipped:
        if was_xfail:
            _COUNTS.xfailed += 1
        else:
            _COUNTS.skipped += 1
    elif report.passed and was_xfail:
        _COUNTS.xpassed += 1


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session: Session, exitstatus: int | pytest.ExitCode) -> None:
    """Fail an otherwise successful run when any outcome was not a clean pass."""
    if _COUNTS.rejected and int(exitstatus) == int(pytest.ExitCode.OK):
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_terminal_summary(
    terminalreporter: TerminalReporter,
    exitstatus: int | pytest.ExitCode,
) -> None:
    """Explain the structured release-gate rejection in pytest output."""
    del exitstatus
    if _COUNTS.rejected:
        terminalreporter.write_sep(
            "=",
            f"release gate rejected outcomes: {_COUNTS.summary()}",
            red=True,
            bold=True,
        )
    terminalreporter.write_line(
        f"{PYTEST_FAILED_NODE_IDS_MARKER}{_failed_node_ids_payload(_FAILED_NODE_IDS)}",
    )


def _failed_node_ids_payload(failed_node_ids: _FailedNodeIds) -> str:
    """Serialize bounded failure IDs for exact command-report ingestion."""
    bounded = _FailedNodeIds(truncated=failed_node_ids.truncated)
    for node_id in failed_node_ids.node_ids:
        bounded.add(node_id)
    node_ids = list(bounded.node_ids)
    truncated = bounded.truncated
    while True:
        payload = json.dumps(
            {"node_ids": node_ids, "truncated": truncated},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        try:
            payload_size = len(payload.encode("utf-8"))
        except UnicodeEncodeError:
            payload_size = PYTEST_FAILED_NODE_IDS_PAYLOAD_MAX_BYTES + 1
        if payload_size <= PYTEST_FAILED_NODE_IDS_PAYLOAD_MAX_BYTES:
            return payload
        truncated = True
        if not node_ids:
            return '{"node_ids":[],"truncated":true}'
        node_ids.pop()
