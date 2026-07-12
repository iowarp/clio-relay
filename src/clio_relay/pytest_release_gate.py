"""Pytest plugin that rejects non-executed release-validation outcomes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import pytest
from _pytest.main import Session
from _pytest.nodes import Item
from _pytest.reports import CollectReport, TestReport
from _pytest.terminal import TerminalReporter


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


_COUNTS: Final[_OutcomeCounts] = _OutcomeCounts()


def pytest_sessionstart(session: Session) -> None:
    """Reset outcome counts before collection starts."""
    del session
    _COUNTS.skipped = 0
    _COUNTS.xfailed = 0
    _COUNTS.xpassed = 0
    _COUNTS.deselected = 0


def pytest_deselected(items: list[Item]) -> None:
    """Record tests removed from execution by collection filters."""
    _COUNTS.deselected += len(items)


def pytest_collectreport(report: CollectReport) -> None:
    """Record collection-time skips such as module-level skip directives."""
    if report.skipped:
        _COUNTS.skipped += 1


def pytest_runtest_logreport(report: TestReport) -> None:
    """Classify each non-executed or expected-failure test outcome."""
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
