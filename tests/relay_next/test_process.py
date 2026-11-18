"""Bounded local commands leave no process or pipe behind."""

from __future__ import annotations

import asyncio
import sys
import time

import pytest

from clio_relay.adapters._process import run_bounded

SLOW = [sys.executable, "-c", "import time; print('up', flush=True); time.sleep(60)"]


@pytest.mark.asyncio
@pytest.mark.filterwarnings("error::pytest.PytestUnraisableExceptionWarning")
async def test_timeout_kills_and_reads_the_pipes_to_their_end() -> None:
    result = await run_bounded(SLOW, timeout_s=1, max_output_bytes=100)
    assert result.timed_out
    assert result.exit_code is None
    assert "up" in result.stdout
    assert result.duration_s < 20


@pytest.mark.asyncio
@pytest.mark.filterwarnings("error::pytest.PytestUnraisableExceptionWarning")
async def test_cancelled_caller_kills_the_command() -> None:
    started = time.monotonic()
    call = asyncio.ensure_future(run_bounded(SLOW, timeout_s=60, max_output_bytes=100))
    await asyncio.sleep(1)
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    assert time.monotonic() - started < 20


@pytest.mark.asyncio
async def test_missing_executable_is_127() -> None:
    result = await run_bounded(["relay-no-such-command"], timeout_s=5, max_output_bytes=100)
    assert result.exit_code == 127


@pytest.mark.asyncio
@pytest.mark.filterwarnings("error::pytest.PytestUnraisableExceptionWarning")
async def test_no_limit_runs_to_the_end_and_reports_quiet_stretches() -> None:
    quiet = [sys.executable, "-c", "import time; time.sleep(1.5); print('end', flush=True)"]
    reports: list[tuple[float, float]] = []

    async def on_stall(idle_s: float, next_s: float) -> None:
        reports.append((idle_s, next_s))

    result = await run_bounded(quiet, max_output_bytes=100, on_stall=on_stall, stall_s=0.3)
    assert result.exit_code == 0
    assert not result.timed_out
    assert "end" in result.stdout
    # Reported at 0.3 s, then after waits that double: 0.6, 1.2.
    assert 2 <= len(reports) <= 4, reports
    assert all(idle >= 0.3 for idle, _ in reports)
    assert reports[1][1] > reports[0][1]


@pytest.mark.asyncio
async def test_output_resets_the_quiet_clock() -> None:
    chatty = [
        sys.executable,
        "-c",
        "import time\nfor _ in range(8):\n    print('x', flush=True); time.sleep(0.1)",
    ]
    reports: list[float] = []

    async def on_stall(idle_s: float, next_s: float) -> None:
        reports.append(idle_s)

    result = await run_bounded(chatty, max_output_bytes=100, on_stall=on_stall, stall_s=0.5)
    assert result.exit_code == 0
    assert reports == []
