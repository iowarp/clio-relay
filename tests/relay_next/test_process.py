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
