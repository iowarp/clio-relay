"""fx-otp: relay holds one signed-in connection, and a lost one asks again."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from tests.relay_ssh.test_ssh_fixtures import build, close_otp, ssh_children, totp_helper

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]


def prompts(log: Path) -> list[str]:
    return [line for line in log.read_text().splitlines() if line]


async def test_signing_in_again_reuses_the_connection(fx: dict[str, str], tmp_path: Path) -> None:
    helper, log = totp_helper(fx, tmp_path)
    adapter = build(fx, tmp_path / "state", "fx-otp", interactive_sign_in=True, askpass=str(helper))
    try:
        assert await adapter.sign_in_needed() is None
        start = adapter.transport.starter()
        await asyncio.gather(start({}), start({}))
        await asyncio.gather(*(adapter.describe() for _ in range(4)))
        assert len(prompts(log)) == 1, prompts(log)
        # Relay holds the connection itself: no ssh process is left on this machine.
        assert ssh_children() == []
    finally:
        await close_otp(fx, adapter)


async def test_a_lost_connection_signs_in_again(fx: dict[str, str], tmp_path: Path) -> None:
    helper, log = totp_helper(fx, tmp_path)
    adapter = build(fx, tmp_path / "state", "fx-otp", interactive_sign_in=True, askpass=str(helper))
    try:
        assert await adapter.sign_in_needed() is None
        assert await adapter.transport.close() == 1
        assert not adapter.transport.signed_in()
        assert await adapter.sign_in_needed() is None
        assert adapter.transport.signed_in()
        await adapter.describe()
        assert len(prompts(log)) == 2, prompts(log)
    finally:
        await close_otp(fx, adapter)
