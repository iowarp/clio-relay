"""fx-otp: a signed-in master is never left behind (W8's stale masters)."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.relay_ssh.test_ssh_fixtures import DEADLINE_S, build, close_otp, totp_helper

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]

REPO = Path(__file__).resolve().parents[2]


def processes_on(control_path: Path) -> list[str]:
    """Return the ssh processes that name ``control_path`` on their command line."""
    found = subprocess.run(
        ["pgrep", "-u", str(os.getuid()), "-f", f"ControlPath={control_path}"],
        capture_output=True,
        text=True,
        check=False,
    )
    return found.stdout.split()


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


async def test_signing_in_again_reuses_the_master(fx: dict[str, str], tmp_path: Path) -> None:
    helper, log = totp_helper(fx, tmp_path)
    adapter = build(fx, tmp_path / "state", "fx-otp", interactive_sign_in=True, askpass=str(helper))
    try:
        assert await adapter.sign_in_needed() is None
        first = await adapter.transport.master_pid()
        assert first
        start = adapter.transport.starter()
        await asyncio.gather(start({}), start({}))
        assert await adapter.transport.master_pid() == first
        # Before, each start left a plain connection running ("disabling multiplexing").
        assert processes_on(adapter.transport.control_path) == [str(first)]
        assert len([line for line in log.read_text().splitlines() if line]) == 1
    finally:
        await close_otp(fx, adapter)


async def test_a_stale_socket_does_not_block_a_sign_in(fx: dict[str, str], tmp_path: Path) -> None:
    helper, _ = totp_helper(fx, tmp_path)
    adapter = build(fx, tmp_path / "state", "fx-otp", interactive_sign_in=True, askpass=str(helper))
    try:
        assert await adapter.sign_in_needed() is None
        killed = await adapter.transport.master_pid()
        assert killed
        os.kill(killed, 9)  # SIGKILL leaves the socket file behind
        while alive(killed):
            await asyncio.sleep(0.2)
        assert adapter.transport.control_path.exists()
        assert await adapter.sign_in_needed() is None
        pid = await adapter.transport.master_pid()
        assert pid and pid != killed
        assert processes_on(adapter.transport.control_path) == [str(pid)]
    finally:
        await close_otp(fx, adapter)


_RELAY = """
import asyncio, json, sys
from pathlib import Path
from tests.relay_ssh.test_ssh_fixtures import build

fx, state, helper = json.loads(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]

async def main():
    adapter = build(fx, state, "fx-otp", interactive_sign_in=True, askpass=helper)
    assert await adapter.sign_in_needed() is None
    print(await adapter.transport.master_pid(), adapter.transport.control_path, flush=True)

asyncio.run(main())
"""


async def test_the_master_ends_with_the_relay_that_started_it(
    fx: dict[str, str], tmp_path: Path
) -> None:
    helper, _ = totp_helper(fx, tmp_path)
    relay = subprocess.run(
        [sys.executable, "-c", _RELAY, json.dumps(fx), str(tmp_path / "state"), str(helper)],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    )
    pid_text, control_path = relay.stdout.split()
    pid = int(pid_text)
    assert pid
    # The relay process has exited; its master goes within the watcher's next look.
    loop = asyncio.get_running_loop()
    deadline = loop.time() + DEADLINE_S
    while alive(pid):
        assert loop.time() < deadline, f"master {pid} still up {DEADLINE_S}s after relay ended"
        await asyncio.sleep(1)
    assert not Path(control_path).exists()
    assert processes_on(Path(control_path)) == []
