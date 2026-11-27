"""An ssh call ends when its remote command exits, not when ssh lets go.

Take 1p: a command had finished on the site, but the session stayed open (idle
``sshd: notty``, keepalives answered), so relay waited on ssh for 23 minutes.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

import pytest

from clio_relay.adapters.ssh import SshConfig, scripts
from clio_relay.adapters.ssh import process as ssh_process
from clio_relay.adapters.ssh.process import Sink
from clio_relay.adapters.ssh.transport import (
    Completed,
    SshTransport,
    ends_with_marker,
    exit_marker,
    run_process,
)

DROPPED = "client_loop: send disconnect: Broken pipe\n"


async def _pump(sink: Sink, *chunks: bytes) -> None:
    stream = asyncio.StreamReader()
    for chunk in chunks:
        stream.feed_data(chunk)
    stream.feed_eof()
    await sink.pump(stream)


def test_every_call_gets_its_own_marker() -> None:
    assert exit_marker() != exit_marker()


@pytest.mark.asyncio
async def test_the_marker_is_read_and_left_out_of_stderr() -> None:
    marker = exit_marker()
    sink = Sink(None, marker)
    await _pump(sink, b"warning: x\n", b"\n" + marker + b"7\n", b"after\n")
    assert sink.exited.is_set()
    assert sink.exit_code == 7
    assert bytes(sink.data) == b"warning: x\nafter\n"


@pytest.mark.asyncio
async def test_a_marker_split_across_reads_is_still_found() -> None:
    marker = exit_marker()
    whole = b"no newline at the end" + b"\n" + marker + b"0\n"
    sink = Sink(None, marker)
    await _pump(sink, *(whole[i : i + 3] for i in range(0, len(whole), 3)))
    assert sink.exit_code == 0
    assert bytes(sink.data) == b"no newline at the end"


@pytest.mark.asyncio
async def test_a_marker_past_the_output_limit_still_counts() -> None:
    marker = exit_marker()
    sink = Sink(8, marker)
    await _pump(sink, b"x" * 100, b"\n" + marker + b"3\n")
    assert sink.exit_code == 3
    assert sink.truncated
    assert bytes(sink.data) == b"x" * 8


@pytest.mark.asyncio
async def test_output_without_a_marker_is_kept_whole() -> None:
    sink = Sink(None, exit_marker())
    await _pump(sink, b"short")
    assert not sink.exited.is_set()
    assert bytes(sink.data) == b"short"


@pytest.mark.asyncio
async def test_the_call_ends_on_the_marker_while_ssh_stays_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ssh_process, "EXIT_GRACE_S", 0.3)
    marker = exit_marker()
    # A stand-in for an ssh whose session outlives the command: it reports the
    # exit, then holds both pipes open.
    held = (
        "import sys, time\n"
        "sys.stdout.write('out\\n'); sys.stdout.flush()\n"
        f"sys.stderr.write('\\n{marker.decode()}4\\n'); sys.stderr.flush()\n"
        "time.sleep(120)\n"
    )
    started = time.monotonic()
    result = await run_process([sys.executable, "-c", held], timeout_s=60, marker=marker)
    assert time.monotonic() - started < 20
    assert result.remote_exit
    assert result.exit_code == 4
    assert not result.timed_out
    assert result.text().splitlines() == ["out"]
    assert marker.decode() not in result.error_text()


@pytest.mark.asyncio
async def test_without_a_marker_the_process_exit_code_stands() -> None:
    result = await run_process(
        [sys.executable, "-c", "raise SystemExit(5)"], timeout_s=60, marker=exit_marker()
    )
    assert not result.remote_exit
    assert result.exit_code == 5


@pytest.mark.asyncio
async def test_a_command_that_ran_is_never_run_again(tmp_path: Path) -> None:
    transport = SshTransport(
        SshConfig(ssh_profile="fx", retries=5), site_id="s", state_dir=tmp_path
    )
    calls = 0

    async def call() -> Completed:
        nonlocal calls
        calls += 1
        return Completed(
            argv=["ssh"], exit_code=255, stdout=b"", stderr=DROPPED.encode(), remote_exit=True
        )

    result = await transport.retrying(call, "exec")
    assert calls == 1
    assert result.exit_code == 255


@pytest.mark.release_platform("posix")
@pytest.mark.asyncio
async def test_a_background_child_holding_the_output_does_not_hold_the_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ssh_process, "EXIT_GRACE_S", 0.3)
    marker = exit_marker()
    script = ends_with_marker("sleep 60 &\necho done\nexit 2", marker)
    started = time.monotonic()
    result = await run_process(["sh", "-c", script], timeout_s=60, marker=marker)
    assert time.monotonic() - started < 15
    assert result.remote_exit
    assert result.exit_code == 2
    assert result.text() == "done\n"
    assert result.error_text() == ""


def _holders(inode: int) -> list[int]:
    found: list[int] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit() or int(entry.name) == os.getpid():
            continue
        try:
            links = [os.readlink(fd) for fd in (entry / "fd").iterdir()]
        except OSError:
            continue
        if f"pipe:[{inode}]" in links:
            found.append(int(entry.name))
    return found


@pytest.mark.release_platform("posix")
@pytest.mark.asyncio
async def test_the_exec_watchdog_leaves_nothing_on_stdin_after_the_command() -> None:
    read, write = os.pipe()
    try:
        shell = await asyncio.create_subprocess_exec(
            "sh",
            "-c",
            scripts.exec_script(["true"]),
            stdin=read,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        os.close(read)
        read = -1
        assert await asyncio.wait_for(shell.wait(), 20) == 0
        inode = os.fstat(write).st_ino
        deadline = time.monotonic() + 5
        while _holders(inode) and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        assert _holders(inode) == []
    finally:
        if read >= 0:
            os.close(read)
        os.close(write)
