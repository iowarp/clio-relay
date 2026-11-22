"""The ``files`` scripts, run with the local ``sh`` in place of a site.

A local transport stands in for ssh, so these check the shell the site would
run: where paths land, chunked writes and their retries, and delete.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Awaitable
from pathlib import Path

import pytest

from clio_relay.adapters.ssh import SshConfig
from clio_relay.adapters.ssh.files import SshFileAccess
from clio_relay.adapters.ssh.transport import Completed, SshTransport, run_process
from clio_relay.jobs.file_rules import PART_SUFFIX
from clio_relay.jobs.handles import StallReport
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.surface.files_transfer import download, upload

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]


class LocalTransport(SshTransport):
    """Runs each script with ``sh`` here, with ``HOME`` set to a test directory."""

    def __init__(self, home: Path) -> None:
        super().__init__(SshConfig(ssh_profile="local"), site_id="local", state_dir=home)
        self.home = home

    async def ssh(
        self,
        script: str,
        *,
        data: bytes | None = None,
        timeout_s: float | None,
        max_output_bytes: int | None = None,
        what: str = "the site",
        check_timeout: bool = True,
        hold_stdin: bool = False,
        on_stall: StallReport | None = None,
        retry: bool = True,
    ) -> Completed:
        env = {**os.environ, "HOME": str(self.home)}
        return await run_process(
            ["sh", "-c", script], timeout_s=timeout_s, stdin=data or b"", env=env
        )


@pytest.fixture
def home(tmp_path: Path) -> Path:
    path = tmp_path / "home"
    path.mkdir()
    return path


@pytest.fixture
def files(home: Path) -> SshFileAccess:
    return SshFileAccess(LocalTransport(home), remote_dir="relay", timeout_s=60)


async def refusal(call: Awaitable[object]) -> Refusal:
    with pytest.raises(Refusal) as refused:
        await call
    return refused.value


async def test_paths_resolve_under_the_work_dir_home_or_root(
    files: SshFileAccess, home: Path, tmp_path: Path
) -> None:
    entry = await files.write("a/b.txt", b"work")
    assert entry.path == f"{home}/relay/a/b.txt"
    assert (home / "relay" / "a" / "b.txt").read_bytes() == b"work"
    await files.write("~/h.txt", b"home")
    assert (home / "h.txt").read_bytes() == b"home"
    await files.write(f"{tmp_path}/abs.txt", b"abs")
    assert (tmp_path / "abs.txt").read_bytes() == b"abs"
    listed = await files.list("")
    assert [entry.path for entry in listed] == [f"{home}/relay/a"]
    assert await files.read("a/b.txt", offset=1, limit=2) == b"or"


@pytest.mark.parametrize("path", ["../out.txt", "a/../../out.txt", "~other/x"])
async def test_paths_that_leave_the_work_dir_are_refused(files: SshFileAccess, path: str) -> None:
    refused = await refusal(files.write(path, b"x"))
    assert refused.reason == RefusalReason.INVALID_ARGUMENTS


async def test_a_chunked_write_takes_retries_and_says_where_to_resume(
    files: SshFileAccess, home: Path
) -> None:
    target = home / "relay" / "big.bin"
    part = await files.write("big.bin", b"0123", final=False)
    assert part.path.endswith(PART_SUFFIX)
    assert part.size == 4
    # The same chunk again, as after a lost answer, changes nothing.
    assert (await files.write("big.bin", b"0123", final=False)).size == 4
    await files.write("big.bin", b"4567", offset=4, final=False)
    assert (await files.write("big.bin", b"4567", offset=4, final=False)).size == 8
    skipped = await refusal(files.write("big.bin", b"zz", offset=12, final=False))
    assert skipped.data["resume_at"] == 8
    other = await refusal(files.write("big.bin", b"zzzz", offset=4, final=False))
    assert other.data["resume_at"] == 8
    assert (await files.stat(f"big.bin{PART_SUFFIX}")).size == 8
    assert not target.exists()
    done = await files.write("big.bin", b"89", offset=8)
    assert done.path == str(target)
    assert target.read_bytes() == b"0123456789"
    assert not Path(f"{target}{PART_SUFFIX}").exists()
    # The last chunk again, after the part was moved into place.
    again = await files.write("big.bin", b"89", offset=8)
    assert again.size == 10
    assert target.read_bytes() == b"0123456789"


async def test_a_chunked_write_checks_overwrite_at_the_start_and_the_end(
    files: SshFileAccess,
) -> None:
    await files.write("kept.txt", b"old")
    refused = await refusal(files.write("kept.txt", b"new", final=False))
    assert refused.reason == RefusalReason.PATH_EXISTS
    await files.write("late.txt", b"ab", final=False)
    await files.write("late.txt", b"x")
    refused = await refusal(files.write("late.txt", b"cd", offset=2))
    assert refused.reason == RefusalReason.PATH_EXISTS
    await files.write("late.txt", b"cd", offset=2, overwrite=True)
    assert await files.read("late.txt") == b"abcd"


async def test_no_upload_in_progress_says_start_at_zero(files: SshFileAccess) -> None:
    refused = await refusal(files.write("none.bin", b"x", offset=5))
    assert refused.data["resume_at"] == 0


async def test_delete_follows_the_rules(files: SshFileAccess, home: Path, tmp_path: Path) -> None:
    await files.write("d/one.txt", b"1")
    missing = await refusal(files.delete("nope"))
    assert missing.reason == RefusalReason.PATH_NOT_FOUND
    full = await refusal(files.delete("d"))
    assert full.reason == RefusalReason.INVALID_ARGUMENTS
    assert full.data["kind"] == "dir"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.txt").write_text("keep")
    (home / "relay" / "link").symlink_to(outside)
    await files.delete("link")
    assert (outside / "keep.txt").exists()
    await files.delete("d/one.txt")
    await files.delete("d")
    await files.write("e/f/g.txt", b"g")
    await files.delete("e", recursive=True)
    assert not (home / "relay" / "e").exists()
    for kept in (".", "~", "~/", "/", f"{home}/relay/"):
        refused = await refusal(files.delete(kept, recursive=True))
        assert refused.reason == RefusalReason.INVALID_ARGUMENTS, kept
        assert "does not delete" in refused.message
    assert (home / "relay").is_dir()
    empty = await refusal(files.delete(""))
    assert empty.reason == RefusalReason.INVALID_ARGUMENTS


async def test_a_path_transfer_is_hashed_on_the_site(
    files: SshFileAccess, home: Path, tmp_path: Path
) -> None:
    files.transfer_chunk_bytes = 4
    source = tmp_path / "src.bin"
    source.write_bytes(b"0123456789")
    moved = await upload(files, str(source), "up/x.bin", overwrite=False)
    assert moved["verified"] == "site"
    assert (home / "relay" / "up" / "x.bin").read_bytes() == b"0123456789"
    assert await files.sha256("up/x.bin") == hashlib.sha256(b"0123456789").hexdigest()
    back = tmp_path / "back.bin"
    got = await download(files, "up/x.bin", str(back), overwrite=False)
    assert got["verified"] == "site"
    assert back.read_bytes() == b"0123456789"
    missing = await refusal(files.sha256("up/none.bin"))
    assert missing.reason == RefusalReason.PATH_NOT_FOUND
