"""Tapis files on the stub, past what the conformance kit checks (docs/files.md).

The stub follows tapis-files: ``range`` is ``start,count``, ``listFiles`` pages,
``insert`` has no append, and ``delete`` is always recursive and removes a
symlink as itself.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from pathlib import Path
from typing import cast

import pytest

from clio_relay.adapters.tapis import files as tapis_files
from clio_relay.adapters.tapis.files import TapisFiles, home_for
from clio_relay.jobs.file_rules import part_path
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.testing.conformance import ConformanceTarget
from tests.fixtures.tapis_stub import TapisStub
from tests.fixtures.tapis_stub.support import seed_host, sign_in, tapis_config, use_memory_keyring

pytestmark = pytest.mark.release_platform("posix")


@pytest.fixture
def site(stub: TapisStub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    use_memory_keyring(monkeypatch)
    asyncio.run(sign_in(stub))
    work = tmp_path / "site"
    seed_host(stub, "files", work)
    return work


@pytest.fixture
def target(stub: TapisStub, site: Path, tmp_path: Path) -> ConformanceTarget:
    return ConformanceTarget(
        adapter="tapis",
        config=tapis_config(stub),
        state_dir=tmp_path / "relay",
        site_id="files",
    )


async def open_files(target: ConformanceTarget) -> TapisFiles:
    return cast(TapisFiles, await target.build().files())


async def refused(reason: RefusalReason, call: Awaitable[object]) -> Refusal:
    with pytest.raises(Refusal) as caught:
        await call
    assert caught.value.reason == reason, caught.value
    return caught.value


@pytest.mark.asyncio
async def test_list_pages_past_one_call(
    target: ConformanceTarget, site: Path, stub: TapisStub, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tapis_files, "LIST_PAGE", 3)
    folder = site / "many"
    folder.mkdir()
    for index in range(7):
        (folder / f"f{index}.txt").write_text(str(index))
    files = await open_files(target)
    names = sorted(Path(entry.path).name for entry in await files.list("many"))
    assert names == [f"f{index}.txt" for index in range(7)]
    asked = [
        request["query"].get("offset")
        for request in stub.state.requests_to("/v3/files/ops/", "GET")
        if request["path"].endswith("/many")
    ]
    assert asked[-3:] == ["0", "3", "6"]
    alone = await files.list("many/f2.txt")
    assert [(entry.kind, entry.size) for entry in alone] == [("file", 1)]


@pytest.mark.asyncio
async def test_read_far_into_a_file(target: ConformanceTarget, site: Path, stub: TapisStub) -> None:
    """Tapis takes ``start,count`` and refuses count < start, so relay asks for more."""
    payload = bytes(range(256)) * 20
    (site / "big.bin").write_bytes(payload)
    files = await open_files(target)
    assert await files.read("big.bin", offset=4000, limit=100) == payload[4000:4100]
    sent = stub.state.requests_to("/v3/files/content/")[-1]["headers"]["range"]
    assert sent == "4000,4000"
    assert await files.read("big.bin", offset=5000) == payload[5000:]
    assert await files.read("big.bin", offset=len(payload)) == b""
    assert await files.read("big.bin", offset=3, limit=0) == b""


@pytest.mark.asyncio
async def test_chunked_write_resumes_with_a_new_adapter(
    target: ConformanceTarget, site: Path, stub: TapisStub
) -> None:
    """Chunks collect in relay's state dir and go up with one ``insert`` on the final one."""
    first = await open_files(target)
    await first.write("up/data.bin", b"0123", final=False)
    assert not (site / "up").exists()
    assert len(list(target.state_dir.rglob("*.relay-part"))) == 1
    later = await open_files(target)
    held = (await later.stat(part_path("up/data.bin"))).size
    assert held == 4
    await later.write("up/data.bin", b"4567", offset=held, final=False)
    await later.write("up/data.bin", b"AB", final=False)
    assert (await later.stat(part_path("up/data.bin"))).size == 2
    before = len(stub.state.requests_to("/v3/files/ops/", "POST"))
    entry = await later.write("up/data.bin", b"CD", offset=2)
    inserts = stub.state.requests_to("/v3/files/ops/", "POST")[before:]
    assert [request["path"].endswith("/up/data.bin") for request in inserts] == [True]
    assert entry.size == 4
    assert (site / "up" / "data.bin").read_bytes() == b"ABCD"
    assert [path.name for path in (site / "up").iterdir()] == ["data.bin"]
    assert list(target.state_dir.rglob("*.relay-part")) == []


@pytest.mark.asyncio
async def test_final_chunk_checks_the_target_again(target: ConformanceTarget, site: Path) -> None:
    files = await open_files(target)
    await files.write("race.txt", b"part ", final=False)
    (site / "race.txt").write_text("someone else")
    await refused(RefusalReason.PATH_EXISTS, files.write("race.txt", b"end", offset=5))
    assert (site / "race.txt").read_text() == "someone else"
    held = (await files.stat(part_path("race.txt"))).size or 0
    await files.write("race.txt", b"end"[held - 5 :], offset=held, overwrite=True)
    assert (site / "race.txt").read_text() == "part end"


@pytest.mark.asyncio
async def test_write_never_lands_in_a_directory(target: ConformanceTarget, site: Path) -> None:
    """Tapis would insert into a directory at the target; relay refuses first."""
    (site / "folder").mkdir()
    files = await open_files(target)
    await refused(RefusalReason.PATH_EXISTS, files.write("folder", b"x", overwrite=True))
    await files.write("folder", b"x", overwrite=True, final=False)
    await refused(RefusalReason.PATH_EXISTS, files.write("folder", b"y", overwrite=True, offset=1))
    assert list((site / "folder").iterdir()) == []


@pytest.mark.asyncio
async def test_delete_a_symlink_leaves_what_it_points_at(
    target: ConformanceTarget, site: Path
) -> None:
    kept = site / "kept"
    kept.mkdir()
    (kept / "inside.txt").write_text("still here")
    (site / "link").symlink_to(kept, target_is_directory=True)
    files = await open_files(target)
    assert (await files.stat("link")).kind == "dir"
    await files.delete("link")
    assert not (site / "link").is_symlink()
    assert (kept / "inside.txt").read_text() == "still here"


@pytest.mark.asyncio
async def test_delete_keeps_the_work_directory_and_above(
    target: ConformanceTarget, site: Path
) -> None:
    (site / "a.txt").write_text("a")
    files = await open_files(target)
    for path in (".", "", str(site), str(site.parent), "/"):
        await refused(RefusalReason.INVALID_ARGUMENTS, files.delete(path, recursive=True))
    assert (site / "a.txt").exists()
    await refused(RefusalReason.PATH_NOT_FOUND, files.delete("nothing-here"))


@pytest.mark.asyncio
async def test_stage_is_content_addressed(target: ConformanceTarget, site: Path) -> None:
    files = await open_files(target)
    digest = await files.stage(b"inputs")
    assert (site / ".relay-content" / digest).read_bytes() == b"inputs"
    assert [path.name for path in (site / ".relay-content").iterdir()] == [digest]
    assert await files.has(digest)
    assert await files.stage(b"inputs") == digest


def test_home_is_known_only_on_a_host_relay_registered() -> None:
    assert home_for("/home/me/.clio-relay/tapis") == "/home/me"
    assert home_for("/scratch/me/relay") == "/scratch/me/relay"
    assert home_for("/.clio-relay/tapis") == "/.clio-relay/tapis"
