"""A transient input never outlives its job on the relay host (RL16)."""

from __future__ import annotations

import pytest

from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.spec import InputRef, JobSpec
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]


def _reads(digest: str, *, transient: bool, seconds: int = 0) -> JobSpec:
    command = f"cat secrets.env; sleep {seconds}" if seconds else "cat secrets.env"
    return JobSpec(
        executable="sh",
        arguments=["-c", command],
        inputs=[InputRef(name="secrets.env", sha256=digest, transient=transient)],
    )


async def test_inputs_are_kept_unless_marked_transient() -> None:
    assert InputRef(name="a", sha256="0" * 64).transient is False


async def test_the_store_drops_a_blob_once(relay_paths: RelayPaths) -> None:
    store = ContentStore(relay_paths.content_dir)
    digest = store.put(b"token=1")
    assert store.drop(digest) is True
    assert not store.has(digest)
    assert store.drop(digest) is False


async def test_a_transient_input_is_dropped_when_its_job_ends(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, "lab", adapter="local", config={"scheduler": "none"})
    service = RelayService(relay_paths)
    secret = service.content.put(b"token=1\n")
    kept = service.content.put(b"mesh\n")
    first = await service.start_run("lab", _reads(secret, transient=True))
    second = await service.start_run("lab", _reads(kept, transient=False))
    for record in (first, second):
        done = await service.wait(record.operation_id, 20, poll_s=0.1)
        assert done.state == OperationState.COMPLETED, done
    assert not service.content.has(secret)
    assert service.content.has(kept)
    first_view = service.view(service.operation(first.operation_id))
    assert "token=1" in first_view["result"]["stdout_tail"]
    assert "secrets.env" not in {item["name"] for item in first_view["outputs"]}
    second_view = service.view(service.operation(second.operation_id))
    assert "secrets.env" in {item["name"] for item in second_view["outputs"]}
    await service.close()


async def test_a_blob_another_open_job_lists_is_kept_until_it_ends(
    relay_paths: RelayPaths,
) -> None:
    add_site(relay_paths, "lab", adapter="local", config={"scheduler": "none"})
    service = RelayService(relay_paths)
    secret = service.content.put(b"token=2\n")
    long = await service.start_run("lab", _reads(secret, transient=True, seconds=30))
    short = await service.start_run("lab", _reads(secret, transient=True))
    done = await service.wait(short.operation_id, 20, poll_s=0.1)
    assert done.state == OperationState.COMPLETED, done
    assert service.content.has(secret)
    await service.cancel(long.operation_id)
    ended = await service.wait(long.operation_id, 20, poll_s=0.1)
    assert ended.state == OperationState.CANCELLED, ended
    assert not service.content.has(secret)
    await service.close()
