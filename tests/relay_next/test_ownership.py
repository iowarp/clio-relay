"""Operations belong to the relay process that holds a lease on them.

A relay of another version only reports an operation whose owner is running,
and an older relay never takes over a newer one's operation.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from clio_relay.jobs.ownership import Owner, ServeLease
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.spec import JobSpec
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]

ECHO = JobSpec(executable="echo", arguments=["hello-relay"])


def _lease(root: Path, version: str, token: str) -> ServeLease:
    return ServeLease(root, version, token)


async def test_a_running_owner_of_another_version_fences_the_operation(tmp_path: Path) -> None:
    new = _lease(tmp_path, "2.0.0a16", "serve_new")
    old = _lease(tmp_path, "2.0.0a13", "serve_old")
    same = _lease(tmp_path, "2.0.0a16", "serve_twin")
    owner = new.owner()
    assert not old.may_act(owner)
    assert same.may_act(owner) and not same.takes_over(owner)
    assert "only reports it" in old.note(owner)
    new.release()
    assert not old.may_act(owner), "an older relay never takes a newer one's operation"
    newer = _lease(tmp_path, "2.0.0a17", "serve_newer")
    assert newer.may_act(owner) and newer.takes_over(owner)
    assert old.may_act(None) and old.takes_over(None)


async def test_an_older_relay_only_reports_and_a_newer_one_takes_over(
    relay_paths: RelayPaths,
) -> None:
    add_site(relay_paths, config={"queue_s": 0.2, "run_s": 0.2})
    owner = RelayService(relay_paths)
    record = await owner.start_run("fx", ECHO)
    assert record.owner is not None and record.owner.serve == owner.lease.token
    root = owner.lease.root

    old = RelayService(relay_paths)
    old.lease = _lease(root, "0.0.1", "serve_old")
    await asyncio.sleep(1.0)
    seen = await old.refresh(record.operation_id)
    assert seen.state == record.state, "a fenced relay leaves the operation alone"
    assert await old.cancel(record.operation_id) == seen
    assert "only reports it" in old.view(seen)["owner"]["note"]
    assert old.operation(record.operation_id).owner == record.owner

    await owner.close()
    assert (await old.refresh(record.operation_id)).state == record.state

    newer = RelayService(relay_paths)
    newer.lease = _lease(root, "999.0", "serve_newer")
    done = await newer.wait(record.operation_id, 20, poll_s=0.1)
    assert done.state == OperationState.COMPLETED
    assert done.owner == Owner(serve="serve_newer", version="999.0")
    assert "note" not in newer.view(done)["owner"]
    await newer.close()
    await old.close()


async def test_an_operation_with_no_owner_is_adopted(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, config={"queue_s": 0.1, "run_s": 0.1})
    first = RelayService(relay_paths)
    record = await first.start_run("fx", ECHO)
    with first.operations.locked(record.operation_id):
        stored = first.operation(record.operation_id)
        stored.owner = None
        first.operations.put(record.operation_id, stored)
    await first.close()
    other = RelayService(relay_paths)
    other.lease = _lease(first.lease.root, "0.0.1", "serve_other")
    done = await other.wait(record.operation_id, 20, poll_s=0.1)
    assert done.state == OperationState.COMPLETED
    assert done.owner == Owner(serve="serve_other", version="0.0.1")
    await other.close()
