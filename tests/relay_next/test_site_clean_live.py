"""site_clean removes nothing while relay's jobs run, and says which; cancel ends them.

Relay's local records are not the only word: the site's scheduler is asked for the
jobs named ``relay-...`` and the job ids relay's records hold, so a job whose
record ended early still stops the clean. The CLI's ``cancel`` is the cancel tool.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest
from fastmcp import Client
from typer.testing import CliRunner

from clio_relay import cli, site_clean
from clio_relay.adapters.base import CleansSite, ListsLiveJobs
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.site_prune import jobs_script, parse_live, parse_report
from clio_relay.surface.server import create_server
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.testing.fixture_adapter import FixtureAdapter

from .conftest import add_site

LIVE = [{"native_id": "28048", "name": "relay-op_abc", "state": "RUNNING"}]


@pytest.fixture
def site(monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[str, Any]]:
    """A fixture adapter that cleans, and whose scheduler lists ``site["live"]``."""
    state: dict[str, Any] = {"live": [], "asked": [], "cleaned": [], "report": {}, "ended": []}

    def clean_targets(self: FixtureAdapter) -> tuple[list[str], list[str]]:
        return ["~/relay-worker"], []

    async def clean(self: FixtureAdapter, targets: list[str], keep: list[str]) -> dict[str, Any]:
        state["cleaned"].append(targets)
        return {"removed": targets, "kept": keep, **state["report"]}

    async def live_jobs(self: FixtureAdapter, known: list[str]) -> list[dict[str, str]]:
        state["asked"].append(known)
        return list(state["live"])

    async def cancel(self: FixtureAdapter, handle: JobHandle) -> JobStatus:
        state["ended"].append(handle.native_id)
        return JobStatus(state=JobState.CANCELLED)

    monkeypatch.setattr(FixtureAdapter, "clean_targets", clean_targets, raising=False)
    monkeypatch.setattr(FixtureAdapter, "clean", clean, raising=False)
    monkeypatch.setattr(FixtureAdapter, "live_jobs", live_jobs, raising=False)
    monkeypatch.setattr(FixtureAdapter, "cancel", cancel)
    yield state
    monkeypatch.undo()
    # ABCMeta caches the positive isinstance, which would outlive the patch.
    cast(Any, CleansSite)._abc_caches_clear()
    cast(Any, ListsLiveJobs)._abc_caches_clear()


def _ended_record(service: RelayService) -> None:
    """A record that ended (as after a manual scancel) while its job still ran."""
    service.operations.put(
        "op_ended",
        OperationRecord(
            operation_id="op_ended",
            kind="run",
            site="fx",
            adapter="fixture",
            spec={},
            spec_digest="e" * 64,
            state=OperationState.FAILED,
            handle=JobHandle(adapter="fixture", native_id="28050"),
        ),
    )


def test_the_live_lines_are_read() -> None:
    text = "live\t28048|relay-op_abc|RUNNING\nremoved\t/x\nlive\t7|install|PENDING\n"
    assert parse_live(text) == [*LIVE, {"native_id": "7", "name": "install", "state": "PENDING"}]
    report = parse_report(text)
    assert report["removed"] == ["/x"] and len(report["live"]) == 2
    assert "live" not in parse_report("removed\t/x\n")


def test_the_plan_refuses_while_the_scheduler_lists_relays_jobs(
    relay_paths: RelayPaths, site: dict[str, Any]
) -> None:
    add_site(relay_paths)
    site["live"] = [*LIVE, {"native_id": "28050", "name": "jarvis-install", "state": "PENDING"}]

    async def scenario() -> Refusal:
        service = RelayService(relay_paths)
        try:
            _ended_record(service)
            with pytest.raises(Refusal) as caught:
                await site_clean.plan(service, "fx")
            return caught.value
        finally:
            await service.close()

    refusal = asyncio.run(scenario())
    assert refusal.reason is RefusalReason.POLICY_REFUSED
    assert site["asked"] == [["28050"]]
    assert "op_abc (job 28048, running)" in refusal.message
    assert "op_ended (job 28050, pending)" in refusal.message
    assert "clio-relay cancel OPERATION" in refusal.message
    assert refusal.data["operations"] == ["op_abc", "op_ended"]
    assert site["cleaned"] == []


def test_the_plan_goes_on_when_the_scheduler_lists_none(
    relay_paths: RelayPaths, site: dict[str, Any]
) -> None:
    add_site(relay_paths)

    async def scenario() -> dict[str, Any]:
        service = RelayService(relay_paths)
        try:
            return await site_clean.plan(service, "fx")
        finally:
            await service.close()

    assert "~/relay-worker" in asyncio.run(scenario())["targets"]


def test_a_prune_that_found_a_live_job_refuses(
    relay_paths: RelayPaths, site: dict[str, Any]
) -> None:
    add_site(relay_paths)

    async def scenario() -> Refusal:
        service = RelayService(relay_paths)
        try:
            planned = await site_clean.plan(service, "fx")
            site["report"] = {"removed": [], "live": LIVE}
            with pytest.raises(Refusal) as caught:
                await site_clean.clean(service, planned)
            return caught.value
        finally:
            await service.close()

    refusal = asyncio.run(scenario())
    assert "op_abc (job 28048, running)" in refusal.message


def test_cancel_ends_a_job_that_outlived_its_record(
    relay_paths: RelayPaths, site: dict[str, Any]
) -> None:
    """The job site clean names under an ended operation is one relay's cancel ends."""
    add_site(relay_paths)

    async def scenario() -> list[str | None]:
        service = RelayService(relay_paths)
        try:
            _ended_record(service)
            await service.cancel("op_ended")
            site["live"] = [{"native_id": "28050", "name": "jarvis", "state": "RUNNING"}]
            record = await service.cancel("op_ended")
            assert record.state is OperationState.FAILED
            return [change.note for change in record.history]
        finally:
            await service.close()

    notes = asyncio.run(scenario())
    assert site["ended"] == ["28050"]
    assert notes[-1] == "cancelled job 28050, which still ran after the operation ended"


def test_the_cli_cancels_like_the_tool(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    add_site(relay_paths, config={"queue_s": 30, "run_s": 30})
    monkeypatch.setenv("CLIO_RELAY_CONFIG_DIR", str(relay_paths.config_dir))
    monkeypatch.setenv("CLIO_RELAY_STATE_DIR", str(relay_paths.state_dir))

    async def start() -> str:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            spec = {"executable": "sleep", "arguments": ["300"]}
            started = await client.call_tool("run", {"site": "fx", "spec": spec, "wait_s": 0})
            return cast(dict[str, Any], started.structured_content)["operation_id"]

    operation_id = asyncio.run(start())
    runner = CliRunner()
    result = runner.invoke(cli.app, ["cancel", operation_id, "+json"])
    assert result.exit_code == 0, result.output
    shown = json.loads(result.stdout)
    assert (shown["operation_id"], shown["state"]) == (operation_id, "cancelled")
    unknown = runner.invoke(cli.app, ["cancel", "op-none"])
    assert unknown.exit_code == 2


@pytest.mark.release_platform("posix")
def test_the_jobs_script_asks_the_scheduler(tmp_path: Path) -> None:
    """A fake ``squeue``: relay's named job and a known id count, another job does not."""
    if shutil.which("bash") is None:
        pytest.skip("no bash")
    squeue = tmp_path / "squeue"
    squeue.write_text(
        "#!/bin/sh\nprintf '%s\\n' '28048|relay-op_abc|RUNNING' '28050|jarvis|PENDING' "
        "'99|someone-else|RUNNING'\n"
    )
    squeue.chmod(0o755)
    env = {**os.environ, "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}"}
    command = jobs_script(["28050"])
    done = subprocess.run(command, shell=True, env=env, capture_output=True, text=True, check=True)
    assert [job["native_id"] for job in parse_live(done.stdout)] == ["28048", "28050"]
