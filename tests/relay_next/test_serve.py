"""The serve verb through relay's surface, with a stand-in service runtime.

W9 owns the runtime. These tests pin the interface relay core gives it: the
operation record, the task status that names the endpoint, the panel data that
only the serve result carries (RL22), and the attach and mount points.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from collections.abc import Iterator
from importlib import metadata
from pathlib import Path
from typing import Any

import pytest
from fastmcp import Client, FastMCP
from fastmcp_tasks.client import (
    call_tool_task,  # pyright: ignore[reportUnknownVariableType]
)
from typer.testing import CliRunner

from clio_relay import cli
from clio_relay.adapters.base import BaseAdapter
from clio_relay.adapters.hooks import HookContext, set_hook
from clio_relay.jobs.handles import Endpoint, EndpointPort, JobState, JobStatus, OutputRef
from clio_relay.jobs.records import OperationRecord
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.changes import ServerChanges
from clio_relay.surface.server import SERVICE_META, create_server
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.surface.stubs import StubAskProvider, StubServe

from .conftest import add_site

pytestmark = pytest.mark.release_platform("posix")

SECRET = "http://relay-host:8080/?token=not-for-describe"
SPEC: dict[str, Any] = {
    "job": {"executable": "sleep", "arguments": ["300"]},
    "ports": [{"name": "viz", "port": 11111}],
}
ENDPOINT = Endpoint(
    service="viz",
    ports=[EndpointPort(name="viz", protocol="tcp", host="relay-host", port=8080, url=SECRET)],
)


class FakeServe:
    """A service runtime that is ready on the first look and stops cleanly."""

    def __init__(self, context: HookContext) -> None:
        self.context = context
        self.attached: list[FastMCP[Any]] = []
        self.started: list[str] = []
        self.stopped: list[str] = []

    def attach(self, server: FastMCP[Any], changes: ServerChanges) -> None:
        self.attached.append(server)

        @server.tool(name="lab_viz_status")
        def lab_viz_status() -> str:  # pyright: ignore[reportUnusedFunction]
            return "mounted by the serve hook"

    async def start(
        self, site: SiteEntry, adapter: BaseAdapter[Any], spec: ServiceSpec, record: OperationRecord
    ) -> Endpoint | None:
        self.started.append(record.operation_id)
        return None

    async def status(
        self, site: SiteEntry, adapter: BaseAdapter[Any], record: OperationRecord
    ) -> tuple[JobStatus, Endpoint | None]:
        return JobStatus(state=JobState.RUNNING, message="listening on 11111"), ENDPOINT

    async def stop(
        self, site: SiteEntry, adapter: BaseAdapter[Any], record: OperationRecord
    ) -> JobStatus:
        self.stopped.append(record.operation_id)
        return JobStatus(
            state=JobState.COMPLETED,
            exit_code=0,
            outputs=[OutputRef(name="state.pvsm", path="/scratch/state.pvsm")],
        )

    def ui_data(self, record: OperationRecord) -> dict[str, Any] | None:
        return {"url": SECRET}

    async def close(self) -> None:
        return None


class MountingAsks(StubAskProvider):
    """The stub ask provider, plus pages mounted on relay's HTTP server."""

    def __init__(self, context: HookContext) -> None:
        super().__init__(context)
        self.mounted: list[FastMCP[Any]] = []

    def mount(self, server: FastMCP[Any]) -> None:
        self.mounted.append(server)


@pytest.fixture
def fake_serve() -> Iterator[list[FakeServe]]:
    made: list[FakeServe] = []

    def factory(context: HookContext) -> FakeServe:
        hook = FakeServe(context)
        made.append(hook)
        return hook

    set_hook("serve", factory)
    yield made
    set_hook("serve", None)


def _lab(paths: RelayPaths) -> None:
    add_site(paths, "lab", adapter="local", config={"scheduler": "none"})


def _structured(result: Any) -> dict[str, Any]:
    structured = result.structured_content
    assert isinstance(structured, dict), result
    return structured  # pyright: ignore[reportUnknownVariableType]


def test_serve_without_tasks_waits_for_the_endpoint(
    relay_paths: RelayPaths, fake_serve: list[FakeServe]
) -> None:
    _lab(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            names = {tool.name for tool in await client.list_tools()}
            assert "lab_viz_status" in names
            result = await client.call_tool("serve", {"site": "lab", "spec": SPEC, "wait_s": 10})
            view = _structured(result)
            assert view["kind"] == "serve"
            assert view["state"] == "running", view
            assert view["result"]["endpoint"]["ports"][0]["port"] == 8080
            assert view["result"]["message"] == "listening on 11111"
            assert result.meta is not None and result.meta[SERVICE_META] == {"url": SECRET}
            text = " ".join(getattr(item, "text", "") for item in result.content)
            assert "viz: relay-host:8080" in text and "token" not in text

            described = await client.call_tool("describe", {"job": view["operation_id"]})
            assert SECRET not in json.dumps(_structured(described))
            assert described.meta is None or SERVICE_META not in described.meta

            observed = await client.call_tool("serve", {"service": view["operation_id"]})
            assert _structured(observed)["state"] == "running"

            stopped = await client.call_tool("serve", {"stop": view["operation_id"]})
            final = _structured(stopped)
            assert final["state"] == "completed"
            assert final["outputs"][0]["name"] == "state.pvsm"

    asyncio.run(scenario())
    hook = fake_serve[0]
    assert len(hook.attached) == 1
    assert len(hook.started) == 1 and hook.stopped == hook.started


def test_a_serve_task_completes_when_the_service_is_ready(
    relay_paths: RelayPaths, fake_serve: list[FakeServe]
) -> None:
    """A task client wakes on a status change, so ready completes the task with the endpoint."""
    _lab(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="auto") as client:
            task = await call_tool_task(client, "serve", {"site": "lab", "spec": SPEC})
            assert RelayService(relay_paths).operation(task.task_id).task
            message = ""
            status = await task.status()
            for _ in range(100):
                status = await task.status()
                message = status.status_message or ""
                if status.status != "working":
                    break
                await asyncio.sleep(0.1)
            assert status.status == "completed", message
            assert "viz relay-host:8080" in message, message
            assert "token" not in message
            ready = await task.result()
            view = _structured(ready)
            assert view["state"] == "running"
            assert view["result"]["ready_at"]
            assert view["result"]["endpoint"]["ports"][0]["name"] == "viz"
            text = " ".join(getattr(item, "text", "") for item in ready.content)
            assert "keeps running until serve(stop=" in text
            stopped = await client.call_tool("serve", {"stop": task.task_id})
            assert _structured(stopped)["state"] == "completed"
            result = await task.result()
            assert _structured(result)["state"] == "completed"
            assert result.meta is not None and result.meta[SERVICE_META] == {"url": SECRET}

    asyncio.run(scenario())


def test_serve_without_a_runtime_refuses_before_recording(relay_paths: RelayPaths) -> None:
    _lab(relay_paths)
    set_hook("serve", StubServe)
    try:

        async def scenario() -> None:
            async with Client(create_server(relay_paths), mode="legacy") as client:
                result = await client.call_tool(
                    "serve", {"site": "lab", "spec": SPEC}, raise_on_error=False
                )
                assert result.is_error
                assert _structured(result)["refusal"]["reason"] == "verb_not_implemented"

        asyncio.run(scenario())
        assert list(RelayService(relay_paths).operations.all()) == []
    finally:
        set_hook("serve", None)


def test_serve_handles_only_services(relay_paths: RelayPaths, fake_serve: list[FakeServe]) -> None:
    _lab(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            ran = await client.call_tool(
                "run", {"site": "lab", "spec": {"executable": "true"}, "wait_s": 30}
            )
            op = _structured(ran)["operation_id"]
            for argument in ("service", "stop"):
                refused = await client.call_tool("serve", {argument: op}, raise_on_error=False)
                assert _structured(refused)["refusal"]["reason"] == "invalid_arguments"
            bare = await client.call_tool("serve", {"site": "lab"}, raise_on_error=False)
            assert _structured(bare)["refusal"]["reason"] == "invalid_arguments"

    asyncio.run(scenario())


def test_run_records_the_job_name_before_submitting(relay_paths: RelayPaths) -> None:
    _lab(relay_paths)

    async def scenario() -> OperationRecord:
        service = RelayService(relay_paths)
        try:
            record = await service.start_run("lab", JobSpec(executable="true"))
            return await service.wait(record.operation_id, 30)
        finally:
            await service.close()

    record = asyncio.run(scenario())
    assert record.state.value == "completed"
    job_name = (record.result or {}).get("job_name")
    assert isinstance(job_name, str) and job_name
    submitting = [move for move in record.history if move.state.value == "submitting"]
    assert submitting and submitting[0].note == f"job name {job_name}"


def test_http_server_gives_asks_the_public_url_and_mounts_pages(relay_paths: RelayPaths) -> None:
    made: list[MountingAsks] = []

    def factory(context: HookContext) -> MountingAsks:
        provider = MountingAsks(context)
        made.append(provider)
        return provider

    set_hook("asks", factory)
    try:
        server = create_server(
            relay_paths,
            transport="http",
            token="t" * 32,
            base_url="https://relay.example:8766/mcp",
            public_base_url="https://relay.example:8766",
        )
        stdio = create_server(relay_paths)
    finally:
        set_hook("asks", None)
    first, second = made
    assert first.context.settings["public_base_url"] == "https://relay.example:8766"
    assert first.mounted == [server]
    assert second.mounted == [] and "public_base_url" not in second.context.settings
    assert stdio is not server


def test_local_adapter_serve_refuses_without_the_runtime(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    _lab(relay_paths)
    monkeypatch.setitem(sys.modules, "clio_relay.serve", None)

    async def scenario() -> None:
        service = RelayService(relay_paths)
        try:
            site = service.site("lab")
            adapter = service.adapter_named(site, "local")
            assert "serve" in type(adapter).implements
            with pytest.raises(Refusal) as refused:
                await adapter.serve(ServiceSpec.model_validate(SPEC), operation_id="op-1")
            assert refused.value.reason == RefusalReason.VERB_NOT_IMPLEMENTED
        finally:
            await service.close()

    asyncio.run(scenario())


def test_cli_grant_files_and_missing_service_runtime(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert cli._grant_files(["Globus=/run/g.json", " tapis = /run/t.json "]) == {  # pyright: ignore[reportPrivateUsage]
        "globus": Path("/run/g.json"),
        "tapis": Path("/run/t.json"),
    }
    runner = CliRunner()
    bad = runner.invoke(cli.app, ["serve", "--grant-file", "nope"])
    assert bad.exit_code == 2 and "PROVIDER=PATH" in bad.output
    half = runner.invoke(cli.app, ["serve", "--http", "--tls-cert", "/x.pem"])
    assert half.exit_code == 2 and "--tls-key" in half.output
    monkeypatch.setenv("CLIO_RELAY_CONFIG_DIR", str(relay_paths.config_dir))
    monkeypatch.setenv("CLIO_RELAY_STATE_DIR", str(relay_paths.state_dir))
    monkeypatch.setitem(sys.modules, "clio_relay.serve", None)
    for argv in (["connect", "op-1"], ["rendezvous", "show"], ["rendezvous", "set", "a=b"]):
        missing = runner.invoke(cli.app, argv)
        assert missing.exit_code == 2, (argv, missing.output)
        assert "service runtime is not installed" in missing.output


def test_clio_relay_runs_the_new_surface() -> None:
    scripts = {item.name: item.value for item in metadata.entry_points(group="console_scripts")}
    assert scripts["clio-relay"] == "clio_relay.cli:main"
    assert "clio-relay-next" not in scripts
    shown = subprocess.run(
        [sys.executable, "-m", "clio_relay", "--help"],
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    ).stdout
    for command in ("serve", "site", "asks", "connect", "rendezvous", "worker"):
        assert command in shown
