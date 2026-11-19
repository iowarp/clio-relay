"""The runtime's pieces a person reads: status lines, view data, handles, settings (RL20-RL23)."""

from __future__ import annotations

import asyncio
import inspect
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from clio_relay.adapters.worker.adapter import WorkerAdapter
from clio_relay.jobs.handles import ExecResult, JobHandle, JobState, JobStatus, OutputRef
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal
from clio_relay.serve import config as serve_settings
from clio_relay.serve import runtime as runtime_module
from clio_relay.serve.desktop import decode, settings, visitor_plan
from clio_relay.serve.mount import Mount, Mounter, tool_prefix
from clio_relay.serve.plan import ServePlan
from clio_relay.serve.report import (
    connect_handle,
    final_message,
    frpc_problem,
    handle_job_dir,
    is_artifact,
)
from clio_relay.serve.runtime import ServeRuntime
from clio_relay.surface.settings import RelayPaths

from .helpers import frp_plan, rendezvous, service

pytestmark = pytest.mark.release_platform("posix")


def runtime_with(tmp_path: Path, plan: ServePlan) -> ServeRuntime:
    runtime = ServeRuntime(tmp_path / "state", transport="http")
    runtime.plans[plan.operation_id] = plan
    return runtime


X509 = (
    "2026-11-18 12:38:59.277 [W] [client/service.go:319] connect to server error: "
    "tls: failed to verify certificate: x509: certificate signed by unknown authority"
)


def test_frpc_problem_names_the_latest_warning(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path)
    log = "2026-11-18 12:38:58.000 [I] [client/service.go:1] start\n" + X509 + "\n"
    problem = frpc_problem(plan, log.encode())
    assert problem is not None
    assert problem.endswith("x509: certificate signed by unknown authority")


def test_frpc_problem_is_gone_after_a_login(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path)
    log = X509 + "\n2026-11-18 12:39:30.000 [I] [client/service.go:2] login to server success\n"
    assert frpc_problem(plan, log.encode()) is None


def test_frpc_problem_hides_secrets(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path)
    log = "2026-11-18 12:38:59.277 [E] [proxy.go:9] bad key shared-key-value\n"
    assert frpc_problem(plan, log.encode()) == "bad key ***"


def test_ui_data_is_the_one_place_with_urls_and_secrets(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path)
    runtime = runtime_with(tmp_path, plan)
    record = cast(OperationRecord, SimpleNamespace(operation_id=plan.operation_id))
    port = plan.ports[0]
    assert runtime.ui_data(record) == {
        "ports": {
            "web": {
                "address": f"127.0.0.1:{port.local_port}",
                "url": f"http://127.0.0.1:{port.local_port}/health",
                "secret": "secret-of-web",
                "secret_env": "SVC_SECRET",
            }
        }
    }


def test_connect_handle_carries_the_key_not_the_token(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path, direct=True)
    handle = connect_handle(plan)
    payload = decode(handle)
    assert payload["sk"] == "shared-key-value"
    assert "frp-token-value" not in str(payload)
    runtime = runtime_with(tmp_path, plan)
    assert runtime.connect_command(plan) == f"clio-relay connect {handle}"
    visitor = visitor_plan(handle, rendezvous(tmp_path), {"web": 45001})
    assert visitor.ports[0].local_port == 45001
    assert visitor.secret_key == "shared-key-value"
    assert visitor.rendezvous is not None and visitor.rendezvous.direct
    with pytest.raises(Refusal, match="not a connect handle"):
        decode("!!!")


def test_final_message_says_how_the_service_ended(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path)
    status = JobStatus(state=JobState.COMPLETED)
    final = final_message
    assert final(plan, status, {"reason": "stopped"}) == "stopped"
    taken = {"reason": "port_taken", "port": 8080, "port_name": "web", "host": "n1"}
    assert final(plan, status, taken) == "refused: port 8080 (web) was already in use on n1"
    plan.rebinds = 2
    assert final(plan, status, {"reason": "exited", "exit_code": 1}) == (
        "the service exited with 1; the desktop port was bound again 2 times"
    )


@pytest.mark.parametrize(
    ("name", "kept"),
    [
        ("result.vtk", True),
        ("out/serve/data.bin", True),
        ("serve/service.log", True),
        ("serve/launch.log", True),
        ("serve/frpc.toml", False),
        ("serve/ready.json", False),
    ],
)
def test_artifacts_drop_the_wrapper_files_but_keep_its_logs(name: str, kept: bool) -> None:
    assert is_artifact(OutputRef(name=name, path=f"/job/{name}")) is kept


def test_tool_prefix() -> None:
    assert tool_prefix("ares", "counter") == "ares_counter_"


def test_a_stops_outputs_carry_provenance(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path).model_copy(update={"job_dir": "/scratch/jobs/op_test"})
    runtime = ServeRuntime(tmp_path / "state", transport="http")
    runtime.plans[plan.operation_id] = plan
    record = OperationRecord(
        operation_id=plan.operation_id,
        kind="serve",
        site="fx",
        adapter="fixture",
        spec={},
        spec_digest="c" * 64,
        state=OperationState.RUNNING,
    )
    runtime.records.put(plan.operation_id, record)
    files = {
        "stopped.json": b'{"reason": "stopped", "exit_code": 0}',
        "artifacts.txt": b"12\tstate.pvsm\n",
    }

    async def read(_plan: ServePlan, _adapter: Any, name: str, *, tail: int = 0) -> bytes | None:
        del tail
        return files.get(name)

    runtime._read = read  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]
    own = {"provenance": {"schema": "a package's own"}, "view": {"path": "/x"}}
    log = OutputRef(name="serve/launch.log", path="/scratch/jobs/op_test/serve/launch.log")
    status = JobStatus(state=JobState.COMPLETED, outputs=[log.model_copy(update={"metadata": own})])
    final = asyncio.run(runtime._finish(plan, None, status))  # pyright: ignore[reportPrivateUsage]
    found = {item.name: item.metadata for item in final.outputs}
    assert found["serve/launch.log"] == own
    block = found["state.pvsm"]["provenance"]
    assert (block["schema"], block["operation_id"], block["site"]) == (
        "relay.provenance.v1",
        "op_test",
        "fx",
    )
    assert (block["uri"], block["size_bytes"]) == ("/scratch/jobs/op_test/state.pvsm", 12)


def test_the_jobs_native_id_reaches_the_record_and_provenance(tmp_path: Path) -> None:
    """The adapter's handle goes on the operation record, recovered or at the end."""
    handle = JobHandle(
        adapter="fixture", native_id="27810", data={"job_dir": "/scratch/jobs/op_test"}
    )
    plan = frp_plan(tmp_path).model_copy(
        update={"job_dir": "/scratch/jobs/op_test", "handle": handle}
    )
    runtime = ServeRuntime(tmp_path / "state", transport="http")
    runtime.plans[plan.operation_id] = plan
    record = OperationRecord(
        operation_id=plan.operation_id,
        kind="serve",
        site="fx",
        adapter="fixture",
        spec={},
        spec_digest="c" * 64,
        state=OperationState.RUNNING,
    )
    runtime.records.put(plan.operation_id, record)
    found = asyncio.run(runtime._handle(plan, None, record))  # pyright: ignore[reportPrivateUsage]
    assert found == handle
    assert record.handle == handle
    stored = runtime.records.get(plan.operation_id)
    assert stored is not None and stored.handle is not None
    assert stored.handle.native_id == "27810"

    runtime.records.put(plan.operation_id, record.model_copy(update={"handle": None}))

    async def read(_plan: ServePlan, _adapter: Any, name: str, *, tail: int = 0) -> bytes | None:
        del tail
        return b"12\tstate.pvsm\n" if name == "artifacts.txt" else None

    runtime._read = read  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]
    final = asyncio.run(
        runtime._finish(plan, None, JobStatus(state=JobState.CANCELLED))  # pyright: ignore[reportPrivateUsage]
    )
    block = final.outputs[0].metadata["provenance"]
    assert block["native_id"] == "27810"
    stored = runtime.records.get(plan.operation_id)
    assert stored is not None and stored.handle is not None


def test_the_job_dir_is_found_inside_a_handle_the_worker_wraps() -> None:
    inner = JobHandle(adapter="local", native_id="801", data={"job_dir": "/w/jobs/op_1"})
    wrapped = JobHandle(
        adapter="worker", native_id="801", data={"remote": inner.model_dump(mode="json")}
    )
    assert handle_job_dir(inner) == "/w/jobs/op_1"
    assert handle_job_dir(wrapped) == "/w/jobs/op_1"
    assert handle_job_dir(JobHandle(adapter="x", native_id="1", data={})) is None


def test_a_running_service_is_brought_up_without_a_client_poll(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A client may stop polling at "running" and wait for tools/list_changed."""
    monkeypatch.setattr(runtime_module, "FOLLOW_FIRST_S", 0.0)
    monkeypatch.setattr(runtime_module, "FOLLOW_MAX_S", 0.0)
    handle = JobHandle(adapter="fixture", native_id="1", data={"job_dir": "/j"})
    plan = frp_plan(tmp_path).model_copy(update={"job_dir": "/j", "handle": handle})
    runtime = ServeRuntime(tmp_path / "state", transport="http")
    runtime.plans[plan.operation_id] = plan
    running = JobStatus(state=JobState.RUNNING)
    calls: list[int] = []

    served = plan.endpoint(note="serving", connect_command=None)

    async def bring_up(_plan: ServePlan, _adapter: Any, status: JobStatus) -> Any:
        calls.append(1)
        return status, (served if len(calls) >= 3 else None)

    class Adapter:
        async def status(self, _handle: JobHandle) -> JobStatus:
            return running

    runtime._running = bring_up  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]

    async def scenario() -> None:
        runtime._follow(plan, Adapter(), handle, running)  # pyright: ignore[reportPrivateUsage]
        runtime._follow(plan, Adapter(), handle, running)  # pyright: ignore[reportPrivateUsage]
        follower = runtime._followers[plan.operation_id]  # pyright: ignore[reportPrivateUsage]
        await follower

    asyncio.run(scenario())
    assert len(calls) == 3
    assert not runtime._followers  # pyright: ignore[reportPrivateUsage]


def test_a_status_call_during_a_stop_does_not_mount_again(tmp_path: Path) -> None:
    """A task poll in flight when a stop arrives must not leave the tools mounted."""
    handle = JobHandle(adapter="fixture", native_id="1", data={"job_dir": "/j"})
    plan = frp_plan(tmp_path).model_copy(update={"job_dir": "/j", "handle": handle})
    runtime = ServeRuntime(tmp_path / "state", transport="http")
    runtime.plans[plan.operation_id] = plan
    record = OperationRecord(
        operation_id=plan.operation_id,
        kind="serve",
        site="fx",
        adapter="fixture",
        spec={},
        spec_digest="d" * 64,
        state=OperationState.RUNNING,
    )
    runtime.records.put(plan.operation_id, record)
    mounted: list[bool] = []
    state = {"job": JobState.RUNNING}

    async def bring_up(_plan: ServePlan, _adapter: Any, status: JobStatus) -> Any:
        mounted.append(True)
        return status, plan.endpoint(note="serving", connect_command=None)

    async def close_desktop(_plan: ServePlan) -> None:
        mounted.append(False)

    async def request_stop(_plan: ServePlan, _adapter: Any, _handle: JobHandle) -> None:
        state["job"] = JobState.CANCELLED

    async def finish(_plan: ServePlan, _adapter: Any, status: JobStatus) -> JobStatus:
        return status

    gate = asyncio.Event()

    class Adapter:
        async def status(self, _handle: JobHandle) -> JobStatus:
            if not gate.is_set():
                await gate.wait()
            return JobStatus(state=state["job"])

    runtime._running = bring_up  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]
    runtime._close_desktop = close_desktop  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]
    runtime._request_stop = request_stop  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]
    runtime._finish = finish  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]
    adapter = Adapter()

    async def scenario() -> JobStatus:
        polling = asyncio.create_task(runtime.status(None, adapter, record))
        await asyncio.sleep(0)
        stopping = asyncio.create_task(runtime.stop(None, adapter, record))
        await asyncio.sleep(0)
        gate.set()
        await polling
        final = await stopping
        state["job"] = JobState.RUNNING
        again, endpoint = await runtime.status(None, adapter, record)
        assert endpoint is None
        assert again.message is not None and again.message.startswith("stopping")
        return final

    final = asyncio.run(scenario())
    assert final.state == JobState.CANCELLED
    assert mounted == [True, False]


def test_a_second_mcp_server_with_the_same_tool_names_is_refused(tmp_path: Path) -> None:
    """Two live servers under one ``<site>_<name>_`` prefix would hide each other's tools."""
    spec = service(
        ports=[{"name": "mcp", "protocol": "mcp", "protection": "secret_env:SVC_SECRET"}]
    )
    runtime = ServeRuntime(tmp_path / "state", transport="http")
    live = frp_plan(tmp_path, spec).model_copy(update={"operation_id": "op_live"})
    runtime._save(live)  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(Refusal, match="op_live already serves an MCP server"):
        runtime._refuse_a_second_mount("fx", "web_app", "op_new")  # pyright: ignore[reportPrivateUsage]
    runtime._refuse_a_second_mount("fx", "other", "op_new")  # pyright: ignore[reportPrivateUsage]
    runtime._refuse_a_second_mount("fx", "web_app", "op_live")  # pyright: ignore[reportPrivateUsage]
    runtime._save(live.model_copy(update={"stop_requested_at": live.created_at}))  # pyright: ignore[reportPrivateUsage]
    runtime._refuse_a_second_mount("fx", "web_app", "op_new")  # pyright: ignore[reportPrivateUsage]

    mounter = Mounter()
    mounter.provider.mounts["op_live"] = Mount("op_live", "fx_web_app_", "u", cast(Any, None))
    with pytest.raises(ValueError, match="already mounted for op_live"):
        asyncio.run(mounter.mount("op_new", "fx_web_app_", "http://127.0.0.1:1/mcp"))


def test_the_login_hop_runs_through_the_workers_hop_exec(tmp_path: Path) -> None:
    calls: list[tuple[list[str], dict[str, Any]]] = []

    class Worker:
        async def hop_exec(
            self,
            command: list[str],
            *,
            timeout_s: float | None = None,
            max_output_bytes: int,
            on_stall: Any = None,
        ) -> ExecResult:
            del on_stall
            calls.append((command, {"timeout_s": timeout_s, "max": max_output_bytes}))
            return ExecResult(argv=command, exit_code=0, stdout="ok\n", stderr="", duration_s=0)

        async def exec(self, *_args: Any, **_kwargs: Any) -> ExecResult:
            raise AssertionError("the worker's hop_exec comes first")

    real = inspect.signature(WorkerAdapter.hop_exec).parameters
    fake = inspect.signature(Worker.hop_exec).parameters
    assert [(p.name, p.kind) for p in real.values()] == [(p.name, p.kind) for p in fake.values()]
    plan = frp_plan(tmp_path)
    runtime = ServeRuntime(tmp_path / "state", transport="http")
    result = asyncio.run(runtime._exec(Worker(), plan, ["true"]))  # pyright: ignore[reportPrivateUsage]
    assert result.stdout == "ok\n"
    assert calls and calls[0][0] == ["true"] and calls[0][1]["max"] > 0


def test_settings_cli_sets_shows_and_clears(tmp_path: Path) -> None:
    paths = RelayPaths(config_dir=tmp_path / "config", state_dir=tmp_path / "state")
    shown = settings(
        paths,
        {
            "address": "relay.example.org",
            "port": "7000",
            "token_file": "~/frp.token",
            "direct": "true",
            "site.ares.hop": "login",
        },
    )
    assert shown["rendezvous"]["direct"] is True
    assert shown["sites"]["ares"]["hop"] == "login"
    with pytest.raises(Refusal, match="unknown setting"):
        settings(paths, {"token": "x"})
    with pytest.raises(Refusal, match="unknown site setting"):
        settings(paths, {"site.ares.nope": "x"})
    with pytest.raises(Refusal, match="invalid serve settings"):
        settings(paths, {"site.ares.hop": "http_proxy"})
    cleared = settings(paths, {"rendezvous": ""})
    assert "rendezvous" not in cleared
    assert cleared["sites"]["ares"]["hop"] == "login"


def test_settings_file_order(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CLIO_RELAY_RENDEZVOUS_FILE", raising=False)
    state = tmp_path / "state"
    assert serve_settings.settings_path(state) == state / "serve" / "rendezvous.json"
    monkeypatch.setenv("CLIO_RELAY_RENDEZVOUS_FILE", str(tmp_path / "env.json"))
    assert serve_settings.settings_path(state) == tmp_path / "env.json"
    chosen = serve_settings.settings_path(state, {"rendezvous_file": str(tmp_path / "ctx.json")})
    assert chosen == tmp_path / "ctx.json"


def test_rendezvous_needs_a_token_source() -> None:
    with pytest.raises(ValueError, match="token_file or token_env"):
        serve_settings.Rendezvous.model_validate({"address": "a", "port": 1})
