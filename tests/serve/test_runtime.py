"""The runtime's pieces a person reads: status lines, view data, handles, settings (RL20-RL23)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from clio_relay.jobs.handles import JobState, JobStatus, OutputRef
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal
from clio_relay.serve import config as serve_settings
from clio_relay.serve.desktop import decode, settings, visitor_plan
from clio_relay.serve.mount import tool_prefix
from clio_relay.serve.plan import ServePlan
from clio_relay.serve.runtime import ServeRuntime, connect_handle, is_artifact
from clio_relay.surface.settings import RelayPaths

from .helpers import frp_plan, rendezvous

pytestmark = pytest.mark.release_platform("posix")


def runtime_with(tmp_path: Path, plan: ServePlan, frpc_log: str | None) -> ServeRuntime:
    runtime = ServeRuntime(tmp_path / "state", transport="http")
    runtime.plans[plan.operation_id] = plan

    async def read(_plan: ServePlan, _adapter: Any, name: str, *, tail: int = 0) -> bytes | None:
        del tail
        return frpc_log.encode() if name == "frpc.log" and frpc_log is not None else None

    runtime._read = read  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]
    return runtime


def frpc_problem(runtime: ServeRuntime, plan: ServePlan) -> str | None:
    return asyncio.run(runtime._frpc_problem(plan, None))  # pyright: ignore[reportPrivateUsage]


X509 = (
    "2026-11-18 12:38:59.277 [W] [client/service.go:319] connect to server error: "
    "tls: failed to verify certificate: x509: certificate signed by unknown authority"
)


def test_frpc_problem_names_the_latest_warning(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path)
    log = "2026-11-18 12:38:58.000 [I] [client/service.go:1] start\n" + X509 + "\n"
    problem = frpc_problem(runtime_with(tmp_path, plan, log), plan)
    assert problem is not None
    assert problem.endswith("x509: certificate signed by unknown authority")


def test_frpc_problem_is_gone_after_a_login(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path)
    log = X509 + "\n2026-11-18 12:39:30.000 [I] [client/service.go:2] login to server success\n"
    assert frpc_problem(runtime_with(tmp_path, plan, log), plan) is None


def test_frpc_problem_hides_secrets(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path)
    log = "2026-11-18 12:38:59.277 [E] [proxy.go:9] bad key shared-key-value\n"
    assert frpc_problem(runtime_with(tmp_path, plan, log), plan) == "bad key ***"


def test_ui_data_is_the_one_place_with_urls_and_secrets(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path)
    runtime = runtime_with(tmp_path, plan, None)
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
    runtime = runtime_with(tmp_path, plan, None)
    assert runtime.connect_command(plan) == f"clio-relay connect {handle}"
    visitor = visitor_plan(handle, rendezvous(tmp_path), {"web": 45001})
    assert visitor.ports[0].local_port == 45001
    assert visitor.secret_key == "shared-key-value"
    assert visitor.rendezvous is not None and visitor.rendezvous.direct
    with pytest.raises(Refusal, match="not a connect handle"):
        decode("!!!")


def test_final_message_says_how_the_service_ended(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path)
    runtime = runtime_with(tmp_path, plan, None)
    status = JobStatus(state=JobState.COMPLETED)
    final = runtime._final_message  # pyright: ignore[reportPrivateUsage]
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
