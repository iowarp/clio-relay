"""A forward that ended for good says why, and a node's host keys are read with short tries."""

from __future__ import annotations

import asyncio
import socket
from pathlib import Path
from typing import Any

import pytest

from clio_relay.jobs.handles import ExecResult
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.serve import keys as keys_module
from clio_relay.serve.forward_exit import classify_exit, port_bound, port_state
from clio_relay.serve.keys import KEY_ROUNDS, ForwardCare, read_host_keys
from clio_relay.serve.plan import ServePlan

from .helpers import frp_plan

KEY = "ares-comp-04 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIExample"


def _ssh_plan(tmp_path: Path) -> ServePlan:
    return frp_plan(tmp_path).model_copy(
        update={"mode": "ssh", "host": "ares-comp-04", "login_host": "ares"}
    )


def test_a_refused_host_key_is_typed_not_retried() -> None:
    lines = ["", "Host key verification failed.", "kex_exchange_identification: closed"]
    found = classify_exit(lines, "ares-comp-04")
    assert found is not None
    assert found.reason == RefusalReason.HOST_KEY_UNVERIFIED
    assert "ares-comp-04" in found.message
    assert found.data["detail"] == "kex_exchange_identification: closed"


def test_a_node_whose_job_ended_says_so() -> None:
    lines = ["Access denied by pam_slurm_adopt: you have no active jobs on this node"]
    found = classify_exit(lines, "ares-comp-04")
    assert found is not None
    assert found.reason == RefusalReason.UNREACHABLE
    assert found.data["job_ended"] is True


def test_a_refused_login_is_typed_and_other_exits_are_retried() -> None:
    found = classify_exit(["user@ares: Permission denied (publickey)."], None)
    assert found is not None and found.reason == RefusalReason.UNREACHABLE
    assert "the node" in found.message
    assert classify_exit(["Connection reset by peer"], "n1") is None


def test_port_state_tells_a_listener_from_a_free_port() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        port = sock.getsockname()[1]
        assert port_state(port) == "in_use"
        assert port_bound(port)
    assert port_state(port) == "free"
    assert not port_bound(port)


def _result(**kw: Any) -> ExecResult:
    return ExecResult(argv=["sh"], duration_s=0.1, **kw)


class _Exec:
    def __init__(self, results: list[ExecResult | Exception]) -> None:
        self.results = results
        self.calls: list[list[str]] = []

    async def exec(self, argv: list[str], **_kw: Any) -> ExecResult:
        self.calls.append(argv)
        found = self.results.pop(0)
        if isinstance(found, Exception):
            raise found
        return found


def test_host_keys_are_read_again_after_a_hung_look(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_wait(_s: float) -> None:
        return None

    monkeypatch.setattr(keys_module.asyncio, "sleep", no_wait)
    adapter = _Exec(
        [
            _result(exit_code=-1, stdout="", stderr="", timed_out=True),
            _result(exit_code=0, stdout=f"# comment\n{KEY}\nshort line\n", stderr=""),
        ]
    )
    found = asyncio.run(read_host_keys(adapter, _ssh_plan(tmp_path)))
    assert found == [KEY]
    assert len(adapter.calls) == 2
    assert adapter.calls[0][-1] == "ares-comp-04"


def test_host_keys_that_never_answer_raise_unreachable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_wait(_s: float) -> None:
        return None

    monkeypatch.setattr(keys_module.asyncio, "sleep", no_wait)
    adapter = _Exec([OSError("hung"), _result(exit_code=1, stdout="", stderr="no such file")])
    with pytest.raises(Refusal) as caught:
        asyncio.run(read_host_keys(adapter, _ssh_plan(tmp_path)))
    assert caught.value.reason == RefusalReason.UNREACHABLE
    assert "no such file" in caught.value.message


def test_no_keys_are_read_for_the_login_host_itself(tmp_path: Path) -> None:
    plan = _ssh_plan(tmp_path).model_copy(update={"host": "ares"})
    assert asyncio.run(read_host_keys(_Exec([]), plan)) == []


class _Care(ForwardCare):
    def __init__(self, keys: list[list[str] | Refusal]) -> None:
        self.forwards = {}
        self._adapters = {}
        self.keys = keys
        self.saved = 0

    def _save(self, plan: ServePlan) -> None:
        self.saved += 1

    async def _host_keys(self, adapter: Any, plan: ServePlan) -> list[str]:
        found = self.keys.pop(0)
        if isinstance(found, Refusal):
            raise found
        return found


def test_unreadable_keys_become_a_typed_service_error_after_some_rounds(tmp_path: Path) -> None:
    hung = Refusal(RefusalReason.UNREACHABLE, "timed out")
    care = _Care([hung] * KEY_ROUNDS)
    plan = _ssh_plan(tmp_path)
    for _ in range(KEY_ROUNDS - 1):
        assert asyncio.run(care._keys_ready(plan, None)) == "timed out"  # pyright: ignore[reportPrivateUsage]
        assert plan.forward_error is None
    asyncio.run(care._keys_ready(plan, None))  # pyright: ignore[reportPrivateUsage]
    assert plan.forward_error is not None
    assert plan.forward_error["reason"] == RefusalReason.HOST_KEY_UNVERIFIED.value


def test_keys_are_read_once_per_node(tmp_path: Path) -> None:
    care = _Care([[KEY]])
    plan = _ssh_plan(tmp_path)
    assert asyncio.run(care._keys_ready(plan, None)) is None  # pyright: ignore[reportPrivateUsage]
    assert plan.host_keys == [KEY] and plan.host_keys_for == "ares-comp-04"
    assert asyncio.run(care._keys_ready(plan, None)) is None  # pyright: ignore[reportPrivateUsage]
    assert care.keys == []
