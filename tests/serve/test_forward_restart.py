"""A desktop forward outlives a restart of relay's MCP server (take 1p).

Any host may restart an MCP server. The forward runs outside relay's process,
so it stays up through a kill, and the next relay process adopts it at its
start, with no tool call and without starting a second one.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from clio_relay.jobs.handles import JobHandle, JobState, JobStatus, utc_now
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal
from clio_relay.serve import forward as forward_module
from clio_relay.serve.forward import Forward, port_bound
from clio_relay.serve.plan import ServePlan
from clio_relay.serve.runtime import ServeRuntime

from .helpers import frp_plan

HOLD = (
    "import os, socket, sys, time\n"
    "s = socket.socket()\n"
    # As frpc and ssh do on POSIX: a connection's TIME_WAIT never holds the port.
    "if os.name != 'nt':\n"
    "    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n"
    "s.bind(('127.0.0.1', int(sys.argv[1])))\n"
    "s.listen()\n"
    "while True:\n"
    "    c, _ = s.accept()\n"
    "    c.sendall(b'up')\n"
    "    c.close()\n"
)


@dataclass
class Held:
    plan: ServePlan
    spawned: list[int]


@pytest.fixture
def held_(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Held]:
    """A plan whose forward is a script holding its port; every one left is killed after."""
    plan = frp_plan(tmp_path)
    script = tmp_path / "hold.py"
    script.write_text(HOLD, encoding="utf-8")
    port = plan.ports[0].local_port

    def command(_self: Forward) -> tuple[list[str], dict[str, str]]:
        return [sys.executable, str(script), str(port)], dict(os.environ)

    monkeypatch.setattr(Forward, "_command", command)
    monkeypatch.setattr(forward_module, "POLL_S", 0.05)
    monkeypatch.setattr(forward_module, "MAX_BACKOFF_S", 0.05)
    pids: list[int] = []
    spawn = Forward._spawn  # pyright: ignore[reportPrivateUsage]

    async def tracked(self: Forward) -> None:
        await spawn(self)
        if self.process is not None:
            pids.append(self.process.pid)

    monkeypatch.setattr(Forward, "_spawn", tracked)
    yield Held(plan, pids)
    for pid in pids:
        _kill(pid)


def _kill(pid: int) -> None:
    if os.name == "nt":
        # /T: a venv's python.exe is a stand-in that starts the real one as its child.
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, check=False)
    else:
        with contextlib.suppress(OSError):
            os.kill(pid, signal.SIGKILL)


def _answers(port: int) -> bool:
    import socket

    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2) as sock:
            return sock.recv(2) == b"up"
    except OSError:
        return False


async def _until(check: Callable[[], bool], what: str, ceiling_s: float = 60) -> None:
    # A ceiling only so a broken forward fails the test instead of hanging it.
    deadline = time.monotonic() + ceiling_s
    while not check():
        if time.monotonic() > deadline:
            raise AssertionError(f"never: {what}")
        await asyncio.sleep(0.05)


def test_a_released_forward_stays_up_and_the_next_one_adopts_it(
    tmp_path: Path, held_: Held
) -> None:
    held = held_.plan
    port = held.ports[0].local_port

    async def scenario() -> None:
        first = Forward(held, tmp_path / "fwd", lambda _c, pid: setattr(held, "forward_pid", pid))
        await first.start()
        await _until(first.bound, "the first forward binds")
        await first.release()
        assert first.alive() and _answers(port)

        second = Forward(held, tmp_path / "fwd", lambda _c, _pid: None)
        await second.start()
        assert second.held and second.process is None
        assert second.pid() == held.forward_pid == held_.spawned[0]
        assert len(held_.spawned) == 1, "adopting must not start a second forward"

        _kill(held_.spawned[0])
        await _until(lambda: len(held_.spawned) == 2 and second.bound(), "it binds again")
        assert _answers(port)
        await second.close()
        assert not port_bound(port)

    asyncio.run(scenario())


def test_a_forward_whose_own_process_lost_the_port_to_another_adopts_it(
    tmp_path: Path, held_: Held
) -> None:
    """Two relay processes: the one that loses the race holds no failing retry loop."""
    held = held_.plan

    async def scenario() -> None:
        winner = Forward(held, tmp_path / "a", lambda _c, _pid: None)
        await winner.start()
        await _until(winner.bound, "the winner binds")
        loser = Forward(held, tmp_path / "b", lambda _c, _pid: None)
        loser.process = subprocess.Popen([sys.executable, "-c", "pass"])
        loser.process.wait()
        loser.task = asyncio.create_task(loser._watch())  # pyright: ignore[reportPrivateUsage]
        await _until(lambda: loser.process is None, "the loser adopts the winner's forward")
        assert loser.restarts == 0 and len(held_.spawned) == 1
        await loser.release()
        await winner.close()

    asyncio.run(scenario())


def test_the_forward_survives_a_hard_kill_of_relays_process(tmp_path: Path, held_: Held) -> None:
    """A host kills relay, and on POSIX its whole process group, as a restart may."""
    held = held_.plan
    port = held.ports[0].local_port
    state = tmp_path / "pid.json"
    parent = tmp_path / "relay.py"
    parent.write_text(
        "import asyncio, json, os, sys\n"
        "from pathlib import Path\n"
        # As the mcp SDK's stdio client holds a server on Windows: a job that kills
        # every process in it on close, and lets none break away.
        "if os.name == 'nt':\n"
        "    import win32api, win32job\n"
        "    job = win32job.CreateJobObject(None, '')\n"
        "    kind = win32job.JobObjectExtendedLimitInformation\n"
        "    info = win32job.QueryInformationJobObject(job, kind)\n"
        "    flag = win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE\n"
        "    info['BasicLimitInformation']['LimitFlags'] |= flag\n"
        "    win32job.SetInformationJobObject(job, kind, info)\n"
        "    win32job.AssignProcessToJobObject(job, win32api.GetCurrentProcess())\n"
        "from clio_relay.serve.forward import Forward\n"
        "from clio_relay.serve.plan import ServePlan\n"
        "plan = ServePlan.model_validate_json(Path(sys.argv[1]).read_text())\n"
        "def command(_self):\n"
        f"    return [sys.executable, {str(tmp_path / 'hold.py')!r}, '{port}'], dict(os.environ)\n"
        "Forward._command = command\n"
        "async def main():\n"
        "    forward = Forward(plan, Path(sys.argv[2]), lambda _c, _p: None)\n"
        "    await forward.start()\n"
        "    while not forward.bound():\n"
        "        await asyncio.sleep(0.05)\n"
        "    Path(sys.argv[3]).write_text(json.dumps({'pid': forward.process.pid}))\n"
        "    await asyncio.sleep(600)\n"
        "asyncio.run(main())\n",
        encoding="utf-8",
    )
    (tmp_path / "plan.json").write_text(held.model_dump_json(), encoding="utf-8")
    argv = [sys.executable, str(parent), str(tmp_path / "plan.json"), str(tmp_path / "fwd")]
    if sys.platform == "win32":
        relay = subprocess.Popen(
            [*argv, str(state)], creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
        )
    else:
        relay = subprocess.Popen([*argv, str(state)], start_new_session=True)
    try:
        deadline = time.monotonic() + 60
        while not state.exists():
            assert relay.poll() is None, "relay's stand-in ended early"
            assert time.monotonic() < deadline, "relay's stand-in never bound the port"
            time.sleep(0.05)
        pid = json.loads(state.read_text())["pid"]
        held_.spawned.append(pid)
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(relay.pid), "/T", "/F"], capture_output=True)
        else:
            os.killpg(relay.pid, signal.SIGKILL)
        relay.wait(30)
        assert _answers(port), "the forward died with relay's process"
    finally:
        if relay.poll() is None:
            relay.kill()


def _record(plan: ServePlan, state: OperationState = OperationState.RUNNING) -> OperationRecord:
    return OperationRecord(
        operation_id=plan.operation_id,
        kind="serve",
        site=plan.site,
        adapter=plan.adapter,
        spec={},
        spec_digest="e" * 64,
        state=state,
    )


def test_relays_start_adopts_a_left_forward_and_binds_a_lost_one(
    tmp_path: Path, held_: Held
) -> None:
    """No tool call: the provider's start runs ``resume``."""
    held = held_.plan
    port = held.ports[0].local_port
    ready = held.model_copy(update={"ready_at": utc_now()})

    async def first_relay() -> None:
        runtime = ServeRuntime(tmp_path / "state", transport="stdio")
        runtime.records.put(ready.operation_id, _record(ready))
        runtime._save(ready)  # pyright: ignore[reportPrivateUsage]
        await runtime.resume()
        await _until(lambda: port_bound(port), "the first relay binds")
        await runtime.close()

    async def next_relay(spawned: int) -> ServeRuntime:
        runtime = ServeRuntime(tmp_path / "state", transport="stdio")
        await runtime.resume()
        await _until(lambda: _answers(port), "the port answers after the restart")
        assert len(held_.spawned) == spawned
        return runtime

    asyncio.run(first_relay())
    assert _answers(port), "relay's exit closed the desktop port"

    async def restarts() -> None:
        adopted = await next_relay(spawned=1)
        assert adopted.forwards[held.operation_id].held
        await adopted.close()
        _kill(held_.spawned[0])
        await _until(lambda: not port_bound(port), "the forward is gone")
        bound_again = await next_relay(spawned=2)
        await bound_again.close()

    asyncio.run(restarts())


def test_an_ended_service_closes_the_forward_an_earlier_relay_left(
    tmp_path: Path, held_: Held, monkeypatch: pytest.MonkeyPatch
) -> None:
    held = held_.plan
    port = held.ports[0].local_port
    handle = JobHandle(adapter="fixture", native_id="9", data={"job_dir": "/j"})
    ready = held.model_copy(update={"ready_at": utc_now(), "handle": handle, "job_dir": "/j"})

    async def left_up() -> int:
        runtime = ServeRuntime(tmp_path / "state", transport="stdio")
        runtime.records.put(ready.operation_id, _record(ready))
        runtime._save(ready)  # pyright: ignore[reportPrivateUsage]
        forward = await runtime._ensure_forward(ready, None)  # pyright: ignore[reportPrivateUsage]
        await _until(forward.bound, "the forward binds")
        await runtime.close()
        return held_.spawned[0]

    pid = asyncio.run(left_up())
    monkeypatch.setattr(forward_module, "_is_ours", lambda found, _marker: found == pid)  # pyright: ignore[reportUnknownLambdaType,reportUnknownArgumentType]

    class Ended:
        async def status(self, _handle: JobHandle) -> JobStatus:
            return JobStatus(state=JobState.COMPLETED)

        async def files(self) -> object:
            raise Refusal.__new__(Refusal)

    async def observe() -> None:
        runtime = ServeRuntime(tmp_path / "state", transport="stdio")
        runtime._resumed = True  # pyright: ignore[reportPrivateUsage]

        async def read(*_args: object, **_kwargs: object) -> bytes | None:
            return None

        runtime._read = read  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]
        record = runtime.records.get(ready.operation_id)
        assert record is not None
        status, endpoint = await runtime.status(None, Ended(), record)
        assert status.state == JobState.COMPLETED and endpoint is None
        await _until(lambda: not port_bound(port), "the left forward is closed")

    asyncio.run(observe())


def test_a_service_stopped_by_another_relay_process_is_not_bound_again(
    tmp_path: Path, held_: Held, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cancel from a second relay process closes the forward the first one watches."""
    held = held_.plan
    port = held.ports[0].local_port
    ready = held.model_copy(update={"ready_at": utc_now()})

    async def scenario() -> None:
        watching = ServeRuntime(tmp_path / "state", transport="stdio")
        forward = await watching._ensure_forward(ready, None)  # pyright: ignore[reportPrivateUsage]
        await _until(forward.bound, "the first relay binds")
        pid = held_.spawned[0]
        monkeypatch.setattr(forward_module, "_is_ours", lambda found, _m: found == pid)  # pyright: ignore[reportUnknownLambdaType,reportUnknownArgumentType]
        stopping = ServeRuntime(tmp_path / "state", transport="stdio")
        await stopping._close_desktop(ready.model_copy(update={"forward_pid": pid}))  # pyright: ignore[reportPrivateUsage]
        await _until(lambda: forward.task is not None and forward.task.done(), "it stops")
        assert len(held_.spawned) == 1 and not port_bound(port)
        await watching.close()

    asyncio.run(scenario())


def test_one_relay_process_runs_a_forward_and_the_next_takes_it_over(
    tmp_path: Path, held_: Held
) -> None:
    """A prestarted relay and a session's share a state dir: only one binds the service."""
    held = held_.plan
    port = held.ports[0].local_port
    ready = held.model_copy(update={"ready_at": utc_now()})

    async def scenario() -> None:
        first = ServeRuntime(tmp_path / "state", transport="stdio")
        first._save(ready)  # pyright: ignore[reportPrivateUsage]
        owner = await first._ensure_forward(ready, None)  # pyright: ignore[reportPrivateUsage]
        await _until(owner.bound, "the owner binds")
        second = ServeRuntime(tmp_path / "state", transport="stdio")
        other = second._plan(ready.operation_id)  # pyright: ignore[reportPrivateUsage]
        left = await second._ensure_forward(other, None)  # pyright: ignore[reportPrivateUsage]
        assert left.elsewhere and left.task is None and left.serving()
        await asyncio.sleep(0.3)
        assert len(held_.spawned) == 1
        await first.close()
        taken = await second._ensure_forward(other, None)  # pyright: ignore[reportPrivateUsage]
        assert not taken.elsewhere and taken.held and taken.lock.held()
        assert len(held_.spawned) == 1 and _answers(port)
        await second.close()

    asyncio.run(scenario())


def test_a_repicked_desktop_port_is_kept_through_a_stale_copy_and_a_restart(
    tmp_path: Path, held_: Held, monkeypatch: pytest.MonkeyPatch
) -> None:
    held = held_.plan
    was = held.ports[0].local_port
    script = tmp_path / "hold.py"

    def command(self: Forward) -> tuple[list[str], dict[str, str]]:
        return [sys.executable, str(script), str(self.plan.ports[0].local_port)], dict(os.environ)

    state = forward_module.port_state

    def reserved(port: int) -> str:
        return "unusable" if port == was else state(port)

    monkeypatch.setattr(Forward, "_command", command)
    monkeypatch.setattr(forward_module, "port_state", reserved)
    ready = held.model_copy(update={"ready_at": utc_now()})

    async def scenario() -> None:
        stale = ServeRuntime(tmp_path / "state", transport="stdio")
        stale._save(ready)  # pyright: ignore[reportPrivateUsage]
        copy = stale._plan(ready.operation_id).model_copy(deep=True)  # pyright: ignore[reportPrivateUsage]
        first = ServeRuntime(tmp_path / "state", transport="stdio")
        owned = await first._ensure_forward(first._plan(ready.operation_id), None)  # pyright: ignore[reportPrivateUsage]
        await _until(owned.bound, "the forward binds the repicked port")
        moved = owned.plan.ports[0].local_port
        assert moved != was
        stored = first.store.get(ready.operation_id)
        assert stored is not None and stored.ports[0].local_port == moved
        stale._save(copy)  # pyright: ignore[reportPrivateUsage]
        stored = first.store.get(ready.operation_id)
        assert stored is not None and stored.ports[0].local_port == moved
        await first.close()
        restarted = ServeRuntime(tmp_path / "state", transport="stdio")
        await restarted.resume()
        forward = restarted.forwards[ready.operation_id]
        assert forward.held and forward.plan.ports[0].local_port == moved
        assert len(held_.spawned) == 1 and _answers(moved)
        await restarted.close()

    asyncio.run(scenario())
