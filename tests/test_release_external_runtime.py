"""Process-identity tests for the release-gate external runtime fixture."""

from __future__ import annotations

import contextlib
import importlib.util
import json
import socket
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any, Protocol, cast

import pytest

ROOT = Path(__file__).parents[1]
RUNTIME_PATH = ROOT / "examples" / "release-gate" / "gateway" / "external_runtime.py"
SERVICE_PATH = ROOT / "examples" / "release-gate" / "gateway" / "http_service.py"


def _load_runtime_module() -> ModuleType:
    """Load the standalone fixture as a module without changing import paths."""
    specification = importlib.util.spec_from_file_location(
        "clio_relay_release_external_runtime",
        RUNTIME_PATH,
    )
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


class _InspectRuntime(Protocol):
    def __call__(
        self,
        state: Any,
        *,
        process_root: Path = ...,
        group_exists: Callable[[int], bool] = ...,
    ) -> bool:
        """Inspect one durable runtime identity."""
        ...


class _TerminateRuntime(Protocol):
    def __call__(
        self,
        state: Any,
        *,
        timeout_seconds: float,
        process_root: Path = ...,
        group_exists: Callable[[int], bool] = ...,
        signal_group: Callable[[int, int], None] | None = ...,
        monotonic: Callable[[], float] = ...,
        sleep: Callable[[float], None] = ...,
    ) -> None:
        """Terminate one durable runtime identity."""
        ...


RUNTIME = _load_runtime_module()
RuntimeState = RUNTIME.RuntimeState  # pyright: ignore[reportAttributeAccessIssue]
OwnershipError = cast(  # pyright: ignore[reportAttributeAccessIssue]
    type[RuntimeError],
    RUNTIME.OwnershipError,
)
inspect_runtime = cast(  # pyright: ignore[reportAttributeAccessIssue]
    _InspectRuntime,
    RUNTIME._inspect_runtime,
)
terminate_runtime = cast(  # pyright: ignore[reportAttributeAccessIssue]
    _TerminateRuntime,
    RUNTIME._terminate_runtime,
)
require_linux_contract = cast(
    Callable[[], None],
    RUNTIME._require_linux_process_contract,  # pyright: ignore[reportAttributeAccessIssue]
)


def _state(*, start_ticks: int = 700, command: tuple[str, ...] | None = None) -> Any:
    """Return one complete durable runtime identity for fixture tests."""
    selected_command = command or ("/usr/bin/python3", "/srv/service.py", "--port", "19080")
    return RuntimeState(
        runtime_id="a" * 32,
        pid=4100,
        pgid=4100,
        session_id=4100,
        proc_start_ticks=start_ticks,
        command_argv=selected_command,
        owner_token="b" * 64,
        service_host="validation.example",
        log_path="/tmp/runtime.log",
    )


def _proc_stat(*, pid: int, pgid: int, session_id: int, start_ticks: int) -> str:
    """Render the Linux stat fields consumed by the ownership fixture."""
    fields = ["S", "1", str(pgid), str(session_id), *("0" for _ in range(15))]
    fields.append(str(start_ticks))
    return f"{pid} (python validation service) {' '.join(fields)}\n"


def _write_process(
    process_root: Path,
    *,
    state: Any,
    start_ticks: int | None = None,
    command: tuple[str, ...] | None = None,
    owner_token: str | None = None,
) -> None:
    """Write a deterministic proc identity document for one process."""
    process = process_root / str(state.pid)
    process.mkdir(parents=True)
    (process / "stat").write_text(
        _proc_stat(
            pid=state.pid,
            pgid=state.pgid,
            session_id=state.session_id,
            start_ticks=state.proc_start_ticks if start_ticks is None else start_ticks,
        ),
        encoding="utf-8",
    )
    selected_command = command or cast(tuple[str, ...], state.command_argv)
    (process / "cmdline").write_bytes(
        b"\0".join(argument.encode() for argument in selected_command) + b"\0"
    )
    selected_token = state.owner_token if owner_token is None else owner_token
    (process / "environ").write_bytes(
        b"PATH=/usr/bin\0"
        + f"CLIO_RELAY_EXTERNAL_RUNTIME_OWNER_TOKEN={selected_token}".encode()
        + b"\0"
    )


def test_status_requires_exact_start_ticks_command_and_owner_token(tmp_path: Path) -> None:
    state = _state()
    process_root = tmp_path / "proc"
    _write_process(process_root, state=state)

    assert inspect_runtime(state, process_root=process_root, group_exists=lambda _pgid: True)

    (process_root / str(state.pid) / "stat").write_text(
        _proc_stat(
            pid=state.pid,
            pgid=state.pgid,
            session_id=state.session_id,
            start_ticks=state.proc_start_ticks + 1,
        ),
        encoding="utf-8",
    )
    with pytest.raises(OwnershipError, match="start ticks.*reused pid"):
        inspect_runtime(state, process_root=process_root, group_exists=lambda _pgid: True)


@pytest.mark.parametrize(
    ("command", "owner_token", "message"),
    [
        (("/usr/bin/python3", "/srv/not-our-service.py"), None, "exact command identity"),
        (None, "c" * 64, "owner token"),
    ],
)
def test_status_fails_closed_on_identity_mismatch(
    tmp_path: Path,
    command: tuple[str, ...] | None,
    owner_token: str | None,
    message: str,
) -> None:
    state = _state()
    process_root = tmp_path / "proc"
    _write_process(
        process_root,
        state=state,
        command=command,
        owner_token=owner_token,
    )

    with pytest.raises(OwnershipError, match=message):
        inspect_runtime(state, process_root=process_root, group_exists=lambda _pgid: True)


def test_status_distinguishes_absence_from_unverified_residual_group(tmp_path: Path) -> None:
    state = _state()
    process_root = tmp_path / "proc"
    process_root.mkdir()

    assert not inspect_runtime(
        state,
        process_root=process_root,
        group_exists=lambda _pgid: False,
    )
    with pytest.raises(OwnershipError, match="leader is absent.*group still exists"):
        inspect_runtime(state, process_root=process_root, group_exists=lambda _pgid: True)


def test_cancel_never_signals_a_reused_pid_or_mismatched_group(tmp_path: Path) -> None:
    state = _state()
    process_root = tmp_path / "proc"
    _write_process(process_root, state=state, start_ticks=state.proc_start_ticks + 1)
    signals: list[tuple[int, int]] = []

    with pytest.raises(OwnershipError, match="reused pid"):
        terminate_runtime(
            state,
            timeout_seconds=1,
            process_root=process_root,
            group_exists=lambda _pgid: True,
            signal_group=lambda pgid, selected_signal: signals.append((pgid, selected_signal)),
        )

    assert signals == []


def test_cancel_signals_verified_group_once_and_polls_exact_absence(tmp_path: Path) -> None:
    state = _state()
    process_root = tmp_path / "proc"
    _write_process(process_root, state=state)
    existence = iter((True, True, False))
    signals: list[tuple[int, int]] = []
    sleeps: list[float] = []

    terminate_runtime(
        state,
        timeout_seconds=1,
        process_root=process_root,
        group_exists=lambda _pgid: next(existence),
        signal_group=lambda pgid, selected_signal: signals.append((pgid, selected_signal)),
        monotonic=lambda: 0.0,
        sleep=sleeps.append,
    )

    assert len(signals) == 1
    assert signals[0][0] == state.pgid
    assert sleeps == [pytest.approx(0.1)]


def test_cancel_times_out_boundedly_without_a_second_unverified_signal(tmp_path: Path) -> None:
    state = _state()
    process_root = tmp_path / "proc"
    _write_process(process_root, state=state)
    clock = iter((0.0, 0.25))
    signals: list[tuple[int, int]] = []

    with pytest.raises(TimeoutError, match="remained present after 0.2 seconds"):
        terminate_runtime(
            state,
            timeout_seconds=0.2,
            process_root=process_root,
            group_exists=lambda _pgid: True,
            signal_group=lambda pgid, selected_signal: signals.append((pgid, selected_signal)),
            monotonic=lambda: next(clock),
            sleep=lambda _seconds: pytest.fail("deadline should be reached before sleeping"),
        )

    assert len(signals) == 1


def _free_tcp_port() -> int:
    """Reserve and release one loopback port for the bounded live fixture test."""
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return cast(int, listener.getsockname()[1])


def _run_runtime_cli(*arguments: str) -> subprocess.CompletedProcess[str]:
    """Run the fixture through its real command-line process boundary."""
    return subprocess.run(
        [sys.executable, str(RUNTIME_PATH), *arguments],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )


def test_real_linux_process_identity_and_group_cancellation(
    tmp_path: Path,
) -> None:
    if sys.platform != "linux":
        with pytest.raises(RuntimeError, match="requires Linux"):
            require_linux_contract()
        return

    state_dir = tmp_path / "state"
    submitted = _run_runtime_cli(
        "submit",
        "--state-dir",
        str(state_dir),
        "--service-script",
        str(SERVICE_PATH),
        "--port",
        str(_free_tcp_port()),
        "--lifetime-seconds",
        "30",
        "--health-nonce",
        "d" * 64,
    )
    assert submitted.returncode == 0, submitted.stderr
    submission = cast(dict[str, object], json.loads(submitted.stdout))
    runtime_id = cast(str, submission["scheduler_job_id"])
    state_path = state_dir / f"{runtime_id}.json"
    original_state_text = state_path.read_text(encoding="utf-8")
    durable_state = cast(dict[str, object], json.loads(original_state_text))
    assert durable_state["pid"] == durable_state["pgid"] == durable_state["session_id"]
    assert isinstance(durable_state["proc_start_ticks"], int)
    assert durable_state["proc_start_ticks"] > 0
    owner_token = cast(str, durable_state["owner_token"])
    assert len(owner_token) == 64 and int(owner_token, 16) > 0
    command_argv = cast(list[str], durable_state["command_argv"])
    assert command_argv[-2:] == ["--health-nonce", "d" * 64]
    try:
        status = _run_runtime_cli("status", "--state-dir", str(state_dir), runtime_id)
        assert status.returncode == 0, status.stderr
        assert cast(dict[str, object], json.loads(status.stdout))["state"] == "running"

        forged_state = dict(durable_state)
        forged_state["proc_start_ticks"] = durable_state["proc_start_ticks"] + 1
        state_path.write_text(json.dumps(forged_state, sort_keys=True), encoding="utf-8")
        try:
            refused_status = _run_runtime_cli(
                "status",
                "--state-dir",
                str(state_dir),
                runtime_id,
            )
            assert refused_status.returncode != 0
            assert "reused pid" in refused_status.stderr
            refused_cancel = _run_runtime_cli(
                "cancel",
                "--state-dir",
                str(state_dir),
                runtime_id,
            )
            assert refused_cancel.returncode != 0
            assert "reused pid" in refused_cancel.stderr
        finally:
            state_path.write_text(original_state_text, encoding="utf-8")

        still_running = _run_runtime_cli("status", "--state-dir", str(state_dir), runtime_id)
        assert still_running.returncode == 0, still_running.stderr
        assert cast(dict[str, object], json.loads(still_running.stdout))["state"] == "running"

        canceled = _run_runtime_cli(
            "cancel",
            "--state-dir",
            str(state_dir),
            "--timeout-seconds",
            "10",
            runtime_id,
        )
        assert canceled.returncode == 0, canceled.stderr

        completed = _run_runtime_cli("status", "--state-dir", str(state_dir), runtime_id)
        assert completed.returncode == 0, completed.stderr
        assert cast(dict[str, object], json.loads(completed.stdout))["state"] == "completed"
    finally:
        with contextlib.suppress(subprocess.TimeoutExpired):
            _run_runtime_cli(
                "cancel",
                "--state-dir",
                str(state_dir),
                "--timeout-seconds",
                "10",
                runtime_id,
            )
